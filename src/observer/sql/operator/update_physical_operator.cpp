/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2021/6/9.
//

#include "sql/operator/update_physical_operator.h"
#include "sql/stmt/update_stmt.h"
#include "storage/trx/trx.h"
#include "storage/table/table.h"

using namespace std;

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table,const string& field_name,  const Value* values)
    : table_(table), field_name_(field_name), values_(std::move(values))
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if(table_ == nullptr){
    LOG_WARN("table invalid in operator");
    return RC::INTERNAL; // 这里的返回值疑似不正确 不管了
  }

  // 获得元数据 拿到record中字段的偏移量
  auto table_meta = table_->table_meta();
  const std::vector<FieldMeta>* field_meta = table_meta.field_metas();
  FieldMeta to_edit;

  RC rc = RC::RECORD_INVALID_KEY;
  for(const FieldMeta& meta : *field_meta){
      if(meta.name() == field_name_){
        to_edit = meta;
        rc = RC::SUCCESS;
        break;
      }
  }
  if(RC::SUCCESS != rc) return rc;

  // 类型检查
  Value real_value = *values_;
  if(to_edit.type() != values_->attr_type()){
    // 试图转换
    rc = Value::cast_to(*values_, to_edit.type(), real_value);
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to cast value. table name:%s,field name:%s,value:%s ",
          table_meta.name(), to_edit.name(), values_->to_string().c_str());
      return rc;
    }
  }
  // 长度检查
  if(real_value.length() > to_edit.len() && to_edit.type() != AttrType::CHARS && to_edit.type() != AttrType::TEXTS) return RC::SCHEMA_FIELD_TYPE_MISMATCH; // 如果长度不匹配则错误

  index_ = table_->find_index_by_field(field_name_.c_str());

  function<bool(Record&)> func = 
  [this, to_edit, real_value]
  (Record& record) -> bool{   // 操作来设置值
    // 更新索引
    if(this->index_ != nullptr) if(RC::SUCCESS != this->index_->delete_entry(record.data(), &record.rid()))
      return false;
    char * start = record.data() + to_edit.offset();
    int copy_len = to_edit.len();
    if(to_edit.type() == AttrType::CHARS && copy_len > real_value.length()){
      copy_len = real_value.length() + 1; // 加一个终止字符
    }
    //处理text
    else if (to_edit.type() == AttrType::TEXTS) {
      // 如果是 TEXT 类型，需要写入文本文件并更新记录中的偏移和长度
      int64_t offset = table_->next_text_offset(); // 获取当前文本文件写入的偏移量
      RC rc = table_->write_text(offset, real_value.length(), real_value.data());
      if (rc != RC::SUCCESS) {
          LOG_WARN("Failed to write TEXT data. field_name=%s", to_edit.name());
          return false; // 返回 false，表示更新失败
      }

      // 更新记录中的偏移量和长度
      int64_t *offset_ptr = reinterpret_cast<int64_t *>(start);                       // 偏移量字段
      int64_t *length_ptr = reinterpret_cast<int64_t *>(start + sizeof(int64_t));    // 长度字段
      *offset_ptr = offset;
      *length_ptr = real_value.length();

      return true; // 更新成功，直接返回
    }
    
    memcpy(start,real_value.data(), copy_len);
    if(this->index_ != nullptr) if(RC::SUCCESS != this->index_->insert_entry(record.data(), &record.rid()))
      return false;
    return true;
  };

  //< 这里进入逻辑部分
  if(children_.empty()){ // 如果没条件直接全部赋值
    rc = RC::SUCCESS;
    RecordFileScanner scanner;
    table_->get_record_scanner(scanner, trx, ReadWriteMode::READ_WRITE);
    Record record;

    RecordFileHandler* recordhandler = table_->record_handler(); // TODO record_handler换成predicate operator的条件遍历
    while(RC::SUCCESS == (rc = scanner.next(record))){
      rc = recordhandler->visit_record(record.rid(), func); // 获取rid并修改值
      if(RC::SUCCESS != rc){
        scanner.close_scan();
        return rc;
      }
    }
    scanner.close_scan();
    if(RC::RECORD_EOF == rc) return RC::SUCCESS;
    return rc;
  }
  else{
    std::unique_ptr<PhysicalOperator> &child = children_[0];

    rc = child->open(trx);
    if (rc != RC::SUCCESS){
      LOG_WARN("failed to open child operator: %s", strrc(rc));
      return rc;
    }

    trx_ =trx;

    while (OB_SUCC(rc = child->next())) {
      Tuple *tuple = child->current_tuple();
      if (nullptr == tuple) {
        LOG_WARN("failed to get current record: %s", strrc(rc));
        return rc;
      }
      RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
      Record   &record    = row_tuple->record();
      records_.emplace_back(std::move(record)); 
    }

    if(RC::RECORD_EOF == rc) rc = RC::SUCCESS;
    
    // 先收集记录再修改
    child->close(); // 关闭筛选器

    RecordFileScanner scanner;
    table_->get_record_scanner(scanner, trx_, ReadWriteMode::READ_WRITE);
    RecordFileHandler* recordhandler = table_->record_handler(); // 使用它来修改数据

    for(Record &record : records_){
      rc = recordhandler->visit_record(record.rid(), func);
      if(RC::SUCCESS != rc){
        scanner.close_scan();
        return rc;
      }
    }
    scanner.close_scan();
    return rc;
  }
}

RC UpdatePhysicalOperator::next() { 
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close() 
{ 
  return RC::SUCCESS; 
}
