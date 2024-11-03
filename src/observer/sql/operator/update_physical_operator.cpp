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
#include "storage/table/table.h"
#include "storage/trx/trx.h"

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

  function<bool(Record&)> func = 
  [this, to_edit](Record& record) -> bool{   // 操作来设置值
    if(to_edit.len() < this->values_->length()) return false;
    char * start = record.data() + to_edit.offset();
    memcpy(start, this->values_->data(), this->values_->length());
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
