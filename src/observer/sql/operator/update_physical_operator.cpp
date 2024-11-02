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

  if (children_.size() < 1){ // 这个child是用来遍历表的
    LOG_WARN("update operator must has one child at least");
    return RC::INTERNAL;
  }

  RC rc = RC::SUCCESS;
  RecordFileScanner scanner;
  table_->get_record_scanner(scanner, trx, ReadWriteMode::READ_WRITE);
  Record record;

  auto table_meta = table_->table_meta();
  const std::vector<FieldMeta>* field_meta = table_meta.field_metas();
  FieldMeta to_edit;

  rc = RC::RECORD_INVALID_KEY;
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

  RecordFileHandler* recordhandler = table_->record_handler();
  while(RC::SUCCESS == (rc = scanner.next(record))){
    rc = recordhandler->visit_record(record.rid(), func); // 获取rid并修改值
    if(RC::SUCCESS != rc){
      scanner.close_scan();
      return rc;
    }
  }
  /*rc = children_[0]->open(trx); // 开启事务

  PhysicalOperator *oper = children_.front().get(); // 获取Scanner
  while(RC::SUCCESS == (rc = oper->next())){
    RowTuple *tuple = static_cast<RowTuple*>(oper->current_tuple());
    if(nullptr == tuple){
      rc = RC::INTERNAL;
      LOG_WARN("failed to get next tuple from operator");
      return rc;
    }

    if(field_index_ == -1){ // 确保找到field索引
      TupleCellSpec spec(table_->name(), field_name_.c_str());
      rc = tuple->find_cell(spec, field_index_);
      if(RC::SUCCESS != rc){
        LOG_WARN("failed to get index of filed name from operator");
        return rc;
      }
    }

    Value   to_change;
    rc = tuple->cell_at(field_index_, to_change);
    if(RC::SUCCESS != rc){
      LOG_WARN("failed to get field by field index");
      return rc;
    }
    to_change.set_value(*values_);
  }
  if(RC::RECORD_EOF == rc) return RC::SUCCESS;*/
  scanner.close_scan();
  if(RC::RECORD_EOF == rc) return RC::SUCCESS;
  return rc;
}

RC UpdatePhysicalOperator::next() { 
  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close() 
{ 
  // children_[0]->close();
  return RC::SUCCESS; 
}
