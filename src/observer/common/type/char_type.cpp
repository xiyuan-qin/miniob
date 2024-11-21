/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"
#include "common/time/datetime.h"

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::DATES:
    {
      result.attr_type_ = AttrType::DATES;
      int y,m,d;
      if (sscanf(val.value_.pointer_value_, "%d-%d-%d", &y, &m, &d) != 3){
        LOG_WARN("invalid date format: %s", val.value_.pointer_value_);
        return RC::INVALID_ARGUMENT;
      }
      bool check_ret = common::check_date(y,m,d);
      if (!check_ret){
        LOG_WARN("invalid date format: %s", val.value_.pointer_value_);
        return RC::INVALID_ARGUMENT;
      }
      result.set_date(y,m,d);
    }break;
    case AttrType::INTS:
    {
      result.attr_type_ = AttrType::INTS;
      int num = 0, nega_flag = 1;
      char *str = val.value_.pointer_value_;
      int len = strlen(str);
      if(len == 0){result.set_int(0);break;}
      if(str[0] == '-') nega_flag = -1;
      else if(str[0] < '0' || str[0] > '9'){result.set_int(0);break;}
      else num = str[0]-'0';
      for(int i = 1; i < len; i ++){
        if(str[i] < '0' || str[i] > '9') break;
        num *= 10;
        num += str[i]-'0';
      }
      result.set_int(num*nega_flag);
    }break;
    case AttrType::FLOATS:
    {
      result.attr_type_ = AttrType::FLOATS;
      float num = 0, nega_flag = 1, k = 0.1;
      int mode = 0;
      char *str = val.value_.pointer_value_;
      int len = strlen(str);
      if(len == 0){result.set_float(0);break;}
      if(str[0] == '-'){ 
        if(len == 1) {result.set_float(0);break;}
        nega_flag = -1;
      }
      else if(str[0] < '0' || str[0] > '9'){result.set_float(0);break;}
      else num = (float)(str[0]-'0');
      for(int i = 1; i < len; i ++){
        if(str[i] == '.'){
          if(mode == 1 || str[0] == '-') break;
          mode = 1;
          continue;
        }
        else if(str[i] < '0' || str[i] > '9') break;
        if(mode == 0)
        {
          num *= 10;
          num += str[i]-'0';
        }
        else if(mode == 1){
          num += ((float)(str[i]-'0')) * k;
          k /= 10;
        }
      }
      result.set_float(num*nega_flag);
    }break;
    case AttrType::TEXTS:
    {
      result.attr_type_ = AttrType::TEXTS;
      result.set_text(val.value_.pointer_value_, strlen(val.value_.pointer_value_));
    }break;
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  if (type == AttrType::DATES) {
    return 1;
  }
  if (type == AttrType::INTS) {
    return 1;
  }
  if (type == AttrType::FLOATS) {
    return 1;
  }
  if (type == AttrType::TEXTS) {
    return 0;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}