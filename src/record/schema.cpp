#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t op = 0;
  //magic number
  MACH_WRITE_TO(uint32_t, buf+op, SCHEMA_MAGIC_NUM);
  op += sizeof(uint32_t);
  //colums
  uint32_t columns_num = columns_.size();
  MACH_WRITE_TO(uint32_t, buf+op, columns_num);
  op += sizeof(uint32_t);
  for (int i = 0; i < columns_num; i++) {
    op += columns_[i]->SerializeTo(buf + op);
  }
  //is manage
  MACH_WRITE_TO(bool, buf+op, is_manage_);
  op += sizeof(bool);
  return op;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t op = 0;
  op += sizeof(uint32_t);
  op += sizeof(uint32_t);
  uint32_t columns_num = columns_.size();
  for (int i = 0; i < columns_num; i++) {
    op += columns_[i]->GetSerializedSize();
  }
  op += sizeof(bool);
  return op;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t op = 0;
  //magic number
  uint32_t magic_num = MACH_READ_UINT32(buf);
  op += sizeof(uint32_t);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "warning");
  //colums
  uint32_t columns_num;
  columns_num = MACH_READ_FROM(uint32_t, buf+op);
  op += sizeof(uint32_t);
  std::vector<Column *> columns;
  for (int i = 0; i < columns_num; i++) {
    Column *temp;
    op += Column::DeserializeFrom(buf+op, temp);
    columns.push_back(temp);
  }
  //is manger
  bool is_manger = MACH_READ_FROM(bool, buf+op);
  op += sizeof(bool);
  schema = new Schema(columns, is_manger);
  return op;
}
