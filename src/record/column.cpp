#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
  : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
  : name_(std::move(column_name)),
    type_(type),
    len_(length),
    table_ind_(index),
    nullable_(nullable),
    unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
  : name_(other->name_),
    type_(other->type_),
    len_(other->len_),
    table_ind_(other->table_ind_),
    nullable_(other->nullable_),
    unique_(other->unique_) {
}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t op = 0;
  //magic number
  MACH_WRITE_TO(uint32_t, buf, COLUMN_MAGIC_NUM);
  op += sizeof(uint32_t);
  //column name
  MACH_WRITE_TO(uint32_t, buf+op, name_.length());
  op += sizeof(uint32_t);
  MACH_WRITE_STRING(buf+op, name_);
  op += name_.length();
  //type
  MACH_WRITE_TO(TypeId, buf+op, type_);
  op += sizeof(TypeId);
  //length
  MACH_WRITE_TO(uint32_t, buf+op, len_);
  op += sizeof(uint32_t);
  //table position
  MACH_WRITE_TO(uint32_t, buf+op, table_ind_);
  op += sizeof(uint32_t);
  //nullable
  MACH_WRITE_TO(bool, buf+op, nullable_);
  op += sizeof(bool);
  //unique
  MACH_WRITE_TO(bool, buf+op, unique_);
  op += sizeof(bool);
  return op;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t op = 0;
  op += sizeof(uint32_t)*4;
  op += name_.length();
  op += sizeof(TypeId);
  op += sizeof(bool)*2;
  return op;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  //magic number
  uint32_t op = 0;
  uint32_t magic_num = MACH_READ_UINT32(buf);
  op += sizeof(uint32_t);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "warning");
  //name
  uint32_t name_length = MACH_READ_FROM(uint32_t, buf+op);
  op += sizeof(uint32_t);
  std::string name;
  for (int i = 0; i < name_length; i++) {
    name += MACH_READ_FROM(char, buf+op);
    op += sizeof(char);
  }
  //type
  TypeId type = MACH_READ_FROM(TypeId, buf+op);
  op += sizeof(TypeId);
  //length
  uint32_t len = MACH_READ_FROM(uint32_t, buf+op);
  op += sizeof(uint32_t);
  //table position
  uint32_t table_ind = MACH_READ_FROM(uint32_t, buf+op);
  op += sizeof(uint32_t);
  //nullable
  bool nullable = MACH_READ_FROM(bool, buf+op);
  op += sizeof(bool);
  bool unique = MACH_READ_FROM(bool, buf+op);
  op += sizeof(bool);
  if (type == kTypeChar) {
    column = new Column(name, type, len, table_ind, nullable, unique);
  } else {
    column = new Column(name, type, table_ind, nullable, unique);
  }
  return op;
}
