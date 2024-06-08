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
      unique_(other->unique_) {}

/**
* TODO: Student Implement
 */
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  MACH_WRITE_INT32(buf+4,type_);
  MACH_WRITE_UINT32(buf+8,len_);
  MACH_WRITE_UINT32(buf+12,table_ind_);
  switch (nullable_)
  {
    case false:
      MACH_WRITE_INT32(buf+16,0);
      break;
    default:
      MACH_WRITE_INT32(buf+16,1);
      break;
  }
  switch (unique_)
  {
    case false:
      MACH_WRITE_INT32(buf+20,0);
      break;
    default:
      MACH_WRITE_INT32(buf+20,1);
      break;
  }
  int name_len=sizeof(name_);
  MACH_WRITE_INT32(buf+24,name_len);
  MACH_WRITE_STRING(buf+28,name_);
  return GetSerializedSize();
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  uint32_t  size=sizeof(name_)+28;
  //uint32_t  size=name_.length() + 18 + sizeof(TypeId);;

  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." 									 << std::endl;
  }
  /* deserialize field from buf */
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Failed to deserialize table info.");
  int tp=MACH_READ_INT32(buf+4);
  TypeId  type=(enum  TypeId)tp;
  uint32_t col_len=MACH_READ_UINT32(buf+8);
  uint32_t col_ind=MACH_READ_UINT32(buf+12);
  int temp;
  bool  nullable;
  temp=MACH_READ_INT32(buf+16);
  if(temp==1){
    nullable=true;
  }else{
    nullable=false;
  }
  bool  unique;
  temp=MACH_READ_INT32(buf+20);
  if(temp==1){
    unique=true;
  }else{
    unique=false;
  }
  int32_t name_len=MACH_READ_INT32(buf+24);
  uint32_t  ofs=28+name_len;
  std::string column_name;
  column_name.assign(buf+28,name_len);
  /* allocate object */
  if (type == kTypeChar) {
    column = new Column(column_name, type, col_len, col_ind, nullable, unique);
  } else {
    column = new Column(column_name, type, col_ind, nullable, unique);
  }
  return ofs;
}
