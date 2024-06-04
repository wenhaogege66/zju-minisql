#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  MACH_WRITE_INT32(buf,columns_.size());
  uint32_t j=4;
  for (auto i:columns_){
    i->SerializeTo(buf+j);
    j+=i->GetSerializedSize();
	}
  switch (is_manage_)
  {
  case true:
    MACH_WRITE_INT32(buf+j+4,1);
    break;
  default:
    MACH_WRITE_INT32(buf+j+4,0);
    break;
  }
  return j+8;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t  size=0;
  for(auto i:columns_){
    size+=i->GetSerializedSize();
  }
  size+=8;
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  if (schema != nullptr) {
    LOG(WARNING) << "Pointer to schema is not null in schema deserialize." 									 << std::endl;
  }
  int size=MACH_READ_INT32(buf);
  std::vector<Column *> columns;
  uint32_t  ofs=4;
  for(int i=0;i<size;i++){
    Column *col;
    uint32_t  t;
    t=Column::DeserializeFrom(buf+ofs,col);
    columns.push_back(col);
    ofs+=t;
  }
  bool is_manage;
  switch (MACH_READ_INT32(buf+ofs))
  {
  case 1:
    is_manage=true;
    break;
  
  default:
    is_manage=false;
    break;
  }
  Schema(columns,is_manage);
  return ofs;
}