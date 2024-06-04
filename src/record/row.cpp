#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here]
  MACH_WRITE_TO(page_id_t,buf,rid_.GetPageId());
  MACH_WRITE_UINT32(buf+4,rid_.GetSlotNum());
  int j=8;
  for(auto i:fields_){
    i->SerializeTo(buf+j);
    j+=i->GetSerializedSize();
  }
  return j;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  page_id_t page_id=MACH_READ_UINT32(buf);
  uint32_t slotnum=MACH_READ_UINT32(buf+4);
  RowId rid(page_id,slotnum);
  rid_=rid;
  std::vector<Field *> fields;
  uint32_t  offset=8;
  uint32_t count=schema->GetColumnCount();
  for(uint32_t i=0;i<count;i++){
    TypeId  type_id;
    Column column=schema->GetColumn(i);
    type_id=column.GetType();
    Field *field=nullptr;
    uint32_t off=Field::DeserializeFrom(buf+offset,type_id,&field,false);
    offset+=off;
    fields_.push_back(field);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t size=0;
  for(auto i:fields_){
    size+=i->GetSerializedSize();
  }
  size+=8;
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
