#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t op = 0;
  //row id
  MACH_WRITE_TO(RowId, buf+op, rid_);
  op += sizeof(RowId);
  //header
  uint32_t field_num = fields_.size();
  unsigned char bitmap[field_num / 8 + 1];
  memset(bitmap, 0, field_num / 8 + 1);
  for (int i = 0; i < field_num; i++) {
    if (!fields_[i]->IsNull()) {
      uint32_t t_b = i / 8;
      uint32_t t_o = i % 8;
      char temp = 1 << (7 - t_o);
      bitmap[t_b] = bitmap[t_b] | temp;
    }
  }
  MACH_WRITE_TO(uint32_t, buf+op, field_num);
  op += sizeof(uint32_t);
  for (int i = 0; i < field_num / 8 + 1; i++) {
    MACH_WRITE_TO(char, buf+op, bitmap[i]);
    op += sizeof(char);
  }
  //field
  for (int i = 0; i < field_num; i++) {
    op += fields_[i]->SerializeTo(buf + op);
  }
  return op;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t op = 0;
  //row id
  RowId row_id = MACH_READ_FROM(RowId, buf+op);
  rid_.Set(row_id.GetPageId(), row_id.GetSlotNum());
  op += sizeof(RowId);
  //header
  uint32_t field_size = MACH_READ_FROM(uint32_t, buf+op);
  op += sizeof(uint32_t);
  unsigned char bitmap[field_size / 8 + 1];
  for (int i = 0; i < field_size / 8 + 1; i++) {
    bitmap[i] = MACH_READ_FROM(char, buf+op);
    op += sizeof(char);
  }
  for (int i = 0; i < field_size; i++) {
    uint32_t t_b = i / 8;
    uint32_t t_a = i % 8;
    char temp = 1 << (7 - t_a);
    //not null
    Field *t_field;
    if ((bitmap[t_b] & temp) != 0) {
      op += Field::DeserializeFrom(buf + op, schema->GetColumn(i)->GetType(), &t_field, false);
    } else {
      op += Field::DeserializeFrom(buf + op, schema->GetColumn(i)->GetType(), &t_field, true);
    }
    fields_.push_back(t_field);
  }

  return op;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t op = 0;
  op += sizeof(RowId);
  uint32_t field_num = fields_.size();
  op += sizeof(uint32_t);
  for (int i = 0; i < field_num / 8 + 1; i++) {
    op += sizeof(char);
  }
  for (int i = 0; i < field_num; i++) {
    op += fields_[i]->GetSerializedSize();
  }
  return op;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column: columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
