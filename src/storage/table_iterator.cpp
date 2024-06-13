#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */

TableIterator::TableIterator() {
}


TableIterator::TableIterator(const TableIterator &other) {
  if (other.row == nullptr) {
    this->row = nullptr;
  } else {
    this->row = new Row(*(other.row));
  }
  this->table_heap_ = other.table_heap_;
  this->rowid.Set(other.rowid.GetPageId(), other.rowid.GetSlotNum());
}

TableIterator::~TableIterator() {
  if (row != nullptr) delete row;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (this->table_heap_ == itr.table_heap_ && this->rowid == itr.rowid) {
    return true;
  }
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if (*this == itr) {
    return false;
  }
  return true;
}

const Row &TableIterator::operator*() {
  return *row;
}

Row *TableIterator::operator->() {
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (itr.row == nullptr) {
    this->row = nullptr;
  } else {
    this->row = new Row(*(itr.row));
  }
  this->table_heap_ = itr.table_heap_;
  this->rowid.Set(itr.rowid.GetPageId(), itr.rowid.GetSlotNum());
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  //find in the same page
  TablePage *this_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rowid.GetPageId()));
  // page not existence
  if (this_page == nullptr) {
    table_heap_->buffer_pool_manager_->UnpinPage(rowid.GetPageId(), false);
    *this = table_heap_->End();
    return *this;
  }
  RowId next_rid;
  //find in this page
  if (this_page->GetNextTupleRid(rowid, &next_rid)) {
    Row *next_row = new Row(next_rid);
    this->table_heap_->GetTuple(next_row, nullptr);
    table_heap_->buffer_pool_manager_->UnpinPage(rowid.GetPageId(), false);
    row = next_row;
    rowid.Set(next_rid.GetPageId(), next_rid.GetSlotNum());
    return *this;
  }
  //the last turple in this page
  //find the next page
  page_id_t next_page_id = this_page->GetNextPageId();
  if (next_page_id == INVALID_PAGE_ID) {
    table_heap_->buffer_pool_manager_->UnpinPage(rowid.GetPageId(), false);
    rowid.Set(INVALID_PAGE_ID, 0);
    row = nullptr;
    return *this;
  }
  this_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
  while (1) {
    if (this_page->GetFirstTupleRid(&next_rid)) {
      Row *next_row = new Row(next_rid);
      next_row->GetFields().clear();
      this->table_heap_->GetTuple(next_row, nullptr);
      table_heap_->buffer_pool_manager_->UnpinPage(rowid.GetPageId(), false);
      rowid.Set(next_rid.GetPageId(), next_rid.GetSlotNum());
      row = next_row;
      return *this;
    } else {
      table_heap_->buffer_pool_manager_->UnpinPage(rowid.GetPageId(), false);
      next_page_id = this_page->GetNextPageId();
      this_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      if (next_page_id == INVALID_PAGE_ID) {
        table_heap_->buffer_pool_manager_->UnpinPage(rowid.GetPageId(), false);
        rowid.Set(INVALID_PAGE_ID, 0);
        row = nullptr;
        return *this;
      }
    }
  }
}

// iter++
TableIterator TableIterator::operator++(int) {
  Row *row_next = new Row(*(this->row));
  TableHeap *this_heap_next = this->table_heap_;
  RowId rid_next = this->rowid;
  ++(*this);
  return TableIterator(this_heap_next, row_next, rid_next);
}
