#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  page_id_t current_page_id=first_page_id_;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
  if (page == nullptr) {
    return false;
  }
  if(page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
    return  true;
  }else{
    page_id_t last_page_id=current_page_id;
    while (1)
    {
      current_page_id=page->GetNextPageId();
      if(current_page_id<=0){
        auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(current_page_id));
        new_page->Init(current_page_id,last_page_id,log_manager_,txn);
        if(new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
          return  true;
        }else{
          last_page_id=current_page_id;
          continue;
        }
      }else{
        auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(current_page_id));
        if(new_page==nullptr){
          return false;
        }
        if(new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_)){
          return  true;
        }else{
          last_page_id=current_page_id;
          continue;
        }
      }
    }
  }
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  Row *old_row;
  old_row->SetRowId(rid);
  if(!page->GetTuple(old_row,schema_,txn,lock_manager_)){
    return  false;
  }
  if(page->UpdateTuple(row,old_row,schema_,txn,lock_manager_,log_manager_)){
    return  false;
  }
  return  true;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
  }
  page->ApplyDelete(rid,txn,log_manager_);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
  }
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    return false;
  }
  page->GetTuple(row,schema_,txn,lock_manager_);
  return  true;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t first_page_id = first_page_id_;
  if (first_page_id_ == INVALID_PAGE_ID) {
    return End();
  }
  TablePage *first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId first_rd;
  //in the first page
  if (first_page->GetFirstTupleRid(&first_rd)) {
    Row *row = new Row(first_rd);
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    GetTuple(row, txn);
    return TableIterator(this, row, first_rd);
  } else {
    page_id_t page_id = first_page->GetNextPageId();
    if (page_id == INVALID_PAGE_ID) {
      buffer_pool_manager_->UnpinPage(first_page_id_, false);
      return End();
    }
    while (1) {
      TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
      if (page->GetFirstTupleRid(&first_rd)) {
        Row *row = new Row(first_rd);
        buffer_pool_manager_->UnpinPage(page_id, false);
        GetTuple(row, txn);
        return TableIterator(this, row, first_rd);
      } else {
        buffer_pool_manager_->UnpinPage(page_id, false);
        page_id = page->GetNextPageId();
        if (page_id == INVALID_PAGE_ID) {
          buffer_pool_manager_->UnpinPage(first_page_id_, false);
          return End();
        }
      }
    }
  }
}
/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { return TableIterator(this, nullptr, RowId{INVALID_PAGE_ID, 0}); }

