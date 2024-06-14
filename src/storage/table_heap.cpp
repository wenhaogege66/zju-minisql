#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // if has too many pages ,then directly insert into the last page
  if (number_of_pages >= 6) {
    auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_));
    if (last_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      //insert success ,dirty page
      return buffer_pool_manager_->UnpinPage(last_page_id_, true);
    }
    TablePage *next_page;
    page_id_t next_page_id;
    next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
    if (next_page_id == INVALID_PAGE_ID || next_page == nullptr) {
      buffer_pool_manager_->UnpinPage(last_page_id_, false);
      return false;
    }
    next_page->Init(next_page_id, last_page_id_, log_manager_, txn);
    last_page->SetNextPageId(next_page_id);
    buffer_pool_manager_->UnpinPage(last_page_id_, true);
    number_of_pages++;
    last_page_id_ = next_page_id;
    last_page = next_page;
    if (last_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      //insert success ,dirty page
      return buffer_pool_manager_->UnpinPage(last_page_id_, true);
    }
  }
  //read the first page
  auto first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  page_id_t page_id = first_page_id_;
  while (true) {
    if (first_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      //insert success ,dirty page
      return buffer_pool_manager_->UnpinPage(page_id, true);
    }
    //find the next page
    page_id_t next_page_id = first_page->GetNextPageId();
    // no next page
    if (next_page_id == INVALID_PAGE_ID) {
      TablePage *next_page;
      next_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      if (next_page_id == INVALID_PAGE_ID || next_page == nullptr) {
        buffer_pool_manager_->UnpinPage(page_id, false);
        return false;
      }
      next_page->Init(next_page_id, page_id, log_manager_, txn);
      first_page->SetNextPageId(next_page_id);
      buffer_pool_manager_->UnpinPage(page_id, true);
      first_page = next_page;
      page_id = next_page_id;
      number_of_pages++;
      last_page_id_ = next_page_id;
    } else {
      buffer_pool_manager_->UnpinPage(page_id, false);
      first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page_id));
      page_id = next_page_id;
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
  page_id_t old_page_id = rid.GetPageId();
  TablePage *old_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(old_page_id));
  if (old_page == nullptr) {
    buffer_pool_manager_->UnpinPage(old_page_id, false);
    return false;
  }
  Row old_row;
  old_row.SetRowId(rid);
  row.SetRowId(rid);
  //success
  int t = old_page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if (t > 0) {
    buffer_pool_manager_->UnpinPage(old_page_id, true);
    return true;
  } else if (t == -3) {
    ApplyDelete(rid, txn);
    InsertTuple(row, txn);
    buffer_pool_manager_->UnpinPage(old_page_id, true);
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(old_page_id, false);
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  page_id_t delete_id = rid.GetPageId();
  TablePage *delete_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(delete_id));
  if (delete_page == nullptr) {
    buffer_pool_manager_->UnpinPage(delete_id, false);
    return;
  }
  delete_page->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(delete_id, true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
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
  page_id_t row_id = row->GetRowId().GetPageId();
  TablePage *find_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row_id));
  if (find_page == nullptr) {
    buffer_pool_manager_->UnpinPage(row_id, false);
    return false;
  }
  if (find_page->GetTuple(row, schema_, txn, lock_manager_)) {
    buffer_pool_manager_->UnpinPage(row_id, false);
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(row_id, false);
    return false;
  }
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id)); // 删除table_heap
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
