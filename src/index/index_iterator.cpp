#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  return page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++() {
  item_index++;
  if(item_index >= page -> GetSize()){
    page_id_t next_id = page -> GetNextPageId();
    if(next_id == INVALID_PAGE_ID){
      page = nullptr;
      //current_page_id = INVALID_PAGE_ID;
    }
    else{
      auto next_page = reinterpret_cast<LeafPage *> (buffer_pool_manager->FetchPage(next_id) -> GetData());
      page = next_page;
      current_page_id = next_id;
      item_index = 0;
      buffer_pool_manager ->UnpinPage(next_id, false);
    }
  }
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}