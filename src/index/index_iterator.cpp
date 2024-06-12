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
  if(item_index-page->GetSize()>=0){
    page_id_t next=page->GetNextPageId();
    if(next==INVALID_PAGE_ID){
      page==nullptr;
      current_page_id=INVALID_PAGE_ID;
      item_index=0;
    }else{
    auto  new_page=reinterpret_cast<LeafPage *> (buffer_pool_manager->FetchPage(next) -> GetData());
    buffer_pool_manager->UnpinPage(current_page_id,false);
    page=new_page;
    current_page_id =next;
    item_index=0;
  }
  }
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}