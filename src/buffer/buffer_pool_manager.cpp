#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  if(page_table_.find(page_id) != page_table_.end()) {
    replacer_->Pin(page_table_[page_id]);
    pages_[page_table_[page_id]].pin_count_++;
    return &pages_[page_table_[page_id]];
  }
  else {
    frame_id_t replace_page;
    if(!free_list_.empty()) {
      replace_page = free_list_.front();
      free_list_.pop_front();
    }
    else {
      if(replacer_->Victim(&replace_page)) {
        page_table_.erase(pages_[replace_page].page_id_);
      }
      else {
        return nullptr;
      }
    }
    if(pages_[replace_page].IsDirty())
      disk_manager_->WritePage(pages_[replace_page].page_id_, pages_[replace_page].data_);
    page_table_[page_id] = replace_page;
    pages_[replace_page].ResetMemory();
    pages_[replace_page].page_id_ = page_id;
    pages_[replace_page].is_dirty_ = false;
    pages_[replace_page].pin_count_ = 1;
    replacer_->Pin(replace_page);
    disk_manager_->ReadPage(page_id, pages_[replace_page].data_);
    return &pages_[replace_page];
  }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t new_page;
  if(!free_list_.empty()) {
    new_page = free_list_.front();
    free_list_.pop_front();
  }
  else {
    if(replacer_->Victim(&new_page)) {
      page_table_.erase(pages_[new_page].page_id_);
    }
    else {
      return nullptr;
    }
  }
  if(pages_[new_page].IsDirty())
    disk_manager_->WritePage(pages_[new_page].page_id_, pages_[new_page].data_);
  page_id = AllocatePage();
  page_table_[page_id] = new_page;
  pages_[new_page].ResetMemory();
  pages_[new_page].page_id_ = page_id;
  pages_[new_page].is_dirty_ = false;
  pages_[new_page].pin_count_ = 1;
  replacer_->Pin(new_page);
  return &pages_[new_page];
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if(page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  else {
    if(pages_[page_table_[page_id]].pin_count_ > 0)
      return false;
    frame_id_t delete_page = page_table_[page_id];
    page_table_.erase(page_id);
    replacer_->Pin(delete_page);
    DeallocatePage(page_id);
    pages_[delete_page].ResetMemory();
    pages_[delete_page].is_dirty_ = false;
    free_list_.emplace_back(delete_page);
    return true;
  }
}


/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(page_table_.find(page_id) != page_table_.end())
  {
    if(pages_[page_table_[page_id]].is_dirty_ != is_dirty)
      pages_[page_table_[page_id]].is_dirty_ = true;
    if(--(pages_[page_table_[page_id]].pin_count_) == 0)
      replacer_->Unpin(page_table_[page_id]);
    return true;
  }
  else
    return false;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.find(page_id) != page_table_.end())
  {
    disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
    pages_[page_table_[page_id]].is_dirty_ = false;
    return true;
  }
  else
    return false;
}

bool BufferPoolManager::FlushAllPages() {
  for(size_t i = 0; i < pool_size_; i++)
    if(pages_[i].page_id_ != INVALID_PAGE_ID)
      if(!FlushPage(pages_[i].page_id_))
        return false;
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}