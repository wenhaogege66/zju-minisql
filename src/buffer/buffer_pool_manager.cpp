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
 //根据逻辑页号获取对应的数据页，如果该数据页不在内存中，则需要从磁盘中进行读取；
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  auto it =  page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.
  if(it != page_table_.end())
  {//这个地方不需要read吗
    replacer_->Pin(it->second);
    pages_[it->second].pin_count_++;
    return pages_+it->second;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  else
  {
    frame_id_t free_page_id = 0;
    if(!free_list_.empty())
    {//freelist里有,随便出一个
      free_page_id= free_list_.front();
      free_list_.pop_front();
    }
    //free空就看replacer
    else if(replacer_->Size() > 0)
    {
      auto *free_page_replace = new frame_id_t;//不能初始化为空，空指针不能赋值
      if(replacer_->Victim(free_page_replace))
      {
          free_page_id = *free_page_replace;
      }
      delete free_page_replace;//别忘了删除
    }
    else
    {//都没了
      return nullptr;
    }

    // 2.     If R is dirty, write it back to the disk.
    if(pages_[free_page_id].is_dirty_)
    {//脏的要先写会磁盘
      disk_manager_->WritePage(pages_[free_page_id].GetPageId(),pages_[free_page_id].GetData());
    }
    // 3.     Delete R from the page table and insert P.
    page_table_.erase(pages_[free_page_id].GetPageId());
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    pages_[free_page_id].ResetMemory();
    pages_[free_page_id].page_id_ = page_id;
    pages_[free_page_id].is_dirty_ = false;
    page_table_[page_id] = free_page_id;
    replacer_->Pin(free_page_id);//开始读
    disk_manager_->ReadPage(page_id,pages_[free_page_id].GetData());
    pages_[free_page_id].pin_count_++;

    return pages_ + free_page_id;
  }
}

/**
 * TODO: Student Implement
 */
 //分配一个新的数据页，并将逻辑页号于page_id中返回；
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t free_page_id = 0;
  if(!free_list_.empty())
  {//freelist里有
    free_page_id= free_list_.front();
    free_list_.pop_front();
  }
  else if (replacer_->Size() > 0) {
//    frame_id_t *free_page_replace = nullptr;//不能初始化为空，空指针不能赋值
    auto *free_page_replace = new frame_id_t;//不能初始化为空，空指针不能赋值
    if(replacer_->Victim(free_page_replace))
    {
      free_page_id = *free_page_replace;
    }
    delete free_page_replace;
  }
  else{ // 1.   If all the pages in the buffer pool are pinned, return nullptr.
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if (pages_[free_page_id].is_dirty_) {//脏就先写回去
    disk_manager_->WritePage(pages_[free_page_id].GetPageId(), pages_[free_page_id].GetData());
  }
  page_table_.erase(pages_[free_page_id].GetPageId());
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_id = AllocatePage();//生成逻辑id
  pages_[free_page_id].ResetMemory();
  pages_[free_page_id].page_id_ = page_id;
  pages_[free_page_id].is_dirty_ = false;
  page_table_[page_id] = free_page_id;//跟踪逻辑id和其在buffer pool中对应的页,用的时候去pages_
  replacer_->Pin(free_page_id);
  pages_[free_page_id].pin_count_ = 1;
  return pages_ + free_page_id;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  auto it =  page_table_.find(page_id);
  if (it == page_table_.end())
  {
    // If P does not exist, return true.
    DeallocatePage(page_id);// 直接从硬盘中删除
    return true;
  }
  else if (pages_[it->second].pin_count_ > 0)
  { // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    return false;
  }
  else{
    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    DeallocatePage(page_id);
    // 同步从replacer移除
    replacer_->Pin(page_table_[page_id]);
    pages_[it->second].ResetMemory();
    pages_[it->second].is_dirty_ = false;
    free_list_.emplace_back(it->second);
    page_table_.erase(page_id);
    return true;
  }
}

/**
 * TODO: Student Implement
 */
 //取消固定一个数据页
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto it =  page_table_.find(page_id);
  if (it == page_table_.end())
    return false;//没找到
  pages_[it->second].pin_count_--;
  if(is_dirty)
  {
    pages_[it->second].is_dirty_ = is_dirty;
  }
  if (pages_[it->second].pin_count_ == 0) {
    //pin count为0时lru_lis中又可以替换这页了
    replacer_->Unpin(page_table_[page_id]);
  }
  return true;
}

/**
 * TODO: Student Implement
 */
 //将数据页转储到磁盘中
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto it =  page_table_.find(page_id);
  if (it == page_table_.end())
    return false;
  //找到则写回，更新脏位
  disk_manager_->WritePage(page_id, pages_[it->second].data_);
  pages_[it->second].is_dirty_ = false;
  return true;}

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
