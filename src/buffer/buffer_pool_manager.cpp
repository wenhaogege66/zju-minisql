#include "buffer/buffer_pool_manager.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

// 用于初始化空页面数据
static const char BLANK_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t buffer_size, DiskManager *disk_mgr)
    : pool_size_(buffer_size), disk_manager_(disk_mgr) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.push_back(i); // 将空闲页帧添加到空闲列表
  }
}

BufferPoolManager::~BufferPoolManager() {
  // 确保在销毁缓冲池管理器前，所有页面都被刷入磁盘
  for (const auto &entry : page_table_) {
    FlushPage(entry.first);
  }
  delete[] pages_; // 释放页面数组
  delete replacer_; // 释放替换策略
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 从页表中查找请求的页面
  if (page_table_.count(page_id) > 0) {
    auto frame_id = page_table_[page_id];
    pages_[frame_id].pin_count_++; // 增加固定计数
    replacer_->Pin(frame_id); // 在替换器中固定该页面
    return &pages_[frame_id];
  } else {
    frame_id_t frame_to_use;
    // 优先使用空闲列表中的页帧
    if (!free_list_.empty()) {
      frame_to_use = free_list_.front();
      free_list_.pop_front();
    } else {
      // 如果没有空闲页帧，从替换器中选择一个页帧
      if (replacer_->Victim(&frame_to_use)) {
        if (pages_[frame_to_use].IsDirty()) {
          disk_manager_->WritePage(pages_[frame_to_use].page_id_, pages_[frame_to_use].data_);
        }
        page_table_.erase(pages_[frame_to_use].page_id_);
      } else {
        return nullptr; // 如果替换器中也没有可用页帧，则返回空
      }
    }

    // 更新页表，并将页面数据从磁盘读取到内存
    page_table_[page_id] = frame_to_use;
    pages_[frame_to_use].ResetMemory();
    pages_[frame_to_use].page_id_ = page_id;
    pages_[frame_to_use].is_dirty_ = false;
    pages_[frame_to_use].pin_count_ = 1;
    disk_manager_->ReadPage(page_id, pages_[frame_to_use].data_);
    replacer_->Pin(frame_to_use); // 将新加载的页面固定
    return &pages_[frame_to_use];
  }
}

Page *BufferPoolManager::NewPage(page_id_t &new_page_id) {
  frame_id_t frame_to_use;
  // 优先从空闲列表中分配页帧
  if (!free_list_.empty()) {
    frame_to_use = free_list_.front();
    free_list_.pop_front();
  } else {
    // 如果没有空闲页帧，从替换器中选择一个页帧
    if (replacer_->Victim(&frame_to_use)) {
      if (pages_[frame_to_use].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_to_use].page_id_, pages_[frame_to_use].data_);
      }
      page_table_.erase(pages_[frame_to_use].page_id_);
    } else {
      return nullptr; // 如果没有可用页帧，返回空
    }
  }

  // 分配新页面并更新页表和元数据
  new_page_id = AllocatePage();
  page_table_[new_page_id] = frame_to_use;
  pages_[frame_to_use].ResetMemory();
  pages_[frame_to_use].page_id_ = new_page_id;
  pages_[frame_to_use].pin_count_ = 1;
  pages_[frame_to_use].is_dirty_ = false;
  replacer_->Pin(frame_to_use); // 固定新页面
  return &pages_[frame_to_use];
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 如果请求删除的页面不存在，直接返回 true
  if (page_table_.count(page_id) == 0) {
    return true;
  }

  auto frame_id = page_table_[page_id];
  // 如果页面的固定计数大于零，不能删除
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  // 从页表中删除该页面，重置其元数据并返回到空闲列表
  page_table_.erase(page_id);
  replacer_->Pin(frame_id);
  DeallocatePage(page_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  free_list_.push_back(frame_id);
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // 检查页面是否在页表中
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  pages_[frame_id].is_dirty_ = is_dirty || pages_[frame_id].is_dirty_;
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id); // 当页面不再固定时，解锁它
  }
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  // 确保页面在页表中存在
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  auto frame_id = page_table_[page_id];
  // 将页面写回磁盘
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

bool BufferPoolManager::FlushAllPages() {
  bool success = true;
  // 将所有有效的页面写回磁盘
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      success = success && FlushPage(pages_[i].page_id_);
    }
  }
  return success;
}

page_id_t BufferPoolManager::AllocatePage() {
  return disk_manager_->AllocatePage(); // 从磁盘管理器分配新页面
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id); // 从磁盘管理器释放页面
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id); // 检查页面是否是空闲的
}

// 调试用，检查所有页面是否都未被固定
bool BufferPoolManager::CheckAllUnpinned() {
  bool all_unpinned = true;
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].pin_count_ != 0) {
      all_unpinned = false;
      LOG(ERROR) << "Page ID " << pages_[i].page_id_ << " is pinned with count: " << pages_[i].pin_count_;
    }
  }
  return all_unpinned;
}
