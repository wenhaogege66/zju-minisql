#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){
  this -> cap = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex>lock(mutex_);
  if(lru_list_.empty())
    return false;
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  page_set_.erase(*frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex>lock(mutex_);
  if(page_set_.find(frame_id) != page_set_.end()){
    lru_list_.remove(frame_id);
    page_set_.erase(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex>lock(mutex_);
  if(page_set_.find(frame_id) == page_set_.end() && lru_list_.size() < cap){
    lru_list_.push_front(frame_id);
    page_set_.insert(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex>lock(mutex_);
  return lru_list_.size();
}