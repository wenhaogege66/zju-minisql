#include "buffer/lru_replacer.h"

#include <algorithm>

LRUReplacer::LRUReplacer(size_t num_pages){

}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (Size() > 0) {
    *frame_id = lru_cache.front();//最前面是最不常访问的
    lru_cache.pop_front();
//    lru_not_pin.remove_if([&frame_id](int cur_val) -> int {return  cur_val == *frame_id;});
    return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto it = find(lru_cache.begin(),lru_cache.end(),frame_id);
  if (it != lru_cache.end()) {
    lru_cache.erase(it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  auto it = find(lru_cache.begin(),lru_cache.end(),frame_id);
  if (it == lru_cache.end())
  {
    lru_cache.emplace_back(frame_id);
  }
}

size_t LRUReplacer::Size() {
  return lru_cache.size();
}