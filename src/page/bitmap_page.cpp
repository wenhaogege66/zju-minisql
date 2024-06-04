#include "page/bitmap_page.h"
#include "iostream"

using namespace std;
#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
 //位图中每个比特（Bit）对应一个数据页的分配情况，用于标记该数据页是否空闲（0表示空闲，1表示已分配）
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ < GetMaxSupportedSize()) {
    page_allocated_++;
    page_offset = next_free_page_;
    // 将添加的那一位置1
    bytes[page_offset / 8] |= (1 << (page_offset % 8));
    uint32_t index = 0;
    while (!IsPageFreeLow(index / 8, index % 8) && index <GetMaxSupportedSize()) {
      index++;
    }
    next_free_page_ = index;
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
//  cout  << "page_offset"  <<page_offset <<endl;
  if (!IsPageFreeLow(page_offset / 8, page_offset % 8)) {
    // 将删掉的那一位置0,优先用刚删的页
    next_free_page_ = page_offset;
    bytes[page_offset / 8] = bytes[page_offset / 8] & (~(1 << (page_offset % 8)));
    page_allocated_--;
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  // 确保page_offset在有效范围内
  if (page_offset > GetMaxSupportedSize()) {
    return false;
  }

  // 检查对应的比特位是否为0（空闲）
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
uint32_t BitmapPage<PageSize>::GetNextFreePage(){
  return next_free_page_;
};


template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  // Check if the bit at the specified position is 0
  return ((bytes[byte_index] >> bit_index) & 1) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;