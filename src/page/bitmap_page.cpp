#include "page/bitmap_page.h"

#include "glog/logging.h"


/**
 * TODO: Student Implement
 */
template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  size_t size_bitmap = GetMaxSupportedSize();
  if (page_allocated_ == size_bitmap) {
    return false;
  }
  page_offset = next_free_page_;
  page_allocated_++;
  unsigned char temp_new = bytes[page_offset / 8];
  unsigned char temp_bit = 1 << (7 - page_offset % 8);
  bytes[page_offset / 8] = temp_new | temp_bit;
  if (page_allocated_ == size_bitmap) {
    next_free_page_ = -1;
  } else {
    for (int i = page_offset; i < size_bitmap; i++) {
      if (IsPageFree(i)) {
        next_free_page_ = i;
        break;
      }
    }
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  unsigned char temp_old = bytes[page_offset / 8];
  if ((temp_old & (1 << (7 - page_offset % 8))) == 0) {
    return false;
  }
  unsigned char temp_bit = ~(1 << (7 - page_offset % 8));
  bytes[page_offset / 8] = temp_old & temp_bit;
  if (page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }
  page_allocated_--;
  return true;
}

/**
 * TODO: Student Implement
 */
template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index = page_offset % 8;
  return IsPageFreeLow(byte_index, bit_index);
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  unsigned char temp_byte = bytes[byte_index];
  unsigned char temp_bit = 1 << (7 - bit_index);
  temp_bit = temp_bit & temp_byte;
  if (temp_bit == 0) {
    return true;
  } else {
    return false;
  }
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
