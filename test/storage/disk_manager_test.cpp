#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  page_id_t temp_bitmap_id;
  uint32_t new_page_id;
  if (meta_page->num_allocated_pages_ == meta_page->num_extents_ * BITMAP_SIZE) {
    meta_page->num_extents_++;
    temp_bitmap_id = meta_page->num_extents_ - 1;
    char buf[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buf);
    bitmap->AllocatePage(new_page_id);
    page_id_t new_id = 1 + temp_bitmap_id * (BITMAP_SIZE + 1);
    WritePhysicalPage(new_id, buf);
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[meta_page->num_extents_ - 1] = 1;
    return LtoF(new_id + new_page_id + 1);
  } else {
    for (int i = 0; i < meta_page->num_extents_; i++) {
      int k = meta_page->extent_used_page_[i];
      if (meta_page->extent_used_page_[i] < BITMAP_SIZE) {
        temp_bitmap_id = i;
        break;
      }
    }
    char bitmap_page_data[PAGE_SIZE];
    page_id_t bitmap_id = 1 + temp_bitmap_id * (1 + BITMAP_SIZE);
    ReadPhysicalPage(bitmap_id, bitmap_page_data);
    BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page_data);
    bitmap->AllocatePage(new_page_id);
    WritePhysicalPage(bitmap_id, bitmap_page_data);
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[temp_bitmap_id]++;
    return LtoF(bitmap_id + new_page_id + 1);
  }
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  char bitmap_page_data[PAGE_SIZE];
  ReadPhysicalPage((logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1) + 1, bitmap_page_data);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page_data);
  if (bitmap->DeAllocatePage(logical_page_id % BITMAP_SIZE)) {
    DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
    WritePhysicalPage((logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1) + 1, bitmap_page_data);
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[logical_page_id / BITMAP_SIZE]--;
    if (meta_page->extent_used_page_[logical_page_id / BITMAP_SIZE] == 0)
      meta_page->num_extents_--;
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char bitmap_page_data[PAGE_SIZE];
  ReadPhysicalPage((logical_page_id / BITMAP_SIZE) * (BITMAP_SIZE + 1) + 1, bitmap_page_data);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page_data);
  return bitmap->IsPageFree(logical_page_id % BITMAP_SIZE);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  page_id_t temp_num = logical_page_id / BITMAP_SIZE;
  page_id_t temp_op = logical_page_id % BITMAP_SIZE;
  page_id_t temp_result = temp_num * (BITMAP_SIZE + 1) + temp_op + 2;
  return temp_result;
}

page_id_t DiskManager::LtoF(page_id_t physics_page_id) {
  page_id_t temp_block = (physics_page_id - 1) / (BITMAP_SIZE + 1);
  page_id_t temp_op = (physics_page_id - 1) % (BITMAP_SIZE + 1);
  return temp_block * BITMAP_SIZE + temp_op - 1;
}


int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
