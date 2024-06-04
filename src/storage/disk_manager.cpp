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
  uint32_t disk_file_meta_page[1024];//管理1024个区（PAGE_SIZE/4）
  memcpy(disk_file_meta_page, meta_data_, 4096);

  //找未满的区
  uint32_t not_full_extent_id = 0;
  //前两页留作记录区数等（0：总的已分配页数；1：分区数）
  while (  disk_file_meta_page[2+not_full_extent_id] == BITMAP_SIZE) {
    not_full_extent_id++;
  };

  auto bitmap_physical_id = (page_id_t)(not_full_extent_id * (BITMAP_SIZE + 1) + 1);//一个区是BITMAP_SIZE + 1，第一个page是meta
  char bitmap[PAGE_SIZE];//
  ReadPhysicalPage(bitmap_physical_id, bitmap);
  auto *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
  uint32_t next_free_page = bitmap_page->GetNextFreePage();
  bitmap_page->AllocatePage(next_free_page);
  size_t logic_page_id;//逻辑id
  logic_page_id = not_full_extent_id * BITMAP_SIZE + next_free_page;//计算逻辑页号，BITMAP_SIZE不用+1

  if (not_full_extent_id >= disk_file_meta_page[1])//上一个区满了，现在分配的是一个新区，记得更新
    disk_file_meta_page[1]+=1;//如果not_full_extent_id大于或等于当前的分区数量，那么将分区数量加1
  disk_file_meta_page[0]++;//增加总的已分配页数。由于disk_file_meta_page数组的第一个元素存储的是总的已分配页数，我们通过增加第一个元素的值来反映新分配的页
  disk_file_meta_page[2+not_full_extent_id]++;//数组从索引2开始存储每个分区的已分配页数量,not_full_extent_id是分区的ID

  memcpy(meta_data_,disk_file_meta_page, 4096);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  WritePhysicalPage(bitmap_physical_id, bitmap);
  return (page_id_t)logic_page_id;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
    // 首先计算位图页的物理页号，该位图页负责管理包含指定逻辑页号的分区的页分配情况
    page_id_t bitmap_physical_id = 1 + MapPageId(logical_page_id) / (1 + BITMAP_SIZE);
    // 为位图页分配内存并从磁盘读取位图页数据
    char bitmap[PAGE_SIZE];
    ReadPhysicalPage(bitmap_physical_id, bitmap);
    // 将读取的位图页数据转换为BitmapPage类型，以便调用其成员函数进行操作
    auto *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
    // 在位图页中标记指定的逻辑页号对应的位为0，表示该页现在是非分配状态（空闲）
    bitmap_page->DeAllocatePage(logical_page_id % BITMAP_SIZE);
    // 为元数据页分配内存并从磁盘读取元数据页数据
    uint32_t disk_file_meta_page[1024];
    // 计算当前非满分区ID，每个分区包含BITMAP_SIZE个数据页
    //逻辑id:0,1,2第一个分区这种
    uint32_t not_full_extent_id = logical_page_id / BITMAP_SIZE;
    // 减少该分区的已分配页计数
    if (--disk_file_meta_page[2 + not_full_extent_id]== 0) {
      // 如果该分区的已分配页计数变为0，则减少总分区数
      disk_file_meta_page[1]--;
    }
    // 减少总分配页计数
    disk_file_meta_page[0]--;

    // 将更新后的元数据复制回meta_data_数组，准备写回磁盘
    memcpy(meta_data_, disk_file_meta_page, 4096);
    // 将更新后的元数据页写回磁盘
    WritePhysicalPage(META_PAGE_ID, meta_data_);
    // 将更新后的位图页写回磁盘，以反映页的释放状态
    WritePhysicalPage(bitmap_physical_id, bitmap);
}


/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  // 判断对应的bitmap中那一bit是0还是1
  // 读取对应的bitmap
  page_id_t bitmap_physical_id = 1 + MapPageId(logical_page_id) / (1 + BITMAP_SIZE);
  char bitmap[PAGE_SIZE];
  ReadPhysicalPage(bitmap_physical_id, bitmap);
  auto *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap);
  // 判断
  if (bitmap_page->IsPageFree(logical_page_id % BITMAP_SIZE))
    return true;
  return false;
}

/**
 * TODO: Student Implement
 */
 //逻辑转物理
//刚开始物理只比逻辑多2，但是随着逻辑的增多，逻辑每多一个分区，物理就又领先一个
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return 2 + logical_page_id + logical_page_id / BITMAP_SIZE;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf{};
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