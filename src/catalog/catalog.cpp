#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  auto *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
 //magic_number 4字节 + table_meta_pages_.size() 4字节 + index_meta_pages_.size() 4字节 = 12
// 每个 table_meta_pages_ 中的元素占用8字节，乘以其大小。
// 每个 index_meta_pages_ 中的元素同样占用8字节，乘以其大小。
uint32_t CatalogMeta::GetSerializedSize() const {
  return 12 + 8 * ( index_meta_pages_.size() + table_meta_pages_.size());
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {

  if (init) {
    //buffer_pool_manager_为空，正常初始化
    catalog_meta_ = CatalogMeta::NewInstance();
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  }
  else{
    //buffer_pool_manager_有页，从中反序列化CatalogMetadata就能获取真正的meta_data_page
    //先找到对应的页
    //22222222
    Page *meta_data_page = buffer_pool_manager_->FetchPage(0);//CATALOG_META_PAGE_ID:logical page id of the catalog meta data为0
//    meta_data_page->RLatch();//上锁
    //反序列化CatalogMetadata
    char * real_page = meta_data_page->GetData();
    catalog_meta_ = CatalogMeta::DeserializeFrom(real_page);
//    meta_data_page->RUnlatch();//关锁
    //正常更新
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    // 更新CatalogManager的数据
    // 根据catalogMeta中的数据更新Table和Index
    for (auto table_meta_page_it : catalog_meta_->table_meta_pages_) {
      dberr_t res =  LoadTable(table_meta_page_it.first, table_meta_page_it.second);
      if(res != DB_SUCCESS)//没成功
      {
        cout << "CatalogManager构造失败" <<endl;
      }
    }
    for (auto index_meta_page_it : catalog_meta_->index_meta_pages_) {
      dberr_t res = LoadIndex(index_meta_page_it.first, index_meta_page_it.second);
      if(res != DB_SUCCESS)//没成功
      {
        cout << "CatalogManager构造失败" <<endl;
      }
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
  // 将改动刷盘
  FlushCatalogMetaPage();
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  //table是否已经存在
  if (table_names_.count(table_name) >0)
    return DB_TABLE_ALREADY_EXIST;
  //TableInfo,TableMetaData,TableHeap
  table_id_t table_id = next_table_id_++;
  //创建元数据
  auto copy_schema = Schema::DeepCopySchema(schema);
  TableHeap *cur_table_heap = TableHeap::Create(buffer_pool_manager_, copy_schema, nullptr, log_manager_, lock_manager_);
  //111111
  TableMetadata *meta_data = TableMetadata::Create(table_id, table_name, cur_table_heap->GetFirstPageId(), copy_schema);
  table_info = TableInfo::Create();
  table_info->Init(meta_data, cur_table_heap);
  tables_[table_id] = table_info;
  table_names_[table_name] = table_id;
  //序列化table_meta_data
  page_id_t meta_page_id;
  Page *meta_data_page = buffer_pool_manager_->NewPage(meta_page_id);
  if(meta_data_page == nullptr)
  {
    return DB_FAILED;
  }
//  meta_data_page->WLatch();
  char * real_meta_page = meta_data_page->GetData();
  meta_data->SerializeTo(real_meta_page);
//  meta_data_page->WUnlatch();
  catalog_meta_->table_meta_pages_[table_id] = meta_page_id;
  // 将改动刷盘
  buffer_pool_manager_->UnpinPage(meta_page_id,true);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto it = table_names_.find(table_name);
  if (it == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  return GetTable(it->second, table_info);
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto each_table : tables_) {
    tables.push_back(each_table.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const string &table_name, const string &index_name,
                                    const vector<string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  //table都没有直接返回，已有index也返回
  if(table_names_.count(table_name) == 0)
  {
    return DB_TABLE_NOT_EXIST;
  }
  if(index_names_[table_name].count(index_name) > 0)
  {
    return DB_INDEX_ALREADY_EXIST;
  }
  index_id_t index_id = next_index_id_++;
  TableInfo *table_info = tables_[table_names_[table_name]];//指向的table
  vector<uint32_t> key_map;
  for (const auto& key_name : index_keys) {
    uint32_t key_index;
    if (table_info->GetSchema()->GetColumnIndex(key_name, key_index) != DB_SUCCESS)
      return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(key_index);
  }

  if (index_names_.find(table_name) == index_names_.end()) {
    unordered_map<string, index_id_t> map;
    map[index_name] = index_id;
    index_names_[table_name] = map;
  }
  else
  {
    index_names_.find(table_name)->second[index_name] = index_id;
  }
  index_info = IndexInfo::Create();//直接修改传入的引用
  IndexMetadata *meta_data = IndexMetadata::Create(index_id, index_name, table_names_.find(table_name)->second, key_map);
  index_info->Init(meta_data, table_info, buffer_pool_manager_);
  indexes_[index_id] = index_info;
  page_id_t meta_data_page_id;
  // 将索引元数据序列化到新页的数据区域，更新catalog_meta_
  Page *meta_data_page = buffer_pool_manager_->NewPage(meta_data_page_id);
  if(meta_data_page == nullptr )
    return  DB_FAILED;
  //  meta_data_page->WLatch();
  meta_data->SerializeTo(meta_data_page->GetData());
  //  meta_data_page->WUnlatch();
  catalog_meta_->index_meta_pages_[index_id] = meta_data_page_id;
  //有写的内容，所以是脏的
  buffer_pool_manager_->UnpinPage(meta_data_page_id, true);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const string &table_name, const string &index_name,
                                 IndexInfo *&index_info) const {
  // 尝试查找表名对应的条目是否存在于表名映射表（table_names_）中
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) {
    return DB_TABLE_NOT_EXIST; // 如果表名不存在于映射表中，返回错误代码DB_TABLE_NOT_EXIST
  }

  // 获取对应的索引存放情况
  // 查找表名在索引映射表（index_names_）中的条目
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_INDEX_NOT_FOUND; // 如果表名不存在于索引映射表中，返回错误代码DB_INDEX_NOT_FOUND
  }
  else
  {
    // 继续查找索引名对应的条目是否存在于索引映射表中
    auto index_id_it = index_names_.find(table_name)->second.find(index_name);
    if (index_id_it == index_names_.find(table_name)->second.end()) {
      return DB_INDEX_NOT_FOUND; // 如果索引名不存在于索引映射表中，返回错误代码DB_INDEX_NOT_FOUND
    }
    // 根据索引ID查找索引信息
    auto it_index_info = indexes_.find(index_id_it->second); // 在索引映射表（indexes_）中查找索引ID对应的条目
    if (it_index_info == indexes_.end()) {
      return DB_FAILED; // 如果没有找到对应的索引信息，返回错误代码DB_FAILED
    }
    // 将找到的索引信息赋值给引用参数index_info
    index_info = it_index_info->second;
  }
  // 如果所有查找都成功，返回成功代码DB_SUCCESS
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const string &table_name, vector<IndexInfo *> &indexes) const {
  if (table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  if (index_names_.find(table_name) != index_names_.end()) {
    //该table有index
    for (const auto &index_map : index_names_.find(table_name)->second) {
      auto indexes_it = indexes_.find(index_map.second);
      indexes.push_back(indexes_it->second);
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // 尝试查找表名对应的条目是否存在于表名映射表（table_names_）中
  auto table_id_it = table_names_.find(table_name);
  if (table_id_it == table_names_.end()) {
    return DB_TABLE_NOT_EXIST; // 如果表名不存在于映射表中，返回错误代码DB_TABLE_NOT_EXIST
  }
  // 获取表ID
  table_id_t table_id = table_id_it->second;
  // 调用缓冲池管理器删除表的根页
  if (!buffer_pool_manager_->DeletePage(tables_[table_id]->GetRootPageId())) {
    return DB_FAILED; // 如果删除失败，返回错误代码DB_FAILED
  }
  // 调用缓冲池管理器删除表的元数据页
  if (!buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id])) {
    return DB_FAILED; // 如果删除失败，返回错误代码DB_FAILED
  }
  // 从表映射表（tables_）中删除表的条目
  tables_.erase(table_id);
  // 从表名映射表（table_names_）中删除表名的条目
  table_names_.erase(table_name);
  // 从目录元数据中删除表的元数据页映射
  catalog_meta_->table_meta_pages_.erase(table_id);
  // 检查索引映射表（index_names_）中是否存在该表的索引
  if (index_names_.find(table_name) != index_names_.end()) {
    // 遍历该表的所有索引，并删除它们
    for (const auto &index_pair : index_names_[table_name]) {
      // 从目录元数据中删除索引的元数据页映射
      catalog_meta_->index_meta_pages_.erase(index_pair.second);
      // 从索引映射表（indexes_）中删除索引的条目
      indexes_.erase(index_pair.second);
    }
    // 从索引映射表（index_names_）中删除该表的索引映射
    index_names_.erase(table_name);
  }
  // 刷新目录元数据页以确保更改被写入磁盘
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // 检查表名是否存在于表名映射表（table_names_）中
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST; // 如果表名不存在，返回错误代码DB_TABLE_NOT_EXIST
  }
  // 查找表名在索引映射表（index_names_）中的索引映射
  auto table_index_it = index_names_.find(table_name);
  if (table_index_it == index_names_.end()) {
    return DB_INDEX_NOT_FOUND; // 如果没有找到索引映射，返回错误代码DB_INDEX_NOT_FOUND
  }
  else
  {
    // 在表的索引映射中查找特定的索引名
    auto index_name_it = table_index_it->second.find(index_name);
    if (index_name_it == table_index_it->second.end())
      return DB_INDEX_NOT_FOUND; // 如果索引名不存在于映射中，返回错误代码DB_INDEX_NOT_FOUND
    else
    {
      // 获取索引ID
      index_id_t index_id = index_name_it->second;
      // 获取索引信息
      IndexInfo *index_info = indexes_[index_id];
      // 销毁索引数据结构
      index_info->GetIndex()->Destroy();
      // 调用缓冲池管理器删除索引的元数据页
      if (!buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]))
        return DB_FAILED; // 如果删除失败，返回错误代码DB_FAILED
      // 如果表只有一个索引，则从索引映射表中删除整个表的映射
      if (table_index_it->second.size() == 1) {
        index_names_.erase(table_index_it);
      } else {
        // 否则，只删除索引映射中的特定索引名
        table_index_it->second.erase(index_name);
      }
      // 从索引信息映射表（indexes_）中删除索引条目
      indexes_.erase(index_id);
      // 从目录元数据中删除索引的元数据页映射
      catalog_meta_->index_meta_pages_.erase(index_id);
    }
  }
  // 刷新目录元数据页以确保更改被写入磁盘
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
// 将CatalogMeta里面的数据写到page中
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // 直接序列化CatalogMetaData到数据页中
  auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if(catalog_meta_page == nullptr)
  {
    return DB_FAILED;
  }
  catalog_meta_page->WLatch();
  char* real_catalog_page =  catalog_meta_page->GetData();
  catalog_meta_->SerializeTo(real_catalog_page);
  catalog_meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);  //写完记得unpin，写过是脏的
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);//存到磁盘
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
// 读取page_id存的table_meta_data,并更新CatalogManager
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  //拿到存meta_data的页
  //alt + j
  auto data_page = buffer_pool_manager_->FetchPage(page_id);
  if(data_page == nullptr)
    return DB_NOT_EXIST;
  // 通过反序列化获得table_meta_data,最终构成cur_table_info
//  data_page->RLatch();
  TableMetadata *table_meta_data = nullptr;
  char * real_table_page = data_page->GetData();
  TableMetadata::DeserializeFrom(real_table_page, table_meta_data);
//  data_page->RUnlatch();
  if(table_id != table_meta_data->GetTableId())
  {
    return  DB_FAILED;
  }
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta_data->GetFirstPageId(), table_meta_data->GetSchema(),
                                            log_manager_, lock_manager_);
  table_names_[table_meta_data->GetTableName()] = table_id;  //更新table_names_
  TableInfo *cur_table_info = TableInfo::Create();//TableInfo()是私有构造函数，不能直接调用
  cur_table_info->Init(table_meta_data, table_heap);
  tables_[table_id] = cur_table_info;//更新tables_
  buffer_pool_manager_->UnpinPage(page_id, false);//别忘了在buffer_pool中取消固定一个数据页，没写过，所以不脏
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  //类似LoadTable
  //拿到存meta_data的页
  auto data_page = buffer_pool_manager_->FetchPage(page_id);
  if(data_page == nullptr)
    return DB_NOT_EXIST;
  // 通过反序列化获得index_meta_data,最终构成cur_index_info
  //IndexInfo()是私有构造函数，不能直接调用
  data_page->RLatch();
  IndexMetadata *index_meta_data;
  char * real_index_page = data_page->GetData();
  IndexMetadata::DeserializeFrom(real_index_page, index_meta_data);
  data_page->RUnlatch();
  if(index_id != index_meta_data->GetIndexId())
  {
    return  DB_FAILED;
  }
  table_id_t  table_id = index_meta_data->GetTableId();
  string index_name = index_meta_data->GetIndexName();
  string table_name = tables_[table_id]->GetTableName();
  //更新map
  if(table_names_.find(table_name) == table_names_.end())
  {
    buffer_pool_manager_->UnpinPage(page_id, false);//别忘了在buffer_pool中取消固定一个数据页，没写过，所以不脏
    return DB_TABLE_NOT_EXIST;
  }
  else
  {
    auto it = index_names_.find(table_name);
    if(it == index_names_.end())
    {
      unordered_map<string,index_id_t> tmp_map;
      tmp_map[index_name] = index_id;
      index_names_[table_name] = tmp_map;
    }
    else
    {
      it->second[index_name] = index_id;
    }
  }
  //插入indexes_
  IndexInfo *cur_index_info = IndexInfo::Create();
  cur_index_info->Init(index_meta_data, tables_[table_id], buffer_pool_manager_);
  indexes_[index_id] = cur_index_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  //调用私有函数前已经确保存在了
  table_info = tables_.find(table_id)->second;
  return DB_SUCCESS;
}