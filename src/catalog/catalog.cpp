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
  CatalogMeta *meta = new CatalogMeta();
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
uint32_t CatalogMeta::GetSerializedSize() const {
  return 4 * 3 + 8 * (table_meta_pages_.size() + index_meta_pages_.size());
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if(init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  }
  else {
    Page *meta_data_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(meta_data_page->GetData());
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
    for(auto table_map: catalog_meta_->table_meta_pages_) {
      LoadTable(table_map.first,table_map.second);
    }
    for(auto table_map: catalog_meta_->index_meta_pages_) {
      LoadIndex(table_map.first,table_map.second);
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
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
  if(table_names_.find(table_name) != table_names_.end())
    return DB_TABLE_ALREADY_EXIST;
  table_info = TableInfo::Create();
  auto copy_schema = Schema::DeepCopySchema(schema);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, copy_schema, txn, log_manager_, lock_manager_);
  TableMetadata *table_meta_data = TableMetadata::Create(next_table_id_++, table_name, table_heap->GetFirstPageId(), copy_schema);
  table_info->Init(table_meta_data, table_heap);
  table_names_[table_name] = next_table_id_ - 1;
  tables_[next_table_id_ - 1] = table_info;
  page_id_t page_id;
  Page *meta_data_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_[next_table_id_ - 1] = page_id;
  table_meta_data->SerializeTo(meta_data_page->GetData());
  buffer_pool_manager_->UnpinPage(page_id,true);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  return GetTable(table_names_[table_name], table_info);
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto table: tables_)
    tables.push_back(table.second);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  if(index_names_[table_name].find(index_name) != index_names_[table_name].end())
    return DB_INDEX_ALREADY_EXIST;
  index_info = IndexInfo::Create();
  vector<uint32_t> key_map;
  for(const auto& index_key_name : index_keys) {
    uint32_t key_index;
    if (tables_[table_names_[table_name]]->GetSchema()->GetColumnIndex(index_key_name, key_index) == DB_COLUMN_NAME_NOT_EXIST)
      return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(key_index);
  }
  IndexMetadata *index_meta_data = IndexMetadata::Create(next_index_id_++, index_name, table_names_[table_name], key_map);
  index_info->Init(index_meta_data, tables_[table_names_[table_name]], buffer_pool_manager_);
  if(index_names_.find(table_name) == index_names_.end())
    index_names_[table_name];
  index_names_[table_name][index_name] = next_index_id_ - 1;
  indexes_[next_index_id_ - 1] = index_info;
  page_id_t page_id;
  Page *meta_data_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_[next_index_id_ - 1] = page_id;
  index_meta_data->SerializeTo(meta_data_page->GetData());
  buffer_pool_manager_->UnpinPage(page_id,true);
  auto it = tables_[table_names_[table_name]]->GetTableHeap()->Begin(txn);
  while (it != tables_[table_names_[table_name]]->GetTableHeap()->End()) {
    auto all_row = &(*it);
    Row row = *all_row;
    Row key_row;
    row.GetKeyFromRow(tables_[table_names_[table_name]]->GetSchema(), index_info->GetIndexKeySchema(), key_row);
    index_info->GetIndex()->InsertEntry(key_row, row.GetRowId(), txn);
    it++;
  }
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  if(index_names_.find(table_name) == index_names_.end())
    return DB_INDEX_NOT_FOUND;
  if(index_names_.find(table_name)->second.find(index_name) == index_names_.find(table_name)->second.end())
    return DB_INDEX_NOT_FOUND;
  index_info = indexes_.find(index_names_.find(table_name)->second.find(index_name)->second)->second;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  if(index_names_.find(table_name) != index_names_.end())
    for(const auto& index_map: index_names_.find(table_name)->second)
      indexes.push_back(indexes_.find(index_map.second)->second);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  table_id_t table_id = table_names_[table_name];
  if(!buffer_pool_manager_->DeletePage(tables_[table_id]->GetRootPageId())||!buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]))
    return DB_FAILED;
  table_names_.erase(table_name);
  tables_.erase(table_id);
  catalog_meta_->table_meta_pages_.erase(table_id);
  if(index_names_.find(table_name) != index_names_.end()) {
    for(const auto& index_map: index_names_.find(table_name)->second) {
      if (!buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_map.second]))
        return DB_FAILED;
      indexes_.erase(index_map.second);
      catalog_meta_->index_meta_pages_.erase(index_map.second);
    }
    index_names_.erase(table_name);
  }
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if(table_names_.find(table_name) == table_names_.end())
    return DB_TABLE_NOT_EXIST;
  if(index_names_.find(table_name) == index_names_.end())
    return DB_INDEX_NOT_FOUND;
  if(index_names_.find(table_name)->second.find(index_name) == index_names_.find(table_name)->second.end())
    return DB_INDEX_NOT_FOUND;
  index_id_t index_id = index_names_[table_name][index_name];
  if(!buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]))
    return DB_FAILED;
  if(index_names_[table_name].size() == 1)
    index_names_.erase(table_name);
  else
    index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  catalog_meta_->index_meta_pages_.erase(index_id);
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *catalog_meta_data = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  catalog_meta_->SerializeTo(catalog_meta_data->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page *meta_data_page = buffer_pool_manager_->FetchPage(page_id);
  TableInfo *table_info = TableInfo::Create();
  TableMetadata *table_meta_data;
  TableMetadata::DeserializeFrom(meta_data_page->GetData(), table_meta_data);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta_data->GetFirstPageId(), table_meta_data->GetSchema(), log_manager_, lock_manager_);
  table_info->Init(table_meta_data, table_heap);
  table_names_[table_meta_data->GetTableName()] = table_id;
  tables_[table_id] = table_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page *meta_data_page = buffer_pool_manager_->FetchPage(page_id);
  IndexInfo *index_info = IndexInfo::Create();
  IndexMetadata *index_meta_data;
  IndexMetadata::DeserializeFrom(meta_data_page->GetData(), index_meta_data);
  index_info->Init(index_meta_data, tables_[index_meta_data->GetTableId()], buffer_pool_manager_);
  string table_name = tables_[index_meta_data->GetTableId()]->GetTableName();
  if(table_names_.find(table_name) == table_names_.end()) {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_FAILED;
  }
  if(index_names_.find(table_name) == index_names_.end())
    index_names_[table_name];
  index_names_[table_name][index_meta_data->GetIndexName()] = index_id;
  indexes_[index_id] = index_info;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if(tables_.find(table_id) == tables_.end())
    return DB_TABLE_NOT_EXIST;
  table_info = tables_[table_id];
  return DB_SUCCESS;
}