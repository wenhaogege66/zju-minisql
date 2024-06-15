#include "catalog/catalog.h"

// Serialize catalog metadata to the given buffer
void CatalogMeta::SerializeTo(char *buffer) const {
  // Check if the serialized size exceeds the page size
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");

  // Write the magic number
  MACH_WRITE_UINT32(buffer, CATALOG_METADATA_MAGIC_NUM);
  buffer += 4;

  // Write the size of table meta pages
  MACH_WRITE_UINT32(buffer, table_meta_pages_.size());
  buffer += 4;

  // Write the size of index meta pages
  MACH_WRITE_UINT32(buffer, index_meta_pages_.size());
  buffer += 4;

  // Write table metadata
  for (const auto& entry : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buffer, entry.first);
    buffer += 4;
    MACH_WRITE_TO(page_id_t, buffer, entry.second);
    buffer += 4;
  }

  // Write index metadata
  for (const auto& entry : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buffer, entry.first);
    buffer += 4;
    MACH_WRITE_TO(page_id_t, buffer, entry.second);
    buffer += 4;
  }
}

// Deserialize catalog metadata from the given buffer
CatalogMeta *CatalogMeta::DeserializeFrom(char *buffer) {
  // Read and check the magic number
  uint32_t magic_num = MACH_READ_UINT32(buffer);
  buffer += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");

  // Read the number of table and index entries
  uint32_t table_count = MACH_READ_UINT32(buffer);
  buffer += 4;
  uint32_t index_count = MACH_READ_UINT32(buffer);
  buffer += 4;

  // Create a new CatalogMeta object
  CatalogMeta *meta = new CatalogMeta();

  // Read and store table metadata
  for (uint32_t i = 0; i < table_count; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buffer);
    buffer += 4;
    auto page_id = MACH_READ_FROM(page_id_t, buffer);
    buffer += 4;
    meta->table_meta_pages_.emplace(table_id, page_id);
  }

  // Read and store index metadata
  for (uint32_t i = 0; i < index_count; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buffer);
    buffer += 4;
    auto page_id = MACH_READ_FROM(page_id_t, buffer);
    buffer += 4;
    meta->index_meta_pages_.emplace(index_id, page_id);
  }

  return meta;
}

// Calculate the serialized size of catalog metadata
uint32_t CatalogMeta::GetSerializedSize() const {
  return 4 * 3 + 8 * (table_meta_pages_.size() + index_meta_pages_.size());
}

// Default constructor for CatalogMeta
CatalogMeta::CatalogMeta() {}

// CatalogManager constructor, loads or initializes the catalog metadata
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance();
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  } else {
    Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData());
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();

    for (const auto& table_entry : catalog_meta_->table_meta_pages_) {
      LoadTable(table_entry.first, table_entry.second);
    }
    for (const auto& index_entry : catalog_meta_->index_meta_pages_) {
      LoadIndex(index_entry.first, index_entry.second);
    }
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
  FlushCatalogMetaPage();
}

// CatalogManager destructor, ensures metadata is flushed and cleans up resources
CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (const auto& table_pair : tables_) {
    delete table_pair.second;
  }
  for (const auto& index_pair : indexes_) {
    delete index_pair.second;
  }
}

// Create a new table in the catalog
dberr_t CatalogManager::CreateTable(const std::string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.count(table_name)) return DB_TABLE_ALREADY_EXIST;

  table_info = TableInfo::Create();
  auto schema_copy = Schema::DeepCopySchema(schema);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema_copy, txn, log_manager_, lock_manager_);
  TableMetadata *table_metadata = TableMetadata::Create(next_table_id_++, table_name, table_heap->GetFirstPageId(), schema_copy);
  table_info->Init(table_metadata, table_heap);

  table_names_[table_name] = next_table_id_ - 1;
  tables_[next_table_id_ - 1] = table_info;

  page_id_t page_id;
  Page *metadata_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_[next_table_id_ - 1] = page_id;
  table_metadata->SerializeTo(metadata_page->GetData());

  buffer_pool_manager_->UnpinPage(page_id, true);
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

// Retrieve table information by table name
dberr_t CatalogManager::GetTable(const std::string &table_name, TableInfo *&table_info) {
  if (!table_names_.count(table_name)) return DB_TABLE_NOT_EXIST;
  return GetTable(table_names_.at(table_name), table_info);
}

// Get all tables in the catalog
dberr_t CatalogManager::GetTables(std::vector<TableInfo *> &tables) const {
  for (const auto& table_entry : tables_) {
    tables.push_back(table_entry.second);
  }
  return DB_SUCCESS;
}

// Create a new index for a table
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const std::string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const std::string &index_type) {
  if (!table_names_.count(table_name)) return DB_TABLE_NOT_EXIST;
  if (index_names_[table_name].count(index_name)) return DB_INDEX_ALREADY_EXIST;

  index_info = IndexInfo::Create();
  std::vector<uint32_t> key_map;

  for (const auto& key : index_keys) {
    uint32_t col_index;
    if (tables_.at(table_names_.at(table_name))->GetSchema()->GetColumnIndex(key, col_index) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(col_index);
  }

  IndexMetadata *index_metadata = IndexMetadata::Create(next_index_id_++, index_name, table_names_.at(table_name), key_map);
  index_info->Init(index_metadata, tables_.at(table_names_.at(table_name)), buffer_pool_manager_);

  index_names_[table_name][index_name] = next_index_id_ - 1;
  indexes_[next_index_id_ - 1] = index_info;

  page_id_t page_id;
  Page *metadata_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->index_meta_pages_[next_index_id_ - 1] = page_id;
  index_metadata->SerializeTo(metadata_page->GetData());

  buffer_pool_manager_->UnpinPage(page_id, true);

  for (auto it = tables_.at(table_names_.at(table_name))->GetTableHeap()->Begin(txn);
       it != tables_.at(table_names_.at(table_name))->GetTableHeap()->End(); ++it) {
    Row row = *it;
    Row key_row;
    row.GetKeyFromRow(tables_.at(table_names_.at(table_name))->GetSchema(), index_info->GetIndexKeySchema(), key_row);
    index_info->GetIndex()->InsertEntry(key_row, row.GetRowId(), txn);
  }

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

// Retrieve index information by table and index names
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name, IndexInfo *&index_info) const {
  if (!table_names_.count(table_name)) return DB_TABLE_NOT_EXIST;
  if (!index_names_.count(table_name) || !index_names_.at(table_name).count(index_name)) return DB_INDEX_NOT_FOUND;

  index_info = indexes_.at(index_names_.at(table_name).at(index_name));
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