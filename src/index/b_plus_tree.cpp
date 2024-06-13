#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {

  // 如果叶子节点或内部节点的最大尺寸未定义，使用默认计算方法。
  if(leaf_max_size == UNDEFINED_SIZE || internal_max_size == UNDEFINED_SIZE){
    // 计算默认的叶子节点最大尺寸：页面大小减去叶子节点头的大小，然后除以每个键值对的大小。
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;

    // 计算默认的内部节点最大尺寸：页面大小减去内部节点头的大小，然后除以每个键-页ID对的大小。
    internal_max_size_ =  (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;

    // 确保内部节点的最大尺寸至少为2。
    if(internal_max_size_ < 2) {
      internal_max_size_ = 2, leaf_max_size_ = 2;
    }
  }

  // 从缓冲池管理器中获取索引根页面，并尝试加载根节点ID。
  auto page = reinterpret_cast<IndexRootsPage *> (buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if(page->GetRootId(index_id_, &root_page_id_) == 0){
    // 如果根节点ID不存在，设置为无效值。
    root_page_id_ = INVALID_PAGE_ID;
  }

  // 释放根页面和根节点的锁定。
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}


void BPlusTree::Destroy(page_id_t current_page_id) {
  // 如果B+树为空，直接返回。
  if(IsEmpty())
    return;

  // 如果当前页面ID无效，使用根页面ID并更新根页面ID。
  if(current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(2); // 2表示移除根节点。
  }

  // 从缓冲池中获取当前页面的数据并转换为B+树页面。
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());

  // 如果当前页面是内部节点，递归删除其子节点。
  if(!page->IsLeafPage()) {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    for(int i = page->GetSize() - 1; i >= 0; --i) {
      Destroy(inner->ValueAt(i));
    }
  }

  // 删除当前页面并释放缓冲池的锁定。
  buffer_pool_manager_->DeletePage(current_page_id);
  buffer_pool_manager_->UnpinPage(current_page_id, false);
}


/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  // 判断根节点ID是否为无效值，若是则返回true，表示B+树为空。
  if(root_page_id_ == INVALID_PAGE_ID) return true;
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *Txn) {
  // 如果B+树为空，返回false。
  if(IsEmpty()) return false;

  // 找到包含该键的叶子页面。
  auto *page = FindLeafPage(key, root_page_id_, false);

  // 获取叶子页面的数据。
  auto node = reinterpret_cast<LeafPage  *> (page->GetData());

  // 在叶子页面中查找该键的值。
  RowId val;
  if(node->Lookup(key, val, processor_)){
    // 如果找到，添加到结果向量中。
    result.push_back(val);

    // 释放叶子页面的锁定，并返回true。
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return true;
  }

  // 如果未找到，释放叶子页面的锁定，并返回false。
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  // 如果B+树为空，创建新的树并插入键值对。
  if(IsEmpty()) {
    StartNewTree(key, value);
    return true;
  } else {
    // 否则，将键值对插入到合适的叶子页面中。
    return InsertIntoLeaf(key, value, transaction);
  }
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // 从缓冲池管理器请求一个新的页面，并将其作为根页面
  auto new_page = buffer_pool_manager_->NewPage(root_page_id_);
  // 如果无法分配新的页面，则抛出错误（内存不足）
  ASSERT(new_page != nullptr, "out of memory");
  // 将新页面的数据部分转换为叶子节点类型
  auto new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  // 初始化叶子节点，设置其ID、无效的父节点ID、键的大小和最大尺寸
  new_node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  // 将第一个键值对插入到新创建的叶子节点中
  new_node->Insert(key, value, processor_);
  // 解锁根页面，同时标记该页面已被修改
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  // 更新根页面ID信息
  UpdateRootPageId(1); // 1表示插入新的根页面
}


/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  // 查找包含给定键的叶子页面
  auto page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false)->GetData());
  // 检查该键是否已存在于叶子节点中
  RowId val;
  if (page->Lookup(key, val, processor_)) {
    // 如果键存在，释放页面锁并返回 false 表示插入失败（键重复）
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  } else {
    // 如果键不存在，插入新的键值对到叶子节点中
    page->Insert(key, value, processor_);
    // 检查插入后叶子节点是否超出其最大容量
    if (page->GetSize() >= page->GetMaxSize()) {
      // 如果超出容量，执行页面分裂操作
      auto new_page = Split(page, transaction);
      // 设置新叶子节点的下一个节点 ID 为当前叶子节点的下一个节点 ID
      new_page->SetNextPageId(page->GetNextPageId());
      // 更新当前叶子节点的下一个节点 ID 为新分裂出的叶子节点的 ID
      page->SetNextPageId(new_page->GetPageId());
      // 将分裂后的新键插入到父节点中
      InsertIntoParent(page, new_page->KeyAt(0), new_page, transaction);
      // 释放新页面的锁定，并标记其已被修改
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    }
    // 释放当前叶子节点的锁定，并标记其已被修改
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }
}


/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  auto page = buffer_pool_manager_ ->NewPage(new_page_id);
  ASSERT(page != nullptr, "out of memory");
  auto new_node = reinterpret_cast<InternalPage *>(page);
  new_node ->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  auto page = buffer_pool_manager_->NewPage(new_page_id);
  ASSERT(page != nullptr, "out of memory");
  auto new_node = reinterpret_cast<LeafPage *>(page);
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(),node->GetMaxSize());
  node->MoveHalfTo(new_node);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Txn *transaction) {
  if(old_node -> IsRootPage()){
    auto page = buffer_pool_manager_ ->NewPage(root_page_id_);
    ASSERT(page != nullptr, "out of memory");
    auto slib = reinterpret_cast <InternalPage *> (page -> GetData());
    slib ->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(),internal_max_size_);
    slib ->PopulateNewRoot(old_node -> GetPageId(), key, new_node -> GetPageId());
    old_node ->SetParentPageId(root_page_id_);
    new_node ->SetParentPageId(root_page_id_);
    UpdateRootPageId(0);
    buffer_pool_manager_ ->UnpinPage(root_page_id_, true);
  }
  else{
    page_id_t par_id = old_node -> GetParentPageId();
    auto fa = reinterpret_cast<InternalPage *> (buffer_pool_manager_ ->FetchPage(par_id) -> GetData());
    fa ->InsertNodeAfter(old_node -> GetPageId(), key, new_node -> GetPageId());
    if(fa -> GetSize() >= fa -> GetMaxSize()){
      auto new_split = Split(fa, transaction);
      InsertIntoParent(fa, new_split ->KeyAt(0), new_split, transaction);
      buffer_pool_manager_ ->UnpinPage(new_split -> GetPageId(), true);
    }
    buffer_pool_manager_ ->UnpinPage(par_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty())return;
  auto leaf_node = reinterpret_cast<LeafPage *> (FindLeafPage(key, root_page_id_, false) -> GetData());
  int siz = leaf_node -> GetSize();
  int siz2 = leaf_node ->RemoveAndDeleteRecord(key, processor_);
  if(siz > siz2){
    CoalesceOrRedistribute(leaf_node, transaction);
    buffer_pool_manager_ ->UnpinPage(leaf_node -> GetPageId(), true);
  }
  else{// Failed delete
    buffer_pool_manager_ ->UnpinPage(leaf_node -> GetPageId(), false);
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if(node->IsRootPage()) {
    return AdjustRoot(node);
  }
  else if (node->GetSize() >= node->GetMinSize()){
    return false;
  }
  else {
    bool res = false;
    page_id_t par_id = node ->GetParentPageId();
    auto par = reinterpret_cast <InternalPage *> (buffer_pool_manager_->FetchPage(par_id) -> GetData());
    int index = par ->ValueIndex(node ->GetPageId());
    int silb;
    if(index == 0)silb = 1;
    else silb = index - 1;
    page_id_t silb_id = par ->ValueAt(silb);
    auto sib_node = reinterpret_cast<N *> (buffer_pool_manager_ ->FetchPage(silb_id) -> GetData());
    if(node->GetSize() + sib_node ->GetSize() >= node->GetMaxSize()) {
      Redistribute(sib_node, node, index);
      res = false;
    } else {
      Coalesce(sib_node, node, par, index);
      res = true;
    }
    buffer_pool_manager_->UnpinPage(par_id, true);
    buffer_pool_manager_->UnpinPage(silb_id, true);
    return res;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int silb;
  if(index == 0)silb = 1;
  else silb = index - 1;
  if(index < silb) {
    neighbor_node ->MoveAllTo(node);
    node ->SetNextPageId(neighbor_node->GetNextPageId());
    parent ->Remove(silb);
  } else {
    node ->MoveAllTo(neighbor_node);
    neighbor_node ->SetNextPageId(node->GetNextPageId());
    parent ->Remove(index);
  }
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  int silb;
  if(index == 0)silb = 1;
  else silb = index - 1;
  if(index < silb) {
    neighbor_node->MoveAllTo(node, parent->KeyAt(silb), buffer_pool_manager_);
    parent->Remove(silb);
  } else {
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
    parent->Remove(index);
  }
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto par = reinterpret_cast<InternalPage *> (buffer_pool_manager_ ->FetchPage(node -> GetParentPageId()) -> GetData());
  if(index == 0){
    neighbor_node ->MoveFirstToEndOf(node);
    par ->SetKeyAt(index + 1, neighbor_node ->KeyAt(0));
  }
  else{
    neighbor_node ->MoveLastToFrontOf(node);
    par ->SetKeyAt(index, node ->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(par -> GetPageId(), true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  auto par = reinterpret_cast<InternalPage *> (buffer_pool_manager_ ->FetchPage(node -> GetParentPageId()) -> GetData());
  if(index == 0){
    neighbor_node ->MoveFirstToEndOf(node, par ->KeyAt(1), buffer_pool_manager_);
    par ->SetKeyAt(index + 1, neighbor_node ->KeyAt(0));
  }
  else{
    neighbor_node ->MoveLastToFrontOf(node, par ->KeyAt(index), buffer_pool_manager_);
    par ->SetKeyAt(index, node ->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(par -> GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node ->IsLeafPage() && old_root_node ->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  else if (!old_root_node ->IsLeafPage() && old_root_node ->GetSize() == 1) {
    auto root = reinterpret_cast<InternalPage *> (old_root_node);
    page_id_t child_id = root -> RemoveAndReturnOnlyChild();
    auto child = reinterpret_cast<BPlusTreePage *> (buffer_pool_manager_ ->FetchPage(child_id) -> GetData());
    child ->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = child_id;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto * page = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, root_page_id_, true)->GetData());
  page_id_t page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto * page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false)->GetData());
  int index = page ->KeyIndex(key, processor_);
  page_id_t page_id = page ->GetPageId();
  buffer_pool_manager_ ->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  if(root_page_id_ == INVALID_PAGE_ID)return IndexIterator();
  auto root = reinterpret_cast<BPlusTreePage *> (buffer_pool_manager_ ->FetchPage(root_page_id_) -> GetData());
  if(root -> IsLeafPage()){
    auto node = reinterpret_cast<LeafPage *>(root);
    auto endleaf = FindLeafPage(node ->KeyAt(node -> GetSize() - 1),root_page_id_);
    buffer_pool_manager_ ->UnpinPage(root_page_id_, false);
    return IndexIterator(endleaf -> GetPageId(), buffer_pool_manager_, node -> GetSize() );
  }
  else{
    auto node = reinterpret_cast<InternalPage *>(root);
    auto endleaf = FindLeafPage(node ->KeyAt(node -> GetSize() - 1),root_page_id_);
    buffer_pool_manager_ ->UnpinPage(root_page_id_, false);
    return IndexIterator(endleaf -> GetPageId(), buffer_pool_manager_, node -> GetSize() );
  }
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  auto page = reinterpret_cast<BPlusTreePage *> (buffer_pool_manager_ ->FetchPage(page_id) -> GetData());
  while(page -> IsLeafPage() != 1){
    auto nxt = reinterpret_cast<InternalPage *> (page);
    page_id_t nxt_id;
    if(leftMost)nxt_id = nxt ->ValueAt(0);
    else nxt_id = nxt ->Lookup(key, processor_);
    auto nxt_page = reinterpret_cast<BPlusTreePage *> (buffer_pool_manager_ ->FetchPage(nxt_id) -> GetData());
    buffer_pool_manager_ ->UnpinPage(page -> GetPageId(), false);
    page = nxt_page;
  }
  return reinterpret_cast<Page *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto page = reinterpret_cast<IndexRootsPage *> (buffer_pool_manager_ ->FetchPage(INDEX_ROOTS_PAGE_ID) -> GetData());\
  if(insert_record == 0){
    page ->Update(index_id_, root_page_id_);
  }
  else if(insert_record == 1){
    page ->Insert(index_id_, root_page_id_);
  }
  else{
    page ->Delete(index_id_);
  }
  buffer_pool_manager_ ->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}