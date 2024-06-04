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
}

void BPlusTree::Destroy(page_id_t current_page_id) {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  //transaction用来干嘛？
  if(IsEmpty())
    return false;
  Page * leaf_page = FindLeafPage(key);
  auto leaf = reinterpret_cast<BPlusTreeLeafPage * >(leaf_page->GetData());
  if (leaf != nullptr) {
    RowId value;
//     * For the given key, check to see whether it exists in the leaf page. If it
    // * does, then store its corresponding value in input "value" and return true.
    // * If the key does not exist, then return false
    //leaf_page的lookup
    if (leaf->Lookup(key, value, processor_)) {
      result.push_back(value);
    }
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
    return true;
  }
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
  if(IsEmpty())//空树
  {
    StartNewTree(key,value);
    return true;
  }
  else
  {//insert into leaf page.
    if(InsertIntoLeaf(key, value, transaction))
    {//插入成功
      return true;
    }
  }
  return false;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  Page* new_page = buffer_pool_manager_->NewPage(root_page_id_);
  if(new_page == nullptr)
  {
    exception err = (const exception &)"out of memory";
    throw exception(err);
  }

  // update b+ tree's root page id
  Page* index_roots_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  if (index_roots_page == nullptr) {
    exception err = (const exception &)"out of memory";
    throw exception(err);
  }
  auto* index_roots = reinterpret_cast<IndexRootsPage *>(index_roots_page->GetData());
  index_roots->Insert(index_id_, root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);

  //insert entry directly into leaf page
  auto root = reinterpret_cast<BPlusTreeLeafPage *>(new_page->GetData());
  root->Init(root_page_id_,INVALID_PAGE_ID,UNDEFINED_SIZE,leaf_max_size_);
  //（根）叶节点无父节点，初始时key_size = 0
  root->Insert(key,value,processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
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
  //find the right leaf page as insertion target
    Page* leaf_page =  FindLeafPage(key);
    if(leaf_page == nullptr)
    {
      exception err = (const exception &)"don't get the leaf_page";
      throw exception(err);
    }
    auto leaf = reinterpret_cast<BPlusTreeLeafPage *>(leaf_page->GetData());
    // look through leaf page to see whether insert key exist or not
    RowId val = value;
    //lookup用的不是const，得重新启一个
    if(leaf->Lookup(key, val, processor_))
    {//已经有了
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
      return false;
    }
    else
    {
      if(leaf->GetSize() >= leaf->GetMaxSize())
      {//此时插入需要分裂（按理来说最多到等，此时如果大于说明之前有方法写错了）
        auto sib_leaf = Split(leaf,transaction);
        if(sib_leaf == nullptr)
        {
          exception err = (const exception &)"don't get the sib_leaf";
          throw exception(err);
        }
        if(processor_.CompareKeys(key,sib_leaf->KeyAt(0)) < 0)
        {//插左边
          leaf->Insert(key,value,processor_);
        }
        else
          sib_leaf->Insert(key,value,processor_);
        //经典链表操作
        sib_leaf->SetNextPageId(leaf->GetNextPageId());
        leaf->SetNextPageId(sib_leaf->GetPageId());
        // * Insert key & value pair into internal page after split
        InsertIntoParent(leaf,sib_leaf->KeyAt(0),sib_leaf,transaction);
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
        buffer_pool_manager_->UnpinPage(sib_leaf->GetPageId(),true);
        return true;
      }
      else
      {//直接插入
        leaf->Insert(key,value,processor_);
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
        return true;
      }
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
  BPlusTreeInternalPage* sib_node;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    exception err = (const exception &)"out of memory";
    throw exception(err);
  }
  sib_node = reinterpret_cast<BPlusTreeInternalPage *>(new_page->GetData());
  sib_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  node->MoveHalfTo(sib_node, buffer_pool_manager_);
  return sib_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  BPlusTreeLeafPage* sib_node;
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    exception err = (const exception &)"out of memory";
    throw exception(err);
  }
  sib_node = reinterpret_cast<BPlusTreeLeafPage *>(new_page->GetData());
  sib_node->Init(new_page_id, INVALID_PAGE_ID,leaf_max_size_ );
  node->MoveHalfTo(sib_node);
  return sib_node;
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
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  //目前只有一层，分裂后需要新的根
  if (old_node->IsRootPage())
  {
    //插到根上
    Page* page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
      exception err = (const exception &)"out of memory";
      throw exception(err);
    }
    //搞个新根
    auto root = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    // * Call this method everytime root page id is changed.
    // insert_record:default value is false. When set to true,insert a record <index_name, current_page_id> into header page instead of updating it.
    UpdateRootPageId(false);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  else
  {
    page_id_t parent_page_id = old_node->GetParentPageId();
    Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    if (parent_page == nullptr) {
      exception err = (const exception &)"can't find parent_page";
      throw exception(err);
    }
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
    if (parent_node->GetSize() < parent_node->GetMaxSize()) {
      //不需要迭代插入
      //  * Insert new_key & new_value pair right after the pair with its value == old_value
      //新的值将在左部分节点值的后面
      parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(parent_page_id);
      //！！！！！！！！！！！！！！！！！！！！！！！注意：这里多半是需要unpin的，之后回来看
      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    }
    else
    {
      // 中间节点向上分裂
      auto sibling_page = Split(parent_node,transaction);
      if (processor_.CompareKeys(key, sibling_page->KeyAt(0)) < 0)
      {
        new_node->SetParentPageId(parent_node->GetPageId());
        parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      }
      else if (processor_.CompareKeys(key, sibling_page->KeyAt(0)) == 0)
      {
        new_node->SetParentPageId(sibling_page->GetPageId());
        sibling_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      }
      else
      {
        new_node->SetParentPageId(sibling_page->GetPageId());
        sibling_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        old_node->SetParentPageId(sibling_page->GetPageId());
      }

      //      buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
      InsertIntoParent(parent_node, sibling_page->KeyAt(0), sibling_page);
      buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
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
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  return false;
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
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  return false;
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
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
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
  return IndexIterator();
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  return IndexIterator();
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator();
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
  //page_id是干嘛用的
  if (IsEmpty())//空则返回
    return nullptr;
  //从根开始遍历
  auto root_node = buffer_pool_manager_->FetchPage(root_page_id_);
  while (!reinterpret_cast<BPlusTreePage *>(root_node->GetData())->IsLeafPage()) {
    //不是叶子则是内部
    auto internal_root_node = reinterpret_cast<BPlusTreeInternalPage *>(root_node->GetData());
    page_id_t child_page_id;
    auto cur_page_id = internal_root_node->GetPageId();
    if (leftMost)
    {//find the left most leaf page
      child_page_id = internal_root_node->ValueAt(0);
    }
    else
    {
//      * Find and return the child pointer(page_id) which points to the child page that contains input "key"
//      * Start the search from the second key(the first key should always be invalid)
      child_page_id = internal_root_node->Lookup(key, processor_);
    }
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    root_node = buffer_pool_manager_->FetchPage(child_page_id);
  }
  // 是叶子节点,退出循环
  return root_node;
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page is defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
}

/**
 * This method is used for debug only, You don't need to modify
 */
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