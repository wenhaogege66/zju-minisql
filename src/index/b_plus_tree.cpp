#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t idx_id, BufferPoolManager *buf_pool_mgr, const KeyManager &key_mgr,
                     int max_leaf_size, int max_internal_size)
    : index_id_(idx_id),
      buffer_pool_manager_(buf_pool_mgr),
      processor_(key_mgr),
      leaf_max_size_(max_leaf_size),
      internal_max_size_(max_internal_size) {

  // Check if the sizes for leaf or internal nodes are not defined
  if(max_leaf_size == UNDEFINED_SIZE || max_internal_size == UNDEFINED_SIZE){
    // Default leaf node size calculation
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;

    // Default internal node size calculation
    internal_max_size_ =  (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;

    // Ensure minimum size for internal nodes
    if(internal_max_size_ < 2) {
      internal_max_size_ = 2;
      leaf_max_size_ = 2;
    }
  }

  // Load the root page ID from the index roots page
  auto root_page = reinterpret_cast<IndexRootsPage *> (buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if(root_page->GetRootId(index_id_, &root_page_id_) == 0){
    // Root ID does not exist, set to invalid
    root_page_id_ = INVALID_PAGE_ID;
  }

  // Unpin the root and index roots page
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  // Check if the B+ tree is empty
  if(IsEmpty())
    return;

  // If the current page ID is invalid, use the root page ID
  if(current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(2); // 2 signifies root removal
  }

  // Fetch and convert the current page to a B+ tree page
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());

  // If it's an internal node, recursively destroy its child nodes
  if(!page->IsLeafPage()) {
    auto *internal_node = reinterpret_cast<InternalPage *>(page);
    for(int i = page->GetSize() - 1; i >= 0; --i) {
      Destroy(internal_node->ValueAt(i));
    }
  }

  // Delete the current page and unpin it from the buffer pool
  buffer_pool_manager_->DeletePage(current_page_id);
  buffer_pool_manager_->UnpinPage(current_page_id, false);
}

// Helper function to check if the B+ tree is empty
bool BPlusTree::IsEmpty() const {
  // Return true if the root page ID is invalid, indicating the B+ tree is empty
  return (root_page_id_ == INVALID_PAGE_ID);
}

// Search and retrieve the value associated with the given key
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  // Return false if the B+ tree is empty
  if(IsEmpty()) return false;

  // Find the leaf page containing the key
  auto *page = FindLeafPage(key, root_page_id_, false);

  // Get the leaf page data
  auto node = reinterpret_cast<LeafPage *>(page->GetData());

  // Lookup the key in the leaf page
  RowId value;
  if(node->Lookup(key, value, processor_)){
    // If found, add to the result and return true
    result.push_back(value);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    return true;
  }

  // If not found, unpin the page and return false
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  return false;
}

// Insert a key-value pair into the B+ tree
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  // If the tree is empty, start a new tree
  if(IsEmpty()) {
    StartNewTree(key, value);
    return true;
  } else {
    // Otherwise, insert into the appropriate leaf page
    return InsertIntoLeaf(key, value, transaction);
  }
}

// Start a new tree by inserting the first key-value pair
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // Request a new page from the buffer pool manager
  auto new_page = buffer_pool_manager_->NewPage(root_page_id_);
  // Check for memory allocation failure
  ASSERT(new_page != nullptr, "Out of memory");

  // Initialize the new page as a leaf node
  auto new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  new_node->Insert(key, value, processor_);

  // Unpin the root page and update the root page ID
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId(1); // 1 indicates new root insertion
}

// Insert a key-value pair into a leaf page
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  // Find the leaf page where the key should be inserted
  auto page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false)->GetData());
  RowId existing_value;
  if (page->Lookup(key, existing_value, processor_)) {
    // If the key exists, return false
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  } else {
    // Otherwise, insert the key-value pair into the leaf page
    page->Insert(key, value, processor_);

    // If the leaf page is full, split the page
    if (page->GetSize() >= page->GetMaxSize()) {
      auto new_page = Split(page, transaction);
      new_page->SetNextPageId(page->GetNextPageId());
      page->SetNextPageId(new_page->GetPageId());
      InsertIntoParent(page, new_page->KeyAt(0), new_page, transaction);
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    }

    // Unpin the current leaf page and return true
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }
}

// Split a full internal page and return the new page
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  auto page = buffer_pool_manager_->NewPage(new_page_id);
  ASSERT(page != nullptr, "Out of memory");

  auto new_node = reinterpret_cast<InternalPage *>(page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  return new_node;
}

// Split a full leaf page and return the new page
BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  auto page = buffer_pool_manager_->NewPage(new_page_id);
  ASSERT(page != nullptr, "Out of memory");

  auto new_node = reinterpret_cast<LeafPage *>(page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
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
  if(old_node->IsRootPage()){
    // Create a new root page and populate it with old_node and new_node.
    auto new_root_page = buffer_pool_manager_->NewPage(root_page_id_);
    ASSERT(new_root_page != nullptr, "out of memory");
    auto new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    // Update parent pointers and root page ID.
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(0);

    // Unpin the new root page.
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    // Insert new_node into the parent of old_node.
    page_id_t parent_id = old_node->GetParentPageId();
    auto parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
    parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    // Check if parent node exceeds maximum size and needs splitting.
    if(parent_node->GetSize() >= parent_node->GetMaxSize()){
      auto new_split = Split(parent_node, transaction);
      InsertIntoParent(parent_node, new_split->KeyAt(0), new_split, transaction);
      buffer_pool_manager_->UnpinPage(new_split->GetPageId(), true);
    }

    // Unpin the parent page after modification.
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty()) return; // If tree is empty, return immediately.

  // Find the leaf node containing the key for deletion.
  auto leaf_node = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false)->GetData());
  int original_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, processor_);

  if(original_size > new_size) {
    // Successful deletion, attempt coalesce or redistribution.
    CoalesceOrRedistribute(leaf_node, transaction);
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  } else {
    // Deletion failed, simply unpin the leaf node without marking modifications.
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  }
}

template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if(node->IsRootPage()) {
    return AdjustRoot(node);
  } else if(node->GetSize() >= node->GetMinSize()) {
    return false; // No further action needed if node size is within limits.
  } else {
    // Find parent and sibling nodes.
    page_id_t parent_id = node->GetParentPageId();
    auto parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_id)->GetData());
    int node_index = parent_node->ValueIndex(node->GetPageId());
    int sibling_index = (node_index == 0) ? 1 : node_index - 1;
    page_id_t sibling_id = parent_node->ValueAt(sibling_index);
    auto sibling_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(sibling_id)->GetData());

    // Decide whether to redistribute or coalesce based on sizes.
    bool res = false;
    if(node->GetSize() + sibling_node->GetSize() >= node->GetMaxSize()) {
      Redistribute(sibling_node, node, sibling_index);
      res = false; // No deletion needed after redistribution.
    } else {
      Coalesce(sibling_node, node, parent_node, sibling_index, transaction);
      res = true; // Deletion needed after coalescing.
    }

    // Unpin pages after operations.
    buffer_pool_manager_->UnpinPage(parent_id, true);
    buffer_pool_manager_->UnpinPage(sibling_id, true);
    return res;
  }
}

bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  // Determine which sibling to merge with based on index.
  int sibling_index = (index == 0) ? 1 : index - 1;

  if(index < sibling_index) {
    // Merge node into neighbor_node.
    neighbor_node->MoveAllTo(node);
    node->SetNextPageId(neighbor_node->GetNextPageId());
    parent->Remove(sibling_index);
  } else {
    // Merge neighbor_node into node.
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    parent->Remove(index);
  }

  // Check if parent node needs further coalescing or redistribution.
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  // Determine which sibling to merge with based on index.
  int sibling_index = (index == 0) ? 1 : index - 1;

  if(index < sibling_index) {
    // Merge node into neighbor_node.
    neighbor_node->MoveAllTo(node, parent->KeyAt(sibling_index), buffer_pool_manager_);
    parent->Remove(sibling_index);
  } else {
    // Merge neighbor_node into node.
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
    parent->Remove(index);
  }

  // Check if parent node needs further coalescing or redistribution.
  return CoalesceOrRedistribute(parent, transaction);
}

void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  // Fetch parent page for adjustments.
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());

  if(index == 0) {
    // Move first entry of neighbor_node to end of node.
    neighbor_node->MoveFirstToEndOf(node);
    parent->SetKeyAt(index + 1, neighbor_node->KeyAt(0));
  } else {
    // Move last entry of neighbor_node to front of node.
    neighbor_node->MoveLastToFrontOf(node);
    parent->SetKeyAt(index, node->KeyAt(0));
  }

  // Unpin parent page after modifications.
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  // Fetch parent page for adjustments.
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());

  if(index == 0) {
    // Move first entry of neighbor_node to end of node.
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1), buffer_pool_manager_);
    parent->SetKeyAt(index + 1, neighbor_node->KeyAt(0));
  } else {
    // Move last entry of neighbor_node to front of node.
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
    parent->SetKeyAt(index, node->KeyAt(0));
  }

  // Unpin parent page after modifications.
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
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