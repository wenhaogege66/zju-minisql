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
  Page* root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto index_root = reinterpret_cast<IndexRootsPage *>(root_page->GetData());
  if(!index_root->GetRootId(index_id, &root_page_id_)) {
    root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if(IsEmpty())
    return;
  if(current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(2);
  }
  auto page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  if(!page->IsLeafPage()) {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    for(int i = page->GetSize() - 1; i >= 0; --i) {
      Destroy(inner->ValueAt(i));
    }
  }
  buffer_pool_manager_->DeletePage(page->GetPageId());
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
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
  // 查找合适的叶子页面作为插入目标
  Page* leaf_page = FindLeafPage(key);
  if(leaf_page == nullptr) {
    // 如果找不到叶子页面，抛出异常
    exception err = (const exception &)"don't get the leaf_page";
    throw exception(err);
  }
  auto leaf = reinterpret_cast<BPlusTreeLeafPage *>(leaf_page->GetData());
  // 检查叶子页面是否存在该键
  RowId val = value;
  // Lookup 不是 const，因为可能需要修改值
  if(leaf->Lookup(key, val, processor_)) {
    // 如果键已存在，则不执行插入操作，并返回 false
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
    return false;
  } else {
    // 检查叶子页面是否已满
    if(leaf->GetSize() >= leaf->GetMaxSize()) {
      // 如果页面已满，则需要分裂页面
      auto sib_leaf = Split(leaf,transaction);
//      if(sib_leaf == nullptr) {
//        // 如果无法分裂页面，抛出异常
//        exception err = (const exception &)"don't get the sib_leaf";
//        throw exception(err);
//      }
      // 确定插入哪个分裂后的页面
      if(processor_.CompareKeys(key,sib_leaf->KeyAt(0)) < 0) {
        // 插入左边页面
        leaf->Insert(key,value,processor_);
      } else {
        // 插入右边新创建的页面
        sib_leaf->Insert(key,value,processor_);
      }
      // 更新页面链接
      sib_leaf->SetNextPageId(leaf->GetNextPageId());
      leaf->SetNextPageId(sib_leaf->GetPageId());
      // 插入新键和值到父页面
      InsertIntoParent(leaf,sib_leaf->KeyAt(0),sib_leaf,transaction);
      // 解除页面锁定
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sib_leaf->GetPageId(),true);
      return true;
    } else {
      // 直接在叶子页面中插入键值对
      leaf->Insert(key,value,processor_);
      // 解除页面锁定
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
  // 请求一个新的页面，如果返回nullptr，表示没有可用的内存页
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    // 如果无法获取新页面，抛出"内存不足"异常
    exception err = (const exception &)"out of memory";
    throw exception(err);
  }
  // 将新页面的数据区域转换为BPlusTreeInternalPage类型
  sib_node = reinterpret_cast<BPlusTreeInternalPage *>(new_page->GetData());
  // 初始化新页面，INVALID_PAGE_ID表示新页面没有父页面
  sib_node->Init(new_page_id, INVALID_PAGE_ID, internal_max_size_);
  // 将一半的键值对从原始页面移动到新页面
  node->MoveHalfTo(sib_node, buffer_pool_manager_);
  // 返回新创建的内部页面
  return sib_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  BPlusTreeLeafPage* sib_leaf;
  // 请求一个新的页面，如果返回nullptr，表示没有可用的内存页
  Page* new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) {
    // 如果无法获取新页面，抛出"内存不足"异常
    exception err = (const exception &)"out of memory";
    throw exception(err);
  }
  // 将新页面的数据区域转换为BPlusTreeLeafPage类型
  sib_leaf = reinterpret_cast<BPlusTreeLeafPage *>(new_page->GetData());
  // 初始化新页面，INVALID_PAGE_ID表示新页面没有父页面
  sib_leaf->Init(new_page_id, INVALID_PAGE_ID, leaf_max_size_);
  // 将一半的键值对从原始页面移动到新页面
  node->MoveHalfTo(sib_leaf);
  // 返回新创建的叶子页面
  return sib_leaf;
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
  // 如果old_node是根页面，需要创建一个新的根页面
  if (old_node->IsRootPage()) {
    // 为新根页面请求一个新页面
    Page* page = buffer_pool_manager_->NewPage(root_page_id_);
    if (page == nullptr) {
      exception err = (const exception &)"out of memory";
      throw exception(err);
    }
    // 将新页面数据区域转换为BPlusTreeInternalPage类型
    auto root = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    // 使用old_node和new_node的页面ID填充新根页面
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 更新old_node和new_node的父页面ID
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    // 更新索引根页面ID
    UpdateRootPageId(false);
    // 解除新根页面的锁定
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  } else {
    // 如果old_node不是根页面，找到old_node的父页面
    page_id_t parent_page_id = old_node->GetParentPageId();
    Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    if (parent_page == nullptr) {
      exception err = (const exception &)"can't find parent_page";
      throw exception(err);
    }
    // 将父页面数据区域转换为BPlusTreeInternalPage类型
    auto parent_node = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
    // 如果父页面未满，直接插入新键和新节点的页面ID
    if (parent_node->GetSize() < parent_node->GetMaxSize()) {
      parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      // 更新new_node的父页面ID
      new_node->SetParentPageId(parent_page_id);
      // 解除父页面的锁定
      buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    } else {
      // 如果父页面已满，需要分裂父页面
      // Split函数将处理父页面的分裂，并返回新创建的兄弟节点
      auto sibling_node = Split(parent_node, transaction);
      if (processor_.CompareKeys(key, sibling_node->KeyAt(0)) < 0) {
        // 将新键和新节点的页面ID插入到父页面的合适位置
        parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(parent_node->GetPageId());
      } else {
        // 将新键和新节点的页面ID插入到分裂后的新兄弟节点
        sibling_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(sibling_node->GetPageId());
      }
      // 递归地调用InsertIntoParent处理父页面的分裂
      InsertIntoParent(parent_node, sibling_node->KeyAt(0), sibling_node, transaction);
      // 解除新兄弟节点和父页面的锁定
      buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
    }
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
//maybe wrong
//void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
//  if(IsEmpty())return;
//  auto leaf_node = reinterpret_cast<LeafPage *> (FindLeafPage(key, root_page_id_, false) -> GetData());
//  int siz = leaf_node -> GetSize();
//  int siz2 = leaf_node ->RemoveAndDeleteRecord(key, processor_);
//  if(siz > siz2){
//    CoalesceOrRedistribute(leaf_node, transaction);
//    buffer_pool_manager_ ->UnpinPage(leaf_node -> GetPageId(), true);
//  }
//  else{// Failed delete
//    buffer_pool_manager_ ->UnpinPage(leaf_node -> GetPageId(), false);
//  }
//}

void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty()) return;
  Page* dest_page = FindLeafPage(key, INVALID_PAGE_ID,false);
  auto * leaf = reinterpret_cast<LeafPage *>(dest_page->GetData());
  int deletekey_index = leaf->KeyIndex(key, processor_);
  int pre_size = leaf->GetSize();
  int new_size = leaf->RemoveAndDeleteRecord(key, processor_);
  if(new_size < leaf->GetMinSize()) {
    // 如果新的页面大小小于最小允许大小，需要合并或再分配
    CoalesceOrRedistribute(leaf, transaction);
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  } else if(new_size == pre_size){
    //啥都没干
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
  }

  // 检查并更新父节点中的键
//  Page *parent_page = buffer_pool_manager_->FetchPage(leaf->GetParentPageId());
//  if (parent_page != nullptr) {
//    auto *parent = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
//    // 找到要删除键的索引位置
//    if (!leaf->IsRootPage() && deletekey_index == 0) {
//      // 删除父节点中的键和子页面ID
////      parent->Remove(parent->Lookup(key,processor_));//ValueIndex
//      // 如果父节点大小低于最小限制，需要合并或分裂
//      if (parent->GetSize() < parent->GetMinSize()) {
//        // 递归地检查和处理父节点的合并或分裂
//        CoalesceOrRedistribute(parent, transaction);
//      }
//    }
//    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
//  }
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
  // 寻找输入页面的兄弟页面的ID
  int sibling_page_id = 0;
  // 如果节点是根页面，调用AdjustRoot进行调整
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  // 获取父页面
  Page* parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  if (parent_page == nullptr) {
    // 如果无法获取父页面，抛出"内存不足"异常
    exception err = (const exception &)"out of memory";
    throw exception(err);
  }
  // 将父页面数据区域转换为BPlusTreeInternalPage类型
  auto parent = reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  // 获取node在父页面中的索引位置
  int node_index = parent->ValueIndex(node->GetPageId());
  if (node_index == 0) {
    // 如果node是父页面中的第一个子节点，兄弟节点在它之后
    sibling_page_id = parent->ValueAt(node_index + 1);
  } else {
    // 否则，兄弟节点在它之前
    sibling_page_id = parent->ValueAt(node_index - 1);
  }
  // 获取兄弟页面
  auto sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);
  if (sibling_page == nullptr) {
    // 如果无法获取兄弟页面，抛出"内存不足"异常
    exception err = (const exception &)"out of memory";
    throw exception(err);
  }
  bool if_delete = false; // 用于标记目标页面是否应该被删除
  bool if_delete_parent = false; // 用于标记父页面是否应该被删除
  // 如果兄弟页面的大小加上当前页面的大小大于页面的最大大小，则进行再分配
  // 将兄弟页面数据区域转换为N类型
  auto sibling = reinterpret_cast<N *>(sibling_page);
  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    Redistribute(sibling, node, node_index);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
  } else {
    // 否则，进行合并
    if (node_index == 0) {
      // 如果node是第一个子节点，node不会被删除，但兄弟页面可能会被删除
      if_delete = false;
      if_delete_parent = Coalesce(node, sibling, parent, 1, transaction);
    } else {
      // 如果node不是第一个子节点，兄弟页面不会被删除，node可能会被删除
      if_delete = true;
      if_delete_parent = Coalesce(sibling, node, parent, node_index, transaction);
    }
    // 解除兄弟页面的锁定
    buffer_pool_manager_->UnpinPage(sibling_page_id, true);
    // 如果合并后兄弟页面不为空，则删除它
    if (!if_delete) {
      buffer_pool_manager_->DeletePage(sibling_page_id);
    }
  }
  // 解除父页面的锁定
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  // 如果父页面需要被删除，则删除它
  if (if_delete_parent) {
    buffer_pool_manager_->DeletePage(parent->GetPageId());
  }
  // 返回if_delete，表示目标页面是否应该被删除
  return if_delete;
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
  // 计算兄弟节点的索引
  int sib_index = (index == 0) ? 1 : index - 1;

  // 如果待合并的节点在左侧，则将兄弟节点的内容移动到当前节点
  if (index < sib_index) {
    neighbor_node->MoveAllTo(node); // 将邻居节点的所有内容移动到当前节点
    node->SetNextPageId(neighbor_node->GetNextPageId()); // 更新当前节点的下一个页面ID
    parent->Remove(sib_index); // 从父页面中移除邻居节点的条目
  } else {
    // 如果待合并的节点在右侧，则将当前节点的内容移动到兄弟节点
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
    parent->Remove(index); // 从父页面中移除当前节点的条目
  }

  // 递归地处理父页面的合并或再分配
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  // 计算兄弟节点的索引
  int sib_index = (index == 0) ? 1 : index - 1;

  // 如果待合并的节点在左侧，则将兄弟节点的内容移动到当前节点
  if (index < sib_index) {
    neighbor_node->MoveAllTo(node, parent->KeyAt(sib_index), buffer_pool_manager_); // 移动内容并更新子页面键
    parent->Remove(sib_index); // 从父页面中移除邻居节点的条目
  } else {
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
    parent->Remove(index); // 从父页面中移除当前节点的条目
  }

  // 递归地处理父页面的合并或再分配
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
  // 获取父页面指针
  auto *parent = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  // 如果index为0，将邻居节点的第一个键值对移动到当前节点的末尾
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    // 更新父节点中新插入键对应的值
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else { // 否则，将邻居节点的最后一个键值对移动到当前节点的开头
    neighbor_node->MoveLastToFrontOf(node);
    // 更新父节点中原来在当前节点的第一个键对应的新值
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  // 解锁父页面
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  // 获取父页面指针
  auto *parent = reinterpret_cast<BPlusTree::InternalPage*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  // 如果index为0，将邻居节点的第一个键值对移动到当前节点的末尾
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(1), buffer_pool_manager_);
    // 更新父节点中新插入键对应的值
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else { // 否则，将邻居节点的最后一个键值对移动到当前节点的开头
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
    // 更新父节点中原来在当前节点的第一个键对应的新值
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  // 解锁父页面
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
  // 如果old_root_node是叶子节点
  if (old_root_node->IsLeafPage()) {
    // 如果叶子节点的大小为0，说明树为空，需要删除根页面
    if (old_root_node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID; // 根页面ID设为-1
      UpdateRootPageId(false); // 更新根页面ID信息
      return true; // 返回true表示根页面需要被删除
    }
    // 如果叶子节点大小不为0，不需要调整根页面
    return false;
  }
  // 如果old_root_node不是叶子节点，则是内部节点
  if (old_root_node->GetSize() == 1) {
    // 如果根节点只有一个子节点，将子节点设置为新根
    auto root = reinterpret_cast<BPlusTreeInternalPage*>(old_root_node);
    root_page_id_ = root->ValueAt(0); // 新根页面ID设为原根节点的第一个子页面ID
    UpdateRootPageId(false); // 更新根页面ID信息
    // 获取新根页面
    auto new_root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    if (new_root_page == nullptr) {
      // 处理获取新根页面失败的情况，可能需要抛出异常或返回错误
      exception err = (const exception &)"out of memory";
      throw exception(err);
    }
    auto new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID); // 新根页面的父页面ID设为无效
    buffer_pool_manager_->UnpinPage(root_page_id_, true); // 解锁新根页面
    return true; // 返回true表示原根页面需要被删除
  }
  // 如果根节点的大小大于1，不需要调整根页面
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
  Page* leaf_page = FindLeafPage(nullptr, INVALID_PAGE_ID, true);
  if(leaf_page == nullptr)
  {
    exception err = (const exception &)"can't find leaf_page";
    throw exception(err);
  }
  auto * page = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  page_id_t page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return IndexIterator(page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page* leaf_page = FindLeafPage(key, INVALID_PAGE_ID, false);
  if(leaf_page == nullptr)
  {
    exception err = (const exception &)"can't find leaf_page";
    throw exception(err);
  }
  auto * page = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int index = page->KeyIndex(key, processor_);
  page_id_t page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return IndexIterator(page_id, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  GenericKey* key{};
  Page *page = FindLeafPage(key, true);
  if(page == nullptr)
  {
    exception err = (const exception &)"can't find leaf_page";
    throw exception(err);
  }
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
     int page_id = page->GetPageId(), index = leaf_page->GetSize()-1;
     buffer_pool_manager_->UnpinPage(page_id, false);
     return IndexIterator(page_id, buffer_pool_manager_, index);
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
  page_id_t start_page_id_ = page_id;
  if(start_page_id_ == INVALID_PAGE_ID)
    start_page_id_ = root_page_id_;
  if (IsEmpty())//空则返回
    return nullptr;
  //开始遍历
  auto root_node = buffer_pool_manager_->FetchPage(start_page_id_);
  auto cur_page = reinterpret_cast<BPlusTreePage *>(root_node->GetData());
  while (!cur_page->IsLeafPage()) {
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
    cur_page = reinterpret_cast<BPlusTreePage *>(root_node->GetData());
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
  // 获取索引根页面，该页面存储了所有索引的根页面ID
  auto root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  if (root_page == nullptr) {
    // 如果无法获取索引根页面，抛出异常
    exception err = (const exception &)"can't find leaf_page";
    throw exception(err);
  }
  // 将索引根页面的数据区域转换为IndexRootsPage类型
  auto index_root_page = reinterpret_cast<IndexRootsPage *>(root_page->GetData());
  // 根据insert_record的值，执行插入或更新操作
  if (insert_record) {
    // 如果insert_record为真，则在索引根页面中插入一个新的索引记录
    index_root_page->Insert(index_id_, root_page_id_);
  } else {
    // 如果insert_record为假，则更新索引根页面中现有的索引记录
    index_root_page->Update(index_id_, root_page_id_);
  }
  // 解锁索引根页面，并标记为脏页，以确保更改被写入磁盘
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
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