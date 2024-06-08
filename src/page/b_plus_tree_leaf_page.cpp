#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int left=0;
  int right=GetSize()-1;
  while(right-left>1){
    int   middle=(left+right)/2;
    if(KM.CompareKeys(key,KeyAt(middle))>=0){
      left=middle;
    }
    if(KM.CompareKeys(key,KeyAt(middle))<0){
      right=middle;
    }
  }
  return  right;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int   size=GetSize()+1;
  int   index=KeyIndex(key,KM);
  PairCopy(PairPtrAt(index+1),PairPtrAt(index),size-index);
  SetValueAt(index,value);
  SetKeyAt(index,key);
  SetSize(size);
  return size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int   new_size=GetSize()/2;
  SetSize(new_size);
  recipient->CopyNFrom(PairPtrAt(new_size),GetSize()-new_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  PairCopy(PairPtrAt(GetSize()),src,size);
  SetSize(GetSize()+size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int   left=0;
  int   right=GetSize()-1;
  while(right-left>1){
    int   middle=(left+right)/2;
    if(KM.CompareKeys(key,KeyAt(middle))==0){
      value=ValueAt(middle);
      return  true;
    }
    if(KM.CompareKeys(key,KeyAt(middle))>0){
      left=middle;
    }
    if(KM.CompareKeys(key,KeyAt(middle))<0){
      right=middle;
    }
  }
  return  false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int   left=0;
  int   right=GetSize()-1;
  int   exist=0;
  int   index=-1;
  //Binary search
  while(right-left>1){
    int   middle=(left+right)/2;
    if(KM.CompareKeys(key,KeyAt(middle))==0){
      exist=1;
      index=middle;
      break;
    }
    if(KM.CompareKeys(key,KeyAt(middle))>0){
      left=middle;
    }
    if(KM.CompareKeys(key,KeyAt(middle))<0){
      right=middle;
    }
  }
  //not exist
  if(exist==0){
    return  GetSize();
  }
  //exist
  PairCopy(PairPtrAt(index),PairPtrAt(index+1),GetSize()-index-1);
  SetSize(GetSize()-1);
  return  GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->PairCopy(recipient->PairPtrAt(recipient->GetSize()),PairPtrAt(0),GetSize());
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  recipient->CopyLastFrom(KeyAt(0),ValueAt(0));
  PairCopy(PairPtrAt(0),PairPtrAt(1),GetSize()-1);
  SetSize(GetSize()-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(),key);
  SetValueAt(GetSize(),value);
  SetSize(GetSize()+1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  SetSize(GetSize()-1);
  recipient->CopyFirstFrom(KeyAt(GetSize()),ValueAt(GetSize()));
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  PairCopy(PairPtrAt(1),PairPtrAt(0),GetSize());
  SetSize(GetSize()+1);
  SetKeyAt(0,key);
  SetValueAt(0,value);
}