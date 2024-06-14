#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if(txn -> GetIsolationLevel() == IsolationLevel::kReadUncommitted){
    txn ->SetState(TxnState::kAborted);
    throw TxnAbortException(txn -> GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
  }
  LockPrepare(txn, rid);
  //找到rid的锁请求队列
  auto &Que = lock_table_[rid];
  //将新的锁请求添加到队列前端，并在map中存储其迭代器。
  txn_id_t txn_id = txn -> GetTxnId();
  Que.EmplaceLockRequest(txn_id, LockMode::kShared);

  CheckAbort(txn, Que);
  if(Que.is_writing_){
    Que.cv_.wait(lock,[&]() {return txn->GetState() == TxnState::kAborted || !Que.is_writing_;});
  }

  auto &shareset = txn -> GetSharedLockSet();
  shareset.emplace(rid);
  Que.sharing_cnt_++;
  Que.GetLockRequestIter(txn_id) -> granted_ = LockMode::kShared;

  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  LockPrepare(txn, rid);
  //找到rid的锁请求队列
  auto &Que = lock_table_[rid];
  //将新的锁请求添加到队列前端，并在map中存储其迭代器。
  txn_id_t txn_id = txn -> GetTxnId();
  Que.EmplaceLockRequest(txn_id, LockMode::kExclusive);

  CheckAbort(txn, Que);
  if(Que.is_writing_ || Que.sharing_cnt_ != 0){
    Que.cv_.wait(lock,[&]() {return txn->GetState() == TxnState::kAborted || (!Que.is_writing_ && Que.sharing_cnt_ == 0);});
  }

  auto &shareset = txn -> GetExclusiveLockSet();
  shareset.emplace(rid);
  Que.GetLockRequestIter(txn_id) -> granted_ = LockMode::kExclusive;
  Que.is_writing_ = true;

  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  txn_id_t txn_id = txn -> GetTxnId();
  LockPrepare(txn, rid);
  auto &Que = lock_table_[rid];
  if(Que.is_upgrading_){
    txn ->SetState(TxnState::kAborted);
    throw TxnAbortException(txn_id, AbortReason::kUpgradeConflict);
  }

  auto now_state = Que.GetLockRequestIter(txn_id);
  if(now_state -> granted_ == LockMode::kExclusive)return true;
  if(Que.is_writing_ || Que.sharing_cnt_ > 1){
    Que.is_upgrading_ = true;
    Que.cv_.wait(lock, [&](){return txn->GetState() == TxnState::kAborted || (!Que.is_writing_ && Que.sharing_cnt_ == 1);});
  }
  CheckAbort(txn, Que);
  txn -> GetSharedLockSet().erase(rid);
  txn -> GetExclusiveLockSet().emplace(rid);
  Que.sharing_cnt_--;
  Que.is_writing_ = true;
  Que.is_upgrading_ = false;
  now_state -> granted_ = LockMode::kExclusive;
  now_state -> lock_mode_ = LockMode::kExclusive;

  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  auto txn_id = txn -> GetTxnId();
  auto &Que = lock_table_[rid];
  txn -> GetExclusiveLockSet().erase(rid);
  txn -> GetSharedLockSet().erase(rid);
  auto now = Que.GetLockRequestIter(txn_id);
  if(!Que.EraseLockRequest(txn_id)){
    return false;
  }
  if(txn ->GetState() == TxnState::kGrowing){
    txn ->SetState(TxnState::kShrinking);
  }

  if(now ->lock_mode_ == LockMode::kShared){
    Que.sharing_cnt_--;
    Que.cv_.notify_all();
  }
  else{
    Que.is_writing_ = false;
    Que.cv_.notify_all();
  }
  return true;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
  if(txn -> GetState() == TxnState::kShrinking){
    txn ->SetState(TxnState::kAborted);
    throw TxnAbortException(txn -> GetTxnId(), AbortReason::kLockOnShrinking);
  }
  if(lock_table_.find(rid) == lock_table_.end()){
    lock_table_[rid];
  }
}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
  if(txn -> GetState() == TxnState::kAborted){
    req_queue.is_upgrading_ = false;
    req_queue.EraseLockRequest(txn -> GetTxnId());
    throw TxnAbortException(txn -> GetTxnId(), AbortReason::kDeadlock);
  }
}

/**
 * TODO: Student Implement
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].insert(t2);
}

/**
 * TODO: Student Implement
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(t2);
}

bool cmp(int a,int b){
  return a > b;
}

/**
 * TODO: Student Implement
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  visited_set_.clear();
  while(!visited_path_.empty()){
    visited_path_.pop();
  }
  std::vector<txn_id_t> tmp;
  for(auto &it1 : waits_for_){
    tmp.push_back(it1.first);
    for(auto &it2 : it1.second){
      tmp.push_back(it2);
    }
  }
  std::sort(tmp.begin(), tmp.end(), cmp);
  for(auto it : tmp){
    revisited_node_ = INVALID_TXN_ID;
    dfs(it);
    if(revisited_node_ != INVALID_TXN_ID){
      newest_tid_in_cycle = revisited_node_;
      while(revisited_node_ != visited_path_.top()){
        newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
        visited_path_.pop();
      }
      return true;
    }
  }
  newest_tid_in_cycle = INVALID_TXN_ID;
  return false;
}

void LockManager::dfs(txn_id_t txn_id){
  if(visited_set_.find(txn_id) != visited_set_.end()){
    revisited_node_ = txn_id;
    return;
  }

  visited_set_.insert(txn_id);
  visited_path_.push(txn_id);

  for(auto it : waits_for_[txn_id]){
    dfs(it);
    if(revisited_node_ != INVALID_TXN_ID)return;
  }

  visited_set_.erase(txn_id);
  visited_path_.pop();
}


void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);

  auto *txn = txn_mgr_->GetTransaction(txn_id);

  for (const auto &row_id: txn->GetSharedLockSet()) {
    for (const auto &lock_req: lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }

  for (const auto &row_id: txn->GetExclusiveLockSet()) {
    for (const auto &lock_req: lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }
}

/**
 * TODO: Student Implement
 */
void LockManager::RunCycleDetection() {
  while(enable_cycle_detection_){
    std::unique_lock<std::mutex> lock(latch_);
    std::unordered_map<txn_id_t ,RowId> mp;
    // 找到所有的事务并且构造依赖关系，没有状态的事务要依赖加锁的事务
    for(auto &it1 : lock_table_){
      auto &Que = it1.second;
      for(auto &it2 : Que.req_list_){
        mp[it2.txn_id_] = it1.first;
        if(it2.granted_ == LockMode::kNone){
          for(auto &it3 : Que.req_list_){
            if(it3.granted_ != LockMode::kNone){
              AddEdge(it2.txn_id_, it3.txn_id_);
            }
          }
        }
      }
    }
    txn_id_t id = INVALID_TXN_ID;
    while(1){
      HasCycle(id);
      if(id == INVALID_TXN_ID)break;
      DeleteNode(id);
      txn_mgr_ ->GetTransaction(id) ->SetState(TxnState::kAborted);
      lock_table_[mp[id]].cv_.notify_all();
    }
    waits_for_.clear();
  }
}

/**
 * TODO: Student Implement
 */
std::vector<std::pair<txn_id_t, txn_id_t> > LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t> > result;
  for(auto &it1 : waits_for_){
    for(auto &it2 : it1.second){
      result.emplace_back(it1.first,it2);
    }
  }
  return result;
}

