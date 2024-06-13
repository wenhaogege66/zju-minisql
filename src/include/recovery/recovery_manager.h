#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase persist_data_{};

  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
 public:
  /**
  * TODO: Student Implement
   */
  void Init(CheckPoint &last_checkpoint) {
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    active_txns_ = last_checkpoint.active_txns_;
    data_ = last_checkpoint.persist_data_;
  }

  /**
  * TODO: Student Implement
   */
  void RedoPhase() {
    auto t = log_recs_.begin();
    for (; t != log_recs_.end() && t->first < persist_lsn_; ++t) {
    }
    for (; t != log_recs_.end(); ++t) {
      LogRecPtr n_rec = t->second;
      if (n_rec->type_ == LogRecType::kInvalid) {
        active_txns_[n_rec->txn_id_] = n_rec->lsn_;
      } else if (n_rec->type_ == LogRecType::kInsert) {
        active_txns_[n_rec->txn_id_] = n_rec->lsn_;
        data_.emplace(n_rec->ins_key_, n_rec->ins_val_);
      } else if (n_rec->type_ == LogRecType::kDelete) {
        active_txns_[n_rec->txn_id_] = n_rec->lsn_;
        data_.erase(n_rec->del_key_);
      } else if (n_rec->type_ == LogRecType::kUpdate) {
        active_txns_[n_rec->txn_id_] = n_rec->lsn_;
        data_.erase(n_rec->old_key_);
        data_[n_rec->new_key_] = n_rec->new_val_;
      } else if (n_rec->type_ == LogRecType::kBegin) {
        active_txns_[n_rec->txn_id_] = n_rec->lsn_;
      } else if (n_rec->type_ == LogRecType::kCommit) {
        active_txns_[n_rec->txn_id_] = n_rec->lsn_;
        active_txns_.erase(n_rec->txn_id_);
      } else if (n_rec->type_ == LogRecType::kAbort) {
        active_txns_[n_rec->txn_id_] = n_rec->lsn_;
        rollback(n_rec->txn_id_);
        active_txns_.erase(n_rec->txn_id_);
      }
    }
  }

  /**
  * TODO: Student Implement
   */
  void UndoPhase() {
    for (auto t: active_txns_) {
      rollback(t.first);
    }
    active_txns_.clear();
  }


  void rollback(txn_id_t txn_id) {
    lsn_t last_lsn = active_txns_[txn_id];
    while (last_lsn != INVALID_LSN) {
      LogRecPtr last_rec = log_recs_[last_lsn];
      if (last_rec->type_ == LogRecType::kBegin) {
        break;
      } else if (last_rec->type_ == LogRecType::kInsert) {
        data_.erase(last_rec->ins_key_);
      } else if (last_rec->type_ == LogRecType::kDelete) {
        data_.emplace(last_rec->del_key_, last_rec->del_val_);
      } else if (last_rec->type_ == LogRecType::kUpdate) {
        data_.erase(last_rec->new_key_);
        data_.emplace(last_rec->old_key_, last_rec->old_val_);
      }
      last_lsn = last_rec->prev_lsn_;
    }
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

 private
     :
  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase data_{}; // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
