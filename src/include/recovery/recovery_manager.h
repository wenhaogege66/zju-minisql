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
        for (auto &[lsn, log_rec] : log_recs_) {
            if (lsn > persist_lsn_) {
                ApplyLog(log_rec);
            }
        }
    }

    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
        for (auto &[txn_id, lsn] : active_txns_) {
            auto log_rec_it = log_recs_.find(lsn);
            while (log_rec_it != log_recs_.end() && log_rec_it->second->txn_id_ == txn_id) {
                UndoLog(log_rec_it->second);
                log_rec_it++;
            }
        }
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    void ApplyLog(LogRecPtr log_rec) {
        switch (log_rec->type_) {
            case LogRecType::kInsert:
                data_[log_rec->key_] = log_rec->new_val_;
                break;
            case LogRecType::kUpdate:
                data_[log_rec->key_] = log_rec->new_val_;
                break;
            case LogRecType::kDelete:
                data_.erase(log_rec->key_);
                break;
            default:
                break;
        }
    }

    void UndoLog(LogRecPtr log_rec) {
        switch (log_rec->type_) {
            case LogRecType::kInsert:
                data_.erase(log_rec->key_);
                break;
            case LogRecType::kUpdate:
                data_[log_rec->key_] = log_rec->old_val_;
                break;
            case LogRecType::kDelete:
                data_[log_rec->key_] = log_rec->old_val_;
                break;
            default:
                break;
        }
    }

    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
