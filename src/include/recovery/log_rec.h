#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;
//using lsn_t = int64_t; // Log Sequence Number
//using txn_id_t = int64_t; // Transaction ID

/**
 * TODO: Student Implement
 */
struct LogRec {
   // LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{};
    KeyType key_{};
    ValType old_val_{};
    ValType new_val_{};

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
     LogRec(txn_id_t txn_id, LogRecType type, KeyType key, ValType old_val, ValType new_val)
        : type_(type), txn_id_(txn_id), lsn_(next_lsn_++), prev_lsn_(INVALID_LSN),
          key_(std::move(key)), old_val_(old_val), new_val_(new_val) {
        if (prev_lsn_map_.find(txn_id) != prev_lsn_map_.end()) {
            prev_lsn_ = prev_lsn_map_[txn_id];
        }
        prev_lsn_map_[txn_id] = lsn_;
    }

    static constexpr lsn_t INVALID_LSN = -1;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
     return std::make_shared<LogRec>(txn_id, LogRecType::kInsert, std::move(ins_key), 0, ins_val);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    return std::make_shared<LogRec>(txn_id, LogRecType::kDelete, std::move(del_key), del_val, 0);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
     return std::make_shared<LogRec>(txn_id, LogRecType::kUpdate, std::move(old_key), old_val, new_val);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    return std::make_shared<LogRec>(txn_id, LogRecType::kBegin, "", 0, 0);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    return std::make_shared<LogRec>(txn_id, LogRecType::kCommit, "", 0, 0);
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    return std::make_shared<LogRec>(txn_id, LogRecType::kAbort, "", 0, 0);
}

#endif  // MINISQL_LOG_REC_H
