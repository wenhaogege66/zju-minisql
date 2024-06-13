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
  LogRec() = default;

  LogRec(LogRecType type, lsn_t lsn, lsn_t prev_lsn, txn_id_t txn_id): type_(type), lsn_(lsn), prev_lsn_(prev_lsn),
                                                                        txn_id_(txn_id) {
                                                                        };

  LogRecType type_{LogRecType::kInvalid};
  lsn_t lsn_{INVALID_LSN};
  lsn_t prev_lsn_{INVALID_LSN};
  txn_id_t txn_id_{INVALID_TXN_ID};

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;


  static lsn_t getprelsnid(txn_id_t txn_id, lsn_t new_id) {
    auto prev_lsn = prev_lsn_map_.find(txn_id);
    if (prev_lsn != prev_lsn_map_.end()) {
      lsn_t ret = prev_lsn->second;
      prev_lsn->second = new_id;
      return ret;
    } else {
      prev_lsn_map_.emplace(txn_id, new_id);
      return INVALID_LSN;
    }
  }

  KeyType ins_key_;
  ValType ins_val_;

  KeyType del_key_;
  ValType del_val_;

  KeyType old_key_;
  ValType old_val_;
  KeyType new_key_;
  ValType new_val_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  lsn_t cur_lsn = LogRec::next_lsn_++;
  LogRecPtr re(new LogRec(LogRecType::kInsert, cur_lsn, LogRec::getprelsnid(txn_id, cur_lsn), txn_id));
  re->ins_key_ = ins_key;
  re->ins_val_ = ins_val;
  return re;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  lsn_t cur_lsn = LogRec::next_lsn_++;
  LogRecPtr re(new LogRec(LogRecType::kDelete, cur_lsn, LogRec::getprelsnid(txn_id, cur_lsn), txn_id));
  re->del_key_ = del_key;
  re->del_val_ = del_val;
  return re;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  lsn_t cur_lsn = LogRec::next_lsn_++;
  LogRecPtr re(new LogRec(LogRecType::kUpdate, cur_lsn, LogRec::getprelsnid(txn_id, cur_lsn), txn_id));
  re->old_key_ = old_key;
  re->old_val_ = old_val;
  re->new_key_ = new_key;
  re->new_val_ = new_val;
  return re;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  lsn_t cur_lsn = LogRec::next_lsn_++;
  LogRecPtr re(new LogRec(LogRecType::kBegin, cur_lsn, LogRec::getprelsnid(txn_id, cur_lsn), txn_id));
  return re;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  lsn_t cur_lsn = LogRec::next_lsn_++;
  LogRecPtr re(new LogRec(LogRecType::kCommit, cur_lsn, LogRec::getprelsnid(txn_id, cur_lsn), txn_id));
  return re;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  lsn_t cur_lsn = LogRec::next_lsn_++;
  LogRecPtr re(new LogRec(LogRecType::kAbort, cur_lsn, LogRec::getprelsnid(txn_id, cur_lsn), txn_id));
  return re;
}

#endif  // MINISQL_LOG_REC_H

