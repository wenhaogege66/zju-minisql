//
// Created by njz on 2023/1/16.
//

#ifndef MINISQL_EXECUTE_CONTEXT_H
#define MINISQL_EXECUTE_CONTEXT_H

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/txn.h"

class ExecuteContext {
 public:
  /**
   * Creates an ExecuteContext for the recovery that is executing the query.
   * @param transaction The recovery executing the query
   * @param catalog The catalog1 that the executor uses
   * @param bpm The buffer pool manager that the executor uses
   */
  ExecuteContext(Txn *transaction, CatalogManager *catalog, BufferPoolManager *bpm)
      : transaction_(transaction), catalog_{catalog}, bpm_{bpm} {}

  ~ExecuteContext() = default;

  DISALLOW_COPY_AND_MOVE(ExecuteContext);

  /** @return the running recovery */
  Txn *GetTransaction() const { return transaction_; }

  /** @return the catalog1 */
  CatalogManager *GetCatalog() { return catalog_; }

  /** @return the buffer pool manager */
  BufferPoolManager *GetBufferPoolManager() { return bpm_; }

 private:
  /** The recovery context associated with this executor context */
  Txn *transaction_;
  /** The datbase catalog1 associated with this executor context */
  CatalogManager *catalog_;
  /** The buffer pool manager associated with this executor context */
  BufferPoolManager *bpm_;
};

#endif  // MINISQL_EXECUTE_CONTEXT_H
