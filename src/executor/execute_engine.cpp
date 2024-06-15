#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
int yyparse(void);

#include "parser/minisql_lex.h"
#include <parser/parser.h>
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.**/
    struct dirent *stdir;
    while((stdir = readdir(dir)) != nullptr) {
      if( strcmp( stdir->d_name , "." ) == 0 ||
          strcmp( stdir->d_name , "..") == 0 ||
          stdir->d_name[0] == '.')
        continue;
      char db_name[256];
      strncpy(db_name, stdir->d_name, strlen(stdir->d_name) - 3);
      dbs_[db_name] = new DBStorageEngine(stdir->d_name, false);
    }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
    // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No database selected" << endl;
    return DB_FAILED;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row: result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column: schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column: schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row: result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf() << std::flush;
  if (ast->type_ == kNodeSelect)
    delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  string db_file_name = "databases/" + db_name + ".db";
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  ofstream db_file(db_file_name, ios::out);
  if (!db_file.is_open()) {
    std::cout << "Failed to create database " << db_name << endl;
    return DB_FAILED;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name + ".db", true)));
  cout << "Database " << db_name << " is created successfully" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("databases/" + db_name + ".db").c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (current_db_ == db_name)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr: dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr: dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "ERROR: No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr: tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr: tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No database selected" << endl;
    return DB_FAILED;
  }

  string table_name = ast->child_->val_;
  auto node = ast->child_->next_->child_;
  vector<Column *> columns;
  vector<vector<string> > unique_columns;
  uint32_t index = 0;
  while (node && node->type_ != kNodeColumnList) {
    string column_name = node->child_->val_;
    string column_type = node->child_->next_->val_;
    bool unique = false;
    bool nullable = true;
    if (node->val_) {
      if (strcmp(node->val_, "unique") == 0) {
        unique = true;
        vector<string> unique_column;
        unique_column.emplace_back(column_name);
        unique_columns.emplace_back(unique_column);
      }
    }

    if (column_type == "int") {
      auto column = new Column(column_name, kTypeInt, index++, nullable, unique);
      columns.emplace_back(column);
    } else if (column_type == "char") {
      char *num = node->child_->next_->child_->val_;
      int32_t length = atoi(num);
      if (length <= 0 || strchr(num, '.')) {
        cout << "Invalid constraint number for 'char'" << endl;
        return DB_FAILED;
      }
      auto column = new Column(column_name, kTypeChar, length, index++, nullable, unique);
      columns.emplace_back(column);
    } else if (column_type == "float") {
      auto column = new Column(column_name, kTypeFloat, index++, nullable, unique);
      columns.emplace_back(column);
    }
    node = node->next_;
  }

  auto table_schema = new TableSchema(columns);
  TableInfo *table_info;
  if (dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, table_schema, nullptr, table_info) ==
      DB_TABLE_ALREADY_EXIST) {
    cout << "ERROR: Table '" << table_name << "' already exists" << endl;
    return DB_TABLE_ALREADY_EXIST;
  }

  if (node) {
    vector<string> index_keys;
    auto pk_node = node->child_;
    while (pk_node) {
      index_keys.emplace_back(pk_node->val_);
      pk_node = pk_node->next_;
    }
    IndexInfo *index_info;
    dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, "pk_" + table_name, index_keys, nullptr, index_info,
                                                 "bptree");
  }

  for (auto unique_column: unique_columns) {
    IndexInfo *index_info;
    dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, table_name + "_" + unique_column[0], unique_column,
                                                 nullptr, index_info, "bptree");
  }

  dbs_[current_db_]->bpm_->FlushAllPages();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No database selected" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  switch (dbs_[current_db_]->catalog_mgr_->DropTable(table_name)) {
    case DB_TABLE_NOT_EXIST:
      cout << "Unknown table '" << current_db_ << "." << table_name << "'" << endl;
      return DB_TABLE_NOT_EXIST;
    case DB_FAILED:
      cout << "ERROR: Table '" << table_name << "' still used" << endl;
      return DB_FAILED;
    default:
      cout << "Drop table '" << table_name << "' OK" << endl;
      return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  if (tables.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  vector<IndexInfo *> indexes;
  for (auto table: tables) {
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
  }
  string index_in_db("Indexes_in_" + current_db_);
  uint max_width = index_in_db.length();
  for (auto index: indexes) {
    if (index->GetIndexName().length() > max_width) max_width = index->GetIndexName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << index_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (auto index: indexes) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << index->GetIndexName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No database selected" << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  vector<string> index_keys;
  IndexInfo *index_info;
  string index_type = "";
  auto node = ast->child_->next_->next_->child_;
  while (node) {
    index_keys.emplace_back(node->val_);
    node = node->next_;
  }
  if (ast->child_->next_->next_->next_) {
    index_type = ast->child_->next_->next_->next_->child_->val_;
  }
  switch (dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, index_keys, nullptr, index_info,
                                                       index_type)) {
    case DB_TABLE_NOT_EXIST:
      cout << "Table '" << current_db_ << "." << table_name << "' doesn't exist" << endl;
      return DB_TABLE_NOT_EXIST;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Duplicate key name '" << index_name << "'" << endl;
      return DB_INDEX_ALREADY_EXIST;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Key column doesn't exist in table" << endl;
      return DB_COLUMN_NAME_NOT_EXIST;
    default:
      cout << "Create index '" << index_name << "' OK" << endl;
      return DB_SUCCESS;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if (dbs_.find(current_db_) == dbs_.end()) {
    cout << "ERROR: No database selected" << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for (auto table: tables) {
    string table_name = table->GetTableName();
    vector<IndexInfo *> indexes;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name, indexes);
    for (auto index: indexes) {
      if (index_name == index->GetIndexName()) {
        if (dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name) == DB_SUCCESS) {
          cout << "Drop index '" << index_name << "' OK" << endl;
          return DB_SUCCESS;
        } else {
          cout << "Drop index '" << index_name << "' FAILED" << endl;
          return DB_FAILED;
        }
      }
    }
  }
  cout << "Can't DROP '" << index_name << "'; check that column/key exists" << endl;
  return DB_INDEX_NOT_FOUND;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  const char *file_name = ast->child_->val_;
  string k = file_name;
  FILE *file = fopen(file_name, "r");
  if (file == nullptr) {
    cout << "No file \"" << file_name << "\"!" << endl;
    return DB_FAILED;
  }
  // command buffer
  const int buf_size = 1024;
  char cmd[buf_size];

  while (!feof(file)) {
    // read from buffer
    memset(cmd, 0, buf_size);
    int i = 0;
    char ch;
    while (!feof(file) && (ch = getc(file)) != ';') {
      cmd[i++] = ch;
    }
    if (feof(file))
      break;
    cmd[i] = ch; // ;

    // create buffer for sql input
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    }

    auto result = Execute(MinisqlGetParserRootNode());

    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();

    // quit condition
    ExecuteInformation(result);
  }
  cout << "Execute file \"" << k << "\" success!" << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
