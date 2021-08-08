#include "duckdb/main/client_context.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/stream_query_result.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/skinnerdb/skinnerdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"
#include "duckdb/parser/statement/prepare_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/relation.hpp"
#include "duckdb/parser/statement/relation_statement.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/planner/pragma_handler.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/skinnerdb/timer.hpp"


#include <iostream>

namespace duckdb {

class ClientContextLock {
public:
	explicit ClientContextLock(mutex &context_lock) : client_guard(context_lock) {
	}

	~ClientContextLock() {
	}

private:
	lock_guard<mutex> client_guard;
};

ClientContext::ClientContext(shared_ptr<DatabaseInstance> database)
    : db(move(database)), transaction(db->GetTransactionManager(), *this), interrupted(false), executor(*this),
      temporary_objects(make_unique<SchemaCatalogEntry>(&db->GetCatalog(), TEMP_SCHEMA, true)), open_result(nullptr) {
	std::random_device rd;
	random_engine.seed(rd());
}

ClientContext::~ClientContext() {
	Destroy();
}

unique_ptr<ClientContextLock> ClientContext::LockContext() {
	printf("unique_ptr<ClientContextLock> ClientContext::LockContext\n");
	return make_unique<ClientContextLock>(context_lock);
}

void ClientContext::Destroy() {
	auto lock = LockContext();
	if (transaction.HasActiveTransaction()) {
		ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		if (!transaction.IsAutoCommit()) {
			transaction.Rollback();
		}
	}
	CleanupInternal(*lock);
}

void ClientContext::Cleanup() {
	auto lock = LockContext();
	CleanupInternal(*lock);
}

unique_ptr<DataChunk> ClientContext::Fetch() {
	auto lock = LockContext();
	if (!open_result) {
		// no result to fetch from
		throw Exception("Fetch was called, but there is no open result (or the result was previously closed)");
	}
	try {
		// fetch the chunk and return it
		auto chunk = FetchInternal(*lock);
		return chunk;
	} catch (Exception &ex) {
		open_result->error = ex.what();
	} catch (...) {
		open_result->error = "Unhandled exception in Fetch";
	}
	open_result->success = false;
	CleanupInternal(*lock);
	return nullptr;
}

string ClientContext::FinalizeQuery(ClientContextLock &lock, bool success) {
	profiler.EndQuery();
	executor.Reset();

	string error;
	if (transaction.HasActiveTransaction()) {
		ActiveTransaction().active_query = MAXIMUM_QUERY_ID;
		query_profiler_history.GetPrevProfilers().emplace_back(transaction.ActiveTransaction().active_query,
		                                                       move(profiler));
		profiler.save_location = query_profiler_history.GetPrevProfilers().back().second.save_location;
		if (query_profiler_history.GetPrevProfilers().size() >= query_profiler_history.GetPrevProfilersSize()) {
			query_profiler_history.GetPrevProfilers().pop_front();
		}
		try {
			if (transaction.IsAutoCommit()) {
				if (success) {
					// query was successful: commit
					transaction.Commit();
				} else {
					// query was unsuccessful: rollback
					transaction.Rollback();
				}
			}
		} catch (Exception &ex) {
			error = ex.what();
		} catch (...) {
			error = "Unhandled exception!";
		}
	}
	return error;
}

void ClientContext::CleanupInternal(ClientContextLock &lock) {
	if (!open_result) {
		// no result currently open
		return;
	}

	auto error = FinalizeQuery(lock, open_result->success);
	if (open_result->success) {
		// if an error occurred while committing report it in the result
		open_result->error = error;
		open_result->success = error.empty();
	}

	open_result->is_open = false;
	open_result = nullptr;

	this->query = string();
}

unique_ptr<DataChunk> ClientContext::FetchInternal(ClientContextLock &) {
	return executor.FetchChunk();
}

shared_ptr<PreparedStatementData> ClientContext::CreatePreparedStatement(ClientContextLock &lock, const string &query,
                                                                         unique_ptr<SQLStatement> statement) {
	printf("shared_ptr<PreparedStatementData> ClientContext::CreatePreparedStatement\n");
	StatementType statement_type = statement->type;
	auto result = make_shared<PreparedStatementData>(statement_type);

	profiler.StartPhase("planner");
	Planner planner(*this);
	planner.CreatePlan(move(statement));    // turn STATEMENT to PLAN: update this->planner: plan, names, types
	D_ASSERT(planner.plan);
	profiler.EndPhase();

	auto plan = move(planner.plan);
	// extract the result column names from the plan
	result->read_only = planner.read_only;
	result->requires_valid_transaction = planner.requires_valid_transaction;
	result->allow_stream_result = planner.allow_stream_result;
	result->names = planner.names;
	result->types = planner.types;
	result->value_map = move(planner.value_map);
	result->catalog_version = Transaction::GetTransaction(*this).catalog_version;

	if (enable_optimizer) {                 // Optimize PLAN
		profiler.StartPhase("optimizer");
		Optimizer optimizer(*planner.binder, *this);
		plan = optimizer.Optimize(move(plan));
		D_ASSERT(plan);
		profiler.EndPhase();
	}

    profiler.StartPhase("physical_planner");
    // now convert logical query plan into a physical query plan
    PhysicalPlanGenerator physical_planner(*this);
    auto physical_plan = physical_planner.CreatePlan(move(plan));
    profiler.EndPhase();

    result->plan = move(physical_plan);

	return result;  // PreparedStatementData result which includes the PLAN
}

int ClientContext::GetProgress() {
	auto my_progress_bar = progress_bar;
	if (!my_progress_bar) {
		return -1;
	}
	return my_progress_bar->GetCurrentPercentage();
}
/* 1. lock
 * 2. query
 * 3. statement_p is the result of CreatePreparedStatement()
 * 4. bound_values: empty vector<Value> where Value holds a single arbitrary value of any type that can be stored in the database.
 * 5. allow_stream_result*/
unique_ptr<QueryResult> ClientContext::ExecutePreparedStatement(ClientContextLock &lock, const string &query,
                                                                shared_ptr<PreparedStatementData> statement_p,
                                                                vector<Value> bound_values, bool allow_stream_result) {
	//f("unique_ptr<QueryResult> ClientContext::ExecutePreparedStatement(ClientContextLock &lock, const string &query,\n");

	auto &statement = *statement_p;     // shared_ptr<PreparedStatementData>: includes optimized PLAN
	if (ActiveTransaction().IsInvalidated() && statement.requires_valid_transaction) {
		throw Exception("Current transaction is aborted (please ROLLBACK)");
	}
	auto &config = DBConfig::GetConfig(*this);  // DBConfig&
	if (config.access_mode == AccessMode::READ_ONLY && !statement.read_only) {
		throw Exception(StringUtil::Format("Cannot execute statement of type \"%s\" in read-only mode!",
		                                   StatementTypeToString(statement.statement_type)));
	}

	// bind the bound values before execution
	statement.Bind(move(bound_values)); //bound_values: empty vector

	bool create_stream_result = statement.allow_stream_result && allow_stream_result;
	if (enable_progress_bar) {
		if (!progress_bar) {
			progress_bar = make_shared<ProgressBar>(&executor, wait_time);
		}
		progress_bar->Start();
	}
	// store the physical plan in the context for calls to Fetch()
	executor.Initialize(statement.plan.get());  /*initialize the PLAN in Executor*/

	auto types = executor.GetTypes();

	D_ASSERT(types == statement.types);

	if (create_stream_result) {
		if (progress_bar) {
			progress_bar->Stop();
		}
		// successfully compiled SELECT clause and it is the last statement
		// return a StreamQueryResult so the client can call Fetch() on it and stream the result
		// 🌞 1st return - only a framework??? where is this FETCH() function and what did it do?
		return make_unique<StreamQueryResult>(statement.statement_type, shared_from_this(), statement.types,
		                                      statement.names, move(statement_p));
	}
	// create a materialized result by continuously fetching
	auto result = make_unique<MaterializedQueryResult>(statement.statement_type, statement.types, statement.names); // 🚩 2rd return
	while (true) {
		auto chunk = FetchInternal(lock);   // chunk->data = "CONSTANT INTEGER: (UNKNOWN COUNT) [ NULL]"

		if (chunk->size() == 0) {
			break;
		}
#ifdef DEBUG
		for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
			if (statement.types[i].id() == LogicalTypeId::VARCHAR) {
				chunk->data[i].UTFVerify(chunk->size());
			}
		}
#endif
		result->collection.Append(*chunk);  // 🚩 2rd return
	}
	if (progress_bar) {
		progress_bar->Stop();
	}
	return move(result);    // 🚩 2rd return
}


unique_ptr<QueryResult> ClientContext::ExecutePreparedStatementWithRLOptimizer(ClientContextLock &lock, const string &query,
                                                                shared_ptr<PreparedStatementData> statement_p,
                                                                vector<Value> bound_values, bool allow_stream_result) {
    printf("unique_ptr<QueryResult> ClientContext::ExecutePreparedStatementWithRLOptimizer \n");
    auto &statement = *statement_p;     // shared_ptr<PreparedStatementData>: includes optimized PLAN
    if (ActiveTransaction().IsInvalidated() && statement.requires_valid_transaction) {
        throw Exception("Current transaction is aborted (please ROLLBACK)");
    }
    auto &config = DBConfig::GetConfig(*this);  // DBConfig&
    if (config.access_mode == AccessMode::READ_ONLY && !statement.read_only) {
        throw Exception(StringUtil::Format("Cannot execute statement of type \"%s\" in read-only mode!",
                                           StatementTypeToString(statement.statement_type)));
    }

    // bind the bound values before execution
    statement.Bind(move(bound_values)); //bound_values: empty vector

    bool create_stream_result = statement.allow_stream_result && allow_stream_result;
    if (enable_progress_bar) {
        if (!progress_bar) {
            progress_bar = make_shared<ProgressBar>(&executor, wait_time);
        }
        progress_bar->Start();
    }
    // store the physical plan in the context for calls to Fetch()
    executor.Initialize(statement.plan.get());  /*initialize the PLAN in Executor*/

    auto types = executor.GetTypes();

    D_ASSERT(types == statement.types);

    if (create_stream_result) {
        if (progress_bar) {
            progress_bar->Stop();
        }
        // successfully compiled SELECT clause and it is the last statement
        // return a StreamQueryResult so the client can call Fetch() on it and stream the result
        // 🌞 1st return - only a framework??? where is this FETCH() function and what did it do?
        return make_unique<StreamQueryResult>(statement.statement_type, shared_from_this(), statement.types,
                                              statement.names, move(statement_p));
    }
    // create a materialized result by continuously fetching
    auto result = make_unique<MaterializedQueryResult>(statement.statement_type, statement.types, statement.names); // 🚩 2rd return

    //FIXME: here
    /*auto chunk = FetchInternal(lock);   // ONLY FETCH ONE CHUNK EACH ExecutePreparedStatementWithRLOptimizer()
    if (chunk->size()!=0) {
        result->collection.Append(*chunk);
    } else {
        query_finished = true;
    }*/
    while (true) {
        auto chunk = FetchInternal(lock);

        if (chunk->size() == 0) {
            break;
        }
#ifdef DEBUG
        for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
            if (statement.types[i].id() == LogicalTypeId::VARCHAR) {
                chunk->data[i].UTFVerify(chunk->size());
            }
        }
#endif
        result->collection.Append(*chunk);
    }

    if (progress_bar) {
        progress_bar->Stop();
    }
    return move(result);
}

void ClientContext::InitialCleanup(ClientContextLock &lock) {
	printf("void ClientContext::InitialCleanup\n");
	//! Cleanup any open results and reset the interrupted flag
	CleanupInternal(lock);
	interrupted = false;
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatements(const string &query) {
	auto lock = LockContext();
	return ParseStatementsInternal(*lock, query);
}

vector<unique_ptr<SQLStatement>> ClientContext::ParseStatementsInternal(ClientContextLock &lock, const string &query) {
	printf("vector<unique_ptr<SQLStatement>> ClientContext::ParseStatementsInternal\n");
	Parser parser;
	parser.ParseQuery(query);

	PragmaHandler handler(*this);   // this = this client_context
	handler.HandlePragmaStatements(lock, parser.statements);

	return move(parser.statements);
}

void ClientContext::HandlePragmaStatements(vector<unique_ptr<SQLStatement>> &statements) {
	auto lock = LockContext();

	PragmaHandler handler(*this);
	handler.HandlePragmaStatements(*lock, statements);
}

unique_ptr<PreparedStatement> ClientContext::PrepareInternal(ClientContextLock &lock,
                                                             unique_ptr<SQLStatement> statement) {
	auto n_param = statement->n_param;
	auto statement_query = statement->query;
	shared_ptr<PreparedStatementData> prepared_data;
	auto unbound_statement = statement->Copy();
	RunFunctionInTransactionInternal(
	    lock, [&]() { prepared_data = CreatePreparedStatement(lock, statement_query, move(statement)); }, false);
	prepared_data->unbound_statement = move(unbound_statement);
	return make_unique<PreparedStatement>(shared_from_this(), move(prepared_data), move(statement_query), n_param);
}

unique_ptr<PreparedStatement> ClientContext::Prepare(unique_ptr<SQLStatement> statement) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);
		return PrepareInternal(*lock, move(statement));
	} catch (std::exception &ex) {
		return make_unique<PreparedStatement>(ex.what());
	}
}

unique_ptr<PreparedStatement> ClientContext::Prepare(const string &query) {
	auto lock = LockContext();
	// prepare the query
	try {
		InitialCleanup(*lock);

		// first parse the query
		auto statements = ParseStatementsInternal(*lock, query);
		if (statements.empty()) {
			throw Exception("No statement to prepare!");
		}
		if (statements.size() > 1) {
			throw Exception("Cannot prepare multiple statements at once!");
		}
		return PrepareInternal(*lock, move(statements[0]));
	} catch (std::exception &ex) {
		return make_unique<PreparedStatement>(ex.what());
	}
}

unique_ptr<QueryResult> ClientContext::Execute(const string &query, shared_ptr<PreparedStatementData> &prepared,
                                               vector<Value> &values, bool allow_stream_result) {
	auto lock = LockContext();
	try {
		InitialCleanup(*lock);
	} catch (std::exception &ex) {
		return make_unique<MaterializedQueryResult>(ex.what());
	}
	LogQueryInternal(*lock, query);
	return RunStatementOrPreparedStatement(*lock, query, nullptr, prepared, &values, allow_stream_result);
}

/*unique_ptr<QueryResult> ClientContext::RunStatementInternal(ClientContextLock &lock, const string &query,
                                                            unique_ptr<SQLStatement> statement,
                                                            bool allow_stream_result) {
	printf("RunStatementInternal: use duckdb-optimizer\n");
	// prepare the query for execution
	auto prepared = CreatePreparedStatement(lock, query, move(statement));  									//return optimized plan
	// by default, no values are bound
	vector<Value> bound_values;
	// execute the prepared statement
	return ExecutePreparedStatement(lock, query, move(prepared), move(bound_values), allow_stream_result);	//run chosen plan
}*/

unique_ptr<QueryResult> ClientContext::RunStatementInternal(ClientContextLock &lock, const string &query,
                                                            unique_ptr<SQLStatement> statement,
                                                            bool allow_stream_result) {
    printf("unique_ptr<QueryResult> ClientContext::RunStatementInternal\n");
    if (enable_rl_join_order_optimizer) {	//this should mix the process of selection and execution, and return a result in the end
        printf("🌈 SkinnerDB RL\n");
        SkinnerDB skinnerDb(profiler, *this);
        auto result = skinnerDb.CreateAndExecuteStatement(lock, query, move(statement), allow_stream_result);
        return result;
    } else {
        printf("🌈 DuckDB DP\n");
        // prepare the query for execution
        auto prepared = CreatePreparedStatement(lock, query, move(statement));  									//return optimized plan
        // by default, no values are bound
        vector<Value> bound_values;
        // execute the prepared statement
        return ExecutePreparedStatement(lock, query, move(prepared), move(bound_values), allow_stream_result);	    //return unique_ptr<QueryResult>
    }

}

unique_ptr<QueryResult> ClientContext::RunStatementOrPreparedStatement(ClientContextLock &lock, const string &query,
                                                                       unique_ptr<SQLStatement> statement,
                                                                       shared_ptr<PreparedStatementData> &prepared,
                                                                       vector<Value> *values,
                                                                       bool allow_stream_result) {
	printf("unique_ptr<QueryResult> ClientContext::RunStatementOrPreparedStatement\n");
	this->query = query;

	unique_ptr<QueryResult> result;
	// check if we are on AutoCommit. In this case we should start a transaction.
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction(); //update active_transactions in transaction_manager.hpp
	}
	ActiveTransaction().active_query = db->GetTransactionManager().GetQueryNumber();
	if (statement && query_verification_enabled) {
		// query verification is enabled
		// create a copy of the statement, and use the copy
		// this way we verify that the copy correctly copies all properties
		auto copied_statement = statement->Copy();
		if (statement->type == StatementType::SELECT_STATEMENT) {
			// in case this is a select query, we verify the original statement
			string error = VerifyQuery(lock, query, move(statement));
			if (!error.empty()) {
				// query failed: abort now
				FinalizeQuery(lock, false);
				// error in verifying query
				return make_unique<MaterializedQueryResult>(error);
			}
		}
		statement = move(copied_statement);
	}
	// start the profiler
	profiler.StartQuery(query);
	try {
		if (statement) {
			//printf("statement is not null");
			result = RunStatementInternal(lock, query, move(statement), allow_stream_result);
		} else {
			auto &catalog = Catalog::GetCatalog(*this);
			if (prepared->unbound_statement && catalog.GetCatalogVersion() != prepared->catalog_version) {
				D_ASSERT(prepared->unbound_statement.get());
				// catalog was modified: rebind the statement before execution
				auto new_prepared = CreatePreparedStatement(lock, query, prepared->unbound_statement->Copy());
				if (prepared->types != new_prepared->types) {
					throw BinderException("Rebinding statement after catalog change resulted inRunStatementInternal(lock, query, move(statement), allow_stream_result); change of types");
				}
				new_prepared->unbound_statement = move(prepared->unbound_statement);
				prepared = move(new_prepared);
			}
			result = ExecutePreparedStatement(lock, query, prepared, *values, allow_stream_result);
		}
	} catch (StandardException &ex) {
		// standard exceptions do not invalidate the current transaction
		result = make_unique<MaterializedQueryResult>(ex.what());
	} catch (std::exception &ex) {
		// other types of exceptions do invalidate the current transaction
		if (transaction.HasActiveTransaction()) {
			ActiveTransaction().Invalidate();
		}
		result = make_unique<MaterializedQueryResult>(ex.what());
	}
	if (!result->success) {
		// initial failures should always be reported as MaterializedResult
		D_ASSERT(result->type != QueryResultType::STREAM_RESULT);
		// query failed: abort now
		FinalizeQuery(lock, false);
		return result;
	}
	// query succeeded, append to list of results
	if (result->type == QueryResultType::STREAM_RESULT) {
		// store as currently open result if it is a stream result
		this->open_result = (StreamQueryResult *)result.get();
	} else {
		// finalize the query if it is not a stream result
		string error = FinalizeQuery(lock, true);
		if (!error.empty()) {
			// failure in committing transaction
			return make_unique<MaterializedQueryResult>(error);
		}
	}
	return result;
}

unique_ptr<QueryResult> ClientContext::RunStatement(ClientContextLock &lock, const string &query,
                                                    unique_ptr<SQLStatement> statement, bool allow_stream_result) {
	printf("-unique_ptr<QueryResult> ClientContext::RunStatement\n");
	shared_ptr<PreparedStatementData> prepared;
	return RunStatementOrPreparedStatement(lock, query, move(statement), prepared, nullptr, allow_stream_result);
}

unique_ptr<QueryResult> ClientContext::RunStatements(ClientContextLock &lock, const string &query,
                                                     vector<unique_ptr<SQLStatement>> &statements,
                                                     bool allow_stream_result) {
	printf("unique_ptr<QueryResult> ClientContext::RunStatements\n");
	// now we have a list of statements
	// iterate over them and execute them one by one
	unique_ptr<QueryResult> result;
	QueryResult *last_result = nullptr;
	for (idx_t i = 0; i < statements.size(); i++) { // iterate statements
		auto &statement = statements[i];
		bool is_last_statement = i + 1 == statements.size();    // TRUE if its the last statement
		auto current_result = RunStatement(lock, query, move(statement), allow_stream_result && is_last_statement);
		// now append the result to the list of results
		if (!last_result) {
			// first result of the query
			result = move(current_result);
			last_result = result.get();
		} else {
			// later results; attach to the result chain
			last_result->next = move(current_result);
			last_result = last_result->next.get();
		}
	}
	return result;
}

void ClientContext::LogQueryInternal(ClientContextLock &, const string &query) {
	printf("void ClientContext::LogQueryInternal\n");
	if (!log_query_writer) {    // can be modified using PragmaLogQueryPath
		return;
	}
	// log query path is set: log the query
	log_query_writer->WriteData((const_data_ptr_t)query.c_str(), query.size());
	log_query_writer->WriteData((const_data_ptr_t) "\n", 1);
	log_query_writer->Flush();
}

unique_ptr<QueryResult> ClientContext::Query(unique_ptr<SQLStatement> statement, bool allow_stream_result) {
	auto lock = LockContext();
	LogQueryInternal(*lock, statement->query.substr(statement->stmt_location, statement->stmt_length));

	vector<unique_ptr<SQLStatement>> statements;
	statements.push_back(move(statement));

	return RunStatements(*lock, query, statements, allow_stream_result);
}

unique_ptr<QueryResult> ClientContext::Query(const string &query, bool allow_stream_result) {
	printf("unique_ptr<QueryResult> ClientContext::Query\n");
	auto lock = LockContext();          // create lock
	LogQueryInternal(*lock, query); // write log file if its enabled. Put query inside also

	vector<unique_ptr<SQLStatement>> statements;
	try {
		InitialCleanup(*lock);  //! Cleanup any open results and reset the interrupted flag
		// parse the query and transform it into a set of statements
		statements = ParseStatementsInternal(*lock, query);
	} catch (std::exception &ex) {
		return make_unique<MaterializedQueryResult>(ex.what());
	}

	if (statements.empty()) {
		// no statements, return empty successful result
		return make_unique<MaterializedQueryResult>(StatementType::INVALID_STATEMENT);
	}

	return RunStatements(*lock, query, statements, allow_stream_result);
}


void ClientContext::Interrupt() {
	interrupted = true;
}

void ClientContext::EnableProfiling() {
	auto lock = LockContext();
	profiler.Enable();
}

void ClientContext::DisableProfiling() {
	auto lock = LockContext();
	profiler.Disable();
}

string ClientContext::VerifyQuery(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement) {
	printf("string ClientContext::VerifyQuery\n");
	D_ASSERT(statement->type == StatementType::SELECT_STATEMENT);
	// aggressive query verification

	// the purpose of this function is to test correctness of otherwise hard to test features:
	// Copy() of statements and expressions
	// Serialize()/Deserialize() of expressions
	// Hash() of expressions
	// Equality() of statements and expressions
	// Correctness of plans both with and without optimizers
	// Correctness of plans both with and without parallelism

	// copy the statement
	auto select_stmt = (SelectStatement *)statement.get();
	auto copied_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());
	auto unoptimized_stmt = unique_ptr_cast<SQLStatement, SelectStatement>(select_stmt->Copy());

	BufferedSerializer serializer;
	select_stmt->Serialize(serializer);
	BufferedDeserializer source(serializer);
	auto deserialized_stmt = SelectStatement::Deserialize(source);
	// all the statements should be equal
	D_ASSERT(copied_stmt->Equals(statement.get()));
	D_ASSERT(deserialized_stmt->Equals(statement.get()));
	D_ASSERT(copied_stmt->Equals(deserialized_stmt.get()));

	// now perform checking on the expressions
#ifdef DEBUG
	auto &orig_expr_list = select_stmt->node->GetSelectList();
	auto &de_expr_list = deserialized_stmt->node->GetSelectList();
	auto &cp_expr_list = copied_stmt->node->GetSelectList();
	D_ASSERT(orig_expr_list.size() == de_expr_list.size() && cp_expr_list.size() == de_expr_list.size());
	for (idx_t i = 0; i < orig_expr_list.size(); i++) {
		// run the ToString, to verify that it doesn't crash
		orig_expr_list[i]->ToString();
		// check that the expressions are equivalent
		D_ASSERT(orig_expr_list[i]->Equals(de_expr_list[i].get()));
		D_ASSERT(orig_expr_list[i]->Equals(cp_expr_list[i].get()));
		D_ASSERT(de_expr_list[i]->Equals(cp_expr_list[i].get()));
		// check that the hashes are equivalent too
		D_ASSERT(orig_expr_list[i]->Hash() == de_expr_list[i]->Hash());
		D_ASSERT(orig_expr_list[i]->Hash() == cp_expr_list[i]->Hash());
	}
	// now perform additional checking within the expressions
	for (idx_t outer_idx = 0; outer_idx < orig_expr_list.size(); outer_idx++) {
		auto hash = orig_expr_list[outer_idx]->Hash();
		for (idx_t inner_idx = 0; inner_idx < orig_expr_list.size(); inner_idx++) {
			auto hash2 = orig_expr_list[inner_idx]->Hash();
			if (hash != hash2) {
				// if the hashes are not equivalent, the expressions should not be equivalent
				D_ASSERT(!orig_expr_list[outer_idx]->Equals(orig_expr_list[inner_idx].get()));
			}
		}
	}
#endif

	// disable profiling if it is enabled
	bool profiling_is_enabled = profiler.IsEnabled();
	if (profiling_is_enabled) {
		profiler.Disable();
	}

	// see below
	auto statement_copy_for_explain = select_stmt->Copy();

	unique_ptr<MaterializedQueryResult> original_result =
	                                        make_unique<MaterializedQueryResult>(StatementType::SELECT_STATEMENT),
	                                    copied_result =
	                                        make_unique<MaterializedQueryResult>(StatementType::SELECT_STATEMENT),
	                                    deserialized_result =
	                                        make_unique<MaterializedQueryResult>(StatementType::SELECT_STATEMENT),
	                                    unoptimized_result =
	                                        make_unique<MaterializedQueryResult>(StatementType::SELECT_STATEMENT);

	// execute the original statement
	try {
		auto result = RunStatementInternal(lock, query, move(statement), false);
		original_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (std::exception &ex) {
		original_result->error = ex.what();
		original_result->success = false;
		interrupted = false;
	}

	// check explain, only if q does not already contain EXPLAIN
	if (original_result->success) {
		auto explain_q = "EXPLAIN " + query;
		auto explain_stmt = make_unique<ExplainStatement>(move(statement_copy_for_explain));
		try {
			RunStatementInternal(lock, explain_q, move(explain_stmt), false);
		} catch (std::exception &ex) {
			return "EXPLAIN failed but query did not (" + string(ex.what()) + ")";
		}
	}

	// now execute the copied statement
	try {
		auto result = RunStatementInternal(lock, query, move(copied_stmt), false);
		copied_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (std::exception &ex) {
		copied_result->error = ex.what();
		copied_result->success = false;
		interrupted = false;
	}
	// now execute the deserialized statement
	try {
		auto result = RunStatementInternal(lock, query, move(deserialized_stmt), false);
		deserialized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (std::exception &ex) {
		deserialized_result->error = ex.what();
		deserialized_result->success = false;
		interrupted = false;
	}
	// now execute the unoptimized statement
	enable_optimizer = false;
	try {
		auto result = RunStatementInternal(lock, query, move(unoptimized_stmt), false);
		unoptimized_result = unique_ptr_cast<QueryResult, MaterializedQueryResult>(move(result));
	} catch (std::exception &ex) {
		unoptimized_result->error = ex.what();
		unoptimized_result->success = false;
		interrupted = false;
	}
	enable_optimizer = true;

	if (profiling_is_enabled) {
		profiler.Enable();
	}

	// now compare the results
	// the results of all runs should be identical
	vector<unique_ptr<MaterializedQueryResult>> results;
	results.push_back(move(copied_result));
	results.push_back(move(deserialized_result));
	results.push_back(move(unoptimized_result));
	vector<string> names = {"Copied Result", "Deserialized Result", "Unoptimized Result"};
	for (idx_t i = 0; i < results.size(); i++) {
		if (original_result->success != results[i]->success) {
			string result = names[i] + " differs from original result!\n";
			result += "Original Result:\n" + original_result->ToString();
			result += names[i] + ":\n" + results[i]->ToString();
			return result;
		}
		if (!original_result->collection.Equals(results[i]->collection)) {
			string result = names[i] + " differs from original result!\n";
			result += "Original Result:\n" + original_result->ToString();
			result += names[i] + ":\n" + results[i]->ToString();
			return result;
		}
	}

	return "";
}

void ClientContext::RegisterFunction(CreateFunctionInfo *info) {
	RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*this);
		catalog.CreateFunction(*this, info);
	});
}

void ClientContext::RunFunctionInTransactionInternal(ClientContextLock &lock, const std::function<void(void)> &fun,
                                                     bool requires_valid_transaction) {
	if (requires_valid_transaction && transaction.HasActiveTransaction() &&
	    transaction.ActiveTransaction().IsInvalidated()) {
		throw Exception("Failed: transaction has been invalidated!");
	}
	// check if we are on AutoCommit. In this case we should start a transaction
	if (transaction.IsAutoCommit()) {
		transaction.BeginTransaction();
	}
	try {
		fun();
	} catch (StandardException &ex) {
		if (transaction.IsAutoCommit()) {
			transaction.Rollback();
		}
		throw;
	} catch (std::exception &ex) {
		if (transaction.IsAutoCommit()) {
			transaction.Rollback();
		} else {
			ActiveTransaction().Invalidate();
		}
		throw;
	}
	if (transaction.IsAutoCommit()) {
		transaction.Commit();
	}
}

void ClientContext::RunFunctionInTransaction(const std::function<void(void)> &fun, bool requires_valid_transaction) {
	auto lock = LockContext();
	RunFunctionInTransactionInternal(*lock, fun, requires_valid_transaction);
}

unique_ptr<TableDescription> ClientContext::TableInfo(const string &schema_name, const string &table_name) {
	unique_ptr<TableDescription> result;
	RunFunctionInTransaction([&]() {
		// obtain the table info
		auto &catalog = Catalog::GetCatalog(*this);
		auto table = catalog.GetEntry<TableCatalogEntry>(*this, schema_name, table_name, true);
		if (!table) {
			return;
		}
		// write the table info to the result
		result = make_unique<TableDescription>();
		result->schema = schema_name;
		result->table = table_name;
		for (auto &column : table->columns) {
			result->columns.emplace_back(column.name, column.type);
		}
	});
	return result;
}

void ClientContext::Append(TableDescription &description, DataChunk &chunk) {
	RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*this);
		auto table_entry = catalog.GetEntry<TableCatalogEntry>(*this, description.schema, description.table);
		// verify that the table columns and types match up
		if (description.columns.size() != table_entry->columns.size()) {
			throw Exception("Failed to append: table entry has different number of columns!");
		}
		for (idx_t i = 0; i < description.columns.size(); i++) {
			if (description.columns[i].type != table_entry->columns[i].type) {
				throw Exception("Failed to append: table entry has different number of columns!");
			}
		}
		table_entry->storage->Append(*table_entry, *this, chunk);
	});
}

void ClientContext::TryBindRelation(Relation &relation, vector<ColumnDefinition> &result_columns) {
	RunFunctionInTransaction([&]() {
		// bind the expressions
		auto binder = Binder::CreateBinder(*this);
		auto result = relation.Bind(*binder);
		D_ASSERT(result.names.size() == result.types.size());
		for (idx_t i = 0; i < result.names.size(); i++) {
			result_columns.emplace_back(result.names[i], result.types[i]);
		}
	});
}

unique_ptr<QueryResult> ClientContext::Execute(const shared_ptr<Relation> &relation) {
	auto lock = LockContext();
	string query;
	if (query_verification_enabled) {
		// run the ToString method of any relation we run, mostly to ensure it doesn't crash
		relation->ToString();
		if (relation->IsReadOnly()) {
			// verify read only statements by running a select statement
			auto select = make_unique<SelectStatement>();
			select->node = relation->GetQueryNode();
			RunStatement(*lock, query, move(select), false);
		}
	}
	auto &expected_columns = relation->Columns();
	auto relation_stmt = make_unique<RelationStatement>(relation);
	auto result = RunStatement(*lock, query, move(relation_stmt), false);
	if (!result->success) {
		return result;
	}
	// verify that the result types and result names of the query match the expected result types/names
	if (result->types.size() == expected_columns.size()) {
		bool mismatch = false;
		for (idx_t i = 0; i < result->types.size(); i++) {
			if (result->types[i] != expected_columns[i].type || result->names[i] != expected_columns[i].name) {
				mismatch = true;
				break;
			}
		}
		if (!mismatch) {
			// all is as expected: return the result
			return result;
		}
	}
	// result mismatch
	string err_str = "Result mismatch in query!\nExpected the following columns: ";
	for (idx_t i = 0; i < expected_columns.size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += expected_columns[i].name + " " + expected_columns[i].type.ToString();
	}
	err_str += "]\nBut result contained the following: ";
	for (idx_t i = 0; i < result->types.size(); i++) {
		err_str += i == 0 ? "[" : ", ";
		err_str += result->names[i] + " " + result->types[i].ToString();
	}
	err_str += "]";
	return make_unique<MaterializedQueryResult>(err_str);
}

} // namespace duckdb
