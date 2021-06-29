//
// Created by Chuying He on 26/06/2021.
//

#include "duckdb/skinnerdb/skinnerdb.hpp"

#include "duckdb/planner/planner.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/skinnerdb/rl_join_order_optimizer.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"



namespace duckdb {
SkinnerDB::SkinnerDB(QueryProfiler &profiler, ClientContext& context): profiler(move(profiler)), context(context) {}

void SkinnerDB::runStatement(shared_ptr<PreparedStatementData> plan){
    //1. execute Query

    //2. update

}


shared_ptr<PreparedStatementData> SkinnerDB::CreatePreparedStatement(ClientContextLock &lock, const string &query,
                                                          unique_ptr<SQLStatement> statement){
    printf("shared_ptr<PreparedStatementData> ClientContext::CreatePreparedStatement(ClientContextLock &lock, const string &query,\n");
    StatementType statement_type = statement->type;
    auto result = make_shared<PreparedStatementData>(statement_type);

    profiler.StartPhase("planner");
    Planner planner(context);
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
    result->catalog_version = Transaction::GetTransaction(context).catalog_version;

    profiler.StartPhase("optimizer");
    Optimizer optimizer(*planner.binder, context);
    plan = optimizer.Optimize(move(plan));
    D_ASSERT(plan);
    profiler.EndPhase();

    profiler.StartPhase("physical_planner");
    // now convert logical query plan into a physical query plan
    PhysicalPlanGenerator physical_planner(context);
    auto physical_plan = physical_planner.CreatePlan(move(plan));
    profiler.EndPhase();

    result->plan = move(physical_plan);

    return result;  // PreparedStatementData result which includes the PLAN
}

/*
 * input: unique_ptr<SQLStatement> statement, QueryProfiler profiler, ClientContext &context
 * output: the final result: unique_ptr<QueryResult>
 * */
unique_ptr<QueryResult> SkinnerDB::Execute(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement, bool allow_stream_result) {
    printf("SkinnerDB::Preprocessing");
    auto prepared = CreatePreparedStatement(lock, query, move(statement));

    /*StatementType statement_type = statement->type;
    // auto result = make_shared<PreparedStatementData>(statement_type);
    auto result = make_shared<PreparedStatementData>(statement_type);

    profiler.StartPhase("planner");
    Planner planner(context);
    planner.CreatePlan(move(statement));
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
    result->catalog_version = Transaction::GetTransaction(context).catalog_version;

    profiler.StartPhase("rl_optimizer");
    RLJoinOrderOptimizer rl_optimizer(context);
    //FIXME: INTERNAL Error: Assertion triggered in file "/Users/chuyinghe/Documents/duckdb-master-rl/src/optimizer/../skinnerdb/rl_join_order_optimizer.cpp" on line 551: filters.empty() && relations.empty()
    // caused by this infinitive loop
    //while (!finished) {
    plan = rl_optimizer.Optimize(move(plan));           // returns unique_ptr<LogicalOperator> by RewritePlan()
    D_ASSERT(plan);
    profiler.EndPhase();

    profiler.StartPhase("physical_planner");    // now convert logical query plan into a physical query plan
    PhysicalPlanGenerator physical_planner(context);
    auto physical_plan = physical_planner.CreatePlan(move(plan));
    profiler.EndPhase();

    result->plan = move(physical_plan);*/

    // return ExecutePreparedStatement(lock, query, move(prepared), move(bound_values), allow_stream_result);	    //return unique_ptr
    vector<Value> bound_values;

    // runStatement(move(prepared)); //shared_ptr<PreparedStatementData>
    //}

    return context.ExecutePreparedStatement(lock, query, move(prepared), move(bound_values), allow_stream_result);;
    // FIXME: Here should return unique_ptr<QueryResult> directly
}

}