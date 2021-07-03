//
// Created by Chuying He on 26/06/2021.
//

#include "duckdb/skinnerdb/skinnerdb.hpp"

#include "duckdb/planner/planner.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/skinnerdb/timer.hpp"


namespace duckdb {
SkinnerDB::SkinnerDB(QueryProfiler &profiler, ClientContext& context): profiler(move(profiler)), context(context) {
}

void SkinnerDB::runStatement(shared_ptr<PreparedStatementData> plan){
    //1. execute Query
    // measure: how long does it take to EXECUTE one CHUNK
    //2. update



}


shared_ptr<PreparedStatementData> SkinnerDB::CreatePreparedStatement(ClientContextLock &lock, const string &query,
                                                          unique_ptr<SQLStatement> statement, RLJoinOrderOptimizer rl_optimizer){
    printf("SkinnerDB::CreatePreparedStatement\n");
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
    plan = optimizer.OptimizeWithRLOptimizer(move(plan), move(rl_optimizer));
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
    // QueryResult final_result(statement->type, statement.t);

    if (!context.query_finished) {  //TODO: fix this??
        RLJoinOrderOptimizer rl_optimizer(context);

        auto prepared = CreatePreparedStatement(lock, query, move(statement), move(rl_optimizer));
        vector<Value> bound_values;

        Timer timer;
        auto result = context.ExecutePreparedStatementWithRLOptimizer(lock, query, move(prepared), move(bound_values), allow_stream_result);;
        double reward = timer.check();
        rl_optimizer.RewardUpdate(reward);

    }


}

}