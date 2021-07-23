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
NodeForUCT* root_node_for_uct;     //initialize the root of the tree
NodeForUCT* chosen_node;

SkinnerDB::SkinnerDB(QueryProfiler &profiler, ClientContext& context): profiler(move(profiler)), context(context) {
}

void SkinnerDB::runStatement(shared_ptr<PreparedStatementData> plan){
    //1. execute Query
    // measure: how long does it take to EXECUTE one CHUNK
    //2. update

}
//shared_ptr<PreparedStatementData>

/*
void move_copy(unique_ptr<LogicalOperator> copy_of_plan){
    std::cout<<" -> moved copy of plan -> \n";
}
*/

unique_ptr<QueryResult> SkinnerDB::CreateAndExecuteStatement(ClientContextLock &lock, const string &query,
                                                          unique_ptr<SQLStatement> statement, bool allow_stream_result){

    auto query_result = unique_ptr<QueryResult>();

    printf("SkinnerDB::CreatePreparedStatement\n");
    StatementType statement_type = statement->type;
    shared_ptr<PreparedStatementData> result = make_shared<PreparedStatementData>(statement_type);

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

    // pre-optimizer
    profiler.StartPhase("pre_optimizer");
    Optimizer optimizer(*planner.binder, context);
    plan = optimizer.OptimizeBeforeRLOptimizer(move(plan));
    D_ASSERT(plan);
    profiler.EndPhase();

    root_node_for_uct = new NodeForUCT{nullptr, nullptr, 0, 0.0, nullptr};

    int loop_count = 0;
    // FIXME: fixme
    while (loop_count < 10) {
    //while (!context.query_finished) {
        std::cout<<" 🦄️ loop_count = " << loop_count <<"\n";
        loop_count += 1;

        profiler.StartPhase("rl_optimizer");
        RLJoinOrderOptimizer rl_optimizer(context);

        // DEEP COPY
        auto copy = plan->clone();
        std::cout<<"address of copy:" << copy << std::endl;
        std::cout<<"address of plan:" << plan<< std::endl;
        //move_copy(move(copy));
        unique_ptr<LogicalOperator> rl_plan = rl_optimizer.Optimize(move(copy));
        std::cout<<"address of copy:" << copy << std::endl;
        std::cout<<"address of plan:" << plan<< std::endl;



        /*profiler.EndPhase();
        profiler.StartPhase("physical_planner");
        // now convert logical query plan into a physical query plan
        PhysicalPlanGenerator physical_planner(context);
        auto physical_plan = physical_planner.CreatePlan(move(rl_plan));    // CHECK HERE, erst kopieren dann
        // auto physical_plan = physical_planner.CreatePlanRL(plan.get());
        profiler.EndPhase();

        result->plan = move(physical_plan);
        //auto prepared = result;

        vector<Value> bound_values;
        Timer timer;
        // query_result = context.ExecutePreparedStatementWithRLOptimizer(lock, query, result.get(), move(bound_values), allow_stream_result);
        query_result = context.ExecutePreparedStatementWithRLOptimizer(lock, query, move(result), move(bound_values), allow_stream_result);

        double reward = timer.check();

        rl_optimizer.RewardUpdate(reward);*/

    }

    //TODO: add up all query_result
    return query_result;
}

/*
 * input: unique_ptr<SQLStatement> statement, QueryProfiler profiler, ClientContext &context
 * output: the final result: unique_ptr<QueryResult>
 * */
/*
unique_ptr<QueryResult> SkinnerDB::Execute(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement, bool allow_stream_result) {
    printf("SkinnerDB::Preprocessing");







}*/

}
