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
unordered_map<JoinRelationSet *, unique_ptr<JoinOrderOptimizer::JoinNode>, Hasher, EqualFn> join_orders;

SkinnerDB::SkinnerDB(QueryProfiler &profiler, ClientContext& context): profiler(profiler), context(context) {
}

void SkinnerDB::runStatement(shared_ptr<PreparedStatementData> plan){
    //1. execute Query
    // measure: how long does it take to EXECUTE one CHUNK
    //2. update

}

void testfunc(unique_ptr<LogicalOperator> plan) {
    // printf("test func to remove one of the copy");
    //std::cout<< plan->GetName();
}

unique_ptr<QueryResult> SkinnerDB::CreateAndExecuteStatement(ClientContextLock &lock, const string &query,
                                                          unique_ptr<SQLStatement> statement, bool allow_stream_result){
    //printf("unique_ptr<QueryResult> SkinnerDB::CreateAndExecuteStatement\n");
    // reset
    root_node_for_uct = new NodeForUCT{nullptr, 0, 0.0, nullptr};
    join_orders.clear();
    chosen_node = nullptr;

    auto query_result = unique_ptr<QueryResult>();
    StatementType statement_type = statement->type;

    // 2. Create plan
    profiler.StartPhase("planner");
    Planner planner(context);
    planner.CreatePlan(move(statement));    // turn STATEMENT to PLAN: update this->planner: plan, names, types
    D_ASSERT(planner.plan);
    profiler.EndPhase();

    // 3. Optimize plan in pre-optimizer
    auto plan = move(planner.plan);
    profiler.StartPhase("pre_optimizer");
    Optimizer optimizer(*planner.binder, context);
    plan = optimizer.OptimizeBeforeRLOptimizer(move(plan));
    D_ASSERT(plan);
    profiler.EndPhase();

    // 4. Define node for reward update

    // first simulation to get the join_orders ------------------------------------------------
    printf("----------------------- first simulation to obtain all possible join orders: ----------------------- \n");
    shared_ptr<PreparedStatementData> result = make_shared<PreparedStatementData>(statement_type);
    result->read_only = planner.read_only;
    result->requires_valid_transaction = planner.requires_valid_transaction;
    result->allow_stream_result = planner.allow_stream_result;
    result->names = planner.names;
    result->types = planner.types;
    result->value_map = move(planner.value_map);
    result->catalog_version = Transaction::GetTransaction(context).catalog_version;

    auto copy = plan->clone();

    RLJoinOrderOptimizer rl_optimizer(context);
    rl_optimizer.plans.clear();
    unique_ptr<LogicalOperator> rl_plan = rl_optimizer.Optimize(move(copy));    //🎨

    profiler.StartPhase("physical_planner");
    PhysicalPlanGenerator physical_planner(context);
    auto physical_plan = physical_planner.CreatePlan(move(rl_plan));
    profiler.EndPhase();

    result->plan = move(physical_plan);
    vector<Value> bound_values;

    query_result = context.ExecutePreparedStatementWithRLOptimizer(lock, query, move(result), move(bound_values), allow_stream_result);

    std::string::size_type pos = query.find('.sql');
    auto job_file_sql = query.substr(2, pos-1);
    std::cout<<job_file_sql << " has  " <<join_orders.size()<<" possible join orders.\n";

    // ------------------------------------------------

    int loop_count = 0;
    printf("----------------------- following simulation to compare: ----------------------- \n");
    for (auto &it:join_orders){
        // chosen_node = it.second.get();
        chosen_node = new NodeForUCT{it.second.get(), 0, 0.0, nullptr};

        Timer timer_prep;
        shared_ptr<PreparedStatementData> result = make_shared<PreparedStatementData>(statement_type);
        result->read_only = planner.read_only;
        result->requires_valid_transaction = planner.requires_valid_transaction;
        result->allow_stream_result = planner.allow_stream_result;
        result->names = planner.names;
        result->types = planner.types;
        result->value_map = move(planner.value_map);
        result->catalog_version = Transaction::GetTransaction(context).catalog_version;
        auto copy = plan->clone();
        RLJoinOrderOptimizer rl_optimizer(context);
        unique_ptr<LogicalOperator> rl_plan = rl_optimizer.Optimize(move(copy));
        profiler.StartPhase("physical_planner");
        PhysicalPlanGenerator physical_planner(context);
        auto physical_plan = physical_planner.CreatePlan(move(rl_plan));
        profiler.EndPhase();
        result->plan = move(physical_plan);
        vector<Value> bound_values;
        double duration_prep = timer_prep.check();

        Timer timer_exec;
        query_result = context.ExecutePreparedStatementWithRLOptimizer(lock, query, move(result), move(bound_values), allow_stream_result);
        double duration_exec = timer_exec.check();


        std::string::size_type pos = query.find('.sql');
        auto job_file_sql = query.substr(2, pos-1);
        // sql,loop,join-order,prep_time,exec_time,total_execution
        std::cout<<job_file_sql<<","<<loop_count<<","<<chosen_node->join_node->order_of_relations<<","<<duration_prep<<","<<duration_exec<<","<<(duration_prep+duration_exec)<< "\n";

        loop_count += 1;
    }

    //Execution ---------------
    // chosen_node = new NodeForUCT{.get(), 0, 0.0, nullptr};
    enable_rl_join_order_optimizer = false;
    printf("----------------------- execution ----------------------- \n");
    result = make_shared<PreparedStatementData>(statement_type);
    result->read_only = planner.read_only;
    result->requires_valid_transaction = planner.requires_valid_transaction;
    result->allow_stream_result = planner.allow_stream_result;
    result->names = planner.names;
    result->types = planner.types;
    result->value_map = move(planner.value_map);
    result->catalog_version = Transaction::GetTransaction(context).catalog_version;

    physical_plan = physical_planner.CreatePlan(move(plan));
    result->plan = move(physical_plan);

    query_result = context.ExecutePreparedStatementWithRLOptimizer(lock, query, move(result), move(bound_values), allow_stream_result);


    // --------------
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
