//
// Created by Chuying He on 26/06/2021.
//
/*
 * Entrance of SkinnerDB Algorithm
 */
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

SkinnerDB::SkinnerDB(QueryProfiler &profiler, ClientContext& context): profiler(profiler), context(context) {
}

unique_ptr<QueryResult> SkinnerDB::CreateAndExecuteStatement(ClientContextLock &lock, const string &query,
                                                          unique_ptr<SQLStatement> statement, bool allow_stream_result){
    Timer timer_prep_preoptimizer;
    //printf("unique_ptr<QueryResult> SkinnerDB::CreateAndExecuteStatement\n");
    // 1. Preparation
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
    root_node_for_uct = new NodeForUCT{nullptr, 0, 0.0, nullptr};
    // 5. Execute query with different Join-order
    chosen_node = nullptr;
    idx_t sample_count = 0;

    double duration_prep_preoptimizer = timer_prep_preoptimizer.check();

    while (sample_count < 1) {
    //while (true) {
        //std::cout<< "sample_count = " <<sample_count <<"\n";
        Timer timer_prep_join_order;

        shared_ptr<PreparedStatementData> result = make_shared<PreparedStatementData>(statement_type);
        result->read_only = planner.read_only;
        result->requires_valid_transaction = planner.requires_valid_transaction;
        result->allow_stream_result = planner.allow_stream_result;
        result->names = planner.names;
        result->types = planner.types;
        result->value_map = move(planner.value_map);
        result->catalog_version = Transaction::GetTransaction(context).catalog_version;

        // 5.2 Optimize plan in RL-Optimizer
        auto copy = plan->clone();  // Clone plan for next iteration

        RLJoinOrderOptimizer rl_optimizer(context);
        if (sample_count == 0) {
            rl_optimizer.plans.clear();
            chosen_node = nullptr;
        }
        unique_ptr<LogicalOperator> rl_plan = rl_optimizer.Selection(move(copy), sample_count);

        // 5.3 Create physical plan
        profiler.StartPhase("physical_planner");
        // now convert logical query plan into a physical query plan
        PhysicalPlanGenerator physical_planner(context);
        auto physical_plan = physical_planner.CreatePlan(move(rl_plan));
        profiler.EndPhase();

        // 5.4 Execute optimized plan + Update reward
        result->plan = move(physical_plan);
        vector<Value> bound_values;

        double duration_prep_join_order = timer_prep_join_order.check();
        //std::cout <<"time_preparation = " <<duration_preparation <<", ";

        Timer timer_execution;
        //TODO: here we need info of the Progress 🐈
        query_result = context.ExecutePreparedStatementWithRLOptimizer(lock, query, move(result), move(bound_values), allow_stream_result);
        double duration_execution = timer_execution.check();

        //Timer timer_prep_backprop;
        rl_optimizer.RewardUpdate((-1)*duration_execution);
        //double duration_prep_backprop = timer_prep_backprop.check();

        std::string::size_type pos = query.find('.sql');
        auto job_file_sql = query.substr(2, pos-1);
        if (chosen_node) {
            /*if (previous_order_of_relations == chosen_node->join_node->order_of_relations) {
                same_order_count +=1;
                if (same_order_count>=5 || sample_count==99) {
                    //std::cout<<"final plan found in loop "<< sample_count << "\n";
                    double time_prep = duration_prep_preoptimizer + duration_prep_join_order;
                    std::cout   << job_file_sql << ", optimizer = RL Optimizer, loop = " << sample_count << ", join_order = "
                                << chosen_node->join_node->order_of_relations << ", reward = " << chosen_node->reward << ", num_of_visits = "
                                << chosen_node->num_of_visits << ", time_preparation = " << time_prep << ", time_execution = "
                                << duration_execution <<", time_total = "<< duration_execution+ time_prep<<"\n";
                    break;
                }
            } else {
                same_order_count = 1;
                previous_order_of_relations = chosen_node->join_node->order_of_relations;
            }*/
            double time_prep = duration_prep_preoptimizer + duration_prep_join_order;
            std::cout   << job_file_sql << ", optimizer = RL Optimizer, loop = " << sample_count << ", join_order = "
                        << chosen_node->join_node->order_of_relations << ", reward = " << chosen_node->reward << ", num_of_visits = "
                        << chosen_node->num_of_visits << ", time_preparation = " << time_prep << ", time_execution = "
                        << duration_execution <<", time_total = "<< duration_execution+ time_prep<<"\n";
        } else {
            std::cout<< "nothing to optimize \n";
        }

        sample_count += 1;
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
