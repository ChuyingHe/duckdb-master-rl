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

#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/optimizer/filter_pullup.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/regex_range_filter.hpp"
#include "duckdb/optimizer/in_clause_rewriter.hpp"


namespace duckdb {
NodeForUCT* root_node_for_uct;     //initialize the root of the tree
NodeForUCT* chosen_node;

//SkinnerDB::SkinnerDB(QueryProfiler &profiler, ClientContext& context): profiler(profiler), context(context) {}
SkinnerDB::SkinnerDB(QueryProfiler& profiler, ClientContext& context, ClientContextLock& lock, const string& query,
                     unique_ptr<SQLStatement> statement, bool allow_stream_result) : profiler(profiler), context(context),
                     lock(lock), query(query), statement(move(statement)), allow_stream_result(allow_stream_result), planner(Planner(context)) {
}

unique_ptr<LogicalOperator> SkinnerDB::Preprocessing() {
    //printf("SkinnerDB::Preprocessing\n");
    profiler.StartPhase("planner");
    planner.CreatePlan(move(statement));    // turn STATEMENT to PLAN: update this->planner: plan, names, types
    D_ASSERT(planner.plan);
    profiler.EndPhase();

    auto plan = move(planner.plan);
    profiler.StartPhase("pre_optimizer");
    Optimizer optimizer(*planner.binder, context);
    plan = optimizer.OptimizeForRL(move(plan)); // pre-optimizer that written by DuckDB
    D_ASSERT(plan);
    profiler.EndPhase();

    return plan;
}

unique_ptr<QueryResult> SkinnerDB::CreateAndExecuteStatement(){
    Timer timer_prep;

    auto query_result = unique_ptr<QueryResult>();
    StatementType statement_type = statement->type;
    auto plan = Preprocessing();

    root_node_for_uct = new NodeForUCT{nullptr, 0, 0.0, nullptr};
    chosen_node = nullptr;
    idx_t simulation_count = 0;

    shared_ptr<PreparedStatementData> result = make_shared<PreparedStatementData>(statement_type);
    result->read_only = planner.read_only;
    result->requires_valid_transaction = planner.requires_valid_transaction;
    result->allow_stream_result = planner.allow_stream_result;
    result->names = planner.names;
    result->types = planner.types;
    result->value_map = move(planner.value_map);
    result->catalog_version = Transaction::GetTransaction(context).catalog_version;

    bool found_optimal_join_order = false;
    unique_ptr<LogicalOperator> rl_plan;

    printf("----- simulation----- \n");
    while (!found_optimal_join_order) {  //️ 🐈 simulation_count = executed_chunk
        Timer timer_simulation;

        // 5.2 Optimize plan in RL-Optimizer
        auto copy = plan->clone();  // Clone plan for next iteration

        RLJoinOrderOptimizer rl_optimizer(context);
        if (simulation_count == 0) {
            rl_optimizer.plans.clear();
            chosen_node = nullptr;
        }
        rl_plan = rl_optimizer.SelectJoinOrder(move(copy), simulation_count);

        // 5.3 Create physical plan
        profiler.StartPhase("physical_planner");
        // now convert logical query plan into a physical query plan
        PhysicalPlanGenerator physical_planner(context);
        auto physical_plan = physical_planner.CreatePlan(move(rl_plan));
        profiler.EndPhase();

        // 5.4 Execute optimized plan + Update reward
        result->plan = move(physical_plan); //only part in result that need to be update
        vector<Value> bound_values;


        //TODO: here we need info of the Progress 🐈
        query_result = context.ContinueJoin(lock, query, result, move(bound_values), allow_stream_result, simulation_count);
        double duration_sim = timer_simulation.check();

        rl_optimizer.RewardUpdate((-1)*duration_sim);

        if (chosen_node) {
            if (previous_order_of_relations == chosen_node->join_node->order_of_relations) {
                same_order_count +=1;
                if (same_order_count>=2) {
                    found_optimal_join_order = true;
                }
                /*
                if (same_order_count>=5 || sample_count==99) {
                    //std::cout<<"final plan found in loop "<< sample_count << "\n";
                    double time_prep = duration_prep_preoptimizer + duration_prep_join_order;
                    std::cout   << job_file_sql << ", optimizer = RL Optimizer, loop = " << sample_count << ", join_order = "
                                << chosen_node->join_node->order_of_relations << ", reward = " << chosen_node->reward << ", num_of_visits = "
                                << chosen_node->num_of_visits << ", time_preparation = " << time_prep << ", time_execution = "
                                << duration_execution <<", time_total = "<< duration_execution+ time_prep<<"\n";
                    break;
                }*/
            } else {
                same_order_count = 1;
                previous_order_of_relations = chosen_node->join_node->order_of_relations;
            }
            //double time_prep = duration_prep_preoptimizer + duration_prep_join_order;
            /*double time_prep =  duration_prep_join_order;
            std::cout << "optimizer = RL Optimizer, loop = " << simulation_count << ", join_order = "
                        << chosen_node->join_node->order_of_relations << ", reward = " << chosen_node->reward << ", num_of_visits = "
                        << chosen_node->num_of_visits << ", time_preparation = " << time_prep << ", time_execution = "
                        << duration_execution <<", time_total = "<< duration_execution+ time_prep<<"\n";*/
            std::cout << "optimizer = RL Optimizer, loop = " << simulation_count << ", join_order = "
                      << chosen_node->join_node->order_of_relations << ", reward = " << chosen_node->reward <<"\n";
        } else {
            std::cout<< "nothing to optimize \n";
        }

        simulation_count += 1;
    }
    double duration_prep = timer_prep.check();

    // Execution
    Timer timer_execution;

    printf("----- execution----- \n");
    enable_rl_join_order_optimizer = false;
    vector<Value> bound_values;
    query_result = context.ContinueJoin(lock, query, result, move(bound_values), allow_stream_result, simulation_count);

    double duration_exec = timer_execution.check();

    // std::cout<<"FINAL join_order = " << chosen_node->join_node->order_of_relations <<"\n";
    std::string::size_type pos = query.find('.sql');
    auto job_file_sql = query.substr(2, pos-1);
    /*std::cout << job_file_sql <<",optimizer=SkinnerDB,loop=" << simulation_count << ",join_order="
              << chosen_node->join_node->order_of_relations << ",time_preparation=" << duration_prep << ",time_execution="
              << duration_exec <<",";*/
    std::cout << job_file_sql <<",SkinnerDB," << simulation_count << ","
              << chosen_node->join_node->order_of_relations << "," << duration_prep << ","
              << duration_exec <<",";


    //TODO: add up all query_result
    return query_result;
}

}
