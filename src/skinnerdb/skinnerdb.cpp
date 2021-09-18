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

#include <algorithm>

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
    std::string::size_type pos = query.find('.sql');
    auto job_file_sql = query.substr(2, pos-1);

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

    //bool found_optimal_join_order = false;
    unique_ptr<LogicalOperator> rl_plan;

    double prev_duration, current_duration, prev_reward, current_reward, max_duration, min_duration, max_min;
    std::vector<double> duration_vec;

    //printf("----- simulation----- ");
    while (true){
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

        context.ContinueJoin(lock, query, result, move(bound_values), allow_stream_result, simulation_count);
        current_duration = timer_simulation.check();

        if (simulation_count > 0) {
            if (simulation_count < 5) {
                duration_vec.push_back(current_duration);

            } else if (simulation_count == 5){
                duration_vec.push_back(current_duration);
                max_duration = *max_element(duration_vec.begin(), duration_vec.end());
                min_duration = *min_element(duration_vec.begin(), duration_vec.end());
                max_min = max_duration-min_duration;

                std::cout<<"max_min"<<max_min<<", min = "<<min_duration<<"\n";
            }
            else {
                current_reward = max_min/(current_duration-min_duration);
                // because the Min and Max are NOT necessarily accurate
                if (current_reward<0) {
                    rl_optimizer.Backpropogation(max_duration*2);
                } else {
                    rl_optimizer.Backpropogation(current_reward);
                }
                //rl_optimizer.Backpropogation(current_reward);
            }
        }

        if (chosen_node) {
            if (same_order_count>=2 || simulation_count>=10) {
            //if (simulation_count>=1000) {
                break;
            } else {
                if (previous_order_of_relations == chosen_node->join_node->order_of_relations) {
                    same_order_count +=1;
                } else {
                    same_order_count = 1;
                    previous_order_of_relations = chosen_node->join_node->order_of_relations;
                }
            }
        }

        // current_duration is total time that consumes by current simulation - backprop doesnt count
        std::cout << job_file_sql<<",Simulation," << simulation_count << "," << chosen_node->join_node->order_of_relations << "," << current_duration <<","<< chosen_node->reward << "\n";

        simulation_count += 1;
    }
    double duration_prep = timer_prep.check();

    // Execution
    Timer timer_execution;
    //printf("----- execution----- \n");
    enable_rl_join_order_optimizer = false;
    vector<Value> bound_values;
    query_result = context.ContinueJoin(lock, query, result, move(bound_values), allow_stream_result, simulation_count);

    double duration_exec = timer_execution.check();

    std::cout<<job_file_sql<<",SkinnerDB," << simulation_count << ","
             << chosen_node->join_node->order_of_relations << "," << duration_prep << ","
             << duration_exec <<",";


    //TODO: add up all query_result
    return query_result;
}

}
