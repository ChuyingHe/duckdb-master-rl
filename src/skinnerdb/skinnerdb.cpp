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

    unique_ptr<LogicalOperator> rl_plan;

    double prev_duration, current_duration, prev_reward, current_reward, delta;

    //printf("----- simulation----- ");
    while (true){
        Timer timer_simulation;

        auto copy = plan->clone();

        RLJoinOrderOptimizer rl_optimizer(context);
        if (simulation_count == 0) {
            rl_optimizer.plans.clear();
            chosen_node = nullptr;
        }
        rl_plan = rl_optimizer.SelectJoinOrder(move(copy), simulation_count);

        profiler.StartPhase("physical_planner");
        PhysicalPlanGenerator physical_planner(context);
        auto physical_plan = physical_planner.CreatePlan(move(rl_plan));
        profiler.EndPhase();

        result->plan = move(physical_plan); //only part in result that need to be update
        vector<Value> bound_values;

        context.ContinueJoin(lock, query, result, move(bound_values), allow_stream_result, simulation_count);
        current_duration = timer_simulation.check();
        if (simulation_count > 0) {
            rl_optimizer.Backpropogation((-1)*current_duration);
        }

        if (chosen_node) {
            if (same_order_count>=5 || simulation_count>=10) {
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

        std::cout <<"Simulation," << simulation_count << "," << chosen_node->join_node->order_of_relations <<", reward = "<< chosen_node->reward<<", visit = "<< chosen_node->num_of_visits<< ", avg="<< chosen_node->reward/chosen_node->num_of_visits << "\n";

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

    std::cout <<"SkinnerDB," << simulation_count << "," << chosen_node->join_node->order_of_relations << "," << duration_prep << ","<< duration_exec <<",";
    return query_result;
}

}
