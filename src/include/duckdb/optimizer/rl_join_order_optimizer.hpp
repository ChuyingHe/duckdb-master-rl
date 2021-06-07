//
// Created by Chuying He on 28/05/2021.
//

#ifndef DUCKDB_RL_JOIN_ORDER_OPTIMIZER_HPP
#define DUCKDB_RL_JOIN_ORDER_OPTIMIZER_HPP

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

#include <functional>

namespace duckdb {

class RLJoinOrderOptimizer {
public:
    explicit RLJoinOrderOptimizer(ClientContext &context) : context(context) { /*constructor, explicit prevent other type of parameter*/
    }
    unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan); /*only public function -  THE ENTRANCE*/

private:
    ClientContext &context;
    idx_t pairs = 0;

    vector<unique_ptr<SingleJoinRelation>> relations;
    unordered_map<idx_t, idx_t> relation_mapping;
    JoinRelationSetManager set_manager;
    QueryGraph query_graph;
    unordered_map<JoinRelationSet *, unique_ptr<JoinOrderOptimizer::JoinNode>> plans;   // includes all the relations, to return
    // FIXME: unordered_map<unordered_set<idx_t>, unique_ptr<JoinOrderOptimizer::JoinNode>> final_plans;
    std::string order_of_rel = "";
    unordered_map<std::string, unique_ptr<JoinOrderOptimizer::JoinNode>> rl_plans;


    vector<unique_ptr<Expression>> filters;
    vector<unique_ptr<FilterInfo>> filter_infos;
    expression_map_t<vector<FilterInfo *>> equivalence_sets;

    bool ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings);
    bool ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                              LogicalOperator *parent = nullptr);
    JoinOrderOptimizer::JoinNode *EmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info);
    bool TryEmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info);

    bool EnumerateCmpRecursive(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> exclusion_set);
    bool EmitCSG(JoinRelationSet *node);
    bool EnumerateCSGRecursive(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set);
    unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinOrderOptimizer::JoinNode *node);
    void GenerateCrossProducts();
    void SolveJoinOrder();
    bool SolveJoinOrderExactly();
    void SolveJoinOrderApproximately();

    unique_ptr<LogicalOperator> UCTChoice();
    void GeneratePlans();
    void RewardUpdate();
    void ContinueJoin(unique_ptr<LogicalOperator> plan, std::chrono::seconds duration);
    void RestoreState();
    void BackupState();
    void IterateTree(JoinRelationSet* union_set, unordered_set<idx_t> exclusion_set);

    unique_ptr<LogicalOperator> ResolveJoinConditions(unique_ptr<LogicalOperator> op);
    std::pair<JoinRelationSet *, unique_ptr<LogicalOperator>>
    GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinOrderOptimizer::JoinNode *node);
};

}





#endif // DUCKDB_RL_JOIN_ORDER_OPTIMIZER_HPP
