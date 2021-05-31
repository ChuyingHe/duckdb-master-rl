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
    //! Represents a node in the join plan
    struct JoinNode {
        JoinRelationSet *set;
        NeighborInfo *info;
        idx_t cardinality;
        idx_t cost;
        JoinNode *left;
        JoinNode *right;

        //! Create a leaf node in the join tree
        JoinNode(JoinRelationSet *set, idx_t cardinality)
                : set(set), info(nullptr), cardinality(cardinality), cost(cardinality), left(nullptr), right(nullptr) {
        }
        //! Create an intermediate node in the join tree
        JoinNode(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right, idx_t cardinality,
                 idx_t cost)
                : set(set), info(info), cardinality(cardinality), cost(cost), left(left), right(right) {
        }
    };

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
    unordered_map<JoinRelationSet *, unique_ptr<JoinNode>> plans;
    vector<unique_ptr<Expression>> filters;
    vector<unique_ptr<FilterInfo>> filter_infos;
    expression_map_t<vector<FilterInfo *>> equivalence_sets;

    bool ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings);
    bool ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                              LogicalOperator *parent = nullptr);
    JoinNode *EmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info);
    bool TryEmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info);

    bool EnumerateCmpRecursive(JoinRelationSet *left, JoinRelationSet *right, unordered_set<idx_t> exclusion_set);
    bool EmitCSG(JoinRelationSet *node);
    bool EnumerateCSGRecursive(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set);
    unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode *node);
    void GenerateCrossProducts();
    void SolveJoinOrder();
    bool SolveJoinOrderExactly();
    void SolveJoinOrderApproximately();

    unique_ptr<LogicalOperator> ResolveJoinConditions(unique_ptr<LogicalOperator> op);
    std::pair<JoinRelationSet *, unique_ptr<LogicalOperator>>
    GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode *node);
};

}





#endif // DUCKDB_RL_JOIN_ORDER_OPTIMIZER_HPP
