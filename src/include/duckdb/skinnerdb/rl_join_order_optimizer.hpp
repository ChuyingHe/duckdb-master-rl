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
#include "duckdb/optimizer/join_order_optimizer.hpp"

#include <functional>

namespace duckdb {
    struct NodeForUCT {
        JoinRelationSet* relations;                             // from this->plans
        JoinOrderOptimizer::JoinNode* join_node;                // from this->plans
        int num_of_visits;
        double reward;
        NodeForUCT* parent;
        vector<NodeForUCT*> children;
        std::string order_of_relations;
        // vector<JoinRelationSet*> validChildren; do we need this?
        // NodeForUCT(): num_of_visits(0), reward(0.0) {}
    };

    extern NodeForUCT* root_node_for_uct;
    extern NodeForUCT* chosen_node; // to tranfer the reward in RewardUpdate

    class RLJoinOrderOptimizer {
    public:
        explicit RLJoinOrderOptimizer(ClientContext &context) : context(
                context) { /*constructor, explicit prevent other type of parameter*/
        }

        unique_ptr <LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan); /*only public function -  THE ENTRANCE*/
        // shared_ptr<LogicalOperator> Optimize(shared_ptr<LogicalOperator> plan);
        // LogicalOperator* Optimize(LogicalOperator* plan);

        void RewardUpdate(double reward);



    private:
        ClientContext &context;
        idx_t pairs = 0;
        int counter = 0;

        vector <unique_ptr<SingleJoinRelation>> relations;
        unordered_map <idx_t, idx_t> relation_mapping;
        JoinRelationSetManager set_manager;
        QueryGraph query_graph;
        unordered_map<JoinRelationSet *, unique_ptr<JoinOrderOptimizer::JoinNode>> plans;   // includes all the relations, to return

        //FIXME: delete this, only for debugging
        std::string order_of_rel = "";
        // unordered_map<JoinRelationSet *, unique_ptr <JoinOrderOptimizer::JoinNode>> rl_plans; // only include plans which includes all the relations
        // unordered_map<JoinRelationSet *, unique_ptr <JoinOrderOptimizer::JoinNode>> all_plans; //include intermediate


        vector <unique_ptr<Expression>> filters;
        vector <unique_ptr<FilterInfo>> filter_infos;
        expression_map_t <vector<FilterInfo *>> equivalence_sets;

        bool ExtractBindings(Expression &expression, unordered_set <idx_t> &bindings);

        bool ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                                  LogicalOperator *parent = nullptr);

        JoinOrderOptimizer::JoinNode *EmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info);

        bool TryEmitPair(JoinRelationSet *left, JoinRelationSet *right, NeighborInfo *info);

        bool EnumerateCmpRecursive(JoinRelationSet *left, JoinRelationSet *right, unordered_set <idx_t> exclusion_set);

        bool EmitCSG(JoinRelationSet *node);

        bool EnumerateCSGRecursive(JoinRelationSet *node, unordered_set <idx_t> &exclusion_set);

        unique_ptr <LogicalOperator> RewritePlan(unique_ptr <LogicalOperator> plan, JoinOrderOptimizer::JoinNode *node);
        // LogicalOperator* RewritePlan(LogicalOperator* plan, JoinOrderOptimizer::JoinNode *node);

        void GenerateCrossProducts();

        void SolveJoinOrder();

        bool SolveJoinOrderExactly();

        void SolveJoinOrderApproximately();



        double CalculateUCB(double avg, int v_p, int v_c);

        void pseudoCode();

        JoinOrderOptimizer::JoinNode* UCTChoice();

        void GeneratePlans();

        // void InitNodes();


        bool ContinueJoin(JoinOrderOptimizer::JoinNode *node, std::chrono::seconds duration);

        void RestoreState();

        void BackupState();

        void IterateTree(JoinRelationSet *union_set, unordered_set <idx_t> exclusion_set, NodeForUCT* parent_node_for_uct);

        unique_ptr <LogicalOperator> ResolveJoinConditions(unique_ptr <LogicalOperator> op);

        std::pair<JoinRelationSet *, unique_ptr < LogicalOperator>>
        GenerateJoins(
        vector <unique_ptr<LogicalOperator>> &extracted_relations, JoinOrderOptimizer::JoinNode
        *node);
    };

}


#endif // DUCKDB_RL_JOIN_ORDER_OPTIMIZER_HPP
