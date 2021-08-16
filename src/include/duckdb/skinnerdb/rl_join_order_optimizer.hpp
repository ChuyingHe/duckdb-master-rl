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
        JoinOrderOptimizer::JoinNode* join_node;                // from this->plans
        int num_of_visits;
        double reward;
        NodeForUCT* parent;
        vector<NodeForUCT*> children;

        idx_t current_table;
        idx_t total_table_amount;
        vector<idx_t> unjoinedTables;
        vector<idx_t> joinedTables;

        NodeForUCT(JoinOrderOptimizer::JoinNode* join_node, int num_of_visits, double reward, NodeForUCT* parent) :
        join_node(join_node), num_of_visits(num_of_visits), reward(reward), parent(parent) {
        }

        NodeForUCT(NodeForUCT const& nfuct) {
            //printf("copy constructor of NodeForUCT\n");
            join_node = nfuct.join_node;
            num_of_visits = nfuct.num_of_visits;
            reward = nfuct.reward;
            parent = nfuct.parent;
            children.reserve(nfuct.children.size());
            for (auto const& elem : nfuct.children) {
                children.push_back(elem);
            }
        }
    };

    extern NodeForUCT* root_node_for_uct;
    extern NodeForUCT* chosen_node; // to tranfer the reward in RewardUpdate

    class RLJoinOrderOptimizer {
    public:
        explicit RLJoinOrderOptimizer(ClientContext &context) : context(
                context) { /*constructor, explicit prevent other type of parameter*/
        }

        unique_ptr <LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan, idx_t sample_count);
        void RewardUpdate(double reward);
        void GeneratePlans();
        static unordered_map<JoinRelationSet *, unique_ptr<JoinOrderOptimizer::JoinNode>, Hasher, EqualFn> plans;   // includes all the relations, to return
        void sample(NodeForUCT& node);
        void Selection(NodeForUCT* node);   //choose the next among potential nodes
        void Expansion(JoinRelationSet* union_set, unordered_set<idx_t> exclusion_set, NodeForUCT* parent_node_for_uct);   //find out potential nodes
        void Simulation();  //rollout
        void Initialization();
        NodeForUCT* GetNodeWithMaxUCT(NodeForUCT* node);

    private:
        ClientContext &context;
        idx_t pairs = 0;
        int counter = 0;

        vector <unique_ptr<SingleJoinRelation>> relations;
        unordered_map <idx_t, idx_t> relation_mapping;
        JoinRelationSetManager set_manager;
        QueryGraph query_graph;
        std::string order_of_rel = "";
        vector <unique_ptr<Expression>> filters;
        vector <unique_ptr<FilterInfo>> filter_infos;
        expression_map_t <vector<FilterInfo *>> equivalence_sets;

        bool ExtractBindings(Expression &expression, unordered_set <idx_t> &bindings);

        bool ExtractJoinRelations(idx_t sample_count, LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                                  LogicalOperator *parent = nullptr);

        // for JoinRelationSet
        unique_ptr<JoinOrderOptimizer::JoinNode> findInPlans(unordered_map<JoinRelationSet *, unique_ptr<JoinOrderOptimizer::JoinNode>> plans, JoinRelationSet* relation_set);

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
