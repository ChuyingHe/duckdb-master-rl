//
// Created by Chuying He on 28/05/2021.
//
#include "duckdb/optimizer/rl_join_order_optimizer.hpp"

#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/pair.hpp"

#include <algorithm>

namespace duckdb {

//using JoinNode = RLJoinOrderOptimizer::JoinNode;


//! Extract the set of relations referred to inside an expression
bool RLJoinOrderOptimizer::ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings) {
    if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
        auto &colref = (BoundColumnRefExpression &)expression;
        D_ASSERT(colref.depth == 0);
        D_ASSERT(colref.binding.table_index != INVALID_INDEX);
        // map the base table index to the relation index used by the JoinOrderOptimizer
        D_ASSERT(relation_mapping.find(colref.binding.table_index) != relation_mapping.end());
        bindings.insert(relation_mapping[colref.binding.table_index]);
    }
    if (expression.type == ExpressionType::BOUND_REF) {
        // bound expression
        bindings.clear();
        return false;
    }
    D_ASSERT(expression.type != ExpressionType::SUBQUERY);
    bool can_reorder = true;
    ExpressionIterator::EnumerateChildren(expression, [&](Expression &expr) {
        if (!ExtractBindings(expr, bindings)) {
            can_reorder = false;
            return;
        }
    });
    return can_reorder;
}

//! Traverse the query tree to find (1) base relations, (2) existing join conditions and (3) filters that can be
//! rewritten into joins. Returns true if there are joins in the tree that can be reordered, false otherwise.
bool RLJoinOrderOptimizer::ExtractJoinRelations(LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                                              LogicalOperator *parent) {
    LogicalOperator *op = &input_op;
    while (op->children.size() == 1 && (op->type != LogicalOperatorType::LOGICAL_PROJECTION &&
                                        op->type != LogicalOperatorType::LOGICAL_EXPRESSION_GET)) {
        if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
            // extract join conditions from filter
            filter_operators.push_back(op);
        }
        if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY ||
            op->type == LogicalOperatorType::LOGICAL_WINDOW) {
            // don't push filters through projection or aggregate and group by
            RLJoinOrderOptimizer optimizer(context);
            op->children[0] = optimizer.Optimize(move(op->children[0]));
            return false;
        }
        op = op->children[0].get();
    }
    bool non_reorderable_operation = false;
    if (op->type == LogicalOperatorType::LOGICAL_UNION || op->type == LogicalOperatorType::LOGICAL_EXCEPT ||
        op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
        op->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
        // set operation, optimize separately in children
        non_reorderable_operation = true;
    }

    if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        auto &join = (LogicalComparisonJoin &)*op;
        if (join.join_type == JoinType::INNER) {
            // extract join conditions from inner join
            filter_operators.push_back(op);
        } else {
            // non-inner join, not reorderable yet
            non_reorderable_operation = true;
            if (join.join_type == JoinType::LEFT && join.right_projection_map.empty()) {
                // for left joins; if the RHS cardinality is significantly larger than the LHS (2x)
                // we convert to doing a RIGHT OUTER JOIN
                // FIXME: for now we don't swap if the right_projection_map is not empty
                // this can be fixed once we implement the left_projection_map properly...
                auto lhs_cardinality = join.children[0]->EstimateCardinality(context);
                auto rhs_cardinality = join.children[1]->EstimateCardinality(context);
                if (rhs_cardinality > lhs_cardinality * 2) {
                    join.join_type = JoinType::RIGHT;
                    std::swap(join.children[0], join.children[1]);
                    for (auto &cond : join.conditions) {
                        std::swap(cond.left, cond.right);
                        cond.comparison = FlipComparisionExpression(cond.comparison);
                    }
                }
            }
        }
    }
    if (non_reorderable_operation) {
        // we encountered a non-reordable operation (setop or non-inner join)
        // we do not reorder non-inner joins yet, however we do want to expand the potential join graph around them
        // non-inner joins are also tricky because we can't freely make conditions through them
        // e.g. suppose we have (left LEFT OUTER JOIN right WHERE right IS NOT NULL), the join can generate
        // new NULL values in the right side, so pushing this condition through the join leads to incorrect results
        // for this reason, we just start a new JoinOptimizer pass in each of the children of the join
        for (auto &child : op->children) {
            RLJoinOrderOptimizer optimizer(context);
            child = optimizer.Optimize(move(child));
        }
        // after this we want to treat this node as one  "end node" (like e.g. a base relation)
        // however the join refers to multiple base relations
        // enumerate all base relations obtained from this join and add them to the relation mapping
        // also, we have to resolve the join conditions for the joins here
        // get the left and right bindings
        unordered_set<idx_t> bindings;
        LogicalJoin::GetTableReferences(*op, bindings);
        // now create the relation that refers to all these bindings
        auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
        for (idx_t it : bindings) {
            relation_mapping[it] = relations.size();
        }
        relations.push_back(move(relation));
        return true;
    }
    if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
        op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
        // inner join or cross product
        bool can_reorder_left = ExtractJoinRelations(*op->children[0], filter_operators, op);
        bool can_reorder_right = ExtractJoinRelations(*op->children[1], filter_operators, op);
        return can_reorder_left && can_reorder_right;
    } else if (op->type == LogicalOperatorType::LOGICAL_GET) {
        // base table scan, add to set of relations
        auto get = (LogicalGet *)op;
        auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
        relation_mapping[get->table_index] = relations.size();
        relations.push_back(move(relation));
        return true;
    } else if (op->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
        // base table scan, add to set of relations
        auto get = (LogicalExpressionGet *)op;
        auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
        relation_mapping[get->table_index] = relations.size();
        relations.push_back(move(relation));
        return true;
    } else if (op->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
        // table function call, add to set of relations
        auto dummy_scan = (LogicalDummyScan *)op;
        auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
        relation_mapping[dummy_scan->table_index] = relations.size();
        relations.push_back(move(relation));
        return true;
    } else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
        auto proj = (LogicalProjection *)op;
        // we run the join order optimizer witin the subquery as well
        RLJoinOrderOptimizer optimizer(context);
        op->children[0] = optimizer.Optimize(move(op->children[0]));
        // projection, add to the set of relations
        auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
        relation_mapping[proj->table_index] = relations.size();
        relations.push_back(move(relation));
        return true;
    }
    return false;
}
static unique_ptr<JoinNode> RLCreateJoinTree(JoinRelationSet *set, NeighborInfo *info, JoinNode *left, JoinNode *right) {
    // for the hash join we want the right side (build side) to have the smallest cardinality
    // also just a heuristic but for now...
    // FIXME: we should probably actually benchmark that as well
    // FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
    if (left->cardinality < right->cardinality) {   /*exchange left and right if right has bigger cardinality*/
        return CreateJoinTree(set, info, right, left);
    }
    // the expected cardinality is the max of the child cardinalities
    // FIXME: we should obviously use better cardinality estimation here
    // but for now we just assume foreign key joins only
    idx_t expected_cardinality;
    if (info->filters.empty()) {
        // cross product
        expected_cardinality = left->cardinality * right->cardinality;
    } else {
        // normal join, expect foreign key join
        expected_cardinality = MaxValue(left->cardinality, right->cardinality);
    }
    // cost is expected_cardinality plus the cost of the previous plans
    idx_t cost = expected_cardinality;
    return make_unique<JoinNode>(set, info, left, right, expected_cardinality, cost);
}

void RLJoinOrderOptimizer::IterateTree(JoinRelationSet* union_set, unordered_set<idx_t> exclusion_set) {
    auto neighbors = query_graph.GetNeighbors(union_set, exclusion_set);        // Get neighbor of current plan: returns vector<idx_t>

    if (!neighbors.empty()) {                                                       // if there is relations left

        for (auto neighbor:neighbors) {
            auto *neighbor_relation = set_manager.GetJoinRelation(neighbor);        //returns JoinRelationSet*
            auto info = query_graph.GetConnection(union_set, neighbor_relation);    //NeighborInfo
            auto &left = intermediate_plans[union_set];
            union_set = set_manager.Union(union_set, neighbor_relation);            //all relations in the current plan
            auto &right = intermediate_plans[neighbor_relation];
            auto new_plan = CreateJoinTree(union_set, info, left.get(), right.get());// unique_ptr<JoinNode>

            intermediate_plans[union_set] = move(new_plan); //add intermediate results to intermediate_plans
            exclusion_set.insert(neighbor); // add self to exclusion_Set
            IterateTree(union_set, exclusion_set);
        }
    } else {
        // if all the relations are in the plan, a.k.a. we reached a leaf node
        //FIXME: add to plan but do not replace the old one, apparently plan take Relations as identifier
    }
}

void RLJoinOrderOptimizer::GeneratePlans() {
    printf("RLJoinOrderOptimizer::GeneratePlans()");
    //this function generate all possible plans and add it to this->plans
    //the number of possible plan depends on the Join-Graph (ONLY use cross-product if there is no other choice)
    //@todo: have a look in join_order_optimizer.cpp, see how they generate the plan and add into this->plans
    for (idx_t i = relations.size()-1; i > 0; i--) {
        auto *start_node = set_manager.GetJoinRelation(i);
        unordered_set<idx_t> exclusion_set;
        exclusion_set.insert(i);    // put current one relation in the exclusion_set
        IterateTree(start_node, exclusion_set);


        // find the neighbors given this exclusion set
        //auto all_neighbors = query_graph.GetAllNeighbors(start_node);

        // now create plans
        printf("neog");
    }

    //TODO: put all items in this->intermediate_plan which contains all the relations in this->plan
}

void RLJoinOrderOptimizer::RewardUpdate() {

}

/*unique_ptr<LogicalOperator> RLJoinOrderOptimizer::UCTChoice() {
    //choose a plan using UCT algorithm and return it
    ;
}*/

void RLJoinOrderOptimizer::ContinueJoin(unique_ptr<LogicalOperator> plan, std::chrono::seconds duration) {
    //execute join order during time budget
}

void RLJoinOrderOptimizer::RestoreState() {
    //restore execution state for this join order
}

void RLJoinOrderOptimizer::BackupState() {
    //backup execution state for join order
}


unique_ptr<LogicalOperator> RLJoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
    printf("\n\n Reinforcement Learning Join Optimizer");
    D_ASSERT(filters.empty() && relations.empty()); // assert that the RLJoinOrderOptimizer has not been used before
    LogicalOperator *op = plan.get();
    vector<LogicalOperator *> filter_operators;

    /*Cases that doesnt need Join Order Optimizer:*/
    //
    if (!ExtractJoinRelations(*op, filter_operators)) {
        return plan;
    }

/*    //@TODO: to be deleted
    std::cout<< "❓RL JOIN ORDER: relations.size = "<<relations.size()<< std::endl;
    if (relations.size() == 5) {
        printf("for debugging");
    }*/

    // at most one relation, nothing to reorder
    if (relations.size() <= 1) {
        return plan;
    }

    /*Cases that needs the Join Order Optimizer:*/
    // filters in the process
    expression_set_t filter_set;    /*unordered_set<BaseExpression *, ExpressionHashFunction, ExpressionEquality>;*/
    for (auto &op : filter_operators) { /*filter_operators is updated in function ExtractJoinRelations()*/
        if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
            /* (1) if operator == LOGICAL_COMPARISON_JOIN*/
            auto &join = (LogicalComparisonJoin &)*op;
            D_ASSERT(join.join_type == JoinType::INNER);
            D_ASSERT(join.expressions.empty());
            for (auto &cond : join.conditions) {
                auto comparison =
                        make_unique<BoundComparisonExpression>(cond.comparison, move(cond.left), move(cond.right));
                if (filter_set.find(comparison.get()) == filter_set.end()) { /*if this comparison doesn't exist in the filter_set, then put it in. find() returns .end() if not found*/
                    filter_set.insert(comparison.get());
                    filters.push_back(move(comparison));
                }
            }
            join.conditions.clear();
        } else {
            /* (2) if no comparison_join, then add op->expressions instead op->conditions */
            for (auto &expression : op->expressions) {
                if (filter_set.find(expression.get()) == filter_set.end()) {
                    filter_set.insert(expression.get());
                    filters.push_back(move(expression));
                }
            }
            op->expressions.clear();
        }
    }
    // create potential edges from the comparisons
    for (idx_t i = 0; i < filters.size(); i++) {
        auto &filter = filters[i];
        auto info = make_unique<FilterInfo>();
        auto filter_info = info.get();  /*raw pointer of info*/
        filter_infos.push_back(move(info));
        // first extract the relation set for the entire filter
        unordered_set<idx_t> bindings;
        ExtractBindings(*filter, bindings); /*update bindings*/
        filter_info->set = set_manager.GetJoinRelation(bindings);
        filter_info->filter_index = i;
        // now check if it can be used as a join predicate
        if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
            auto comparison = (BoundComparisonExpression *)filter.get();
            // extract the bindings that are required for the left and right side of the comparison
            unordered_set<idx_t> left_bindings, right_bindings;
            ExtractBindings(*comparison->left, left_bindings);
            ExtractBindings(*comparison->right, right_bindings);
            if (!left_bindings.empty() && !right_bindings.empty()) {
                // both the left and the right side have bindings
                // first create the relation sets, if they do not exist
                filter_info->left_set = set_manager.GetJoinRelation(left_bindings);
                filter_info->right_set = set_manager.GetJoinRelation(right_bindings);
                // we can only create a meaningful edge if the sets are not exactly the same -> not self connected
                if (filter_info->left_set != filter_info->right_set) {
                    // check if the sets are disjoint
                    if (Disjoint(left_bindings, right_bindings)) {
                        // they are disjoint, we only need to create one set of edges in the join graph
                        query_graph.CreateEdge(filter_info->left_set, filter_info->right_set, filter_info);
                        query_graph.CreateEdge(filter_info->right_set, filter_info->left_set, filter_info);
                    } else {
                        continue;
                        // the sets are not disjoint, we create two sets of edges
                        // auto left_difference = set_manager.Difference(filter_info->left_set, filter_info->right_set);
                        // auto right_difference = set_manager.Difference(filter_info->right_set,
                        // filter_info->left_set);
                        // // -> LEFT <-> RIGHT \ LEFT
                        // query_graph.CreateEdge(filter_info->left_set, right_difference, filter_info);
                        // query_graph.CreateEdge(right_difference, filter_info->left_set, filter_info);
                        // // -> RIGHT <-> LEFT \ RIGHT
                        // query_graph.CreateEdge(left_difference, filter_info->right_set, filter_info);
                        // query_graph.CreateEdge(filter_info->right_set, left_difference, filter_info);
                    }
                    continue;
                }
            }
        }
    }

    for (idx_t i = 0; i < relations.size(); i++) {
        auto &rel = *relations[i];
        auto node = set_manager.GetJoinRelation(i); /*returns a JoinRelationSet*/
        // plans[node] = make_unique<JoinNode>(node, rel.op->EstimateCardinality(context));    /*add nodes to the plan*/
        intermediate_plans[node] = make_unique<JoinNode>(node, rel.op->EstimateCardinality(context));    /*add nodes to the intermediate plan*/
    }


    // plans generation: 1) initialize each of the single-node plans
    /*for (idx_t i = 0; i < relations.size(); i++) {
        auto &rel = *relations[i];
        auto node = set_manager.GetJoinRelation(i); *//*returns a JoinRelationSet*//*
        plans[node] = make_unique<JoinNode>(node, rel.op->EstimateCardinality(context));    *//*add nodes to the plan*//*
    }*/
    // plans generation: 2) generate all the possible plans
    printf("before GeneratePlans");
    GeneratePlans();
    // auto final_plan = UCTChoice();

    // TODO: add plans which include all the relations into this->plans.

    // NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
    // function ensures that a unique combination of relations will have a unique JoinRelationSet object.
    // TODO: execute plan instead of returning a plan

    return plan;
}

}