//
// Created by Chuying He on 28/05/2021.
//
/*
 * Components for SkinnerDB algorithm
 */
#include "duckdb/skinnerdb/rl_join_order_optimizer.hpp"

#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/main/config.hpp"

#include <algorithm>

namespace duckdb {

unordered_map<JoinRelationSet *, unique_ptr<JoinOrderOptimizer::JoinNode>, Hasher, EqualFn> RLJoinOrderOptimizer::plans;

//! Returns true if A and B are disjoint, false otherwise
template <class T>
static bool RL_Disjoint(unordered_set<T> &a, unordered_set<T> &b) {
    for (auto &entry : a) {
        if (b.find(entry) != b.end()) {
            return false;
        }
    }
    return true;
}

static unique_ptr<LogicalOperator> RL_ExtractJoinRelation(SingleJoinRelation &rel) {
    auto &children = rel.parent->children;
    for (idx_t i = 0; i < children.size(); i++) {
        if (children[i].get() == rel.op) {
            // found it! take ownership of it from the parent
            auto result = move(children[i]);
            children.erase(children.begin() + i);
            return result;
        }
    }
    throw Exception("Could not find relation in parent node (?)");
}

static unique_ptr<LogicalOperator> RL_PushFilter(unique_ptr<LogicalOperator> node, unique_ptr<Expression> expr) {
    // push an expression into a filter
    // first check if we have any filter to push it into
    if (node->type != LogicalOperatorType::LOGICAL_FILTER) {
        // we don't, we need to create one
        auto filter = make_unique<LogicalFilter>();
        filter->children.push_back(move(node));
        node = move(filter);
    }
    // push the filter into the LogicalFilter
    D_ASSERT(node->type == LogicalOperatorType::LOGICAL_FILTER);
    auto filter = (LogicalFilter *)node.get();
    filter->expressions.push_back(move(expr));
    return node;
}

//! Create a new JoinTree node by joining together two previous JoinTree nodes
static unique_ptr<JoinOrderOptimizer::JoinNode> RL_CreateJoinTree(JoinRelationSet *set, NeighborInfo *info, JoinOrderOptimizer::JoinNode *left, JoinOrderOptimizer::JoinNode *right) {
    // for the hash join we want the right side (build side) to have the smallest cardinality
    // also just a heuristic but for now...
    // FIXME: we should probably actually benchmark that as well
    // FIXME: should consider different join algorithms, should we pick a join algorithm here as well? (probably)
    if (left->cardinality < right->cardinality) {   /*exchange left and right if right has bigger cardinality*/
        return RL_CreateJoinTree(set, info, right, left);
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
    return make_unique<JoinOrderOptimizer::JoinNode>(set, info, left, right, expected_cardinality, cost);
}

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
bool RLJoinOrderOptimizer::ExtractJoinRelations(idx_t sample_count, LogicalOperator &input_op, vector<LogicalOperator *> &filter_operators,
                                              LogicalOperator *parent) {
    //printf("bool RLJoinOrderOptimizer::ExtractJoinRelations\n");
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
            op->children[0] = optimizer.SelectJoinOrder(move(op->children[0]), sample_count);
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
            child = optimizer.SelectJoinOrder(move(child), sample_count);
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
        bool can_reorder_left = ExtractJoinRelations(sample_count, *op->children[0], filter_operators, op);
        bool can_reorder_right = ExtractJoinRelations(sample_count, *op->children[1], filter_operators, op);
        return can_reorder_left && can_reorder_right;
    } else if (op->type == LogicalOperatorType::LOGICAL_GET) {
        // base table scan, add to set of relations
        auto get = (LogicalGet *)op;    // here op->bind_data is added
        auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
        relation_mapping[get->table_index] = relations.size();  //relations.size() returns the number of tables
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
        op->children[0] = optimizer.SelectJoinOrder(move(op->children[0]), sample_count);
        // projection, add to the set of relations
        auto relation = make_unique<SingleJoinRelation>(&input_op, parent);
        relation_mapping[proj->table_index] = relations.size();
        relations.push_back(move(relation));
        return true;
    }
    return false;
}

pair<JoinRelationSet *, unique_ptr<LogicalOperator>>
RLJoinOrderOptimizer::GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinOrderOptimizer::JoinNode *node) {
    JoinRelationSet *left_node = nullptr, *right_node = nullptr;
    JoinRelationSet *result_relation;
    unique_ptr<LogicalOperator> result_operator;
    if (node->left && node->right) {    /*both left and right exist*/
        // generate the left and right children
        auto left = GenerateJoins(extracted_relations, node->left);
        auto right = GenerateJoins(extracted_relations, node->right);
        if (node->info->filters.empty()) {
            // no filters, create a cross product
            auto join = make_unique<LogicalCrossProduct>();
            join->children.push_back(move(left.second));
            join->children.push_back(move(right.second));
            result_operator = move(join);
        } else {
            // we have filters, create a join node
            auto join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
            join->children.push_back(move(left.second));
            join->children.push_back(move(right.second));
            // set the join conditions from the join node
            for (auto &f : node->info->filters) {
                // extract the filter from the operator it originally belonged to
                D_ASSERT(filters[f->filter_index]);
                auto condition = move(filters[f->filter_index]);
                // now create the actual join condition
                D_ASSERT((JoinRelationSet::IsSubset(left.first, f->left_set) &&
                          JoinRelationSet::IsSubset(right.first, f->right_set)) ||
                         (JoinRelationSet::IsSubset(left.first, f->right_set) &&
                          JoinRelationSet::IsSubset(right.first, f->left_set)));
                JoinCondition cond;
                D_ASSERT(condition->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
                auto &comparison = (BoundComparisonExpression &)*condition;
                // we need to figure out which side is which by looking at the relations available to us
                bool invert = !JoinRelationSet::IsSubset(left.first, f->left_set);
                cond.left = !invert ? move(comparison.left) : move(comparison.right);
                cond.right = !invert ? move(comparison.right) : move(comparison.left);
                if (condition->type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
                    cond.comparison = ExpressionType::COMPARE_EQUAL;
                    cond.null_values_are_equal = true;
                } else {
                    cond.comparison = condition->type;
                }
                if (invert) {
                    // reverse comparison expression if we reverse the order of the children
                    cond.comparison = FlipComparisionExpression(cond.comparison);
                }
                join->conditions.push_back(move(cond));
            }
            D_ASSERT(!join->conditions.empty());
            result_operator = move(join);
        }
        left_node = left.first;
        right_node = right.first;
        result_relation = set_manager.RLUnion(left_node, right_node);
    } else {
        // base node, get the entry from the list of extracted relations
        D_ASSERT(node->set->count == 1);
        D_ASSERT(extracted_relations[node->set->relations[0]]);
        result_relation = node->set;
        result_operator = move(extracted_relations[node->set->relations[0]]);
    }
    // check if we should do a pushdown on this node
    // basically, any remaining filter that is a subset of the current relation will no longer be used in joins
    // hence we should push it here
    for (auto &filter_info : filter_infos) {
        // check if the filter has already been extracted
        auto info = filter_info.get();
        if (filters[info->filter_index]) {
            // now check if the filter is a subset of the current relation
            // note that infos with an empty relation set are a special case and we do not push them down
            if (info->set->count > 0 && JoinRelationSet::IsSubset(result_relation, info->set)) {
                auto filter = move(filters[info->filter_index]);
                // if it is, we can push the filter
                // we can push it either into a join or as a filter
                // check if we are in a join or in a base table
                if (!left_node || !info->left_set) {
                    // base table or non-comparison expression, push it as a filter
                    result_operator = RL_PushFilter(move(result_operator), move(filter));
                    continue;
                }
                // the node below us is a join or cross product and the expression is a comparison
                // check if the nodes can be split up into left/right
                bool found_subset = false;
                bool invert = false;
                if (JoinRelationSet::IsSubset(left_node, info->left_set) &&
                    JoinRelationSet::IsSubset(right_node, info->right_set)) {
                    found_subset = true;
                } else if (JoinRelationSet::IsSubset(right_node, info->left_set) &&
                           JoinRelationSet::IsSubset(left_node, info->right_set)) {
                    invert = true;
                    found_subset = true;
                }
                if (!found_subset) {
                    // could not be split up into left/right
                    result_operator = RL_PushFilter(move(result_operator), move(filter));
                    continue;
                }
                // create the join condition
                JoinCondition cond;
                D_ASSERT(filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
                auto &comparison = (BoundComparisonExpression &)*filter;
                // we need to figure out which side is which by looking at the relations available to us
                cond.left = !invert ? move(comparison.left) : move(comparison.right);
                cond.right = !invert ? move(comparison.right) : move(comparison.left);
                cond.comparison = comparison.type;
                if (invert) {
                    // reverse comparison expression if we reverse the order of the children
                    cond.comparison = FlipComparisionExpression(comparison.type);
                }
                // now find the join to push it into
                auto node = result_operator.get();
                if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
                    node = node->children[0].get();
                }
                if (node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
                    // turn into comparison join
                    auto comp_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
                    comp_join->children.push_back(move(node->children[0]));
                    comp_join->children.push_back(move(node->children[1]));
                    comp_join->conditions.push_back(move(cond));
                    if (node == result_operator.get()) {
                        result_operator = move(comp_join);
                    } else {
                        D_ASSERT(result_operator->type == LogicalOperatorType::LOGICAL_FILTER);
                        result_operator->children[0] = move(comp_join);
                    }
                } else {
                    D_ASSERT(node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
                    auto &comp_join = (LogicalComparisonJoin &)*node;
                    comp_join.conditions.push_back(move(cond));
                }
            }
        }
    }
    return make_pair(result_relation, move(result_operator));
}

void RLJoinOrderOptimizer::IterateTree(JoinRelationSet* union_set, unordered_set<idx_t> exclusion_set, NodeForUCT* parent_node_for_uct) {
    //std::cout<< "IT: union_set = " << union_set->ToString() <<", exclusion_set.size="<<exclusion_set.size()<<", join oder of parent=" << parent_node_for_uct->join_node->order_of_relations <<"\n";
    //printf(".");
    auto neighbors = query_graph.GetNeighbors(union_set, exclusion_set);        // Get neighbor of current plan: returns vector<idx_t>

    // Depth-First Traversal 无向图的深度优先搜索
    if (!neighbors.empty()) {                                                       // if there is relations left
        for (auto neighbor:neighbors) {
            auto *neighbor_relation = set_manager.GetJoinRelation(neighbor);        //returns JoinRelationSet*

            //fix filter problem
            auto info = query_graph.GetConnection(union_set, neighbor_relation);    //NeighborInfo
            NeighborInfo* info_copy_ptr = new NeighborInfo(*info);

            auto &left = plans[union_set];
            auto new_set = set_manager.RLUnion(union_set, neighbor_relation);       //returns JoinRelationSet *
            auto &right = plans[neighbor_relation];

            JoinRelationSet* new_set_copy_ptr = new JoinRelationSet(*new_set);
            auto new_plan = RL_CreateJoinTree(new_set_copy_ptr, info_copy_ptr, left.get(), right.get());

            auto entry = plans.find(new_set);
            NodeForUCT* current_node_for_uct;
            if (entry == plans.end()) {
                plans[new_set_copy_ptr] = move(new_plan);
                plans[new_set_copy_ptr]->order_of_relations.append(parent_node_for_uct->join_node->order_of_relations);
                plans[new_set_copy_ptr]->order_of_relations.append(std::to_string(neighbor));
                plans[new_set_copy_ptr]->order_of_relations.append("-");

                current_node_for_uct = new NodeForUCT{plans[new_set_copy_ptr].get(), 0, 0.0, parent_node_for_uct};
                parent_node_for_uct->children.push_back(current_node_for_uct);
            }

            exclusion_set.clear();
            for (idx_t i = 0; i < new_set->count; ++i) {
                exclusion_set.insert(new_set->relations[i]);
            }

            IterateTree(new_set, exclusion_set, current_node_for_uct);
        }
    }
}
/* this function generate all possible plans and add it to this->plans
 * the number of possible plan depends on the Join-Graph (ONLY use cross-product if there is no other choice)*/
/*void RLJoinOrderOptimizer::GeneratePlans() {
    //printf("void RLJoinOrderOptimizer::GeneratePlans\n");
    // 1) initialize each of the single-table plans
    for (idx_t i = 0; i < relations.size(); i++) {
        auto &rel = *relations[i];

        auto node = set_manager.GetJoinRelation(i);
        JoinRelationSet* copy_node_ptr = new JoinRelationSet(*node);
        plans[copy_node_ptr] = make_unique<JoinOrderOptimizer::JoinNode>(move(copy_node_ptr), rel.op->EstimateCardinality(context));
        plans[copy_node_ptr]->order_of_relations.append(std::to_string(i));
        plans[copy_node_ptr]->order_of_relations.append("-");
        NodeForUCT* node_for_uct = new NodeForUCT{plans[copy_node_ptr].get(), 0, 0.0, root_node_for_uct};
        root_node_for_uct->children.push_back(node_for_uct);
    }

    // 2) plans include more than one table
    for (idx_t i = 0; i < relations.size(); i++) {
        JoinRelationSet* start_node = set_manager.GetJoinRelation(i);
        unordered_set<idx_t> exclusion_set;
        exclusion_set.insert(i);    // put current one relation in the exclusion_set

        IterateTree(start_node, exclusion_set, root_node_for_uct->children.at(i));
    }

    //std::cout<< "\n 🐶 amount of plans = "<<plans.size()<<"\n";
}*/
void RLJoinOrderOptimizer::Expansion(JoinRelationSet* union_set, unordered_set<idx_t> exclusion_set, NodeForUCT* parent_node_for_uct) {
    //printf("Expansion \n");
    auto neighbors = query_graph.GetNeighbors(union_set, exclusion_set);        // Get neighbor of current plan: returns vector<idx_t>

    // Depth-First Traversal 无向图的深度优先搜索
    if (!neighbors.empty()) {                                                       // if there is relations left
        for (auto neighbor:neighbors) {
            auto *neighbor_relation = set_manager.GetJoinRelation(neighbor);        //returns JoinRelationSet*

            //fix filter problem
            auto info = query_graph.GetConnection(union_set, neighbor_relation);    //NeighborInfo
            NeighborInfo* info_copy_ptr = new NeighborInfo(*info);

            auto &left = plans[union_set];
            auto new_set = set_manager.RLUnion(union_set, neighbor_relation);       //returns JoinRelationSet *
            auto &right = plans[neighbor_relation];

            JoinRelationSet* new_set_copy_ptr = new JoinRelationSet(*new_set);
            auto new_plan = RL_CreateJoinTree(new_set_copy_ptr, info_copy_ptr, left.get(), right.get());

            auto entry = plans.find(new_set);
            NodeForUCT* current_node_for_uct;
            if (entry == plans.end()) {
                plans[new_set_copy_ptr] = move(new_plan);
                plans[new_set_copy_ptr]->order_of_relations.append(parent_node_for_uct->join_node->order_of_relations);
                plans[new_set_copy_ptr]->order_of_relations.append(std::to_string(neighbor));
                plans[new_set_copy_ptr]->order_of_relations.append("-");

                current_node_for_uct = new NodeForUCT{plans[new_set_copy_ptr].get(), 0, 0.0, parent_node_for_uct};
                current_node_for_uct->current_table = neighbor;
                parent_node_for_uct->children.push_back(current_node_for_uct);
            }

            /*exclusion_set.clear();
            for (idx_t i = 0; i < new_set->count; ++i) {
                exclusion_set.insert(new_set->relations[i]);
            }

            IterateTree(new_set, exclusion_set, current_node_for_uct);*/
        }
    }
    Selection(parent_node_for_uct);
}

NodeForUCT* RLJoinOrderOptimizer::GetNodeWithMaxUCT(NodeForUCT* node) { //case "node->children.empty()" has been eliminated
    NodeForUCT* result;
    auto max = -1000000000000;

    for (auto const& child:node->children) {
        double avg = child->reward/(child->num_of_visits);
        double ucb = CalculateUCB(avg, node->num_of_visits, child->num_of_visits);
        if (ucb > max) {
            max = ucb;
            result = child;
        }
    }
    result->num_of_visits+=1;
    return result;
}

void RLJoinOrderOptimizer::Selection(NodeForUCT* node) {
    //printf("Selection \n");
    if (node->children.empty()) {   //[3] current node is a LEAF// node.joinedTable.size() == node.total_table_amo
        //[4] Simulation/Rollout
        //node->num_of_visits+=1;
       // std::cout<< "size of plan = "<<plans.size() << ", join order of final_plan="<< node->join_node->order_of_relations <<"\n";

        //[5] Reward update (did in skinnerdb.cpp)
        //[6] Progress Tracker
        //[7] final_plan
        chosen_node = node;
    } else {                        //current node still has child
        //[1.1] unexplored node exist
        for (auto const& child:node->children) {
            if (child->num_of_visits == 0) {
                child->num_of_visits+=1;
                child->total_table_amount = node->total_table_amount;
                //child->parent = node;

                child->unjoinedTables = node->unjoinedTables;
                auto pos = find(child->unjoinedTables.begin(), child->unjoinedTables.end(), child->current_table);
                child->unjoinedTables.erase(pos);
                child->joinedTables = node->joinedTables;
                child->joinedTables.push_back(child->current_table);

                // child->children;  vector<NodeForUCT*> children; -------------------------

                unordered_set<idx_t> exclusion_set;
                for (auto const& jt:child->joinedTables) {
                    exclusion_set.insert(jt);
                }

                JoinRelationSet* new_set;
                /*if (node->parent) { //node is not ROOT
                    new_set = set_manager.RLUnion(node->join_node->set, child->join_node->set);
                    Expansion(new_set, exclusion_set, child);
                } else {
                    Expansion(child->join_node->set, exclusion_set, child);
                }*/
                Expansion(child->join_node->set, exclusion_set, child);
                return;
            }
        }
        //[1.2] all nodes have been visited - choose the one gives largest UCT
        Selection(GetNodeWithMaxUCT(node));
    }
}
void RLJoinOrderOptimizer::Initialization() {
    root_node_for_uct->total_table_amount = relations.size();
    for (idx_t i = 0; i < root_node_for_uct->total_table_amount; i++) {
        auto &rel = *relations[i];

        auto node = set_manager.GetJoinRelation(i);
        JoinRelationSet* copy_node_ptr = new JoinRelationSet(*node);
        plans[copy_node_ptr] = make_unique<JoinOrderOptimizer::JoinNode>(move(copy_node_ptr), rel.op->EstimateCardinality(context));
        plans[copy_node_ptr]->order_of_relations.append(std::to_string(i));
        plans[copy_node_ptr]->order_of_relations.append("-");
        NodeForUCT* node_for_uct = new NodeForUCT{plans[copy_node_ptr].get(), 0, 0.0, root_node_for_uct};
        node_for_uct->current_table = i;

        root_node_for_uct->unjoinedTables.push_back(i);
        root_node_for_uct->children.push_back(node_for_uct);
    }
}

void RLJoinOrderOptimizer::GeneratePlans() {
    //printf("void RLJoinOrderOptimizer::GeneratePlans\n");
    // [0] Initialization


    // [1] Selection
    Selection(root_node_for_uct);


    /*for (idx_t i = 0; i < relations.size(); i++) {
        JoinRelationSet* start_node = set_manager.GetJoinRelation(i);
        unordered_set<idx_t> exclusion_set;
        exclusion_set.insert(i);    // put current one relation in the exclusion_set

        IterateTree(start_node, exclusion_set, root_node_for_uct->children.at(i));
    }*/

    //std::cout<< "\n 🐶 amount of plans = "<<plans.size()<<"\n";
}

// 1) use chosen_node
// 2) use input parameter
// are they the same? maybe yes, because chosen_node is updated
void RLJoinOrderOptimizer::Backpropogation(double reward) {
    //printf("void RLJoinOrderOptimizer::RewardUpdate\n");
    // update the current leaf-node
    if (chosen_node) {
        chosen_node->reward += reward;

        // update node's parent - until the root note
        NodeForUCT* parent_ptr = chosen_node->parent;
        while (parent_ptr) {
            parent_ptr->reward +=reward;
            parent_ptr = parent_ptr->parent;
        }
    }
}

/*
 * SkinnerDB-Paper:
 * avg = Rc
 * weight = w
 * node->parent->num_of_visits = Vp, num_of_visits for its parent
 * node->num_of_visits = Vc, num_of_visits for itself
 * */
double RLJoinOrderOptimizer::CalculateUCB(double avg, int v_p, int v_c) {
    double weight = sqrt(2);
    if (v_c == 0) {
        return avg;
    }
    return ( avg + weight * sqrt(log(v_p)/v_c) );
}

/*void RLJoinOrderOptimizer::pseudoCode() {
    bool finished = false;  //indicator: whether the whole query has been executed or not
    // auto state = 0;             //恢复执行状态?

    while (!finished) {
        auto chosen_plan = UCTChoice();
        RestoreState();
        finished = ContinueJoin(chosen_plan, std::chrono::seconds(5));  // (1) Rollout/SIMULATION
        BackupState();  // (2) BACKPROPAGATION
    }
}*/
JoinOrderOptimizer::JoinNode* RLJoinOrderOptimizer::UCTChoice() {
    //printf("JoinOrderOptimizer::JoinNode* RLJoinOrderOptimizer::UCTChoice\n");
    /*auto next = root_node_for_uct;
    // determine the second-last node
    while (!next->children.empty()) {
        next->num_of_visits += 1;
        auto max = 0;
        NodeForUCT* chosen_next;
        auto children = next->children; // should be vector of ptr
        for (auto const& n : children) {
            double avg = (n->num_of_visits==0)? 1000000: (n->reward/n->num_of_visits);
            auto ucb = CalculateUCB(avg, n->parent->num_of_visits, n->num_of_visits);
            if (ucb > max) {
                max = ucb;
                chosen_next = n;
            }
        }
        next = chosen_next;      // for next iteration in this while loop
    }
    // determine the last node
    next->num_of_visits += 1;
    chosen_node = next; //the first and the only child
    return chosen_node->join_node;*/

    auto next = root_node_for_uct;
    // determine the second-last node
    while (!next->children.empty()) {
        next->num_of_visits += 1;
        auto max = -1000000000000;
        NodeForUCT* chosen_next;
        auto children = next->children; // should be vector of ptr
        for (auto const& n : children) {
            double avg = (n->num_of_visits==0)? 0: (n->reward/n->num_of_visits);
            auto ucb = CalculateUCB(avg, n->parent->num_of_visits, n->num_of_visits);
            if (ucb > max) {
                max = ucb;
                chosen_next = n;
            }
        }
        next = chosen_next;      // for next iteration in this while loop
    }
    // determine the last node
    next->num_of_visits += 1;
    chosen_node = next; //the first and the only child
    return chosen_node->join_node;

}

bool RLJoinOrderOptimizer::ContinueJoin(JoinOrderOptimizer::JoinNode* node, std::chrono::seconds duration) {
    //execute join order during time budget
    return false;
}

void RLJoinOrderOptimizer::RestoreState() {
    /*
     * Goal:
     * 1) restore execution state for this join order
     * 2) share as much progress as possible among different join orders
     *
     * TODO:
     * 1) offset counters: to count tuples for each table, record how many tuples have been executed
     * 2) check whether partial join-order has been executed or not, if yes, fast-forward it, use the generated result instead of executing it again
     * */

}

void RLJoinOrderOptimizer::BackupState() {
    //backup execution state for join order
    // RewardUpdate(node's parent node)
}

// plan: from previous optimizer
// node: final_plan chosen by UCTChoice()
// (move(plan), final_plan);
unique_ptr<LogicalOperator> RLJoinOrderOptimizer::RewritePlan(unique_ptr<LogicalOperator> plan, JoinOrderOptimizer::JoinNode *node) {
    //printf("unique_ptr<LogicalOperator> RLJoinOrderOptimizer::RewritePlan\n");
    // now we have to rewrite the plan
    bool root_is_join = plan->children.size() > 1;
    // first we will extract all relations from the main plan
    vector<unique_ptr<LogicalOperator>> extracted_relations;
    for (auto &relation : relations) {
        extracted_relations.push_back(RL_ExtractJoinRelation(*relation));  // get unique_ptr<LogicalOperator> from each relation (SingleJoinRelation)
    }
    // now we generate the actual joins, returns pair<JoinRelationSet *, unique_ptr<LogicalOperator>>
    auto join_tree = GenerateJoins(extracted_relations, node);  // node comes from final_plan->second.get()
    // perform the final pushdown of remaining filters
    for (auto &filter : filters) {
        // check if the filter has already been extracted
        if (filter) {
            // if not we need to push it
            join_tree.second = RL_PushFilter(move(join_tree.second), move(filter));
        }
    }
    // find the first join in the relation to know where to place this node
    if (root_is_join) {
        // first node is the join, return it immediately ---> WHY?
        return move(join_tree.second);
    }
    D_ASSERT(plan->children.size() == 1);
    // have to move up through the relations
    auto op = plan.get();
    auto parent = plan.get();
    while (op->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
           op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        D_ASSERT(op->children.size() == 1);
        parent = op;
        op = op->children[0].get();
    }

    // have to replace at this node
    parent->children[0] = move(join_tree.second);
    return plan;
}


/*

void RLJoinOrderOptimizer::sample(NodeForUCT& node) {
    node.num_of_visits+=1;

    if (!node.join_node) {     //root

        //current possibility
        for (idx_t i = 0; i < relations.size(); i++) {
            auto& rel = *relations[i];
            auto rel_set = set_manager.GetJoinRelation(i);
            JoinRelationSet* copy_node_ptr = new JoinRelationSet(*rel_set);

            auto join_node = JoinOrderOptimizer::JoinNode(move(copy_node_ptr), rel.op->EstimateCardinality(context));
            NodeForUCT* node_for_uct = new NodeForUCT{&join_node, 0, 0.0, root_node_for_uct};
            node_for_uct->join_node->order_of_relations.append(std::to_string(i));
            node_for_uct->parent = &node;
            node_for_uct->current_rel = i;
            //因为是root所以children的数量=unjoinedTables的数量

            node.unjoinedTables.push_back(i);   //joinedTables=NULL
            node.children.push_back(move(node_for_uct));
        }

        //choose next one - 1
        for (auto const& elem : node.children) {
            if (elem->num_of_visits==0) {
                //chose current elem
                elem->num_of_visits+=1;
                elem->unjoinedTables.insert(elem->unjoinedTables.end(), node.unjoinedTables.begin(), node.unjoinedTables.end());
                //elem->unjoinedTables.erase(elem->current_rel);

                std::vector<idx_t>::iterator position = std::find(elem->unjoinedTables.begin(), elem->unjoinedTables.end(), elem->current_rel);
                if (position != elem->unjoinedTables.end()) // == myVector.end() means the element was not found
                    elem->unjoinedTables.erase(position);

                elem->joinedTables.insert(elem->joinedTables.end(), node.joinedTables.begin(), node.joinedTables.end());
                elem->joinedTables.push_back(elem->current_rel);

                unordered_set<idx_t> exclusion_set;
                for (auto const& jt:elem->joinedTables) {
                    exclusion_set.insert(jt);
                }
                //auto new_set = elem->join_node->set;
                // auto neighbors = query_graph.GetNeighbors(new_set, exclusion_set);    // returns vector<idx_t>
                IterateTree(elem->join_node->set, exclusion_set, elem);
                */
/*for (auto const& neighbor:neighbors) {

                }*//*

                sample(*elem);
                break;
            }
        }
        //choose next one - 2 - all the children has been visited
        for (auto const& elem : node.children) {

        }
    }
}
*/


unique_ptr<LogicalOperator> RLJoinOrderOptimizer::SelectJoinOrder(unique_ptr<LogicalOperator> plan, idx_t sample_count) {
    //printf("unique_ptr<LogicalOperator> RLJoinOrderOptimizer::Optimize\n");
    /* extract relations from the logical plan:*/
    D_ASSERT(filters.empty() && relations.empty()); // assert that the RLJoinOrderOptimizer has not been used before

    LogicalOperator *op = plan.get();
    vector<LogicalOperator *> filter_operators;

    if (!ExtractJoinRelations(sample_count, *op, filter_operators)) {
        return plan;
    }

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
        ExtractBindings(*filter, bindings); // Extract the set of relations referred to inside an expression
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
                    if (RL_Disjoint(left_bindings, right_bindings)) {
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

    if (sample_count==0) {
        Initialization();
    }
    root_node_for_uct->num_of_visits+=1;
    Selection(root_node_for_uct);

    // GeneratePlans();
    int test_count_complete = 0;
    for (auto const& plan:plans) {
        if (plan.first->count == 4) {
            test_count_complete += 1;
        }
    }
    // std::cout <<"relations_amount = " << 4 << ", plan size=" << test_count_complete<<"\n";

    //sample(*root_node_for_uct);

   //auto final_plan = UCTChoice();      // returns JoinOrderOptimizer::JoinNode*
    return RewritePlan(move(plan), chosen_node->join_node);   // returns EXECUTABLE of the chosen_plan unique_ptr<LogicalOperator>
}

// Simulation = Execution
/*unique_ptr<QueryResult> RLJoinOrderOptimizer::TestContinueJoin(ClientContextLock &lock, const string &query,
                                             shared_ptr<PreparedStatementData> statement_p,
                                             vector<Value> bound_values, bool allow_stream_result) {
    auto &statement = *statement_p;
    if (context.transaction.ActiveTransaction().IsInvalidated() && statement.requires_valid_transaction) {
        throw Exception("Current transaction is aborted (please ROLLBACK)");
    }

    auto &config = DBConfig::GetConfig(context);
    if (config.access_mode == AccessMode::READ_ONLY && !statement.read_only) {
        throw Exception(StringUtil::Format("Cannot execute statement of type \"%s\" in read-only mode!",
                                           StatementTypeToString(statement.statement_type)));
    }

    statement.Bind(move(bound_values));

    bool create_stream_result = statement.allow_stream_result && allow_stream_result;
    if (context.enable_progress_bar) {  //progress runs in another thread, parallel to the main thread
        if (!context.progress_bar) {
            context.progress_bar = make_shared<ProgressBar>(&context.executor, context.wait_time);
        }
        context.progress_bar->Start();
    }

    context.executor.Initialize(statement.plan.get());

    auto types = context.executor.GetTypes();
    D_ASSERT(types == statement.types);

    if (create_stream_result) {
        if (context.progress_bar) {
            context.progress_bar->Stop();
        }
        return make_unique<StreamQueryResult>(statement.statement_type, shared_ptr<ClientContext>(*context), statement.types,
                                              statement.names, move(statement_p));
    }

    auto result = make_unique<MaterializedQueryResult>(statement.statement_type, statement.types, statement.names);

    while (true) {}

}*/

}