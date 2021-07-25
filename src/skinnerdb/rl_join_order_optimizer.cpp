//
// Created by Chuying He on 28/05/2021.
//
#include "duckdb/skinnerdb/rl_join_order_optimizer.hpp"

#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/common/pair.hpp"

#include <algorithm>

namespace duckdb {


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
        auto get = (LogicalGet *)op;    // here op->bind_data is added
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
    auto neighbors = query_graph.GetNeighbors(union_set, exclusion_set);        // Get neighbor of current plan: returns vector<idx_t>

    // Depth-First Traversal 无向图的深度优先搜索
    if (!neighbors.empty()) {                                                       // if there is relations left
        for (auto neighbor:neighbors) {
            order_of_rel.append(std::to_string(neighbor));
            auto *neighbor_relation = set_manager.GetJoinRelation(neighbor);        //returns JoinRelationSet*
            auto info = query_graph.GetConnection(union_set, neighbor_relation);    //NeighborInfo
            auto &left = plans[union_set];
            auto new_set = set_manager.RLUnion(union_set, neighbor_relation);
            auto &right = plans[neighbor_relation];
            auto new_plan = RL_CreateJoinTree(new_set, info, left.get(), right.get());// unique_ptr<JoinNode>

            auto entry = plans.find(new_set);
            NodeForUCT* current_node_for_uct;
            if (entry == plans.end()) {
                plans[new_set] = move(new_plan);    //include all plans(intermediate & final)

                current_node_for_uct = new NodeForUCT{new_set, plans[new_set].get(), 0, 0.0, parent_node_for_uct};
                current_node_for_uct->order_of_relations.append(order_of_rel);

                parent_node_for_uct->children.push_back(current_node_for_uct);
            }

            exclusion_set.clear();
            for (idx_t i = 0; i < new_set->count; ++i) {
                exclusion_set.insert(new_set->relations[i]);
            }

            IterateTree(new_set, exclusion_set, current_node_for_uct);

            order_of_rel = order_of_rel.substr(0, order_of_rel.size()-1);
        }
    }
}
/* this function generate all possible plans and add it to this->plans
 * the number of possible plan depends on the Join-Graph (ONLY use cross-product if there is no other choice)*/
void RLJoinOrderOptimizer::GeneratePlans() {
    printf("\nGeneratePlans\n");
    // 🚩 create-node-for-UCT-Tree:  level0 node, a.k.a. root node

    // NodeForUCT* root_node_for_uct = new NodeForUCT{nullptr, nullptr, 0, 0.0, nullptr};
    //tree.push_back(root_node_for_uct);

    // 1) initialize each of the single-table plans
    for (idx_t i = 0; i < relations.size(); i++) {
        auto &rel = *relations[i];
        auto node = set_manager.GetJoinRelation(i);

        //create plans
        plans[node] = make_unique<JoinOrderOptimizer::JoinNode>(node, rel.op->EstimateCardinality(context));

        //🚩 create-node-for-UCT-Tree
        NodeForUCT* node_for_uct = new NodeForUCT{node, plans[node].get(), 0, 0.0, root_node_for_uct};
        node_for_uct->order_of_relations.append(std::to_string(i));

        //🚩 create-node-for-UCT-Tree: level0.children =  level1 nodes
        root_node_for_uct->children.push_back(node_for_uct);
    }

    // 2) plans include more than one table
    for (idx_t i = 0; i < relations.size(); i++) {
        auto start_node = set_manager.GetJoinRelation(i);
        unordered_set<idx_t> exclusion_set;
        exclusion_set.insert(i);    // put current one relation in the exclusion_set

        order_of_rel.clear();
        order_of_rel.append(std::to_string(i));
        IterateTree(start_node, exclusion_set, root_node_for_uct->children.at(i));
    }
}
/*use elems in this->plans for struct
void RLJoinOrderOptimizer::InitNodes() {
    //1) unordered_map item --> struct item
    //2) connect struct items
    int i = 0;
    for (auto& plan:plans) {
        NodeForUCT* node = new NodeForUCT{plan.first, plan.second.get(), 0, 0.0};
        //node->parent
        auto test1 = *(node->relations->relations.get());
        auto test2 = *(node->relations->relations.get()+1);
        auto test3 = *(node->relations->relations.get()+2);
        auto test4 = *(node->relations->relations.get()+3);
        auto test5 = *(node->relations->relations.get()+4);
        auto test = node->relations->count;

        tree.push_back(node);
        i++;
    }


    std::cout << tree.size() <<"\n";

    NodeForUCT root = {};
    // level1, *.first.count == 1
    for (auto& item:plans) {
        if (item.first->count == 1) {
            root.children.push_back(item);
        }
    }
}
*/

// 1) use chosen_node
// 2) use input parameter
// are they the same? maybe yes, because chosen_node is updated
void RLJoinOrderOptimizer::RewardUpdate(double reward) {
    // update the current leaf-node
    if (chosen_node) {
        printf("RewardUpdate");
        chosen_node->reward += reward;

        std::cout << "chosen_node = " <<chosen_node->order_of_relations << " that has the reward = " <<chosen_node->reward << "\n";
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

/*
 * UCB1(NODE[c]) = A + weight-factor * sqrt( (log(visit-of-c's-parent))/(visits-of-c) )
 * A = average-reward-for-c, aka. =  reward-of-c/visit-of-c
 */
JoinOrderOptimizer::JoinNode* RLJoinOrderOptimizer::UCTChoice() {
    printf("\nUCTChoice\n");
    //choose a plan using UCT algorithm and return it
    // all possible plans are save in this->rl_plans
    // Case 1: current Node is NOT a leaf - calculate UCB, choose the biggest one
    while (!root_node_for_uct->children.empty()) {   //TODO: therefore no EXPANSION needed? ❓
        auto max = 0;
        NodeForUCT* chosen_child;
        for (auto& child:root_node_for_uct->children){   // loop through all the possible node - from current node
            double avg = (child->num_of_visits==0)? 1000000: (child->reward/child->num_of_visits);   // unvisited node has an infinite avg

            auto ucb = CalculateUCB(avg, child->parent->num_of_visits, child->num_of_visits);

            // if this child has ucb>max , then chosen_child = this child
            if (ucb > max) {
                max = ucb;
                chosen_child = child;
            }
        }
        // node in current level is chosen, now enter next level
        root_node_for_uct = chosen_child;
    }
    // Case 2: current Node is a leaf, return leaf
    chosen_node = root_node_for_uct;    // for RewardUpdate
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
unique_ptr<LogicalOperator> RLJoinOrderOptimizer::RewritePlan(unique_ptr<LogicalOperator> plan, JoinOrderOptimizer::JoinNode *node) {
    std::cout <<"\n RewritePlan \n";
    // now we have to rewrite the plan
    bool root_is_join = plan->children.size() > 1;

    // first we will extract all relations from the main plan
    vector<unique_ptr<LogicalOperator>> extracted_relations;
    for (auto &relation : relations) {
        extracted_relations.push_back(RL_ExtractJoinRelation(*relation));  // all relations we have in the Query
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



unique_ptr<LogicalOperator> RLJoinOrderOptimizer::Optimize(unique_ptr<LogicalOperator> plan) {
    printf("\n\n Optimize \n\n");
    D_ASSERT(filters.empty() && relations.empty()); // assert that the RLJoinOrderOptimizer has not been used before
    LogicalOperator *op = plan.get();
    vector<LogicalOperator *> filter_operators;

    /*Cases that doesnt need Join Order Optimizer:*/
    //
    if (!ExtractJoinRelations(*op, filter_operators)) {
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

    // plans generation: 2) generate all the possible plans
    GeneratePlans();
    std::cout<< "\n 🐶 amount of plans = "<<plans.size()<<"\n";

    unordered_set<idx_t> bindings;
    for (idx_t i = 0; i < relations.size(); i++) {
        bindings.insert(i);
    }


    //FIXME: this is for debugging
    /*auto total_relation = set_manager.GetJoinRelation(bindings);
    auto final_plan = plans.find(total_relation);
    for (auto& item: plans) {
        if (item.first->count == 5) {
            return RewritePlan(move(plan), item.second.get());
        }
    }*/

    auto final_plan = UCTChoice();      // returns JoinOrderOptimizer::JoinNode*

    // NOTE: we can just use pointers to JoinRelationSet* here because the GetJoinRelation
    // function ensures that a unique combination of relations will have a unique JoinRelationSet object.
    // TODO: execute plan instead of returning a plan

    return RewritePlan(move(plan), final_plan);   // returns EXECUTABLE of the chosen_plan unique_ptr<LogicalOperator>
}



}