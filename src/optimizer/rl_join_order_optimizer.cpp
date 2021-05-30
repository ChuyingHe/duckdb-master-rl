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

using JoinNode = JoinOrderOptimizer::JoinNode;


}