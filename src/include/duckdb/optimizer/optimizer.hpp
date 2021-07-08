//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/skinnerdb/rl_join_order_optimizer.hpp"

namespace duckdb {
class Binder;

class Optimizer {
public:


	Optimizer(Binder &binder, ClientContext &context);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);
    unique_ptr<LogicalOperator> OptimizeBeforeRLOptimizer(unique_ptr<LogicalOperator> plan);

    ClientContext &context;
	Binder &binder;
	ExpressionRewriter rewriter;
};

} // namespace duckdb
