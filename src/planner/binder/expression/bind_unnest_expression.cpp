#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult SelectBinder::BindUnnest(FunctionExpression &function, idx_t depth) {
	// bind the children of the function expression
	string error;
	if (function.children.size() != 1) {
		return BindResult(binder.FormatError(function, "Unnest() needs exactly one child expressions"));
	}
	BindChild(function.children[0], depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}
	auto &child = (BoundExpression &)*function.children[0];
	LogicalType child_type = child.expr->return_type;
	if (child_type.id() != LogicalTypeId::LIST) {
		return BindResult(binder.FormatError(function, "Unnest() can only be applied to lists"));
	}
	LogicalType return_type = LogicalType::ANY;
	D_ASSERT(child_type.child_types().size() <= 1);
	if (child_type.child_types().size() == 1) {
		return_type = child_type.child_types()[0].second;
	}

	auto result = make_unique<BoundUnnestExpression>(return_type);
	result->child = move(child.expr);

	auto unnest_index = node.unnests.size();
	node.unnests.push_back(move(result));

	// TODO what if we have multiple unnests in the same projection list? ignore for now

	// now create a column reference referring to the aggregate
	auto colref = make_unique<BoundColumnRefExpression>(
	    function.alias.empty() ? node.unnests[unnest_index]->ToString() : function.alias, return_type,
	    ColumnBinding(node.unnest_index, unnest_index), depth);
	// move the aggregate expression into the set of bound aggregates
	return BindResult(move(colref));
}

} // namespace duckdb
