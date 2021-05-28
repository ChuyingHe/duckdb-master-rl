#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(CastExpression &expr, idx_t depth) {
	// first try to bind the child of the cast expression
	string error = Bind(&expr.child, depth);
	if (!error.empty()) {
		return BindResult(error);
	}
	if (expr.try_cast) {
		throw NotImplementedException("TRY_CAST not implemented yet in binder");
	}
	// the children have been successfully resolved
	auto &child = (BoundExpression &)*expr.child;
	if (child.expr->type == ExpressionType::VALUE_PARAMETER) {
		auto &parameter = (BoundParameterExpression &)*child.expr;
		// parameter: move types into the parameter expression itself
		parameter.return_type = expr.cast_type;
	} else {
		// otherwise add a cast to the target type
		child.expr = BoundCastExpression::AddCastToType(move(child.expr), expr.cast_type);
	}
	return BindResult(move(child.expr));
}
} // namespace duckdb
