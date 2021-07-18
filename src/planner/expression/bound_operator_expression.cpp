#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

BoundOperatorExpression::BoundOperatorExpression(ExpressionType type, LogicalType return_type)
    : Expression(type, ExpressionClass::BOUND_OPERATOR, move(return_type)) {
}

string BoundOperatorExpression::ToString() const {
	auto op = ExpressionTypeToOperator(type);
	if (!op.empty()) {
		// use the operator string to represent the operator
		if (children.size() == 1) {
			return op + "(" + children[0]->GetName() + ")";
		} else if (children.size() == 2) {
			return children[0]->GetName() + " " + op + " " + children[1]->GetName();
		}
	}
	// if there is no operator we render it as a function
	auto result = ExpressionTypeToString(type) + "(";
	result += StringUtil::Join(children, children.size(), ", ",
	                           [](const unique_ptr<Expression> &child) { return child->GetName(); });
	result += ")";
	return result;
}

bool BoundOperatorExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundOperatorExpression *)other_p;
	if (children.size() != other->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	return true;
}
BoundOperatorExpression::BoundOperatorExpression(BoundOperatorExpression const& boe) : Expression(boe) {
    children.reserve(boe.children.size());
    for (auto const &elem : boe.children) {
        children.push_back(elem->Copy());
    }
}
unique_ptr<Expression> BoundOperatorExpression::Copy() {
	return make_unique<BoundOperatorExpression>(*this);
}

} // namespace duckdb
