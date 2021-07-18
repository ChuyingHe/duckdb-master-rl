#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parser/expression_util.hpp"

namespace duckdb {

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, LogicalType::BOOLEAN) {
}

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left,
                                                       unique_ptr<Expression> right)
    : BoundConjunctionExpression(type) {
	children.push_back(move(left));
	children.push_back(move(right));
}

string BoundConjunctionExpression::ToString() const {
	string result = "(" + children[0]->ToString();
	for (idx_t i = 1; i < children.size(); i++) {
		result += " " + ExpressionTypeToOperator(type) + " " + children[i]->ToString();
	}
	return result + ")";
}

bool BoundConjunctionExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundConjunctionExpression *)other_p;
	return ExpressionUtil::SetEquals(children, other->children);
}

BoundConjunctionExpression::BoundConjunctionExpression(BoundConjunctionExpression const& bce) : Expression(bce) {
    children.reserve(bce.children.size());

    for (auto const& elem : bce.children) {
        children.push_back(elem->Copy());
    }
}

unique_ptr<Expression> BoundConjunctionExpression::Copy() {
	return make_unique<BoundConjunctionExpression>(*this);
}

} // namespace duckdb
