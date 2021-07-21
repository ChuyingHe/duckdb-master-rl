#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

BoundComparisonExpression::BoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left,
                                                     unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::BOUND_COMPARISON, LogicalType::BOOLEAN), left(move(left)), right(move(right)) {
}

string BoundComparisonExpression::ToString() const {
	return left->GetName() + ExpressionTypeToOperator(type) + right->GetName();
}

bool BoundComparisonExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundComparisonExpression *)other_p;
	if (!Expression::Equals(left.get(), other->left.get())) {
		return false;
	}
	if (!Expression::Equals(right.get(), other->right.get())) {
		return false;
	}
	return true;
}
BoundComparisonExpression::BoundComparisonExpression(BoundComparisonExpression const& bce) : Expression(bce) {
    left = bce.left->Copy();
    right = bce.right->Copy();
}
unique_ptr<Expression> BoundComparisonExpression::Copy() {
    return make_unique<BoundComparisonExpression>(*this);
}

} // namespace duckdb
