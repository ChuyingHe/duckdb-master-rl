#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

BoundReferenceExpression::BoundReferenceExpression(string alias, LogicalType type, idx_t index)
    : Expression(ExpressionType::BOUND_REF, ExpressionClass::BOUND_REF, move(type)), index(index) {
	this->alias = move(alias);
}
BoundReferenceExpression::BoundReferenceExpression(LogicalType type, idx_t index)
    : BoundReferenceExpression(string(), move(type), index) {
}

string BoundReferenceExpression::ToString() const {
	return "#" + to_string(index);
}

bool BoundReferenceExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundReferenceExpression *)other_p;
	return other->index == index;
}

hash_t BoundReferenceExpression::Hash() const {
	return CombineHash(Expression::Hash(), duckdb::Hash<idx_t>(index));
}
BoundReferenceExpression::BoundReferenceExpression(BoundReferenceExpression const& bre) : Expression(bre) {
    index = bre.index;
}
unique_ptr<Expression> BoundReferenceExpression::Copy() {
	return make_unique<BoundReferenceExpression>(*this);
}

} // namespace duckdb
