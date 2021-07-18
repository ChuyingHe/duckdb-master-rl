#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

BoundFunctionExpression::BoundFunctionExpression(LogicalType return_type, ScalarFunction bound_function,
                                                 vector<unique_ptr<Expression>> arguments,
                                                 unique_ptr<FunctionData> bind_info, bool is_operator)
    : Expression(ExpressionType::BOUND_FUNCTION, ExpressionClass::BOUND_FUNCTION, move(return_type)),
      function(move(bound_function)), children(move(arguments)), bind_info(move(bind_info)), is_operator(is_operator) {
}

bool BoundFunctionExpression::HasSideEffects() const {
	return function.has_side_effects ? true : Expression::HasSideEffects();
}

bool BoundFunctionExpression::IsFoldable() const {
	// functions with side effects cannot be folded: they have to be executed once for every row
	return function.has_side_effects ? false : Expression::IsFoldable();
}

string BoundFunctionExpression::ToString() const {
	string result = function.name + "(";
	result += StringUtil::Join(children, children.size(), ", ",
	                           [](const unique_ptr<Expression> &child) { return child->GetName(); });
	result += ")";
	return result;
}

hash_t BoundFunctionExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(result, function.Hash());
}

bool BoundFunctionExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundFunctionExpression *)other_p;
	if (other->function != function) {
		return false;
	}
	if (children.size() != other->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	if (!FunctionData::Equals(bind_info.get(), other->bind_info.get())) {
		return false;
	}
	return true;
}
BoundFunctionExpression::BoundFunctionExpression(BoundFunctionExpression const& bfe) : Expression(bfe), function(bfe.function) {
    children.reserve(bfe.children.size());
    for (auto &child : bfe.children) {
        children.push_back(child->Copy());
    }
    bind_info = bfe.bind_info->Copy();
    is_operator = bfe.is_operator;
}

unique_ptr<Expression> BoundFunctionExpression::Copy() {
	return make_unique<BoundFunctionExpression>(*this);
}

} // namespace duckdb
