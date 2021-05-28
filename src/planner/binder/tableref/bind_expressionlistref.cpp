#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_expressionlistref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(ExpressionListRef &expr) {
	auto result = make_unique<BoundExpressionListRef>();
	result->types = expr.expected_types;
	result->names = expr.expected_names;
	// bind value list
	InsertBinder binder(*this, context);
	binder.target_type = LogicalType(LogicalTypeId::INVALID);
	for (idx_t list_idx = 0; list_idx < expr.values.size(); list_idx++) {
		auto &expression_list = expr.values[list_idx];
		if (result->names.empty()) {
			// no names provided, generate them
			for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
				result->names.push_back("col" + to_string(val_idx));
			}
		}

		vector<unique_ptr<Expression>> list;
		if (result->types.empty()) {
			// for the first list, we set the expected types as the types of these expressions
			for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
				LogicalType result_type;
				auto expr = binder.Bind(expression_list[val_idx], &result_type);
				result->types.push_back(result_type);
				list.push_back(move(expr));
			}
		} else {
			// for subsequent lists, we apply the expected types we found in the first list
			for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
				binder.target_type = result->types[val_idx];
				list.push_back(binder.Bind(expression_list[val_idx]));
			}
		}
		result->values.push_back(move(list));
	}
	result->bind_index = GenerateTableIndex();
	bind_context.AddGenericBinding(result->bind_index, expr.alias, result->names, result->types);
	return move(result);
}

} // namespace duckdb
