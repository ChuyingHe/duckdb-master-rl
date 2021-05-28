#include "duckdb/planner/expression_binder/alter_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

AlterBinder::AlterBinder(Binder &binder, ClientContext &context, string table, vector<ColumnDefinition> &columns,
                         vector<column_t> &bound_columns, LogicalType target_type)
    : ExpressionBinder(binder, context), table(move(table)), columns(columns), bound_columns(bound_columns) {
	this->target_type = move(target_type);
}

BindResult AlterBinder::BindExpression(unique_ptr<ParsedExpression> *expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = **expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in alter statement");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in alter statement");
	case ExpressionClass::COLUMN_REF:
		return BindColumn((ColumnRefExpression &)expr);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string AlterBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in alter statement";
}

BindResult AlterBinder::BindColumn(ColumnRefExpression &colref) {
	if (!colref.table_name.empty() && colref.table_name != table) {
		throw BinderException("Cannot reference table %s from within alter statement for table %s!", colref.table_name,
		                      table);
	}
	for (idx_t i = 0; i < columns.size(); i++) {
		if (colref.column_name == columns[i].name) {
			bound_columns.push_back(i);
			return BindResult(make_unique<BoundReferenceExpression>(columns[i].type, bound_columns.size() - 1));
		}
	}
	throw BinderException("Table does not contain column %s referenced in alter statement!", colref.column_name);
}

} // namespace duckdb
