#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

BindResult SelectBinder::BindAggregate(FunctionExpression &aggr, AggregateFunctionCatalogEntry *func, idx_t depth) {
	// first bind the child of the aggregate expression (if any)
	this->bound_aggregate = true;
	unique_ptr<Expression> bound_filter;
	AggregateBinder aggregate_binder(binder, context);
	string error, filter_error;

	// Now we bind the filter (if any)
	if (aggr.filter) {
		aggregate_binder.BindChild(aggr.filter, 0, error);
	}

	for (auto &child : aggr.children) {
		aggregate_binder.BindChild(child, 0, error);
	}
	if (!error.empty()) {
		// failed to bind child
		if (aggregate_binder.BoundColumns()) {
			for (idx_t i = 0; i < aggr.children.size(); i++) {
				// however, we bound columns!
				// that means this aggregation belongs to this node
				// check if we have to resolve any errors by binding with parent binders
				bool success = aggregate_binder.BindCorrelatedColumns(aggr.children[i]);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (!success) {
					throw BinderException(error);
				}
				auto &bound_expr = (BoundExpression &)*aggr.children[i];
				ExtractCorrelatedExpressions(binder, *bound_expr.expr);
			}
			if (aggr.filter) {
				bool success = aggregate_binder.BindCorrelatedColumns(aggr.filter);
				// if there is still an error after this, we could not successfully bind the aggregate
				if (!success) {
					throw BinderException(error);
				}
				auto &bound_expr = (BoundExpression &)*aggr.filter;
				ExtractCorrelatedExpressions(binder, *bound_expr.expr);
			}
		} else {
			// we didn't bind columns, try again in children
			return BindResult(error);
		}
	}
	if (!filter_error.empty()) {
		return BindResult(filter_error);
	}

	if (aggr.filter) {
		auto &child = (BoundExpression &)*aggr.filter;
		bound_filter = move(child.expr);
	}
	// all children bound successfully
	// extract the children and types
	vector<LogicalType> types;
	vector<LogicalType> arguments;
	vector<unique_ptr<Expression>> children;
	for (idx_t i = 0; i < aggr.children.size(); i++) {
		auto &child = (BoundExpression &)*aggr.children[i];
		types.push_back(child.expr->return_type);
		arguments.push_back(child.expr->return_type);
		children.push_back(move(child.expr));
	}

	// bind the aggregate
	idx_t best_function = Function::BindFunction(func->name, func->functions, types, error);
	if (best_function == INVALID_INDEX) {
		throw BinderException(binder.FormatError(aggr, error));
	}
	// found a matching function!
	auto &bound_function = func->functions[best_function];

	auto aggregate = AggregateFunction::BindAggregateFunction(context, bound_function, move(children),
	                                                          move(bound_filter), aggr.distinct);

	auto return_type = aggregate->return_type;

	// check for all the aggregates if this aggregate already exists
	idx_t aggr_index;
	auto entry = node.aggregate_map.find(aggregate.get());
	if (entry == node.aggregate_map.end()) {
		// new aggregate: insert into aggregate list
		aggr_index = node.aggregates.size();
		node.aggregate_map.insert(make_pair(aggregate.get(), aggr_index));
		node.aggregates.push_back(move(aggregate));
	} else {
		// duplicate aggregate: simplify refer to this aggregate
		aggr_index = entry->second;
	}

	// now create a column reference referring to the aggregate
	auto colref =
	    make_unique<BoundColumnRefExpression>(aggr.alias.empty() ? node.aggregates[aggr_index]->ToString() : aggr.alias,
	                                          return_type, ColumnBinding(node.aggregate_index, aggr_index), depth);
	// move the aggregate expression into the set of bound aggregates
	return BindResult(move(colref));
}
} // namespace duckdb
