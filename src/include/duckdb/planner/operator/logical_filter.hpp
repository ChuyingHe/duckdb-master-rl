//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalFilter represents a filter operation (e.g. WHERE or HAVING clause)
class LogicalFilter : public LogicalOperator {
public:
	explicit LogicalFilter(unique_ptr<Expression> expression);
	LogicalFilter();

    LogicalFilter(LogicalFilter const& lf) : LogicalOperator(lf) {
        projection_map = lf.projection_map;
    }

	vector<idx_t> projection_map;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	bool SplitPredicates() {
		return SplitPredicates(expressions);
	}
	//! Splits up the predicates of the LogicalFilter into a set of predicates
	//! separated by AND Returns whether or not any splits were made
	static bool SplitPredicates(vector<unique_ptr<Expression>> &expressions);

    std::unique_ptr<LogicalOperator> clone() const override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
