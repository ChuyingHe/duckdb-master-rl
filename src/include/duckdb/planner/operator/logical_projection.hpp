//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalProjection represents the projection list in a SELECT clause
class LogicalProjection : public LogicalOperator {
public:
	LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list);

    LogicalProjection(LogicalProjection const &lp) : LogicalOperator(lp) {
        table_index = lp.table_index;
    }

	idx_t table_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;
    unique_ptr<LogicalOperator> clone() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
