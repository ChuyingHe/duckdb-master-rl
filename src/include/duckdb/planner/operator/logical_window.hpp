//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalWindow : public LogicalOperator {
public:
	explicit LogicalWindow(idx_t window_index)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_WINDOW), window_index(window_index) {
	}

    LogicalWindow(LogicalWindow const& lw) : LogicalOperator(lw) {
        window_index = lw.window_index;
	}

	idx_t window_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;
    std::unique_ptr<LogicalOperator> clone() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
