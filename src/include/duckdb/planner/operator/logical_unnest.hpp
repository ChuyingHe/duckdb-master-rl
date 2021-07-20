//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_unnest.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalUnnest : public LogicalOperator {
public:
	explicit LogicalUnnest(idx_t unnest_index)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_UNNEST), unnest_index(unnest_index) {
	}

    LogicalUnnest(LogicalUnnest const& lu) : LogicalOperator(lu) {
        unnest_index = lu.unnest_index;
	}

	idx_t unnest_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;
    unique_ptr<LogicalOperator> clone() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
