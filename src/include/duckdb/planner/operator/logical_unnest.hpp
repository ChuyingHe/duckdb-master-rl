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

	idx_t unnest_index;

    // FOR DEBUG
    LogicalUnnest() : LogicalOperator(LogicalOperatorType::LOGICAL_UNNEST) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalUnnest>();
    }

public:
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
