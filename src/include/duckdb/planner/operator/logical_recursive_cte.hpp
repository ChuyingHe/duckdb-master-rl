//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalRecursiveCTE : public LogicalOperator {
public:
	LogicalRecursiveCTE(idx_t table_index, idx_t column_count, bool union_all, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom, LogicalOperatorType type)
	    : LogicalOperator(type), union_all(union_all), table_index(table_index), column_count(column_count) {
		D_ASSERT(type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE);
		children.push_back(move(top));
		children.push_back(move(bottom));
	}

	bool union_all;
	idx_t table_index;
	idx_t column_count;

    // FOR DEBUG
    LogicalRecursiveCTE() : LogicalOperator(LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalRecursiveCTE>();
    }

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, column_count);
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
