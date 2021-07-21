//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_delim_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDelimGet represents a duplicate eliminated scan belonging to a DelimJoin
class LogicalDelimGet : public LogicalOperator {
public:
	LogicalDelimGet(idx_t table_index, vector<LogicalType> types)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DELIM_GET), table_index(table_index) {
		D_ASSERT(types.size() > 0);
		chunk_types = types;
	}

	//! The table index in the current bind context
	idx_t table_index;
	//! The types of the chunk
	vector<LogicalType> chunk_types;

    // FOR DEBUG
    LogicalDelimGet() : LogicalOperator(LogicalOperatorType::LOGICAL_DELIM_GET) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalDelimGet>();
    }

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, chunk_types.size());
	}

protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = chunk_types;
	}
};
} // namespace duckdb
