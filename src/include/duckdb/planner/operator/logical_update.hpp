//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalUpdate : public LogicalOperator {
public:
	explicit LogicalUpdate(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE), table(table) {
	}

	TableCatalogEntry *table;
	vector<column_t> columns;
	vector<unique_ptr<Expression>> bound_defaults;
	bool is_index_update;

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
