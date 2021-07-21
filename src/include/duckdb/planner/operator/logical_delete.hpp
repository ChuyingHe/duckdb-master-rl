//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalDelete : public LogicalOperator {
public:
	explicit LogicalDelete(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE), table(table) {
	}

	TableCatalogEntry *table;

    // FOR DEBUG
    LogicalDelete() : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalDelete>();
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
