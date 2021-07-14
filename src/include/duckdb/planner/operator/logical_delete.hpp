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

    LogicalDelete(LogicalDelete const &ld) : LogicalOperator(ld) {
        table = ld.table;
	}

	TableCatalogEntry *table;

    std::unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalDelete>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
