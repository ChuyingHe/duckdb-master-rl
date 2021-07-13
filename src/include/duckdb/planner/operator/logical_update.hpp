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

    LogicalUpdate(LogicalUpdate const& lu) : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE),
    table(lu.table), columns(lu.columns),  is_index_update(lu.is_index_update) {
	    //FIXME: TableCatalogEntry

        bound_defaults.reserve(lu.bound_defaults.size());
        for (auto const& bd: lu.bound_defaults) {
            bound_defaults.push_back(bd->Copy());
        }
	}

	TableCatalogEntry *table;
	vector<column_t> columns;
	vector<unique_ptr<Expression>> bound_defaults;
	bool is_index_update;

    std::unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalUpdate>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
