//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalInsert represents an insertion of data into a base table
class LogicalInsert : public LogicalOperator {
public:
	explicit LogicalInsert(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT), table(table) {
	}

    LogicalInsert(LogicalInsert const &li) : LogicalOperator(li) {
        insert_values.reserve(li.insert_values.size());
        for(const auto& row: li.insert_values) {
            vector<unique_ptr<Expression>> tmp;
            for(const auto& exp: row) {
                tmp.push_back(exp->Copy());    // elem: unique_ptr<Expression> Copy()
            }
            insert_values.push_back(tmp); // temp: vector<unique_ptr<Expression>>
        }

        column_index_map = li.column_index_map;
        expected_types = li.expected_types;
        table = li.table;
        bound_defaults.reserve(li.bound_defaults.size());
        for (auto const& bd:li.bound_defaults) {
            bound_defaults.push_back(bd->Copy());
        }
	}

	vector<vector<unique_ptr<Expression>>> insert_values;
	//! The insertion map ([table_index -> index in result, or INVALID_INDEX if not specified])
	vector<idx_t> column_index_map;
	//! The expected types for the INSERT statement (obtained from the column types)
	vector<LogicalType> expected_types;
	//! The base table to insert into
	TableCatalogEntry *table;
	//! The default statements used by the table
	vector<unique_ptr<Expression>> bound_defaults;

    std::unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalInsert>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
