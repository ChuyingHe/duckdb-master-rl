//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class TableCatalogEntry;

class LogicalPrepare : public LogicalOperator {
public:
	LogicalPrepare(string name, shared_ptr<PreparedStatementData> prepared, unique_ptr<LogicalOperator> logical_plan)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_PREPARE), name(name), prepared(move(prepared)) {
		children.push_back(move(logical_plan));
	}

	string name;
	shared_ptr<PreparedStatementData> prepared;

    // FOR DEBUG
    LogicalPrepare() : LogicalOperator(LogicalOperatorType::LOGICAL_PREPARE) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalPrepare>();
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BOOLEAN);
	}
};
} // namespace duckdb
