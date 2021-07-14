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

    LogicalPrepare(LogicalPrepare const &lp) : LogicalOperator(lp) {
        name = lp.name;
        prepared = std::make_shared<PreparedStatementData>(*lp.prepared);
    }

    string name;
	shared_ptr<PreparedStatementData> prepared;

    std::unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalPrepare>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BOOLEAN);
	}
};
} // namespace duckdb
