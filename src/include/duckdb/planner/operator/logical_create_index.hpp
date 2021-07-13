//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateIndex : public LogicalOperator {
public:
	LogicalCreateIndex(TableCatalogEntry &table, vector<column_t> column_ids,
	                   vector<unique_ptr<Expression>> expressions, unique_ptr<CreateIndexInfo> info)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX), table(table), column_ids(column_ids),
	      info(std::move(info)) {
		for (auto &expr : expressions) {
			this->unbound_expressions.push_back(expr->Copy());
		}
		this->expressions = move(expressions);
	}

    LogicalCreateIndex(LogicalCreateIndex const &lci) : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX),
    table(lci.table), column_ids(lci.column_ids), info(make_unique<CreateIndexInfo>(*lci.info))
    {
        unbound_expressions.reserve(lci.unbound_expressions.size());
        for (const auto& expression: lci.unbound_expressions) {
            unbound_expressions.push_back(expression->Copy());
        }
    }

    std::unique_ptr<LogicalOperator> clone() const {
        /*vector<unique_ptr<Expression>> expressions;

        expressions.reserve(this->expressions.size());
        for (const auto& expression: this->expressions) {
            expressions.push_back(expression->Copy());
        }
        // info = make_unique<CreateIndexInfo>(*this->info);
        CreateIndexInfo cii = *this->info;

        return make_unique<LogicalCreateIndex>(this->table, this->column_ids, expressions, make_unique<CreateIndexInfo>(*this->info));*/
        return make_unique<LogicalCreateIndex>(*this);
    }

	//! The table to create the index for
	TableCatalogEntry &table;
	//! Column IDs needed for index creation
	vector<column_t> column_ids;
	// Info for index creation
	unique_ptr<CreateIndexInfo> info;
	//! Unbound expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
