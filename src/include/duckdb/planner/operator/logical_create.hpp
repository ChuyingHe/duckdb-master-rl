//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

//! LogicalCreate represents a CREATE operator
class LogicalCreate : public LogicalOperator {
public:
	LogicalCreate(LogicalOperatorType type, unique_ptr<CreateInfo> info, SchemaCatalogEntry *schema = nullptr)
	    : LogicalOperator(type), schema(schema), info(move(info)) {
	}

    LogicalCreate(LogicalCreate const &lc): LogicalOperator(lc.type),
    schema(lc.schema), info(this->info->Copy()) {
	}

	SchemaCatalogEntry *schema;
	unique_ptr<CreateInfo> info;

    std::unique_ptr<LogicalOperator> clone() const {
        // return make_unique<LogicalCreate>(this->type, this->info->Copy(), this->schema);
        return make_unique<LogicalCreate>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
