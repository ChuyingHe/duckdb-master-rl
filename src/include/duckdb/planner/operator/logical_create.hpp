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

	SchemaCatalogEntry *schema;
	unique_ptr<CreateInfo> info;

    // FOR DEBUG
    /*LogicalCreate() : LogicalOperator(type) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalCreate>();
    }*/
    // FOR IMPLEMENTATION
    LogicalCreate(LogicalCreate const &lc) : LogicalOperator(lc) {
        schema = lc.schema;
        info = lc.info->Copy();
    }

    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalCreate>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
