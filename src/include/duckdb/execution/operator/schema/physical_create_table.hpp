//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

//! Physically CREATE TABLE statement
class PhysicalCreateTable : public PhysicalOperator {
public:
	PhysicalCreateTable(LogicalOperator &op, SchemaCatalogEntry *schema, unique_ptr<BoundCreateTableInfo> info,
	                    idx_t estimated_cardinality);

    PhysicalCreateTable(PhysicalCreateTable const& pct) : PhysicalOperator(PhysicalOperatorType::CREATE_TABLE, pct.types, pct.estimated_cardinality),
    schema(pct.schema), info(pct.info->clone()) {
    }

	//! Schema to insert to
	SchemaCatalogEntry *schema;
	//! Table name to create
	unique_ptr<BoundCreateTableInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalCreateTable>(*this);
    }
};
} // namespace duckdb
