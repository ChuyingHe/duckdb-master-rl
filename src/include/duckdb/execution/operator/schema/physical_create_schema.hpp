//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

//! PhysicalCreateSchema represents a CREATE SCHEMA command
class PhysicalCreateSchema : public PhysicalOperator {
public:
	explicit PhysicalCreateSchema(unique_ptr<CreateSchemaInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_SCHEMA, {LogicalType::BIGINT}, estimated_cardinality),
	      info(move(info)) {
	}
    PhysicalCreateSchema(PhysicalCreateSchema const& pcs) : PhysicalOperator(PhysicalOperatorType::CREATE_SCHEMA, {LogicalType::BIGINT}, pcs.estimated_cardinality),
                                                            info(pcs.info->duplicate()) {
	}

	unique_ptr<CreateSchemaInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalCreateSchema>(*this);
    }
};

} // namespace duckdb
