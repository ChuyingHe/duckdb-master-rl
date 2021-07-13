//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

//! PhysicalCreateFunction represents a CREATE FUNCTION command
class PhysicalCreateFunction : public PhysicalOperator {
public:
	explicit PhysicalCreateFunction(unique_ptr<CreateMacroInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_MACRO, {LogicalType::BIGINT}, estimated_cardinality),
	      info(move(info)) {
	}
    PhysicalCreateFunction(PhysicalCreateFunction const& pcf) : PhysicalOperator(PhysicalOperatorType::CREATE_MACRO, {LogicalType::BIGINT}, pcf.estimated_cardinality),
                                                                info(pcf.info->clone()) {
	}
	unique_ptr<CreateMacroInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalCreateFunction>(*this);
    }
};

} // namespace duckdb
