//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_sequence.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"

namespace duckdb {

//! PhysicalCreateSequence represents a CREATE SEQUENCE command
class PhysicalCreateSequence : public PhysicalOperator {
public:
	explicit PhysicalCreateSequence(unique_ptr<CreateSequenceInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_SEQUENCE, {LogicalType::BIGINT}, estimated_cardinality),
	      info(move(info)) {
	}
    PhysicalCreateSequence(PhysicalCreateSequence const& pcs): PhysicalOperator(PhysicalOperatorType::CREATE_SEQUENCE, {LogicalType::BIGINT}, pcs.estimated_cardinality),
                                                               info(pcs.info->duplicate()) {
	}

	unique_ptr<CreateSequenceInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalCreateSequence>(*this);
    }
};

} // namespace duckdb
