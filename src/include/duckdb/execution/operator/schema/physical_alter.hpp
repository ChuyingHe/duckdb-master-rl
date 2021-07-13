//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_alter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

//! PhysicalAlter represents an ALTER TABLE command
class PhysicalAlter : public PhysicalOperator {
public:
	explicit PhysicalAlter(unique_ptr<AlterInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::ALTER, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(move(info)) {
	}
    PhysicalAlter(PhysicalAlter const& pa) : PhysicalOperator(PhysicalOperatorType::ALTER, {LogicalType::BOOLEAN}, pa.estimated_cardinality),
                                             info(pa.info->Copy()) {
	}

	unique_ptr<AlterInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalAlter>(*this);
    }
};

} // namespace duckdb
