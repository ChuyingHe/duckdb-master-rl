//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_vacuum.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"

namespace duckdb {

//! PhysicalVacuum represents a VACUUM operation (e.g. VACUUM or ANALYZE)
class PhysicalVacuum : public PhysicalOperator {
public:
	explicit PhysicalVacuum(unique_ptr<VacuumInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::VACUUM, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(move(info)) {
	}

    PhysicalVacuum(PhysicalVacuum const& pv) : PhysicalOperator(PhysicalOperatorType::VACUUM, {LogicalType::BOOLEAN}, estimated_cardinality),
                                               info(pv.info->duplicate()) {
	}

	unique_ptr<VacuumInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalVacuum>(*this);
    }

};

} // namespace duckdb
