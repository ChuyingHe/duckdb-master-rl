//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_dummy_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalDummyScan : public PhysicalOperator {
public:
	explicit PhysicalDummyScan(vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN, move(types), estimated_cardinality) {
	}
    PhysicalDummyScan(PhysicalDummyScan const& pds) : PhysicalOperator(PhysicalOperatorType::DUMMY_SCAN, pds.types, pds.estimated_cardinality) {
	}
public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalDummyScan>(*this);
    }
};
} // namespace duckdb
