//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_chunk_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalChunkCollectionScan scans a Chunk Collection
class PhysicalChunkScan : public PhysicalOperator {
public:
	PhysicalChunkScan(vector<LogicalType> types, PhysicalOperatorType op_type, idx_t estimated_cardinality)
	    : PhysicalOperator(op_type, move(types), estimated_cardinality), collection(nullptr) {
	}
    PhysicalChunkScan(PhysicalChunkScan const& pcs) : PhysicalOperator(pcs),
                                                      collection(pcs.collection), owned_collection(pcs.owned_collection->Copy()) {
	}

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalChunkScan>(*this);
    }

public:
	// the chunk collection to scan
	ChunkCollection *collection;
	//! Owned chunk collection, if any
	unique_ptr<ChunkCollection> owned_collection;
};

} // namespace duckdb
