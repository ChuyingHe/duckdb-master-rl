//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_recursive_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {
class Pipeline;

class PhysicalRecursiveCTE : public PhysicalOperator {
public:
	PhysicalRecursiveCTE(vector<LogicalType> types, bool union_all, unique_ptr<PhysicalOperator> top,
	                     unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality);
	~PhysicalRecursiveCTE() override;

	bool union_all;
	std::shared_ptr<ChunkCollection> working_table;
	ChunkCollection intermediate_table;
	vector<unique_ptr<Pipeline>> pipelines;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void FinalizeOperatorState(PhysicalOperatorState &state_p, ExecutionContext &context) override;

private:
	//! Probe Hash Table and eliminate duplicate rows
	idx_t ProbeHT(DataChunk &chunk, PhysicalOperatorState *state);

	void ExecuteRecursivePipelines(ExecutionContext &context);
};

} // namespace duckdb
