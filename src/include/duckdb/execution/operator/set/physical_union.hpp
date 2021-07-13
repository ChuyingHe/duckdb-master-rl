//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class PhysicalUnion : public PhysicalOperator {
public:
	PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom,
	              idx_t estimated_cardinality);
    PhysicalUnion(PhysicalUnion const& pu) : PhysicalOperator(PhysicalOperatorType::UNION, pu.types, pu.estimated_cardinality) {
    }

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void FinalizeOperatorState(PhysicalOperatorState &state_p, ExecutionContext &context) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalUnion>(*this);
    }
};

} // namespace duckdb
