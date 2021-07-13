//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalExecute : public PhysicalOperator {
public:
	explicit PhysicalExecute(PhysicalOperator *plan)
	    : PhysicalOperator(PhysicalOperatorType::EXECUTE, plan->types, -1), plan(plan) {
	}
    PhysicalExecute(PhysicalExecute const& pe) : PhysicalOperator(PhysicalOperatorType::EXECUTE, pe.plan->types, pe.estimated_cardinality),
    plan(pe.plan) {
	}

	PhysicalOperator *plan;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void FinalizeOperatorState(PhysicalOperatorState &state_p, ExecutionContext &context) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalExecute>(*this);
    }
};

} // namespace duckdb
