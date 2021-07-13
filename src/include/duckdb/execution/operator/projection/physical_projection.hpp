//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
public:
	PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                   idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::PROJECTION, move(types), estimated_cardinality),
	      select_list(move(select_list)) {
	}

    PhysicalProjection(PhysicalProjection const& pp) : PhysicalOperator(PhysicalOperatorType::PROJECTION, pp.types, pp.estimated_cardinality) {
        select_list.reserve(pp.select_list.size());
        for (auto const& exp: pp.select_list) {
            select_list.push_back(exp->Copy());
        }
	}

	vector<unique_ptr<Expression>> select_list;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) override;

	string ParamsToString() const override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalProjection>(*this);
    }
};

} // namespace duckdb
