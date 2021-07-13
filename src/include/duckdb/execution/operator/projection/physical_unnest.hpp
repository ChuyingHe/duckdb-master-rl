//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_unnest.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
class PhysicalUnnest : public PhysicalOperator {
public:
	PhysicalUnnest(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality,
	               PhysicalOperatorType type = PhysicalOperatorType::UNNEST);

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	//! The projection list of the SELECT statement (that contains aggregates)
	vector<unique_ptr<Expression>> select_list;

    PhysicalUnnest(PhysicalUnnest const& pu) : PhysicalOperator(type, pu.types, pu.estimated_cardinality) {
        select_list.reserve(pu.select_list.size());
        for (auto const& exp:pu.select_list) {
            select_list.push_back(exp->Copy());
        }
    }

public:
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalUnnest>(*this);
    }
};

} // namespace duckdb
