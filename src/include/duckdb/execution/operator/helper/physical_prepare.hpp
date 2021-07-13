//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_prepare.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

namespace duckdb {

class PhysicalPrepare : public PhysicalOperator {
public:
	PhysicalPrepare(string name, shared_ptr<PreparedStatementData> prepared, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::PREPARE, {LogicalType::BOOLEAN}, estimated_cardinality), name(name),
	      prepared(move(prepared)) {
	}

	string name;
	shared_ptr<PreparedStatementData> prepared;

    PhysicalPrepare(PhysicalPrepare const& pp) : PhysicalOperator(PhysicalOperatorType::PREPARE, {LogicalType::BOOLEAN}, pp.estimated_cardinality),
    name(pp.name), prepared(move(pp.prepared)) {    //std::move() didnt call copy constructor
    }

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalPrepare>(*this);
    }
};

} // namespace duckdb
