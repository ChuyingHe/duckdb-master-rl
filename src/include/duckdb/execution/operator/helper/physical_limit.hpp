//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
namespace duckdb {

//! PhyisicalLimit represents the LIMIT operator
class PhysicalLimit : public PhysicalOperator {
public:
	PhysicalLimit(vector<LogicalType> types, idx_t limit, idx_t offset, unique_ptr<Expression> limit_expression,
	              unique_ptr<Expression> offset_expression, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::LIMIT, move(types), estimated_cardinality), limit(limit),
	      offset(offset), limit_expression(move(limit_expression)), offset_expression(move(offset_expression)) {
	}

	idx_t limit;
	idx_t offset;
	unique_ptr<Expression> limit_expression;
	unique_ptr<Expression> offset_expression;

    PhysicalLimit(PhysicalLimit const& pl) : PhysicalOperator(PhysicalOperatorType::LIMIT, pl.types, pl.estimated_cardinality),
    limit(pl.limit), offset(pl.offset), limit_expression(pl.limit_expression->Copy()), offset_expression(pl.offset_expression->Copy()) {
    }

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalLimit>(*this);
    }
};

} // namespace duckdb
