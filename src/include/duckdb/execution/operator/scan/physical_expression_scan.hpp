//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_expression_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalExpressionScan scans a set of expressions
class PhysicalExpressionScan : public PhysicalOperator {
public:
	PhysicalExpressionScan(vector<LogicalType> types, vector<vector<unique_ptr<Expression>>> expressions,
	                       idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::EXPRESSION_SCAN, move(types), estimated_cardinality),
	      expressions(move(expressions)) {
	}
    PhysicalExpressionScan(PhysicalExpressionScan const& pes) : PhysicalOperator(PhysicalOperatorType::EXPRESSION_SCAN, pes.types, pes.estimated_cardinality)
    {
        expressions.reserve(pes.expressions.size());
        for(const auto& row: pes.expressions) {
            vector<unique_ptr<Expression>> tmp;
            for(const auto& exp: row) {
                tmp.push_back(exp->Copy());    // elem: unique_ptr<Expression> Copy()
            }
            expressions.push_back(tmp); // temp: vector<unique_ptr<Expression>>
        }
	}

	//! The set of expressions to scan
	vector<vector<unique_ptr<Expression>>> expressions;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalExpressionScan>(*this);
    }
};

} // namespace duckdb
