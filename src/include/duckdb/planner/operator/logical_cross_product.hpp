//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalCrossProduct represents a cross product between two relations
class LogicalCrossProduct : public LogicalOperator {
public:
	LogicalCrossProduct();
    LogicalCrossProduct(LogicalCrossProduct const &lcp) : LogicalOperator(LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
    };

public:
	vector<ColumnBinding> GetColumnBindings() override;

    std::unique_ptr<LogicalOperator> clone() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
