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

    // FOR DEBUG
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalCrossProduct>();
    }

public:
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
