//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalProjection represents the projection list in a SELECT clause
class LogicalProjection : public LogicalOperator {
public:
	LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list);

	idx_t table_index;

    // FOR DEBUG
    /*LogicalProjection() : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalProjection>();
    }*/

    // FOR IMPLEMENTATION
    LogicalProjection(LogicalProjection const &lp) : LogicalOperator(lp) {
        table_index = lp.table_index;
    }
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalProjection>(*this);
    }

public:
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
