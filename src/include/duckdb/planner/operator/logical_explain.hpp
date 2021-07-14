//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_explain.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalExplain : public LogicalOperator {
public:
	explicit LogicalExplain(unique_ptr<LogicalOperator> plan) : LogicalOperator(LogicalOperatorType::LOGICAL_EXPLAIN) {
		children.push_back(move(plan));
	}

    LogicalExplain(LogicalExplain const &le) : LogicalOperator(le){
        physical_plan = le.physical_plan;
        logical_plan_unopt = le.logical_plan_unopt;
        logical_plan_opt = le.logical_plan_opt;
	}

	string physical_plan;
	string logical_plan_unopt;
	string logical_plan_opt;

    std::unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalExplain>(*this);
    }

protected:
	void ResolveTypes() override {
		types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	}
	vector<ColumnBinding> GetColumnBindings() override {
		return {ColumnBinding(0, 0), ColumnBinding(0, 1)};
	}
};
} // namespace duckdb
