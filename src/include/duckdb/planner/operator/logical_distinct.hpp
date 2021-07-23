//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_distinct.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalDistinct filters duplicate entries from its child operator
class LogicalDistinct : public LogicalOperator {
public:
	LogicalDistinct() : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT) {
	}
	explicit LogicalDistinct(vector<unique_ptr<Expression>> targets)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT), distinct_targets(move(targets)) {
	}
	//! The set of distinct targets (optional).
	vector<unique_ptr<Expression>> distinct_targets;

    // FOR DEBUG
    /*unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalDistinct>();
    }*/
    // FOR IMPLEMENTATION
    LogicalDistinct(LogicalDistinct const &ld) : LogicalOperator(ld) {
        distinct_targets.reserve(ld.distinct_targets.size());
        for (auto const& elem : ld.distinct_targets) {
            distinct_targets.push_back(std::move(elem->Copy()));
        }
    }

    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalDistinct>(*this);
    }

public:
	string ParamsToString() const override;

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
