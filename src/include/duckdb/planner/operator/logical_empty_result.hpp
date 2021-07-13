//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_empty_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalEmptyResult returns an empty result. This is created by the optimizer if it can reason that ceratin parts of
//! the tree will always return an empty result.
class LogicalEmptyResult : public LogicalOperator {
public:
	explicit LogicalEmptyResult(unique_ptr<LogicalOperator> op);

    LogicalEmptyResult(LogicalEmptyResult const &ler) : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT),
    return_types(ler.return_types), bindings(ler.bindings) {
    }

	//! The set of return types of the empty result
	vector<LogicalType> return_types;
	//! The columns that would be bound at this location (if the subtree was not optimized away)
	vector<ColumnBinding> bindings;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return bindings;
	}

    std::unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalEmptyResult>(*this);
        // return make_unique_base<LogicalOperator, LogicalEmptyResult>(*this);
    }

protected:
	void ResolveTypes() override {
		this->types = return_types;
	}
};
} // namespace duckdb
