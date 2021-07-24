//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! LogicalComparisonJoin represents a join that involves comparisons between the LHS and RHS
class LogicalComparisonJoin : public LogicalJoin {
public:
	explicit LogicalComparisonJoin(JoinType type,
	                               LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN);

	//! The conditions of the join
	vector<JoinCondition> conditions;
	//! Used for duplicate-eliminated joins
	vector<LogicalType> delim_types;

    // FOR IMPLEMENTATION
    LogicalComparisonJoin(LogicalComparisonJoin const &lcj) : LogicalJoin(lcj) {
        //conditions = lcj.conditions;
        /*conditions.reserve(lcj.conditions.size());
        for (auto const& elem : lcj.conditions) {
            JoinCondition copy_elem;
            conditions.push_back(copy_elem);
        }*/
        conditions = lcj.conditions;
        delim_types = lcj.delim_types;
    }
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalComparisonJoin>(*this);
    }

public:
	string ParamsToString() const override;

public:
	static unique_ptr<LogicalOperator> CreateJoin(JoinType type, unique_ptr<LogicalOperator> left_child,
	                                              unique_ptr<LogicalOperator> right_child,
	                                              unordered_set<idx_t> &left_bindings,
	                                              unordered_set<idx_t> &right_bindings,
	                                              vector<unique_ptr<Expression>> &expressions);
};

} // namespace duckdb
