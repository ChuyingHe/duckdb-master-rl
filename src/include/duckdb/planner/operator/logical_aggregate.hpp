//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalAggregate : public LogicalOperator {
public:
	LogicalAggregate(idx_t group_index, idx_t aggregate_index, vector<unique_ptr<Expression>> select_list);

	//! The table index for the groups of the LogicalAggregate
	idx_t group_index;
	//! The table index for the aggregates of the LogicalAggregate
	idx_t aggregate_index;
	//! The set of groups (optional).
	vector<unique_ptr<Expression>> groups;
	//! Group statistics (optional)
	vector<unique_ptr<BaseStatistics>> group_stats;

    // FOR DEBUG
    /*LogicalAggregate() : LogicalOperator(LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalAggregate>();
    }*/
    // FOR IMPLEMENTATION
    LogicalAggregate(LogicalAggregate const &la) : LogicalOperator(la) {
        group_index = la.group_index;
        aggregate_index = la.aggregate_index;
        groups.reserve(la.groups.size());
        for (auto const& group : la.groups) {
            groups.push_back(group->Copy());
        }
        group_stats.reserve(la.group_stats.size());
        for (auto const& gs : la.group_stats) {
            group_stats.push_back(gs->Copy());
        }
    }
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalAggregate>(*this);
    }

public:
	string ParamsToString() const override;

	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
