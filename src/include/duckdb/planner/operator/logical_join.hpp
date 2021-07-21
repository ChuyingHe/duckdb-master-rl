//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
public:
	explicit LogicalJoin(JoinType type, LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_JOIN);

	// Gets the set of table references that are reachable from this node
	static void GetTableReferences(LogicalOperator &op, unordered_set<idx_t> &bindings);
	static void GetExpressionBindings(Expression &expr, unordered_set<idx_t> &bindings);

	//! The type of the join (INNER, OUTER, etc...)
	JoinType join_type;
	//! Table index used to refer to the MARK column (in case of a MARK join)
	idx_t mark_index;
	//! The columns of the LHS that are output by the join
	vector<idx_t> left_projection_map;
	//! The columns of the RHS that are output by the join
	vector<idx_t> right_projection_map;

    // FOR DEBUG
    LogicalJoin() : LogicalOperator(LogicalOperatorType::LOGICAL_JOIN) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalJoin>();
    }

public:
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
