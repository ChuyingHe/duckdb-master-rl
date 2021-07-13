//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_top_n.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalTopN represents a comibination of ORDER BY and LIMIT clause, using Min/Max Heap
class LogicalTopN : public LogicalOperator {
public:
	LogicalTopN(vector<BoundOrderByNode> orders, int64_t limit, int64_t offset)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_TOP_N), orders(move(orders)), limit(limit), offset(offset) {
	}

    LogicalTopN(LogicalTopN const& ltopn) : LogicalOperator(LogicalOperatorType::LOGICAL_TOP_N),
    limit(ltopn.limit), offset(ltopn.offset) {
        orders.reserve(ltopn.orders.size());
        for (auto const& order : ltopn.orders) {
            BoundOrderByNode bobn(order);
            orders.push_back(bobn);
        }
	}

	vector<BoundOrderByNode> orders;
	//! The maximum amount of elements to emit
	int64_t limit;
	//! The offset from the start to begin emitting elements
	int64_t offset;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

    std::unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalTopN>(*this);
    }

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
