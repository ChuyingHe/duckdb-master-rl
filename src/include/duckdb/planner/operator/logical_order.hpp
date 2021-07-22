//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_order.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalOrder represents an ORDER BY clause, sorting the data
class LogicalOrder : public LogicalOperator {
public:
	explicit LogicalOrder(vector<BoundOrderByNode> orders)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_ORDER_BY), orders(move(orders)) {
	}

	vector<BoundOrderByNode> orders;

	string ParamsToString() const override {
		string result;
		for (idx_t i = 0; i < orders.size(); i++) {
			if (i > 0) {
				result += "\n";
			}
			result += orders[i].expression->GetName();
		}
		return result;
	}

    // FOR DEBUG
    /*LogicalOrder() : LogicalOperator(LogicalOperatorType::LOGICAL_ORDER_BY) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalOrder>();
    }*/
    // FOR IMPLEMENTATION
    LogicalOrder(LogicalOrder const &lo) : LogicalOperator(lo), orders(lo.orders) {
    }
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalOrder>(*this);
    }

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
