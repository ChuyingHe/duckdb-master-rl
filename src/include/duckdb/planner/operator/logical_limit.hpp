//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_limit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalLimit represents a LIMIT clause
class LogicalLimit : public LogicalOperator {
public:
	LogicalLimit(int64_t limit_val, int64_t offset_val, unique_ptr<Expression> limit, unique_ptr<Expression> offset)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT), limit_val(limit_val), offset_val(offset_val),
	      limit(move(limit)), offset(move(offset)) {
	}
    LogicalLimit(LogicalLimit const &ll) : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT),
    limit_val(ll.limit_val), offset_val(ll.offset_val), limit(ll.limit->Copy()), offset(ll.offset->Copy())  {
	}

	//! Limit and offset values in case they are constants, used in optimizations.
	int64_t limit_val;
	int64_t offset_val;
	//! The maximum amount of elements to emit
	unique_ptr<Expression> limit;
	//! The offset from the start to begin emitting elements
	unique_ptr<Expression> offset;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}
    std::unique_ptr<LogicalOperator> clone() const override {
        // return make_unique<LogicalLimit>(this->limit_val, this->offset_val, this->limit->Copy(), this->offset->Copy());
        return make_unique<LogicalLimit>(*this);
    }

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
