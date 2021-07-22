//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class LogicalSet : public LogicalOperator {
public:
	LogicalSet(std::string name_p, Value value_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_SET), name(name_p), value(value_p) {
	}
	std::string name;
	Value value;

    // FOR DEBUG
    /*LogicalSet() : LogicalOperator(LogicalOperatorType::LOGICAL_SET) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalSet>();
    }*/
    // FOR IMPLEMENTATION
    LogicalSet(LogicalSet const &ls) : LogicalOperator(ls) {
        name = ls.name;
        value = ls.value.Copy();
    }

    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalSet>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BOOLEAN);
	}
};

} // namespace duckdb
