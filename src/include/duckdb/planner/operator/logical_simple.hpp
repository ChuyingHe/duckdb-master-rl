//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_simple.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalSimple represents a simple logical operator that only passes on the parse info
class LogicalSimple : public LogicalOperator {
public:
	LogicalSimple(LogicalOperatorType type, unique_ptr<ParseInfo> info) : LogicalOperator(type), info(move(info)) {
	}

	unique_ptr<ParseInfo> info;

    // FOR DEBUG
    /*LogicalSimple() : LogicalOperator(LogicalOperatorType::LOGICAL_SHOW) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalSimple>();
    }*/

    // FOR IMPLEMENTATION
    LogicalSimple(LogicalSimple const &ls) : LogicalOperator(ls.type), info(ls.info->clone()) {
    }
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalSimple>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BOOLEAN);
	}
};
} // namespace duckdb
