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

    LogicalSimple(LogicalSimple const &ls) : LogicalOperator(ls.type),
    info(ls.info->clone()) {
	}

	unique_ptr<ParseInfo> info;

    std::unique_ptr<LogicalOperator> clone() const override {
        // return make_unique<LogicalSimple>(this->type, this->info->clone());
        return make_unique<LogicalSimple>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BOOLEAN);
	}
};
} // namespace duckdb
