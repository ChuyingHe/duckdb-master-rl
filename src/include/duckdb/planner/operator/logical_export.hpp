//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_export.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class LogicalExport : public LogicalOperator {
public:
	LogicalExport(CopyFunction function, unique_ptr<CopyInfo> copy_info)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPORT), function(function), copy_info(move(copy_info)) {
	}
	CopyFunction function;
	unique_ptr<CopyInfo> copy_info;

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BOOLEAN);
	}
};

} // namespace duckdb
