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
    LogicalExport(LogicalExport const &le) : LogicalOperator(le) ,
    function(le.function) {
        copy_info = le.copy_info->Copy();
	}

	CopyFunction function;
	unique_ptr<CopyInfo> copy_info;

    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalExport>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BOOLEAN);
	}
};

} // namespace duckdb
