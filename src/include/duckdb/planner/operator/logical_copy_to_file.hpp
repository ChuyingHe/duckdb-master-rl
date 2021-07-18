//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_copy_to_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/copy_function.hpp"

namespace duckdb {

class LogicalCopyToFile : public LogicalOperator {
public:
	LogicalCopyToFile(CopyFunction function, unique_ptr<FunctionData> bind_data)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_COPY_TO_FILE), function(function), bind_data(move(bind_data)) {
	}

	CopyFunction function;
	unique_ptr<FunctionData> bind_data;

    LogicalCopyToFile(LogicalCopyToFile const &lctf) : LogicalOperator(lctf), function(lctf.function) {
        bind_data = lctf.bind_data->Copy();
    }

    std::unique_ptr<LogicalOperator> clone() const {
        return make_unique<LogicalCopyToFile>(*this);
    }

protected:
	void ResolveTypes() override {
		types.push_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
