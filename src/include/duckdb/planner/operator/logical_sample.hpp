//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

//! LogicalSample represents a SAMPLE clause
class LogicalSample : public LogicalOperator {
public:
	LogicalSample(unique_ptr<SampleOptions> sample_options_p, unique_ptr<LogicalOperator> child)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE), sample_options(move(sample_options_p)) {
		children.push_back(move(child));
	}

	//! The sample options
	unique_ptr<SampleOptions> sample_options;

    // FOR DEBUG
    LogicalSample() : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalSample>();
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
