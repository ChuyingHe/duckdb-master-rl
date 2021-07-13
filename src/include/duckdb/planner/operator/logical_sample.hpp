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

    LogicalSample(LogicalSample const &ls) : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE),
                                             sample_options(ls.sample_options->Copy()){
        children.reserve(ls.children.size());
        for (auto const& child: ls.children) {
            children.push_back(child->clone());
        }
	}

	//! The sample options
	unique_ptr<SampleOptions> sample_options;

public:
	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

    std::unique_ptr<LogicalOperator> clone() const override {
        //return make_unique<LogicalSample>(this->sample_options->Copy(), this->children[0]->clone());
        return make_unique<LogicalSample>(*this);
    }

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};

} // namespace duckdb
