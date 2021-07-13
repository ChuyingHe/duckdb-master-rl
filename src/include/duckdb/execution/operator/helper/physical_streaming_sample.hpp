//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_streaming_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_sink.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"

namespace duckdb {

//! PhysicalStreamingSample represents a streaming sample using either system or bernoulli sampling
class PhysicalStreamingSample : public PhysicalOperator {
public:
	PhysicalStreamingSample(vector<LogicalType> types, SampleMethod method, double percentage, int64_t seed,
	                        idx_t estimated_cardinality);

    PhysicalStreamingSample(PhysicalStreamingSample const& pss) : PhysicalOperator(PhysicalOperatorType::STREAMING_SAMPLE, pss.types, pss.estimated_cardinality),
    method(pss.method), percentage(pss.percentage), seed(pss.seed) {
    }

	SampleMethod method;
	double percentage;
	int64_t seed;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	string ParamsToString() const override;

    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalStreamingSample>(*this);
    }

private:
	void SystemSample(DataChunk &input, DataChunk &result, PhysicalOperatorState *state);
	void BernoulliSample(DataChunk &input, DataChunk &result, PhysicalOperatorState *state);
};

} // namespace duckdb
