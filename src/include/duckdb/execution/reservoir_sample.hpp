//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/reservoir_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "pcg_random.hpp"

#include <queue>

namespace duckdb {

class BaseReservoirSampling {
public:
	explicit BaseReservoirSampling(int64_t seed);
	BaseReservoirSampling();

    BaseReservoirSampling(BaseReservoirSampling const& brs) {
        //rng = brs.rng;
        //uniform_dist = brs.uniform_dist;
        reservoir_weights = brs.reservoir_weights;
        next_index = brs.next_index;
        min_threshold = brs.min_threshold;
        min_entry = brs.min_entry;
        current_count = brs.current_count;
    }

	void InitializeReservoir(idx_t cur_size, idx_t sample_size);

	void SetNextEntry();

	void ReplaceElement();

	//! These are only for pcg generator
	unique_ptr<pcg32> rng;
	unique_ptr<std::uniform_real_distribution<double>> uniform_dist;

	//! Priority queue of [random element, index] for each of the elements in the sample
	std::priority_queue<std::pair<double, idx_t>> reservoir_weights;
	//! The next element to sample
	idx_t next_index;
	//! The reservoir threshold of the current min entry
	double min_threshold;
	//! The reservoir index of the current min entry
	idx_t min_entry;
	//! The current count towards next index (i.e. we will replace an entry in next_index - current_count tuples)
	idx_t current_count;
};

class BlockingSample {
public:
	explicit BlockingSample(int64_t seed) : random(seed), reservoirSampling(seed) {
	}
	virtual ~BlockingSample() {
	}

    BlockingSample(BlockingSample const& bs): random(bs.random), reservoirSampling(bs.reservoirSampling) {
	}

    virtual unique_ptr<BlockingSample> clone() = 0;

	//! Add a chunk of data to the sample
	virtual void AddToReservoir(DataChunk &input) = 0;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	// sample is completely built.
	virtual unique_ptr<DataChunk> GetChunk() = 0;

protected:
	//! The random generator
	RandomEngine random;
	//! The reservoir sampling
	BaseReservoirSampling reservoirSampling;
};

//! The reservoir sample class maintains a streaming sample of fixed size "sample_count"
class ReservoirSample : public BlockingSample {
public:
	ReservoirSample(idx_t sample_count, int64_t seed) : BlockingSample(seed), sample_count(sample_count) {
	}

	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

    ReservoirSample(ReservoirSample const& rs) : BlockingSample(rs) {
        sample_count = rs.sample_count;
        reservoir = rs.reservoir;
    }

    unique_ptr<BlockingSample> clone() override {
        return make_unique<ReservoirSample>(*this);
    }

    unique_ptr<ReservoirSample> duplicate() {
        return make_unique<ReservoirSample>(*this);
    }

    //! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunk() override;

private:
	//! Replace a single element of the input
	void ReplaceElement(DataChunk &input, idx_t index_in_chunk);

	//! Fills the reservoir up until sample_count entries, returns how many entries are still required
	idx_t FillReservoir(DataChunk &input);

private:
	//! The size of the reservoir sample
	idx_t sample_count;
	//! The current reservoir
	ChunkCollection reservoir;
};

//! The reservoir sample sample_size class maintains a streaming sample of variable size
class ReservoirSamplePercentage : public BlockingSample {
	constexpr static idx_t RESERVOIR_THRESHOLD = 100000;

public:
	ReservoirSamplePercentage(double percentage, int64_t seed);

    ReservoirSamplePercentage(ReservoirSamplePercentage const& rsp) : BlockingSample(rsp) {
        sample_percentage = rsp.sample_percentage;
        reservoir_sample_size = rsp.reservoir_sample_size;
        current_sample = rsp.current_sample->duplicate();

        finished_samples.reserve(rsp.finished_samples.size());
        for (auto const& elem: rsp.finished_samples) {
            finished_samples.push_back(elem->duplicate());
        }

        current_count = rsp.current_count;
        is_finalized = rsp.is_finalized;
    }

	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunk() override;

    unique_ptr<BlockingSample> clone() override {
        return make_unique<ReservoirSamplePercentage>(*this);
    }

private:
	void Finalize();

private:
	//! The sample_size to sample
	double sample_percentage;
	//! The fixed sample size of the sub-reservoirs
	idx_t reservoir_sample_size;
	//! The current sample
	unique_ptr<ReservoirSample> current_sample;
	//! The set of finished samples of the reservoir sample
	vector<unique_ptr<ReservoirSample>> finished_samples;
	//! The amount of tuples that have been processed so far
	idx_t current_count = 0;
	//! Whether or not the stream is finalized. The stream is automatically finalized on the first call to GetChunk();
	bool is_finalized;
};

} // namespace duckdb
