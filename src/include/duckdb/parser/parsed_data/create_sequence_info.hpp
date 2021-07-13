//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_sequence_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

struct CreateSequenceInfo : public CreateInfo {
	CreateSequenceInfo()
	    : CreateInfo(CatalogType::SEQUENCE_ENTRY), name(string()), usage_count(0), increment(1), min_value(1),
	      max_value(NumericLimits<int64_t>::Maximum()), start_value(1), cycle(false) {
	}
    CreateSequenceInfo(CreateSequenceInfo const& csi) : CreateInfo(CatalogType::SEQUENCE_ENTRY),
    name(csi.name), usage_count(csi.usage_count), increment(csi.increment), min_value(csi.min_value),
    max_value(csi.max_value), start_value(csi.start_value), cycle(csi.cycle) {
	}

	//! Sequence name to create
	string name;
	//! Usage count of the sequence
	uint64_t usage_count;
	//! The increment value
	int64_t increment;
	//! The minimum value of the sequence
	int64_t min_value;
	//! The maximum value of the sequence
	int64_t max_value;
	//! The start value of the sequence
	int64_t start_value;
	//! Whether or not the sequence cycles
	bool cycle;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateSequenceInfo>();
		CopyProperties(*result);
		result->name = name;
		result->usage_count = usage_count;
		result->increment = increment;
		result->min_value = min_value;
		result->max_value = max_value;
		result->start_value = start_value;
		result->cycle = cycle;
		return move(result);
	}

    unique_ptr<ParseInfo> clone() const override {
        Copy();
    }

    unique_ptr<CreateSequenceInfo> duplicate() {
	    return make_unique<CreateSequenceInfo>(*this);
	}
};

} // namespace duckdb
