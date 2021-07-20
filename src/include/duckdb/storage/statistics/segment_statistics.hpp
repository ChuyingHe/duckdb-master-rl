//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/segment_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {
struct TableFilter;

class SegmentStatistics {
public:
	SegmentStatistics(LogicalType type, idx_t type_size);
	SegmentStatistics(LogicalType type, idx_t type_size, unique_ptr<BaseStatistics> statistics);

    SegmentStatistics(SegmentStatistics const& ss) {
        type = ss.type;
        type_size = ss.type_size;
        statistics = ss.statistics->Copy();
    }

	LogicalType type;
	idx_t type_size;

	//! Type-specific statistics of the segment
	unique_ptr<BaseStatistics> statistics;

public:
	bool CheckZonemap(TableFilter &filter);
	void Reset();
};

} // namespace duckdb
