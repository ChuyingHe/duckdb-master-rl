//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/persistent_table_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/table/segment_tree.hpp"

namespace duckdb {
class BaseStatistics;
class PersistentSegment;

class PersistentColumnData {
public:
	virtual ~PersistentColumnData();

    PersistentColumnData(PersistentColumnData &pcd) {
        total_rows = pcd.total_rows;
        stats = pcd.stats->Copy();
        segments.reserve(pcd.segments.size());
        for (const auto& sg:pcd.segments) {
            segments.push_back(make_unique<PersistentSegment>(*sg));
        }
    }

	vector<unique_ptr<PersistentSegment>> segments;
	unique_ptr<BaseStatistics> stats;
	idx_t total_rows = 0;
};

class StandardPersistentColumnData : public PersistentColumnData {
public:
	unique_ptr<PersistentColumnData> validity;
};

class PersistentTableData {
public:
	explicit PersistentTableData(idx_t column_count);
	~PersistentTableData();

    PersistentTableData(PersistentTableData &ptd) {
        column_data.reserve(ptd.column_data.size());
        for (const auto &cd:ptd.column_data) {
            column_data.push_back(make_unique<PersistentColumnData>(*cd));
        }
        versions = ptd.versions;
    }

	vector<unique_ptr<PersistentColumnData>> column_data;
	shared_ptr<SegmentTree> versions;
};

} // namespace duckdb
