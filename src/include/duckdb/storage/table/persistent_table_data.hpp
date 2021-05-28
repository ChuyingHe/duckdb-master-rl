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

	vector<unique_ptr<PersistentColumnData>> column_data;
	shared_ptr<SegmentTree> versions;
};

} // namespace duckdb
