//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/validity_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/block.hpp"
#include "duckdb/storage/uncompressed_segment.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {
class BlockHandle;
class DatabaseInstance;
class SegmentStatistics;
class Vector;
struct VectorData;

class ValiditySegment : public UncompressedSegment {
public:
	ValiditySegment(DatabaseInstance &db, idx_t row_start, block_id_t block_id = INVALID_BLOCK);
	~ValiditySegment();

public:
	void InitializeScan(ColumnScanState &state) override;
	void FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) override;
	idx_t Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) override;
	void RevertAppend(idx_t start_row) override;

    std::unique_ptr<UncompressedSegment> clone() const override {
        //ValiditySegment(DatabaseInstance &db, idx_t row_start, block_id_t block_id = INVALID_BLOCK);
        return make_unique<ValiditySegment>(this->db, this->row_start, INVALID_BLOCK);
    }

protected:
	void FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) override;
};

} // namespace duckdb
