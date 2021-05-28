#include "duckdb/storage/numeric_segment.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

static NumericSegment::append_function_t GetAppendFunction(PhysicalType type);

NumericSegment::NumericSegment(DatabaseInstance &db, PhysicalType type, idx_t row_start, block_id_t block_id)
    : UncompressedSegment(db, type, row_start) {
	// set up the different functions for this type of segment
	this->append_function = GetAppendFunction(type);

	// figure out how many vectors we want to store in this block
	this->type_size = GetTypeIdSize(type);
	this->vector_size = type_size * STANDARD_VECTOR_SIZE;
	this->max_vector_count = Storage::BLOCK_SIZE / vector_size;
	// FIXME: this is a fix for test/sql/storage/checkpointed_self_append_tinyint.test
	// it is only required because of ToTemporary()
	// this should be removed when ToTemporary() is removed
	if (max_vector_count > 80) {
		max_vector_count = 80;
	}

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id == INVALID_BLOCK) {
		// no block id specified: allocate a buffer for the uncompressed segment
		this->block = buffer_manager.RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
	} else {
		this->block = buffer_manager.RegisterBlock(block_id);
	}
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void NumericSegment::InitializeScan(ColumnScanState &state) {
	// pin the primary buffer
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	state.primary_handle = buffer_manager.Pin(block);
}

//===--------------------------------------------------------------------===//
// Fetch base data
//===--------------------------------------------------------------------===//
void NumericSegment::FetchBaseData(ColumnScanState &state, idx_t vector_index, Vector &result) {
	D_ASSERT(vector_index < max_vector_count);
	D_ASSERT(vector_index * STANDARD_VECTOR_SIZE <= tuple_count);

	auto data = state.primary_handle->node->buffer;
	auto offset = vector_index * vector_size;

	idx_t count = GetVectorCount(vector_index);

	auto source_data = data + offset;

	// fetch the nullmask and copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result), source_data, count * type_size);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void NumericSegment::FetchRow(ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	// get the vector index
	idx_t vector_index = row_id / STANDARD_VECTOR_SIZE;
	idx_t id_in_vector = row_id - vector_index * STANDARD_VECTOR_SIZE;
	D_ASSERT(vector_index < max_vector_count);

	// first fetch the data from the base table
	auto vector_ptr = handle->node->buffer + vector_index * vector_size;

	memcpy(FlatVector::GetData(result) + result_idx * type_size, vector_ptr + id_in_vector * type_size, type_size);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
idx_t NumericSegment::Append(SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	auto handle = buffer_manager.Pin(block);

	idx_t initial_count = tuple_count;
	while (count > 0) {
		// get the vector index of the vector to append to and see how many tuples we can append to that vector
		idx_t vector_index = tuple_count / STANDARD_VECTOR_SIZE;
		if (vector_index == max_vector_count) {
			break;
		}
		idx_t current_tuple_count = tuple_count - vector_index * STANDARD_VECTOR_SIZE;
		idx_t append_count = MinValue(STANDARD_VECTOR_SIZE - current_tuple_count, count);

		// now perform the actual append
		append_function(stats, handle->node->buffer + vector_size * vector_index, current_tuple_count, data, offset,
		                append_count);

		count -= append_count;
		offset += append_count;
		tuple_count += append_count;
	}
	return tuple_count - initial_count;
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
template <class T>
static void AppendLoop(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, VectorData &adata,
                       idx_t offset, idx_t count) {
	auto sdata = (T *)adata.data;
	auto tdata = (T *)target;
	if (!adata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			bool is_null = !adata.validity.RowIsValid(source_idx);
			if (!is_null) {
				NumericStatistics::Update<T>(stats, sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			NumericStatistics::Update<T>(stats, sdata[source_idx]);
			tdata[target_idx] = sdata[source_idx];
		}
	}
}

static NumericSegment::append_function_t GetAppendFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return AppendLoop<int8_t>;
	case PhysicalType::INT16:
		return AppendLoop<int16_t>;
	case PhysicalType::INT32:
		return AppendLoop<int32_t>;
	case PhysicalType::INT64:
		return AppendLoop<int64_t>;
	case PhysicalType::UINT8:
		return AppendLoop<uint8_t>;
	case PhysicalType::UINT16:
		return AppendLoop<uint16_t>;
	case PhysicalType::UINT32:
		return AppendLoop<uint32_t>;
	case PhysicalType::UINT64:
		return AppendLoop<uint64_t>;
	case PhysicalType::INT128:
		return AppendLoop<hugeint_t>;
	case PhysicalType::FLOAT:
		return AppendLoop<float>;
	case PhysicalType::DOUBLE:
		return AppendLoop<double>;
	case PhysicalType::INTERVAL:
		return AppendLoop<interval_t>;
	default:
		throw NotImplementedException("Unimplemented type for uncompressed segment");
	}
}

} // namespace duckdb
