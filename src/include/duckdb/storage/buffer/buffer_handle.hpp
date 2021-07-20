//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/buffer_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/storage_info.hpp"

namespace duckdb {
class BlockHandle;
class FileBuffer;

class BufferHandle {
public:
	BufferHandle(shared_ptr<BlockHandle> handle, FileBuffer *node);
	~BufferHandle();
    BufferHandle(BufferHandle const& bh);
	//! The block handle
	shared_ptr<BlockHandle> handle;
	//! The managed buffer node
	FileBuffer *node;

	data_ptr_t Ptr();

    unique_ptr<BufferHandle> clone();
};

} // namespace duckdb
