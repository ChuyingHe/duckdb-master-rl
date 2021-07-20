//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/buffer/block_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {
class BufferHandle;
class BufferManager;
class DatabaseInstance;
class FileBuffer;

enum class BlockState : uint8_t { BLOCK_UNLOADED = 0, BLOCK_LOADED = 1 };

class BlockHandle {
	friend struct BufferEvictionNode;
	friend class BufferHandle;
	friend class BufferManager;

public:
	BlockHandle(DatabaseInstance &db, block_id_t block_id);
	BlockHandle(DatabaseInstance &db, block_id_t block_id, unique_ptr<FileBuffer> buffer, bool can_destroy,
	            idx_t alloc_size);
	~BlockHandle();

    weak_ptr<BlockHandle> Copy() {
        auto copy = make_shared<BlockHandle>(db, block_id);

        copy->state = state;
        copy->readers = readers.load();
        copy->buffer = buffer->Copy();
        // ignore the const copy->can_destroy = can_destroy;
        copy->eviction_timestamp = eviction_timestamp.load();
        return move(copy);
    }

    shared_ptr<BlockHandle> Copy_shared() {
        auto copy = make_shared<BlockHandle>(db, block_id);

        copy->state = state;
        copy->readers = readers.load();
        copy->buffer = buffer->Copy();
        // ignore the const copy->can_destroy = can_destroy;
        copy->eviction_timestamp = eviction_timestamp.load();
        return copy;
    }

	DatabaseInstance &db;


public:
	block_id_t BlockId() {
		return block_id;
	}

private:
	static unique_ptr<BufferHandle> Load(shared_ptr<BlockHandle> &handle);
	void Unload();
	bool CanUnload();

	//! The block-level lock
	mutex lock;
	//! Whether or not the block is loaded/unloaded
	BlockState state;
	// amount of concurrent readers
	atomic<int32_t> readers;
	//! The block id of the block
	const block_id_t block_id;
	//! Pointer to loaded data (if any)
	unique_ptr<FileBuffer> buffer;
	//! Internal eviction timestamp
	atomic<idx_t> eviction_timestamp;
	//! Whether or not the buffer can be destroyed (only used for temporary buffers)
	const bool can_destroy;
	//! The memory usage of the block
	idx_t memory_usage;
};

} // namespace duckdb
