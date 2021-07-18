#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"

namespace duckdb {

BlockHandle::BlockHandle(DatabaseInstance &db, block_id_t block_id_p)
    : db(db), readers(0), block_id(block_id_p), buffer(nullptr), eviction_timestamp(0), can_destroy(false) {
	eviction_timestamp = 0;
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = Storage::BLOCK_ALLOC_SIZE;
}

BlockHandle::BlockHandle(DatabaseInstance &db, block_id_t block_id_p, unique_ptr<FileBuffer> buffer_p,
                         bool can_destroy_p, idx_t alloc_size)
    : db(db), readers(0), block_id(block_id_p), eviction_timestamp(0), can_destroy(can_destroy_p) {
	D_ASSERT(alloc_size >= Storage::BLOCK_SIZE);
	buffer = move(buffer_p);
	state = BlockState::BLOCK_LOADED;
	memory_usage = alloc_size;
}

BlockHandle::~BlockHandle() {
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	// no references remain to this block: erase
	if (state == BlockState::BLOCK_LOADED) {
		// the block is still loaded in memory: erase it
		buffer.reset();
		buffer_manager.current_memory -= memory_usage;
	}
	buffer_manager.UnregisterBlock(block_id, can_destroy);
}

unique_ptr<BufferHandle> BlockHandle::Load(shared_ptr<BlockHandle> &handle) {
	if (handle->state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(handle->buffer);
		return make_unique<BufferHandle>(handle, handle->buffer.get());
	}
	handle->state = BlockState::BLOCK_LOADED;

	auto &buffer_manager = BufferManager::GetBufferManager(handle->db);
	auto &block_manager = BlockManager::GetBlockManager(handle->db);
	if (handle->block_id < MAXIMUM_BLOCK) {
		// FIXME: currently we still require a lock for reading blocks from disk
		// this is mainly down to the block manager only having a single pointer into the file
		// this is relatively easy to fix later on
		lock_guard<mutex> buffer_lock(buffer_manager.manager_lock);
		auto block = make_unique<Block>(handle->block_id);
		block_manager.Read(*block);
		handle->buffer = move(block);
	} else {
		if (handle->can_destroy) {
			return nullptr;
		} else {
			handle->buffer = buffer_manager.ReadTemporaryBuffer(handle->block_id);
		}
	}
	return make_unique<BufferHandle>(handle, handle->buffer.get());
}

void BlockHandle::Unload() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded: nothing to do
		return;
	}
	D_ASSERT(CanUnload());
	D_ASSERT(memory_usage >= Storage::BLOCK_SIZE);
	state = BlockState::BLOCK_UNLOADED;

	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id >= MAXIMUM_BLOCK && !can_destroy) {
		// temporary block that cannot be destroyed: write to temporary file
		buffer_manager.WriteTemporaryBuffer((ManagedBuffer &)*buffer);
	}
	buffer.reset();
	buffer_manager.current_memory -= memory_usage;
}

bool BlockHandle::CanUnload() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded
		return false;
	}
	if (readers > 0) {
		// there are active readers
		return false;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(db);
	if (block_id >= MAXIMUM_BLOCK && !can_destroy && buffer_manager.temp_directory.empty()) {
		// in order to unload this block we need to write it to a temporary buffer
		// however, no temporary directory is specified!
		// hence we cannot unload the block
		return false;
	}
	return true;
}

struct BufferEvictionNode {
	BufferEvictionNode(weak_ptr<BlockHandle> handle_p, idx_t timestamp_p)
	    : handle(move(handle_p)), timestamp(timestamp_p) {
		D_ASSERT(!handle.expired());
	}

	weak_ptr<BlockHandle> handle;
	idx_t timestamp;

	bool CanUnload(BlockHandle &handle) {
		if (timestamp != handle.eviction_timestamp) {
			// handle was used in between
			return false;
		}
		return handle.CanUnload();
	}
};

typedef duckdb_moodycamel::ConcurrentQueue<unique_ptr<BufferEvictionNode>> eviction_queue_t;

struct EvictionQueue {
	eviction_queue_t q;

	std::unique_ptr<EvictionQueue> Copy() {
		return make_unique<EvictionQueue>(q);
	}
};

BufferManager::BufferManager(DatabaseInstance &db, string tmp, idx_t maximum_memory)
    : db(db), current_memory(0), maximum_memory(maximum_memory), temp_directory(move(tmp)),
      queue(make_unique<EvictionQueue>()), temporary_id(MAXIMUM_BLOCK) {
	auto &fs = FileSystem::GetFileSystem(db);
	if (!temp_directory.empty()) {
		fs.CreateDirectory(temp_directory);
	}
}

BufferManager::~BufferManager() {
	auto &fs = FileSystem::GetFileSystem(db);
	if (!temp_directory.empty()) {
		fs.RemoveDirectory(temp_directory);
	}
}

shared_ptr<BlockHandle> BufferManager::RegisterBlock(block_id_t block_id) {
	lock_guard<mutex> lock(manager_lock);
	// check if the block already exists
	auto entry = blocks.find(block_id);
	if (entry != blocks.end()) {
		// already exists: check if it hasn't expired yet
		auto existing_ptr = entry->second.lock();
		if (existing_ptr) {
			//! it hasn't! return it
			return existing_ptr;
		}
	}
	// create a new block pointer for this block
	auto result = make_shared<BlockHandle>(db, block_id);
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

shared_ptr<BlockHandle> BufferManager::RegisterMemory(idx_t alloc_size, bool can_destroy) {
	// first evict blocks until we have enough memory to store this buffer
	if (!EvictBlocks(alloc_size, maximum_memory)) {
		throw OutOfRangeException("Not enough memory to complete operation: could not allocate block of %lld bytes",
		                          alloc_size);
	}

	// allocate the buffer
	auto temp_id = ++temporary_id;
	auto buffer = make_unique<ManagedBuffer>(db, alloc_size, can_destroy, temp_id);

	// create a new block pointer for this block
	return make_shared<BlockHandle>(db, temp_id, move(buffer), can_destroy, alloc_size);
}

unique_ptr<BufferHandle> BufferManager::Allocate(idx_t alloc_size) {
	auto block = RegisterMemory(alloc_size, true);
	return Pin(block);
}

unique_ptr<BufferHandle> BufferManager::Pin(shared_ptr<BlockHandle> &handle) {
	// lock the block
	lock_guard<mutex> lock(handle->lock);
	// check if the block is already loaded
	if (handle->state == BlockState::BLOCK_LOADED) {
		// the block is loaded, increment the reader count and return a pointer to the handle
		handle->readers++;
		return handle->Load(handle);
	}
	// evict blocks until we have space for the current block
	if (!EvictBlocks(handle->memory_usage, maximum_memory)) {
		throw OutOfRangeException("Not enough memory to complete operation: failed to pin block");
	}
	// now we can actually load the current block
	D_ASSERT(handle->readers == 0);
	handle->readers = 1;
	return handle->Load(handle);
}

void BufferManager::Unpin(shared_ptr<BlockHandle> &handle) {
	lock_guard<mutex> lock(handle->lock);
	D_ASSERT(handle->readers > 0);
	handle->readers--;
	if (handle->readers == 0) {
		handle->eviction_timestamp++;
		queue->q.enqueue(make_unique<BufferEvictionNode>(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
		// FIXME: do some house-keeping to prevent the queue from being flooded with many old blocks
	}
}

bool BufferManager::EvictBlocks(idx_t extra_memory, idx_t memory_limit) {
	unique_ptr<BufferEvictionNode> node;
	current_memory += extra_memory;
	while (current_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue->q.try_dequeue(node)) {
			current_memory -= extra_memory;
			return false;
		}
		// get a reference to the underlying block pointer
		auto handle = node->handle.lock();
		if (!handle) {
			continue;
		}
		if (!node->CanUnload(*handle)) {
			// early out: we already know that we cannot unload this node
			continue;
		}
		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node->CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			continue;
		}
		// hooray, we can unload the block
		// release the memory and mark the block as unloaded
		handle->Unload();
	}
	return true;
}

void BufferManager::UnregisterBlock(block_id_t block_id, bool can_destroy) {
	if (block_id >= MAXIMUM_BLOCK) {
		// in-memory buffer: destroy the buffer
		if (!can_destroy) {
			// buffer could have been offloaded to disk: remove the file
			DeleteTemporaryFile(block_id);
		}
	} else {
		lock_guard<mutex> lock(manager_lock);
		// on-disk block: erase from list of blocks in manager
		blocks.erase(block_id);
	}
}
void BufferManager::SetLimit(idx_t limit) {
	lock_guard<mutex> buffer_lock(manager_lock);
	// try to evict until the limit is reached
	if (!EvictBlocks(0, limit)) {
		throw OutOfRangeException(
		    "Failed to change memory limit to new limit %lld: could not free up enough memory for the new limit",
		    limit);
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(0, limit)) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfRangeException(
		    "Failed to change memory limit to new limit %lld: could not free up enough memory for the new limit",
		    limit);
	}
}

string BufferManager::GetTemporaryPath(block_id_t id) {
	auto &fs = FileSystem::GetFileSystem(db);
	return fs.JoinPath(temp_directory, to_string(id) + ".block");
}

void BufferManager::WriteTemporaryBuffer(ManagedBuffer &buffer) {
	D_ASSERT(!temp_directory.empty());
	D_ASSERT(buffer.size + Storage::BLOCK_HEADER_SIZE >= Storage::BLOCK_ALLOC_SIZE);
	// get the path to write to
	auto path = GetTemporaryPath(buffer.id);
	// create the file and write the size followed by the buffer contents
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write(&buffer.size, sizeof(idx_t), 0);
	buffer.Write(*handle, sizeof(idx_t));
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(block_id_t id) {
	if (temp_directory.empty()) {
		throw Exception("Out-of-memory: cannot read buffer because no temporary directory is specified!\nTo enable "
		                "temporary buffer eviction set a temporary directory in the configuration");
	}
	idx_t alloc_size;
	// open the temporary file and read the size
	auto path = GetTemporaryPath(id);
	auto &fs = FileSystem::GetFileSystem(db);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	handle->Read(&alloc_size, sizeof(idx_t), 0);

	// now allocate a buffer of this size and read the data into that buffer
	auto buffer = make_unique<ManagedBuffer>(db, alloc_size + Storage::BLOCK_HEADER_SIZE, false, id);
	buffer->Read(*handle, sizeof(idx_t));
	return move(buffer);
}

void BufferManager::DeleteTemporaryFile(block_id_t id) {
	auto &fs = FileSystem::GetFileSystem(db);
	auto path = GetTemporaryPath(id);
	if (fs.FileExists(path)) {
		fs.RemoveFile(path);
	}
}

} // namespace duckdb
