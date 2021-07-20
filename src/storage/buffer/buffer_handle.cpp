#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BufferHandle::BufferHandle(shared_ptr<BlockHandle> handle, FileBuffer *node) : handle(move(handle)), node(node) {
}

BufferHandle::~BufferHandle() {
	auto &buffer_manager = BufferManager::GetBufferManager(handle->db);
	buffer_manager.Unpin(handle);
}

BufferHandle::BufferHandle(BufferHandle const& bh) {
	handle = bh.handle->Copy_shared();
	FileBuffer node_content = *bh.node;
	node = &node_content;
}

data_ptr_t BufferHandle::Ptr() {
	return node->buffer;
}

unique_ptr<BufferHandle> BufferHandle::clone() {
	return make_unique<BufferHandle>(*this);
}

} // namespace duckdb
