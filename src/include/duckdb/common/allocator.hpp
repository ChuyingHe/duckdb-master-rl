//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class Allocator;
class ClientContext;
class DatabaseInstance;

struct PrivateAllocatorData {
	virtual ~PrivateAllocatorData() {
	}
};

typedef data_ptr_t (*allocate_function_ptr_t)(PrivateAllocatorData *private_data, idx_t size);
typedef void (*free_function_ptr_t)(PrivateAllocatorData *private_data, data_ptr_t pointer);

class AllocatedData {
public:
	AllocatedData(Allocator &allocator, data_ptr_t pointer);
	~AllocatedData();

	data_ptr_t get() {
		return pointer;
	}
	const_data_ptr_t get() const {
		return pointer;
	}
	void Reset();

private:
	Allocator &allocator;
	data_ptr_t pointer;
};

class Allocator {
public:
	Allocator();
	Allocator(allocate_function_ptr_t allocate_function_p, free_function_ptr_t free_function_p,
	          unique_ptr<PrivateAllocatorData> private_data);

	data_ptr_t AllocateData(idx_t size);
	void FreeData(data_ptr_t pointer);

	unique_ptr<AllocatedData> Allocate(idx_t size) {
		return make_unique<AllocatedData>(*this, AllocateData(size));
	}

	static data_ptr_t DefaultAllocate(PrivateAllocatorData *private_data, idx_t size) {
		return new data_t[size];
	}
	static void DefaultFree(PrivateAllocatorData *private_data, data_ptr_t pointer) {
		delete[] pointer;
	}
	static Allocator &Get(ClientContext &context);
	static Allocator &Get(DatabaseInstance &db);

private:
	allocate_function_ptr_t allocate_function;
	free_function_ptr_t free_function;

	unique_ptr<PrivateAllocatorData> private_data;
};

} // namespace duckdb
