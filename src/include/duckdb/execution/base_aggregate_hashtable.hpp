//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/base_aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {
class BoundAggregateExpression;
class BufferManager;

struct AggregateObject {
	AggregateObject(AggregateFunction function, FunctionData *bind_data, idx_t child_count, idx_t payload_size,
	                bool distinct, PhysicalType return_type, Expression *filter = nullptr)
	    : function(move(function)), bind_data(bind_data), child_count(child_count), payload_size(payload_size),
	      distinct(distinct), return_type(return_type), filter(filter) {
	}
    AggregateObject(AggregateObject const& ao) : function(ao.function) {
        FunctionData bind_data_content = *ao.bind_data;
        bind_data = &bind_data_content;
        child_count = ao.child_count;
        payload_size = ao.payload_size;
        distinct = ao.distinct;
        return_type = ao.return_type;
        filter = ao.filter;
	}
	AggregateFunction function;
	FunctionData *bind_data;
	idx_t child_count;
	idx_t payload_size;
	bool distinct;
	PhysicalType return_type;
	Expression *filter = nullptr;

	static vector<AggregateObject> CreateAggregateObjects(const vector<BoundAggregateExpression *> &bindings);
};

class BaseAggregateHashTable {
public:
	BaseAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types,
	                       vector<LogicalType> payload_types, vector<AggregateObject> aggregate_objects);
	virtual ~BaseAggregateHashTable() {
	}

    BaseAggregateHashTable(BaseAggregateHashTable const& baht) :  buffer_manager(baht.buffer_manager) {
        aggregates = baht.aggregates;
        group_types = baht.group_types;
        payload_types = baht.payload_types;
        group_width = baht.group_width;
        group_padding = baht.group_padding;
        payload_width = baht.payload_width;
        //FIXME: unique_ptr<data_t[]> empty_payload_data; how to
	}

	static idx_t Align(idx_t n) {
		return ((n + 7) / 8) * 8;
	}

protected:
	BufferManager &buffer_manager;
	//! The aggregates to be computed
	vector<AggregateObject> aggregates;
	//! The types of the group columns stored in the hashtable
	vector<LogicalType> group_types;
	//! The types of the payload columns stored in the hashtable
	vector<LogicalType> payload_types;
	//! The size of the groups in bytes
	idx_t group_width;
	//! some optional padding to align payload
	idx_t group_padding;
	//! The size of the payload (aggregations) in bytes
	idx_t payload_width;

	//! The empty payload data
	unique_ptr<data_t[]> empty_payload_data;

protected:
	void CallDestructors(Vector &state_vector, idx_t count);
};

} // namespace duckdb
