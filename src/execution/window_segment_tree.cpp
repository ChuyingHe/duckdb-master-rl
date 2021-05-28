#include "duckdb/execution/window_segment_tree.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"

#include <cmath>

namespace duckdb {

WindowSegmentTree::WindowSegmentTree(AggregateFunction &aggregate, FunctionData *bind_info, LogicalType result_type,
                                     ChunkCollection *input)
    : aggregate(aggregate), bind_info(bind_info), result_type(move(result_type)), state(aggregate.state_size()),
      bounds(0, 0), internal_nodes(0), input_ref(input) {
#if STANDARD_VECTOR_SIZE < 512
	throw NotImplementedException("Window functions are not supported for vector sizes < 512");
#endif
	Value ptr_val = Value::POINTER((idx_t)state.data());
	statep.Reference(ptr_val);
	statep.Normalify(STANDARD_VECTOR_SIZE);

	if (input_ref && input_ref->ColumnCount() > 0) {
		inputs.Initialize(input_ref->Types());
		// if we have a frame-by-frame method, share the single state
		if (aggregate.frame) {
			AggregateInit();
		} else if (aggregate.combine) {
			ConstructTree();
		}
	}
}

WindowSegmentTree::~WindowSegmentTree() {
	if (!aggregate.destructor) {
		// nothing to destroy
		return;
	}
	// call the destructor for all the intermediate states
	data_ptr_t address_data[STANDARD_VECTOR_SIZE];
	Vector addresses(LogicalType::POINTER, (data_ptr_t)address_data);
	idx_t count = 0;
	for (idx_t i = 0; i < internal_nodes; i++) {
		address_data[count++] = data_ptr_t(levels_flat_native.get() + i * state.size());
		if (count == STANDARD_VECTOR_SIZE) {
			aggregate.destructor(addresses, count);
			count = 0;
		}
	}
	if (count > 0) {
		aggregate.destructor(addresses, count);
	}

	if (aggregate.frame) {
		(void)AggegateFinal();
	}
}

void WindowSegmentTree::AggregateInit() {
	aggregate.initialize(state.data());
}

Value WindowSegmentTree::AggegateFinal() {
	Vector statev(Value::POINTER((idx_t)state.data()));
	Vector result(result_type);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(result, false);
	aggregate.finalize(statev, bind_info, result, 1);

	if (aggregate.destructor) {
		aggregate.destructor(statev, 1);
	}
	return result.GetValue(0);
}

void WindowSegmentTree::ExtractFrame(idx_t begin, idx_t end) {
	inputs.Reset();
	inputs.SetCardinality(end - begin);

	const idx_t start_in_vector = begin % STANDARD_VECTOR_SIZE;
	const auto input_count = input_ref->ColumnCount();
	if (start_in_vector + inputs.size() <= STANDARD_VECTOR_SIZE) {
		auto &chunk = input_ref->GetChunkForRow(begin);
		for (idx_t i = 0; i < input_count; ++i) {
			auto &v = inputs.data[i];
			auto &vec = chunk.data[i];
			v.Slice(vec, start_in_vector);
			v.Verify(inputs.size());
		}
	} else {
		// we cannot just slice the individual vector!
		auto &chunk_a = input_ref->GetChunkForRow(begin);
		auto &chunk_b = input_ref->GetChunkForRow(end);
		idx_t chunk_a_count = chunk_a.size() - start_in_vector;
		idx_t chunk_b_count = inputs.size() - chunk_a_count;
		for (idx_t i = 0; i < input_count; ++i) {
			auto &v = inputs.data[i];
			VectorOperations::Copy(chunk_a.data[i], v, chunk_a.size(), start_in_vector, 0);
			VectorOperations::Copy(chunk_b.data[i], v, chunk_b_count, 0, chunk_a_count);
		}
	}
}

void WindowSegmentTree::WindowSegmentValue(idx_t l_idx, idx_t begin, idx_t end) {
	D_ASSERT(begin <= end);
	if (begin == end) {
		return;
	}

	if (end - begin >= STANDARD_VECTOR_SIZE) {
		throw InternalException("Cannot compute window aggregation: bounds are too large");
	}

	Vector s;
	s.Slice(statep, 0);
	if (l_idx == 0) {
		ExtractFrame(begin, end);
		aggregate.update(&inputs.data[0], bind_info, input_ref->ColumnCount(), s, inputs.size());
	} else {
		inputs.Reset();
		inputs.SetCardinality(end - begin);
		// find out where the states begin
		data_ptr_t begin_ptr = levels_flat_native.get() + state.size() * (begin + levels_flat_start[l_idx - 1]);
		// set up a vector of pointers that point towards the set of states
		Vector v(LogicalType::POINTER);
		auto pdata = FlatVector::GetData<data_ptr_t>(v);
		for (idx_t i = 0; i < inputs.size(); i++) {
			pdata[i] = begin_ptr + i * state.size();
		}
		v.Verify(inputs.size());
		aggregate.combine(v, s, inputs.size());
	}
}

void WindowSegmentTree::ConstructTree() {
	D_ASSERT(input_ref);
	D_ASSERT(inputs.ColumnCount() > 0);

	// compute space required to store internal nodes of segment tree
	internal_nodes = 0;
	idx_t level_nodes = input_ref->Count();
	do {
		level_nodes = (level_nodes + (TREE_FANOUT - 1)) / TREE_FANOUT;
		internal_nodes += level_nodes;
	} while (level_nodes > 1);
	levels_flat_native = unique_ptr<data_t[]>(new data_t[internal_nodes * state.size()]);
	levels_flat_start.push_back(0);

	idx_t levels_flat_offset = 0;
	idx_t level_current = 0;
	// level 0 is data itself
	idx_t level_size;
	// iterate over the levels of the segment tree
	while ((level_size = (level_current == 0 ? input_ref->Count()
	                                         : levels_flat_offset - levels_flat_start[level_current - 1])) > 1) {
		for (idx_t pos = 0; pos < level_size; pos += TREE_FANOUT) {
			// compute the aggregate for this entry in the segment tree
			AggregateInit();
			WindowSegmentValue(level_current, pos, MinValue<idx_t>(level_size, pos + TREE_FANOUT));

			memcpy(levels_flat_native.get() + (levels_flat_offset * state.size()), state.data(), state.size());

			levels_flat_offset++;
		}

		levels_flat_start.push_back(levels_flat_offset);
		level_current++;
	}

	// Corner case: single element in the window
	if (levels_flat_offset == 0) {
		aggregate.initialize(levels_flat_native.get());
	}
}

Value WindowSegmentTree::Compute(idx_t begin, idx_t end) {
	D_ASSERT(input_ref);

	// No arguments, so just count
	if (inputs.ColumnCount() == 0) {
		return Value::Numeric(result_type, end - begin);
	}

	// If we have a frame function, use that
	if (aggregate.frame) {
		// Reference the previous inputs
		DataChunk prevs;
		prevs.InitializeEmpty(inputs.GetTypes());
		prevs.Reference(inputs);

		// Extract the new inputs
		ExtractFrame(begin, end);

		// Frame boundaries
		auto prev_bounds = bounds;
		bounds = FrameBounds(begin, end);

		Vector result(result_type);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, false);

		aggregate.frame(inputs.data.data(), prevs.data.data(), bind_info, inputs.ColumnCount(), state.data(), bounds,
		                prev_bounds, result);
		return result.GetValue(0);
	}

	AggregateInit();

	// Aggregate everything at once if we can't combine states
	if (!aggregate.combine) {
		WindowSegmentValue(0, begin, end);
		return AggegateFinal();
	}

	for (idx_t l_idx = 0; l_idx < levels_flat_start.size() + 1; l_idx++) {
		idx_t parent_begin = begin / TREE_FANOUT;
		idx_t parent_end = end / TREE_FANOUT;
		if (parent_begin == parent_end) {
			WindowSegmentValue(l_idx, begin, end);
			return AggegateFinal();
		}
		idx_t group_begin = parent_begin * TREE_FANOUT;
		if (begin != group_begin) {
			WindowSegmentValue(l_idx, begin, group_begin + TREE_FANOUT);
			parent_begin++;
		}
		idx_t group_end = parent_end * TREE_FANOUT;
		if (end != group_end) {
			WindowSegmentValue(l_idx, group_end, end);
		}
		begin = parent_begin;
		end = parent_end;
	}

	return AggegateFinal();
}

} // namespace duckdb
