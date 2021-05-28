#include "duckdb.hpp"

#include "duckdb/common/arrow.hpp"
#include "duckdb/function/table/arrow.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

struct ArrowScanFunctionData : public TableFunctionData {
	ArrowArrayStream *stream = nullptr;
	ArrowSchema schema_root;

	void ReleaseSchema() {
		if (schema_root.release) {
			for (idx_t child_idx = 0; child_idx < (idx_t)schema_root.n_children; child_idx++) {
				auto &child = *schema_root.children[child_idx];
				if (child.release) {
					child.release(&child);
				}
			}
			schema_root.release(&schema_root);
		}
	}

	~ArrowScanFunctionData() override {
		ReleaseSchema();
		if (stream) {
			stream->release(stream);
			stream = nullptr;
		}
	}
};

struct ArrowScanState : public FunctionOperatorData {
	explicit ArrowScanState(unique_ptr<ArrowArray> current_chunk) : chunk(move(current_chunk)) {
	}

	void ReleaseArray() {
		if (chunk->release) {
			for (idx_t child_idx = 0; child_idx < (idx_t)chunk->n_children; child_idx++) {
				auto &child = *chunk->children[child_idx];
				if (child.release) {
					child.release(&child);
				}
			}
			chunk->release(chunk.get());
		}
	}
	unique_ptr<ArrowArray> chunk;
	idx_t chunk_offset = 0;
	idx_t chunk_idx = 0;
	vector<column_t> column_ids;
};

static unique_ptr<FunctionData> ArrowScanBind(ClientContext &context, vector<Value> &inputs,
                                              unordered_map<string, Value> &named_parameters,
                                              vector<LogicalType> &input_table_types, vector<string> &input_table_names,
                                              vector<LogicalType> &return_types, vector<string> &names) {

	auto res = make_unique<ArrowScanFunctionData>();
	auto &data = *res;
	auto stream_factory_ptr = inputs[0].GetValue<uintptr_t>();
	ArrowArrayStream *(*stream_factory_produce)(uintptr_t stream_factory_ptr);
	stream_factory_produce = (ArrowArrayStream * (*)(uintptr_t stream_factory_ptr)) inputs[1].GetValue<uintptr_t>();
	data.stream = stream_factory_produce(stream_factory_ptr);
	if (!data.stream) {
		throw InvalidInputException("arrow_scan: NULL pointer passed");
	}

	D_ASSERT(data.stream->get_schema);
	if (data.stream->get_schema(data.stream, &data.schema_root)) {
		throw InvalidInputException("arrow_scan: get_schema failed(): %s",
		                            string(data.stream->get_last_error(data.stream)));
	}

	if (!data.schema_root.release) {
		throw InvalidInputException("arrow_scan: released schema passed");
	}

	if (data.schema_root.n_children < 1) {
		throw InvalidInputException("arrow_scan: empty schema passed");
	}

	for (idx_t col_idx = 0; col_idx < (idx_t)data.schema_root.n_children; col_idx++) {
		auto &schema = *data.schema_root.children[col_idx];
		if (!schema.release) {
			throw InvalidInputException("arrow_scan: released schema passed");
		}
		if (schema.dictionary) {
			throw NotImplementedException("arrow_scan: dictionary vectors not supported yet");
		}
		auto format = string(schema.format);
		if (format == "n") {
			return_types.push_back(LogicalType::SQLNULL);
		} else if (format == "b") {
			return_types.push_back(LogicalType::BOOLEAN);
		} else if (format == "c") {
			return_types.push_back(LogicalType::TINYINT);
		} else if (format == "s") {
			return_types.push_back(LogicalType::SMALLINT);
		} else if (format == "i") {
			return_types.push_back(LogicalType::INTEGER);
		} else if (format == "l") {
			return_types.push_back(LogicalType::BIGINT);
		} else if (format == "C") {
			return_types.push_back(LogicalType::UTINYINT);
		} else if (format == "S") {
			return_types.push_back(LogicalType::USMALLINT);
		} else if (format == "I") {
			return_types.push_back(LogicalType::UINTEGER);
		} else if (format == "L") {
			return_types.push_back(LogicalType::UBIGINT);
		} else if (format == "f") {
			return_types.push_back(LogicalType::FLOAT);
		} else if (format == "g") {
			return_types.push_back(LogicalType::DOUBLE);
		} else if (format == "d:38,0") { // decimal128
			return_types.push_back(LogicalType::HUGEINT);
		} else if (format == "u") {
			return_types.push_back(LogicalType::VARCHAR);
		} else if (format == "tsn:") {
			return_types.emplace_back(LogicalTypeId::TIMESTAMP_NS);
		} else if (format == "tsu:") {
			return_types.emplace_back(LogicalTypeId::TIMESTAMP);
		} else if (format == "tsm:") {
			return_types.emplace_back(LogicalTypeId::TIMESTAMP_MS);
		} else if (format == "tss:") {
			return_types.emplace_back(LogicalTypeId::TIMESTAMP_SEC);
		} else if (format == "tdD") {
			return_types.push_back(LogicalType::DATE);
		} else if (format == "ttm") {
			return_types.push_back(LogicalType::TIME);
		} else {
			throw NotImplementedException("Unsupported Internal Arrow Type %s", format);
		}
		auto name = string(schema.name);
		if (name.empty()) {
			name = string("v") + to_string(col_idx);
		}
		names.push_back(name);
	}
	data.ReleaseSchema();
	return move(res);
}

static unique_ptr<FunctionOperatorData> ArrowScanInit(ClientContext &context, const FunctionData *bind_data,
                                                      vector<column_t> &column_ids, TableFilterCollection *filters) {
	auto current_chunk = make_unique<ArrowArray>();
	auto result = make_unique<ArrowScanState>(move(current_chunk));
	result->column_ids = column_ids;
	return move(result);
}

void ArrowToDuckDB(ArrowScanState &scan_state, DataChunk &output) {
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &array = *scan_state.chunk->children[col_idx];
		if (!array.release) {
			throw InvalidInputException("arrow_scan: released array passed");
		}
		if (array.length != scan_state.chunk->length) {
			throw InvalidInputException("arrow_scan: array length mismatch");
		}
		if (array.dictionary) {
			throw NotImplementedException("arrow_scan: dictionary vectors not supported yet");
		}
		if (array.null_count != 0 && array.buffers[0]) {
			auto &mask = FlatVector::Validity(output.data[col_idx]);

			auto bit_offset = scan_state.chunk_offset + array.offset;
			auto n_bitmask_bytes = (output.size() + 8 - 1) / 8;

			mask.EnsureWritable();
			if (bit_offset % 8 == 0) {
				//! just memcpy nullmask
				memcpy((void *)mask.GetData(), (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes);
			} else {
				//! need to re-align nullmask
				bitset<STANDARD_VECTOR_SIZE + 8> temp_nullmask;
				memcpy(&temp_nullmask, (uint8_t *)array.buffers[0] + bit_offset / 8, n_bitmask_bytes + 1);

				temp_nullmask >>= (bit_offset % 8); // why this has to be a right shift is a mystery to me
				memcpy((void *)mask.GetData(), (data_ptr_t)&temp_nullmask, n_bitmask_bytes);
			}
		}
		switch (output.data[col_idx].GetType().id()) {
		case LogicalTypeId::SQLNULL:
			output.data[col_idx].Reference(Value());
			break;
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::HUGEINT:
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_NS:
			FlatVector::SetData(output.data[col_idx], (data_ptr_t)array.buffers[1] +
			                                              GetTypeIdSize(output.data[col_idx].GetType().InternalType()) *
			                                                  (scan_state.chunk_offset + array.offset));
			break;

		case LogicalTypeId::VARCHAR: {
			auto offsets = (uint32_t *)array.buffers[1] + array.offset + scan_state.chunk_offset;
			auto cdata = (char *)array.buffers[2];

			for (idx_t row_idx = 0; row_idx < output.size(); row_idx++) {
				if (FlatVector::IsNull(output.data[col_idx], row_idx)) {
					continue;
				}
				auto cptr = cdata + offsets[row_idx];
				auto str_len = offsets[row_idx + 1] - offsets[row_idx];

				auto utf_type = Utf8Proc::Analyze(cptr, str_len);
				if (utf_type == UnicodeType::INVALID) {
					throw std::runtime_error("Invalid UTF8 string encoding");
				}
				FlatVector::GetData<string_t>(output.data[col_idx])[row_idx] =
				    StringVector::AddString(output.data[col_idx], cptr, str_len);
			}

			break;
		}
		case LogicalTypeId::TIME: {
			// convert time from milliseconds to microseconds
			auto src_ptr = (uint32_t *)array.buffers[1] + scan_state.chunk_offset;
			auto tgt_ptr = (dtime_t *)FlatVector::GetData(output.data[col_idx]);
			for (idx_t row = 0; row < output.size(); row++) {
				auto source_idx = scan_state.chunk_offset + row;
				tgt_ptr[row] = dtime_t(int64_t(src_ptr[source_idx]) * 1000);
			}
			break;
		}
		default:
			throw std::runtime_error("Unsupported type " + output.data[col_idx].GetType().ToString());
		}
	}
}

static void ArrowScanFunction(ClientContext &context, const FunctionData *bind_data,
                              FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
	auto &data = (ArrowScanFunctionData &)*bind_data;
	auto &state = (ArrowScanState &)*operator_state;

	//! have we run out of data on the current chunk? move to next one
	if (state.chunk_offset >= (idx_t)state.chunk->length) {
		state.chunk_offset = 0;
		state.ReleaseArray();
		auto current_chunk = make_unique<ArrowArray>();
		if (data.stream->get_next(data.stream, current_chunk.get(), state.chunk_idx++)) {
			throw InvalidInputException("arrow_scan: get_next failed(): %s",
			                            string(data.stream->get_last_error(data.stream)));
		}
		state.chunk = move(current_chunk);
	}

	//! have we run out of chunks? we are done
	if (!state.chunk->release) {
		return;
	}

	if ((idx_t)state.chunk->n_children != output.ColumnCount()) {
		throw InvalidInputException("arrow_scan: array column count mismatch");
	}

	output.SetCardinality(MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->length - state.chunk_offset));
	ArrowToDuckDB(state, output);
	output.Verify();
	state.chunk_offset += output.size();
}

struct ParallelArrowScanState : public ParallelState {
	ParallelArrowScanState() {
	}

	std::mutex lock;
	idx_t current_chunk_idx = 0;
};

static void ArrowScanFunctionParallel(ClientContext &context, const FunctionData *bind_data,
                                      FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output,
                                      ParallelState *parallel_state_p) {
	auto &state = (ArrowScanState &)*operator_state;
	//! Out of tuples in this chunk
	if (state.chunk_offset >= (idx_t)state.chunk->length) {
		return;
	}
	if ((idx_t)state.chunk->n_children != output.ColumnCount()) {
		throw InvalidInputException("arrow_scan: array column count mismatch");
	}
	output.SetCardinality(MinValue<int64_t>(STANDARD_VECTOR_SIZE, state.chunk->length - state.chunk_offset));
	ArrowToDuckDB(state, output);
	output.Verify();
	state.chunk_offset += output.size();
}

idx_t ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	auto &data = (const ArrowScanFunctionData &)*bind_data_p;
	return data.stream->number_of_batches;
}

unique_ptr<ParallelState> ArrowScanInitParallelState(ClientContext &context, const FunctionData *bind_data_p) {
	return make_unique<ParallelArrowScanState>();
}

bool ArrowScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                FunctionOperatorData *operator_state, ParallelState *parallel_state_p) {
	auto &bind_data = (const ArrowScanFunctionData &)*bind_data_p;
	auto &parallel_state = (ParallelArrowScanState &)*parallel_state_p;
	auto &state = (ArrowScanState &)*operator_state;
	{
		lock_guard<mutex> parallel_lock(parallel_state.lock);
		if (parallel_state.current_chunk_idx >= bind_data.stream->number_of_batches) {
			return false;
		}
		state.chunk_idx = parallel_state.current_chunk_idx;
		parallel_state.current_chunk_idx++;
	}
	state.chunk_offset = 0;
	state.ReleaseArray();
	auto current_chunk = make_unique<ArrowArray>();
	if (bind_data.stream->get_next(bind_data.stream, current_chunk.get(), state.chunk_idx)) {
		throw InvalidInputException("arrow_scan: get_next failed(): %s",
		                            string(bind_data.stream->get_last_error(bind_data.stream)));
	}
	state.chunk = move(current_chunk);

	return true;
}

unique_ptr<FunctionOperatorData> ArrowScanParallelInit(ClientContext &context, const FunctionData *bind_data_p,
                                                       ParallelState *state, vector<column_t> &column_ids,
                                                       TableFilterCollection *filters) {
	auto current_chunk = make_unique<ArrowArray>();
	auto result = make_unique<ArrowScanState>(move(current_chunk));
	result->column_ids = column_ids;
	if (!ArrowScanParallelStateNext(context, bind_data_p, result.get(), state)) {
		return nullptr;
	}
	return move(result);
}

void ArrowTableFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet arrow("arrow_scan");
	arrow.AddFunction(TableFunction({LogicalType::POINTER, LogicalType::POINTER}, ArrowScanFunction, ArrowScanBind,
	                                ArrowScanInit, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                ArrowScanMaxThreads, ArrowScanInitParallelState, ArrowScanFunctionParallel,
	                                ArrowScanParallelInit, ArrowScanParallelStateNext));
	set.AddFunction(arrow);
}

void BuiltinFunctions::RegisterArrowFunctions() {
	ArrowTableFunction::RegisterFunction(*this);
}
} // namespace duckdb
