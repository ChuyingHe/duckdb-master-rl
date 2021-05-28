//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/arrow/array_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "duckdb/common/constants.hpp"
#include "duckdb/common/arrow.hpp"
#include "pybind_wrapper.hpp"
namespace duckdb {
class PythonTableArrowArrayStreamFactory {
public:
	explicit PythonTableArrowArrayStreamFactory(const py::object &arrow_table) : arrow_table(arrow_table) {};
	~PythonTableArrowArrayStreamFactory() {
		arrow_table = py::none();
	}
	static ArrowArrayStream *Produce(uintptr_t factory);
	py::object arrow_table;
	ArrowArrayStream *stream = nullptr;
};
class PythonTableArrowArrayStream {
public:
	explicit PythonTableArrowArrayStream(const py::object &arrow_table, PythonTableArrowArrayStreamFactory *factory);
	static void InitializeFunctionPointers(ArrowArrayStream *stream);
	ArrowArrayStream stream;
	PythonTableArrowArrayStreamFactory *factory;

private:
	static int GetSchema(ArrowArrayStream *stream, struct ArrowSchema *out);
	static int GetNext(ArrowArrayStream *stream, struct ArrowArray *out, int chunk_idx);
	static void Release(ArrowArrayStream *stream);
	static const char *GetLastError(ArrowArrayStream *stream);

	std::string last_error;
	py::object arrow_table;
	py::list batches;
};
} // namespace duckdb