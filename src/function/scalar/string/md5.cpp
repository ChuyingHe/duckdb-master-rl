#include "duckdb/function/scalar/string_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

static void MD5Function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];

	UnaryExecutor::Execute<string_t, string_t>(input, result, args.size(), [&](string_t input) {
		auto hash = StringVector::EmptyString(result, MD5Context::MD5_HASH_LENGTH_TEXT);
		MD5Context context;
		context.Add(input);
		context.FinishHex(hash.GetDataWriteable());
		hash.Finalize();
		return hash;
	});
}

void MD5Fun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("md5",                  // name of the function
	                               {LogicalType::VARCHAR}, // argument list
	                               LogicalType::VARCHAR,   // return type
	                               MD5Function));          // pointer to function implementation
}

} // namespace duckdb
