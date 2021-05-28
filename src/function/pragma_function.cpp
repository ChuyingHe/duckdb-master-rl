#include "duckdb/function/pragma_function.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

PragmaFunction::PragmaFunction(string name, PragmaType pragma_type, pragma_query_t query, pragma_function_t function,
                               vector<LogicalType> arguments, LogicalType varargs)
    : SimpleNamedParameterFunction(move(name), move(arguments), move(varargs)), type(pragma_type), query(query),
      function(function) {
}

PragmaFunction PragmaFunction::PragmaCall(const string &name, pragma_query_t query, vector<LogicalType> arguments,
                                          LogicalType varargs) {
	return PragmaFunction(name, PragmaType::PRAGMA_CALL, query, nullptr, move(arguments), move(varargs));
}

PragmaFunction PragmaFunction::PragmaCall(const string &name, pragma_function_t function, vector<LogicalType> arguments,
                                          LogicalType varargs) {
	return PragmaFunction(name, PragmaType::PRAGMA_CALL, nullptr, function, move(arguments), move(varargs));
}

PragmaFunction PragmaFunction::PragmaStatement(const string &name, pragma_query_t query) {
	vector<LogicalType> types;
	return PragmaFunction(name, PragmaType::PRAGMA_STATEMENT, query, nullptr, types, LogicalType::INVALID);
}

PragmaFunction PragmaFunction::PragmaStatement(const string &name, pragma_function_t function) {
	vector<LogicalType> types;
	return PragmaFunction(name, PragmaType::PRAGMA_STATEMENT, nullptr, function, types, LogicalType::INVALID);
}

PragmaFunction PragmaFunction::PragmaAssignment(const string &name, pragma_query_t query, LogicalType type) {
	vector<LogicalType> types {move(type)};
	return PragmaFunction(name, PragmaType::PRAGMA_ASSIGNMENT, query, nullptr, types, LogicalType::INVALID);
}

PragmaFunction PragmaFunction::PragmaAssignment(const string &name, pragma_function_t function, LogicalType type) {
	vector<LogicalType> types {move(type)};
	return PragmaFunction(name, PragmaType::PRAGMA_ASSIGNMENT, nullptr, function, types, LogicalType::INVALID);
}

string PragmaFunction::ToString() {
	switch (type) {
	case PragmaType::PRAGMA_STATEMENT:
		return StringUtil::Format("PRAGMA %s", name);
	case PragmaType::PRAGMA_ASSIGNMENT:
		return StringUtil::Format("PRAGMA %s=%s", name, arguments[0].ToString());
	case PragmaType::PRAGMA_CALL: {
		return StringUtil::Format("PRAGMA %s", SimpleNamedParameterFunction::ToString());
	}
	default:
		return "UNKNOWN";
	}
}

} // namespace duckdb
