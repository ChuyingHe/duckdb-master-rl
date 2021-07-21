//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_macro_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/macro_function.hpp"

namespace duckdb {

struct CreateMacroInfo : public CreateFunctionInfo {
	CreateMacroInfo() : CreateFunctionInfo(CatalogType::MACRO_ENTRY) {
	}

	unique_ptr<MacroFunction> function;

public:

	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateMacroInfo>();
		result->function = function->Copy();
		result->name = name;
		CopyProperties(*result);
		return result;
	}
    std::unique_ptr<ParseInfo> clone() const override {
        return Copy();
    }

    std::unique_ptr<CreateMacroInfo> clone() {
        auto result = make_unique<CreateMacroInfo>();
        result->function = function->Copy();
        result->name = name;
        CopyProperties(*result);
        return result;
    }
};

} // namespace duckdb
