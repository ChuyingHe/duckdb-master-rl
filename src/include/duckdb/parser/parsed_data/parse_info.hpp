//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/parse_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct ParseInfo {
	virtual ~ParseInfo() {
	}

    virtual unique_ptr<ParseInfo> clone() const = 0; // Virtual constructor (copying)
};

} // namespace duckdb
