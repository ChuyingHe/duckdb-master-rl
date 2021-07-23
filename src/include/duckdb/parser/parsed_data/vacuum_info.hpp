//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/vacuum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct VacuumInfo : public ParseInfo {
	// nothing for now

    // FOR IMPLEMENTATION
    VacuumInfo(VacuumInfo const& vi) {
    }
    unique_ptr<ParseInfo> clone() const override {
        return make_unique<VacuumInfo>(*this);
    }
};

} // namespace duckdb
