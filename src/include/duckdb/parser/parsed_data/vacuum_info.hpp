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
    VacuumInfo(VacuumInfo const& vi) {
    }

    unique_ptr<ParseInfo> clone() const override {
        return make_unique<VacuumInfo>(*this);
    }

    std::unique_ptr<VacuumInfo> duplicate() const {
        return make_unique<VacuumInfo>(*this);
    }
};

} // namespace duckdb
