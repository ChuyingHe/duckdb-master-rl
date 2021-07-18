//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/show_select_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

struct ShowSelectInfo : public ParseInfo {
	//! Types of projected columns
	vector<LogicalType> types;
	//! The QueryNode of select query
	unique_ptr<QueryNode> query;
	//! Aliases of projected columns
	vector<string> aliases;

    ShowSelectInfo(ShowSelectInfo const& ssi): ParseInfo(ssi) {
        types = ssi.types;
        query = ssi.query->Copy();
        aliases = ssi.aliases;
    }

    unique_ptr<ParseInfo> clone() const override {
        return make_unique<ShowSelectInfo>(*this);
    }

	unique_ptr<ShowSelectInfo> Copy() {
        return make_unique<ShowSelectInfo>(*this);
	}
};

} // namespace duckdb
