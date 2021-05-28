//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! TableFilter represents a filter pushed down into the table scan.
struct TableFilter {
	TableFilter(Value constant, ExpressionType comparison_type, idx_t column_index)
	    : constant(std::move(constant)), comparison_type(comparison_type), column_index(column_index) {};

	Value constant;
	ExpressionType comparison_type;
	idx_t column_index;
};

struct TableFilterSet {
	unordered_map<idx_t, vector<TableFilter>> filters;
};

} // namespace duckdb
