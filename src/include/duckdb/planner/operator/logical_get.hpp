//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

//! LogicalGet represents a scan operation from a data source
class LogicalGet : public LogicalOperator {
public:
	LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
	           vector<LogicalType> returned_types, vector<string> returned_names);

	//! The table index in the current bind context
	idx_t table_index;
	//! The function that is called
	TableFunction function;
	//! The bind data of the function
	unique_ptr<FunctionData> bind_data;
	//! The types of ALL columns that can be returned by the table function
	vector<LogicalType> returned_types;
	//! The names of ALL columns that can be returned by the table function
	vector<string> names;
	//! Bound column IDs
	vector<column_t> column_ids;
	//! Filters pushed down for table scan
	vector<TableFilter> table_filters;

	string GetName() const override;
	string ParamsToString() const override;

    // FOR DEBUG
    /*LogicalGet() : LogicalOperator(LogicalOperatorType::LOGICAL_GET) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalGet>();
    }*/

    // FOR IMPLEMENTATION
    LogicalGet(LogicalGet const& lg) : LogicalOperator(lg) {
        table_index = lg.table_index;
        function = lg.function;
        bind_data = lg.bind_data->Copy();
        returned_types = lg.returned_types;
        names = lg.names;
        column_ids = lg.column_ids;
        table_filters = lg.table_filters;
    }
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalGet>(*this);
    }

public:
	vector<ColumnBinding> GetColumnBindings() override;

	idx_t EstimateCardinality(ClientContext &context) override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
