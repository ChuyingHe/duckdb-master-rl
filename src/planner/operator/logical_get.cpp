#include "duckdb/planner/operator/logical_get.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

LogicalGet::LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
                       vector<LogicalType> returned_types, vector<string> returned_names)
    : LogicalOperator(LogicalOperatorType::LOGICAL_GET), table_index(table_index), function(move(function)),
      bind_data(move(bind_data)), returned_types(move(returned_types)), names(move(returned_names)) {
}

string LogicalGet::GetName() const {
	return StringUtil::Upper(function.name);
}

string LogicalGet::ParamsToString() const {
	string result;
	for (auto &filter : table_filters) {
		result +=
		    names[filter.column_index] + ExpressionTypeToOperator(filter.comparison_type) + filter.constant.ToString();
		result += "\n";
	}
	if (!function.to_string) {
		return string();
	}
	return function.to_string(bind_data.get());
}

vector<ColumnBinding> LogicalGet::GetColumnBindings() {
	if (column_ids.empty()) {
		return {ColumnBinding(table_index, 0)};
	}
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		result.emplace_back(table_index, i);
	}
	return result;
}

void LogicalGet::ResolveTypes() {
	if (column_ids.empty()) {
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}
	for (auto &index : column_ids) {
		if (index == COLUMN_IDENTIFIER_ROW_ID) {
			types.push_back(LOGICAL_ROW_TYPE);
		} else {
			types.push_back(returned_types[index]);
		}
	}
}

std::unique_ptr<LogicalOperator> LogicalGet::clone() const {
    //return make_unique<LogicalGet>(this->table_index, this->function, this->bind_data->Copy(), this->returned_types, this->names);
    return make_unique<LogicalGet>(*this);
}

idx_t LogicalGet::EstimateCardinality(ClientContext &context) {
	if (function.cardinality) {
		auto node_stats = function.cardinality(context, bind_data.get());   // bind_data.get() is null?
		if (node_stats && node_stats->has_estimated_cardinality) {
			return node_stats->estimated_cardinality;
		}
	}
	return 1;
}

} // namespace duckdb
