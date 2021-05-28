#include "duckdb/planner/table_binding.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/bind_context.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

Binding::Binding(const string &alias, vector<LogicalType> coltypes, vector<string> colnames, idx_t index)
    : alias(alias), index(index), types(move(coltypes)), names(move(colnames)) {
	D_ASSERT(types.size() == names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		auto &name = names[i];
		D_ASSERT(!name.empty());
		if (name_map.find(name) != name_map.end()) {
			throw BinderException("table \"%s\" has duplicate column name \"%s\"", alias, name);
		}
		name_map[name] = i;
	}
	TableCatalogEntry::AddLowerCaseAliases(name_map);
}

bool Binding::HasMatchingBinding(const string &column_name) {
	auto entry = name_map.find(column_name);
	return entry != name_map.end();
}

BindResult Binding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto column_entry = name_map.find(colref.column_name);
	if (column_entry == name_map.end()) {
		return BindResult(StringUtil::Format("Values list \"%s\" does not have a column named \"%s\"", alias.c_str(),
		                                     colref.column_name.c_str()));
	}
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_entry->second;
	LogicalType sql_type = types[column_entry->second];
	if (colref.alias.empty()) {
		colref.alias = names[column_entry->second];
	}
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), sql_type, binding, depth));
}

TableBinding::TableBinding(const string &alias, vector<LogicalType> types_p, vector<string> names_p, LogicalGet &get,
                           idx_t index, bool add_row_id)
    : Binding(alias, move(types_p), move(names_p), index), get(get) {
	if (add_row_id) {
		if (name_map.find("rowid") == name_map.end()) {
			name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
		}
	}
}

BindResult TableBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto entry = name_map.find(colref.column_name);
	if (entry == name_map.end()) {
		return BindResult(StringUtil::Format("Table \"%s\" does not have a column named \"%s\"", colref.table_name,
		                                     colref.column_name));
	}
	auto col_index = entry->second;
	// fetch the type of the column
	LogicalType col_type;
	if (entry->second == COLUMN_IDENTIFIER_ROW_ID) {
		// row id: BIGINT type
		col_type = LogicalType::BIGINT;
	} else {
		// normal column: fetch type from base column
		col_type = types[col_index];
		if (colref.alias.empty()) {
			colref.alias = names[entry->second];
		}
	}

	auto &column_ids = get.column_ids;
	// check if the entry already exists in the column list for the table
	ColumnBinding binding;

	binding.column_index = column_ids.size();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		if (column_ids[i] == col_index) {
			binding.column_index = i;
			break;
		}
	}
	if (binding.column_index == column_ids.size()) {
		// column binding not found: add it to the list of bindings
		column_ids.push_back(col_index);
	}
	binding.table_index = index;
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), col_type, binding, depth));
}

MacroBinding::MacroBinding(vector<LogicalType> types_p, vector<string> names_p, string macro_name_p)
    : Binding("0_macro_parameters", move(types_p), move(names_p), -1), macro_name(move(macro_name_p)) {
}

BindResult MacroBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto entry = name_map.find(colref.column_name);
	if (entry == name_map.end()) {
		return BindResult(
		    StringUtil::Format("Macro \"%s\" does not have a parameter named \"%s\"", macro_name, colref.column_name));
	}
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = entry->second;

	// we are binding a parameter to create the macro, no arguments are supplied
	return BindResult(make_unique<BoundColumnRefExpression>(colref.GetName(), types[entry->second], binding, depth));
}

unique_ptr<ParsedExpression> MacroBinding::ParamToArg(ColumnRefExpression &colref) {
	auto entry = name_map.find(colref.column_name);
	if (entry == name_map.end()) {
		throw BinderException("Macro \"%s\" does not have a parameter named \"%s\"", macro_name, colref.column_name);
	}
	auto arg = arguments[entry->second]->Copy();
	arg->alias = colref.alias;
	return arg;
}

} // namespace duckdb
