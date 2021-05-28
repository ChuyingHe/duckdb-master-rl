#include "duckdb/planner/bind_context.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"

#include <algorithm>

namespace duckdb {

string BindContext::GetMatchingBinding(const string &column_name) {
	string result;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		auto is_using_binding = GetUsingBinding(column_name, kv.first);
		if (is_using_binding) {
			continue;
		}
		if (binding->HasMatchingBinding(column_name)) {
			if (!result.empty() || is_using_binding) {
				throw BinderException("Ambiguous reference to column name \"%s\" (use: \"%s.%s\" "
				                      "or \"%s.%s\")",
				                      column_name, result, column_name, kv.first, column_name);
			}
			result = kv.first;
		}
	}
	return result;
}

vector<string> BindContext::GetSimilarBindings(const string &column_name) {
	vector<pair<string, idx_t>> scores;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		for (auto &name : binding->names) {
			idx_t distance = StringUtil::LevenshteinDistance(name, column_name);
			scores.emplace_back(binding->alias + "." + name, distance);
		}
	}
	return StringUtil::TopNStrings(scores);
}

void BindContext::AddUsingBinding(const string &column_name, UsingColumnSet set) {
	using_columns[column_name].push_back(move(set));
}

UsingColumnSet *BindContext::GetUsingBinding(const string &column_name) {
	auto entry = using_columns.find(column_name);
	if (entry == using_columns.end()) {
		return nullptr;
	}
	if (entry->second.size() > 1) {
		string error = "Ambiguous column reference: column \"" + column_name + "\" can refer to either:\n";
		for (auto &using_set : entry->second) {
			string result_bindings;
			for (auto &binding : using_set.bindings) {
				if (result_bindings.empty()) {
					result_bindings = "[";
				} else {
					result_bindings += ", ";
				}
				result_bindings += binding;
				result_bindings += ".";
				result_bindings += column_name;
			}
			error += result_bindings + "]";
		}
		throw BinderException(error);
	}
	return &entry->second[0];
}

UsingColumnSet *BindContext::GetUsingBinding(const string &column_name, const string &binding_name) {
	if (binding_name.empty()) {
		return GetUsingBinding(column_name);
	}
	auto entry = using_columns.find(column_name);
	if (entry == using_columns.end()) {
		return nullptr;
	}
	for (auto &using_set : entry->second) {
		auto &bindings = using_set.bindings;
		if (bindings.find(binding_name) != bindings.end()) {
			return &using_set;
		}
	}
	return nullptr;
}

void BindContext::RemoveUsingBinding(const string &column_name, UsingColumnSet *set) {
	if (!set) {
		return;
	}
	auto entry = using_columns.find(column_name);
	D_ASSERT(entry != using_columns.end());
	auto &bindings = entry->second;
	for (size_t i = 0; i < bindings.size(); i++) {
		if (&bindings[i] == set) {
			bindings.erase(bindings.begin() + i);
			break;
		}
	}
	if (bindings.empty()) {
		using_columns.erase(column_name);
	}
}

unordered_set<string> BindContext::GetMatchingBindings(const string &column_name) {
	unordered_set<string> result;
	for (auto &kv : bindings) {
		auto binding = kv.second.get();
		if (binding->HasMatchingBinding(column_name)) {
			result.insert(kv.first);
		}
	}
	return result;
}

Binding *BindContext::GetCTEBinding(const string &ctename) {
	auto match = cte_bindings.find(ctename);
	if (match == cte_bindings.end()) {
		return nullptr;
	}
	return match->second.get();
}

Binding *BindContext::GetBinding(const string &name, string &out_error) {
	auto match = bindings.find(name);
	if (match == bindings.end()) {
		// alias not found in this BindContext
		vector<string> candidates;
		for (auto &kv : bindings) {
			candidates.push_back(kv.first);
		}
		string candidate_str =
		    StringUtil::CandidatesMessage(StringUtil::TopNLevenshtein(candidates, name), "Candidate tables");
		out_error = StringUtil::Format("Referenced table \"%s\" not found!%s", name, candidate_str);
		return nullptr;
	}
	return match->second.get();
}

BindResult BindContext::BindColumn(ColumnRefExpression &colref, idx_t depth) {
	if (colref.table_name.empty()) {
		return BindResult(StringUtil::Format("Could not bind alias \"%s\"!", colref.column_name));
	}

	string error;
	auto binding = GetBinding(colref.table_name, error);
	if (!binding) {
		return BindResult(error);
	}
	return binding->Bind(colref, depth);
}

string BindContext::BindColumn(PositionalReferenceExpression &ref, string &table_name, string &column_name) {
	idx_t current_position = ref.index - 1;
	idx_t total_columns = 0;
	for (auto &entry : bindings_list) {
		idx_t entry_column_count = entry.second->names.size();
		if (current_position < entry_column_count) {
			table_name = entry.first;
			column_name = entry.second->names[current_position];
			return string();
		} else {
			total_columns += entry_column_count;
			current_position -= entry_column_count;
		}
	}
	return StringUtil::Format("Positional reference %d out of range (total %d columns)", ref.index, total_columns);
}

BindResult BindContext::BindColumn(PositionalReferenceExpression &ref, idx_t depth) {
	string table_name, column_name;

	string error = BindColumn(ref, table_name, column_name);
	if (!error.empty()) {
		return BindResult(error);
	}
	auto column_ref = make_unique<ColumnRefExpression>(column_name, table_name);
	return BindColumn(*column_ref, depth);
}

void BindContext::GenerateAllColumnExpressions(vector<unique_ptr<ParsedExpression>> &new_select_list,
                                               const string &relation_name) {
	if (bindings_list.empty()) {
		throw BinderException("SELECT * expression without FROM clause!");
	}
	if (relation_name.empty()) { // SELECT * case
		// bind all expressions of each table in-order
		unordered_set<UsingColumnSet *> handled_using_columns;
		for (auto &entry : bindings_list) {
			auto binding = entry.second;
			for (auto &column_name : binding->names) {
				// check if this column is a USING column
				auto using_binding = GetUsingBinding(column_name, binding->alias);
				if (using_binding) {
					// it is!
					// check if we have already emitted the using column
					if (handled_using_columns.find(using_binding) != handled_using_columns.end()) {
						// we have! bail out
						continue;
					}
					// we have not! output the using column
					if (using_binding->primary_binding.empty()) {
						// no primary binding: output a coalesce
						auto coalesce = make_unique<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
						for (auto &child_binding : using_binding->bindings) {
							coalesce->children.push_back(make_unique<ColumnRefExpression>(column_name, child_binding));
						}
						new_select_list.push_back(move(coalesce));
					} else {
						// primary binding: output the qualified column ref
						new_select_list.push_back(
						    make_unique<ColumnRefExpression>(column_name, using_binding->primary_binding));
					}
					handled_using_columns.insert(using_binding);
					continue;
				}
				new_select_list.push_back(make_unique<ColumnRefExpression>(column_name, binding->alias));
			}
		}
	} else { // SELECT tbl.* case
		string error;
		auto binding = GetBinding(relation_name, error);
		if (!binding) {
			throw BinderException(error);
		}
		for (auto &column_name : binding->names) {
			new_select_list.push_back(make_unique<ColumnRefExpression>(column_name, binding->alias));
		}
	}
}

void BindContext::AddBinding(const string &alias, unique_ptr<Binding> binding) {
	if (bindings.find(alias) != bindings.end()) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias);
	}
	bindings_list.emplace_back(alias, binding.get());
	bindings[alias] = move(binding);
}

void BindContext::AddBaseTable(idx_t index, const string &alias, const vector<string> &names,
                               const vector<LogicalType> &types, LogicalGet &get) {
	AddBinding(alias, make_unique<TableBinding>(alias, types, names, get, index, true));
}

void BindContext::AddTableFunction(idx_t index, const string &alias, const vector<string> &names,
                                   const vector<LogicalType> &types, LogicalGet &get) {
	AddBinding(alias, make_unique<TableBinding>(alias, types, names, get, index));
}

vector<string> BindContext::AliasColumnNames(const string &table_name, const vector<string> &names,
                                             const vector<string> &column_aliases) {
	vector<string> result;
	if (column_aliases.size() > names.size()) {
		throw BinderException("table \"%s\" has %lld columns available but %lld columns specified", table_name,
		                      names.size(), column_aliases.size());
	}
	// use any provided column aliases first
	for (idx_t i = 0; i < column_aliases.size(); i++) {
		result.push_back(column_aliases[i]);
	}
	// if not enough aliases were provided, use the default names for remaining columns
	for (idx_t i = column_aliases.size(); i < names.size(); i++) {
		result.push_back(names[i]);
	}
	return result;
}

void BindContext::AddSubquery(idx_t index, const string &alias, SubqueryRef &ref, BoundQueryNode &subquery) {
	auto names = AliasColumnNames(alias, subquery.names, ref.column_name_alias);
	AddGenericBinding(index, alias, names, subquery.types);
}

void BindContext::AddGenericBinding(idx_t index, const string &alias, const vector<string> &names,
                                    const vector<LogicalType> &types) {
	AddBinding(alias, make_unique<Binding>(alias, types, names, index));
}

void BindContext::AddCTEBinding(idx_t index, const string &alias, const vector<string> &names,
                                const vector<LogicalType> &types) {
	auto binding = make_shared<Binding>(alias, types, names, index);

	if (cte_bindings.find(alias) != cte_bindings.end()) {
		throw BinderException("Duplicate alias \"%s\" in query!", alias);
	}
	cte_bindings[alias] = move(binding);
	cte_references[alias] = std::make_shared<idx_t>(0);
}

void BindContext::AddContext(BindContext other) {
	for (auto &binding : other.bindings) {
		if (bindings.find(binding.first) != bindings.end()) {
			throw BinderException("Duplicate alias \"%s\" in query!", binding.first);
		}
		bindings[binding.first] = move(binding.second);
	}
	for (auto &binding : other.bindings_list) {
		bindings_list.push_back(move(binding));
	}
	for (auto &entry : other.using_columns) {
		for (auto &alias : entry.second) {
#ifdef DEBUG
			for (auto &other_alias : using_columns[entry.first]) {
				for (auto &col : alias.bindings) {
					D_ASSERT(other_alias.bindings.find(col) == other_alias.bindings.end());
				}
			}
#endif
			using_columns[entry.first].push_back(alias);
		}
	}
}

} // namespace duckdb
