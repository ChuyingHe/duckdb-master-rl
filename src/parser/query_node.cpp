#include "duckdb/parser/query_node.hpp"

#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

bool QueryNode::Equals(const QueryNode *other) const {
	if (!other) {
		return false;
	}
	if (this == other) {
		return true;
	}
	if (other->type != this->type) {
		return false;
	}
	if (modifiers.size() != other->modifiers.size()) {
		return false;
	}
	for (idx_t i = 0; i < modifiers.size(); i++) {
		if (!modifiers[i]->Equals(other->modifiers[i].get())) {
			return false;
		}
	}
	// WITH clauses (CTEs)
	if (cte_map.size() != other->cte_map.size()) {
		return false;
	}
	for (auto &entry : cte_map) {
		auto other_entry = other->cte_map.find(entry.first);
		if (other_entry == other->cte_map.end()) {
			return false;
		}
		if (entry.second->aliases != other_entry->second->aliases) {
			return false;
		}
		if (!entry.second->query->Equals(other_entry->second->query.get())) {
			return false;
		}
	}
	return other->type == type;
}

void QueryNode::CopyProperties(QueryNode &other) {
	for (auto &modifier : modifiers) {
		other.modifiers.push_back(modifier->Copy());
	}
	for (auto &kv : cte_map) {
		auto kv_info = make_unique<CommonTableExpressionInfo>();
		for (auto &al : kv.second->aliases) {
			kv_info->aliases.push_back(al);
		}
		kv_info->query = unique_ptr_cast<SQLStatement, SelectStatement>(kv.second->query->Copy());
		other.cte_map[kv.first] = move(kv_info);
	}
}

void QueryNode::Serialize(Serializer &serializer) {
	serializer.Write<QueryNodeType>(type);
	serializer.Write<idx_t>(modifiers.size());
	for (idx_t i = 0; i < modifiers.size(); i++) {
		modifiers[i]->Serialize(serializer);
	}
	// cte_map
	D_ASSERT(cte_map.size() <= NumericLimits<uint32_t>::Maximum());
	serializer.Write<uint32_t>((uint32_t)cte_map.size());
	for (auto &cte : cte_map) {
		serializer.WriteString(cte.first);
		serializer.WriteStringVector(cte.second->aliases);
		cte.second->query->Serialize(serializer);
	}
}

unique_ptr<QueryNode> QueryNode::Deserialize(Deserializer &source) {
	unique_ptr<QueryNode> result;
	auto type = source.Read<QueryNodeType>();
	auto modifier_count = source.Read<idx_t>();
	vector<unique_ptr<ResultModifier>> modifiers;
	for (idx_t i = 0; i < modifier_count; i++) {
		modifiers.push_back(ResultModifier::Deserialize(source));
	}
	// cte_map
	auto cte_count = source.Read<uint32_t>();
	unordered_map<string, unique_ptr<CommonTableExpressionInfo>> cte_map;
	for (idx_t i = 0; i < cte_count; i++) {
		auto name = source.Read<string>();
		auto info = make_unique<CommonTableExpressionInfo>();
		source.ReadStringVector(info->aliases);
		info->query = SelectStatement::Deserialize(source);
		cte_map[name] = move(info);
	}
	switch (type) {
	case QueryNodeType::SELECT_NODE:
		result = SelectNode::Deserialize(source);
		break;
	case QueryNodeType::SET_OPERATION_NODE:
		result = SetOperationNode::Deserialize(source);
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = RecursiveCTENode::Deserialize(source);
		break;
	default:
		throw SerializationException("Could not deserialize Query Node: unknown type!");
	}
	result->modifiers = move(modifiers);
	result->cte_map = move(cte_map);
	return result;
}

} // namespace duckdb
