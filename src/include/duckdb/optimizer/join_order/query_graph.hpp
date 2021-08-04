//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/query_graph.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/common/vector.hpp"

#include <functional>

namespace duckdb {
class Expression;
class LogicalOperator;

struct FilterInfo {
	idx_t filter_index;
	JoinRelationSet *left_set = nullptr;
	JoinRelationSet *right_set = nullptr;
	JoinRelationSet *set = nullptr;

    FilterInfo(){}

    FilterInfo(FilterInfo const& fi) {
        printf("FilterInfo Copy Constructor\n");
        filter_index = fi.filter_index;
        left_set = new JoinRelationSet(*fi.left_set);
        right_set = new JoinRelationSet(*fi.right_set);
        set = new JoinRelationSet(*fi.set);
    }
    /* FilterInfo Copy() {
        FilterInfo fi;

        fi.filter_index = filter_index;
        fi.left_set = new JoinRelationSet(*left_set);
        fi.right_set = new JoinRelationSet(*right_set);
        fi.set = new JoinRelationSet(*set);
        return fi;
    }*/
};

struct FilterNode {
	vector<FilterInfo *> filters;
	unordered_map<idx_t, unique_ptr<FilterNode>> children;
};

struct NeighborInfo {
	JoinRelationSet *neighbor;
	vector<FilterInfo *> filters;

    NeighborInfo() {}

    NeighborInfo(NeighborInfo const& ni) {
        printf("NeighborInfo copy constructor\n");
        neighbor = new JoinRelationSet(*ni.neighbor);

        filters.reserve(ni.filters.size());
        for (auto const& elem: ni.filters) {
            // FilterInfo filter_content = elem->Copy();
            filters.push_back(new FilterInfo(*elem));
        }
    }
};

//! The QueryGraph contains edges between relations and allows edges to be created/queried
class QueryGraph {
public:
	//! Contains a node with info about neighboring relations and child edge infos
	struct QueryEdge {
		vector<unique_ptr<NeighborInfo>> neighbors;
		unordered_map<idx_t, unique_ptr<QueryEdge>> children;
	};

public:
	string ToString() const;
	void Print();

	//! Create an edge in the edge_set
	void CreateEdge(JoinRelationSet *left, JoinRelationSet *right, FilterInfo *info);
	//! Returns a connection if there is an edge that connects these two sets, or nullptr otherwise
	NeighborInfo *GetConnection(JoinRelationSet *node, JoinRelationSet *other);
	//! Enumerate the neighbors of a specific node that do not belong to any of the exclusion_set. Note that if a
	//! neighbor has multiple nodes, this function will return the lowest entry in that set.
	vector<idx_t> GetNeighbors(JoinRelationSet *node, unordered_set<idx_t> &exclusion_set);
	//! Enumerate all neighbors of a given JoinRelationSet node
	void EnumerateNeighbors(JoinRelationSet *node, const std::function<bool(NeighborInfo *)> &callback);
	// get all connected neighbors
    vector<idx_t> GetAllNeighbors(JoinRelationSet *node);

private:
	//! Get the QueryEdge of a specific node
	QueryEdge *GetQueryEdge(JoinRelationSet *left);

	QueryEdge root;
};

} // namespace duckdb
