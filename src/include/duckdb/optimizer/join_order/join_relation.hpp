//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/join_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include <iostream>

namespace duckdb {
class LogicalOperator;

//! Represents a single relation and any metadata accompanying that relation
struct SingleJoinRelation {
	LogicalOperator *op;
	LogicalOperator *parent;

	SingleJoinRelation() {
	}
	SingleJoinRelation(LogicalOperator *op, LogicalOperator *parent) : op(op), parent(parent) {
	}
};


//! Set of relations, used in the join graph.
struct JoinRelationSet {
	JoinRelationSet(unique_ptr<idx_t[]> relations, idx_t count) : relations(move(relations)), count(count) {
	}

	string ToString() const;

	unique_ptr<idx_t[]> relations;
	idx_t count;

	static bool IsSubset(JoinRelationSet *super, JoinRelationSet *sub);

    JoinRelationSet(JoinRelationSet const& jrs) {
        //printf("copy constructor of JoinRelationSet \n");
        count = jrs.count;
        relations = unique_ptr<idx_t[]>(new idx_t[count]);
        for (idx_t i = 0; i < count; i++) {
            relations[i] = jrs.relations[i];
        }
    }
};

/*for unordered_map*/
class Hasher {
public:
    size_t operator() (JoinRelationSet* joinRelationSet) const {
        //printf("Hashing called\n");
        size_t hash = 0;
        for(idx_t i=0;i<joinRelationSet->count;i++){
            hash += joinRelationSet->relations[i];
        }

        return hash;
    }
};

class EqualFn {
public:
    bool operator() (JoinRelationSet* joinRelationSet1, JoinRelationSet* joinRelationSet2) const {
        /*printf("Equal called\n");
        std::cout <<"joinRelationSet1->count = "<<joinRelationSet1->count << "\n";
        std::cout <<"joinRelationSet2->count = "<<joinRelationSet2->count << "\n";*/
        if (joinRelationSet1->count != joinRelationSet2->count) {
            return false;
        }
        for (idx_t i = 0; i<joinRelationSet1->count;i++) {
            if (joinRelationSet1->relations[i] == joinRelationSet2->relations[i]){
                /*std::cout <<"joinRelationSet1->relations[i] = "<<joinRelationSet1->relations[i] << "\n";
                std::cout <<"joinRelationSet2->relations[i]"<<joinRelationSet2->relations[i] << "\n";*/
                if (i == joinRelationSet1->count-1) {
                    return true;
                }
            } else {
                return false;
            }
        }
        return false;
    }
};

//! The JoinRelationTree is a structure holding all the created JoinRelationSet objects and allowing fast lookup on to
//! them
class JoinRelationSetManager {
public:
	//! Contains a node with a JoinRelationSet and child relations
	// FIXME: this structure is inefficient, could use a bitmap for lookup instead (todo: profile)
	struct JoinRelationTreeNode {
		unique_ptr<JoinRelationSet> relation;
		unordered_map<idx_t, unique_ptr<JoinRelationTreeNode>> children;
	};

public:
	//! Create or get a JoinRelationSet from a single node with the given index
	JoinRelationSet *GetJoinRelation(idx_t index);
	//! Create or get a JoinRelationSet from a set of relation bindings
	JoinRelationSet *GetJoinRelation(unordered_set<idx_t> &bindings);
	//! Create or get a JoinRelationSet from a (sorted, duplicate-free!) list of relations
	JoinRelationSet *GetJoinRelation(unique_ptr<idx_t[]> relations, idx_t count);
	//! Union two sets of relations together and create a new relation set
	JoinRelationSet *Union(JoinRelationSet *left, JoinRelationSet *right);
	// Union function for RL algorithm:
    JoinRelationSet *RLUnion(JoinRelationSet *left, JoinRelationSet *right);
    //! Create the set difference of left \ right (i.e. all elements in left that are not in right)
	JoinRelationSet *Difference(JoinRelationSet *left, JoinRelationSet *right);

private:
	JoinRelationTreeNode root;
};

} // namespace duckdb
