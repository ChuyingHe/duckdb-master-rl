//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/parsed_data/bound_create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"

namespace duckdb {
class CatalogEntry;

struct BoundCreateTableInfo {
	explicit BoundCreateTableInfo(unique_ptr<CreateInfo> base) : base(move(base)) {
	}

    BoundCreateTableInfo(BoundCreateTableInfo &bcti) {
        schema = bcti.schema;
        base->CopyProperties(*bcti.base);
        name_map = bcti.name_map;

        constraints.reserve(bcti.constraints.size());
        for (const auto &con : bcti.constraints) {
            constraints.push_back(con->Copy());
        }
        bound_constraints.reserve(bcti.bound_constraints.size());
        for (const auto &bc:bcti.bound_constraints) {
            bound_constraints.push_back(make_unique<BoundConstraint>(*bc));
        }
        bound_defaults.reserve(bcti.bound_defaults.size());
        for (const auto &bd:bcti.bound_defaults) {
            bound_defaults.push_back(bd->Copy());
        }

        dependencies = bcti.dependencies;
        data = make_unique<PersistentTableData>(*bcti.data);
        query = bcti.query->clone();
	}

	//! The schema to create the table in
	SchemaCatalogEntry *schema;
	//! The base CreateInfo object
	unique_ptr<CreateInfo> base;
	//! The map of column names -> column index, used during binding
	unordered_map<string, column_t> name_map;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! List of bound constraints on the table
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	//! Bound default values
	vector<unique_ptr<Expression>> bound_defaults;
	//! Dependents of the table (in e.g. default values)
	unordered_set<CatalogEntry *> dependencies;
	//! The existing table data on disk (if any)
	unique_ptr<PersistentTableData> data;
	//! CREATE TABLE from QUERY
	unique_ptr<LogicalOperator> query;

	CreateTableInfo &Base() {
		return (CreateTableInfo &)*base;
	}

	unique_ptr<BoundCreateTableInfo> clone() {
        return make_unique<BoundCreateTableInfo>(*this);
	}
};

} // namespace duckdb
