//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"

#include "duckdb/storage/data_table.hpp"

#include <fstream>

namespace duckdb {

//! Physically CREATE INDEX statement
class PhysicalCreateIndex : public PhysicalOperator {
public:
	PhysicalCreateIndex(LogicalOperator &op, TableCatalogEntry &table, vector<column_t> column_ids,
	                    vector<unique_ptr<Expression>> expressions, unique_ptr<CreateIndexInfo> info,
	                    vector<unique_ptr<Expression>> unbinded_expressions, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, op.types, estimated_cardinality), table(table),
	      column_ids(column_ids), expressions(move(expressions)), info(std::move(info)),
	      unbound_expressions(move(unbinded_expressions)) {
	}
    PhysicalCreateIndex(PhysicalCreateIndex const& pci) : PhysicalOperator(PhysicalOperatorType::CREATE_INDEX, pci.types, pci.estimated_cardinality),
    table(pci.table), column_ids(pci.column_ids) {
        expressions.reserve(pci.expressions.size());
        for (auto const& exp: pci.expressions) {
            expressions.push_back(exp->Copy());
        }

        info = pci.info->duplicate();

        unbound_expressions.reserve(pci.unbound_expressions.size());
        for (auto const& exp: pci.unbound_expressions) {
            unbound_expressions.push_back(exp->Copy());
        }
	}

	//! The table to create the index for
	TableCatalogEntry &table;
	//! The list of column IDs required for the index
	vector<column_t> column_ids;
	//! Set of expressions to index by
	vector<unique_ptr<Expression>> expressions;
	//! Info for index creation
	unique_ptr<CreateIndexInfo> info;
	//! Unbinded expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalCreateIndex>(*this);
    }
};
} // namespace duckdb
