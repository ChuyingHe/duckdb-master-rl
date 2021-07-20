//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_index_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/enums/index_type.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

struct CreateIndexInfo : public CreateInfo {
	CreateIndexInfo() : CreateInfo(CatalogType::INDEX_ENTRY) {
	}
    CreateIndexInfo(CreateIndexInfo const& cii) : CreateInfo(cii) {
        index_type = cii.index_type;
        index_name = cii.index_name;
        unique = cii.unique;
        table = cii.table->Copy_BaseTableRef();
        expressions.reserve(cii.expressions.size());
        for (auto const& elem: cii.expressions) {
            expressions.push_back(elem->Copy());
        }
	}

	//! Index Type (e.g., B+-tree, Skip-List, ...)
	IndexType index_type;
	//! Name of the Index
	string index_name;
	//! If it is an unique index
	bool unique = false;
	//! The table to create the index on
	unique_ptr<BaseTableRef> table;
	//! Set of expressions to index by
	vector<unique_ptr<ParsedExpression>> expressions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateIndexInfo>();
		CopyProperties(*result);
		result->index_type = index_type;
		result->index_name = index_name;
		result->unique = unique;
		result->table = unique_ptr_cast<TableRef, BaseTableRef>(table->Copy());
		for (auto &expr : expressions) {
			result->expressions.push_back(expr->Copy());
		}
		return move(result);
	}
    std::unique_ptr<ParseInfo> clone() const override {
        return Copy();
    }

    unique_ptr<CreateIndexInfo> duplicate() {
        auto result = make_unique<CreateIndexInfo>();
        CopyProperties(*result);
        result->index_type = index_type;
        result->index_name = index_name;
        result->unique = unique;
        result->table = unique_ptr_cast<TableRef, BaseTableRef>(table->Copy());
        for (auto &expr : expressions) {
            result->expressions.push_back(expr->Copy());
        }
        return result;
	}

};

} // namespace duckdb
