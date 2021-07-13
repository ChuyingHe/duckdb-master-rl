//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
//! Represents a TableReference to a base table in the schema
class BaseTableRef : public TableRef {
public:
	BaseTableRef() : TableRef(TableReferenceType::BASE_TABLE), schema_name(INVALID_SCHEMA) {
	}

    BaseTableRef(BaseTableRef &btr) : TableRef(TableReferenceType::BASE_TABLE), schema_name(INVALID_SCHEMA){
	    schema_name = btr.schema_name;
	    table_name = btr.schema_name;
	    column_name_alias = btr.column_name_alias;
	}

	//! Schema name
	string schema_name;
	//! Table name
	string table_name;
	//! Alises for the column names
	vector<string> column_name_alias;

public:
	string ToString() const override {
		return "GET(" + schema_name + "." + table_name + ")";
	}

	bool Equals(const TableRef *other_p) const override;

	unique_ptr<TableRef> Copy() override;

    unique_ptr<BaseTableRef> Copy_BaseTableRef();

	//! Serializes a blob into a BaseTableRef
	void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into a BaseTableRef
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};
} // namespace duckdb
