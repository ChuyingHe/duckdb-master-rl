//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/enums/catalog_type.hpp"

namespace duckdb {

enum class AlterType : uint8_t { INVALID = 0, ALTER_TABLE = 1, ALTER_VIEW = 2 };

struct AlterInfo : public ParseInfo {
	AlterInfo(AlterType type, string schema, string name) : type(type), schema(schema), name(name) {
	}
	~AlterInfo() override {
	}

	AlterType type;
	//! Schema name to alter
	string schema;
	//! Entry name to alter
	string name;

public:
	virtual CatalogType GetCatalogType() = 0;
	virtual unique_ptr<AlterInfo> Copy() const = 0;
    virtual void Serialize(Serializer &serializer);
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source);
};

//===--------------------------------------------------------------------===//
// Alter Table
//===--------------------------------------------------------------------===//
enum class AlterTableType : uint8_t {
	INVALID = 0,
	RENAME_COLUMN = 1,
	RENAME_TABLE = 2,
	ADD_COLUMN = 3,
	REMOVE_COLUMN = 4,
	ALTER_COLUMN_TYPE = 5,
	SET_DEFAULT = 6
};

struct AlterTableInfo : public AlterInfo {
	AlterTableInfo(AlterTableType type, string schema, string table)
	    : AlterInfo(AlterType::ALTER_TABLE, schema, table), alter_table_type(type) {
	}
	~AlterTableInfo() override {
	}

	AlterTableType alter_table_type;

public:
	CatalogType GetCatalogType() override {
		return CatalogType::TABLE_ENTRY;
	}
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source);
};

//===--------------------------------------------------------------------===//
// RenameColumnInfo
//===--------------------------------------------------------------------===//
struct RenameColumnInfo : public AlterTableInfo {
	RenameColumnInfo(string schema, string table, string old_name_p, string new_name_p)
	    : AlterTableInfo(AlterTableType::RENAME_COLUMN, move(schema), move(table)), old_name(move(old_name_p)),
	      new_name(move(new_name_p)) {
	}
	~RenameColumnInfo() override {
	}
    RenameColumnInfo(RenameColumnInfo const& rci) : AlterTableInfo(rci) {
	    old_name = rci.old_name;
	    new_name = rci.new_name;
	}

    //! Column old name
	string old_name;
	//! Column new name
	string new_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
    unique_ptr<ParseInfo> clone() const override;
};

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
struct RenameTableInfo : public AlterTableInfo {
	RenameTableInfo(string schema, string table, string new_name)
	    : AlterTableInfo(AlterTableType::RENAME_TABLE, schema, table), new_table_name(new_name) {
	}
	~RenameTableInfo() override {
	}

    RenameTableInfo(RenameTableInfo const& rti) : AlterTableInfo(rti) {
	    new_table_name = rti.new_table_name;
	}

	//! Relation new name
	string new_table_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
    unique_ptr<ParseInfo> clone() const override;
};

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
struct AddColumnInfo : public AlterTableInfo {
	AddColumnInfo(string schema, string table, ColumnDefinition new_column)
	    : AlterTableInfo(AlterTableType::ADD_COLUMN, schema, table), new_column(move(new_column)) {
	}
	~AddColumnInfo() override {
	}
    AddColumnInfo(AddColumnInfo const& aci) : AlterTableInfo(aci), new_column(aci.new_column) {
	}

	//! New column
	ColumnDefinition new_column;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
    unique_ptr<ParseInfo> clone() const override;
};

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
struct RemoveColumnInfo : public AlterTableInfo {
	RemoveColumnInfo(string schema, string table, string removed_column, bool if_exists)
	    : AlterTableInfo(AlterTableType::REMOVE_COLUMN, schema, table), removed_column(move(removed_column)),
	      if_exists(if_exists) {
	}
	~RemoveColumnInfo() override {
	}
    RemoveColumnInfo(RemoveColumnInfo const& rci) : AlterTableInfo(rci) {
	    removed_column = rci.removed_column;
	    if_exists = rci.if_exists;
	}

	//! The column to remove
	string removed_column;
	//! Whether or not an error should be thrown if the column does not exist
	bool if_exists;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
    unique_ptr<ParseInfo> clone() const override;
};

//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
struct ChangeColumnTypeInfo : public AlterTableInfo {
	ChangeColumnTypeInfo(string schema, string table, string column_name, LogicalType target_type,
	                     unique_ptr<ParsedExpression> expression)
	    : AlterTableInfo(AlterTableType::ALTER_COLUMN_TYPE, schema, table), column_name(move(column_name)),
	      target_type(move(target_type)), expression(move(expression)) {
	}
	~ChangeColumnTypeInfo() override {
	}
    ChangeColumnTypeInfo(ChangeColumnTypeInfo const& ccti) : AlterTableInfo(ccti) {
	    column_name = ccti.column_name;
	    target_type = ccti.target_type;
        expression = ccti.expression->Copy();
	}

	//! The column name to alter
	string column_name;
	//! The target type of the column
	LogicalType target_type;
	//! The expression used for data conversion
	unique_ptr<ParsedExpression> expression;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
    unique_ptr<ParseInfo> clone() const override;
};

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
struct SetDefaultInfo : public AlterTableInfo {
	SetDefaultInfo(string schema, string table, string column_name, unique_ptr<ParsedExpression> new_default)
	    : AlterTableInfo(AlterTableType::SET_DEFAULT, schema, table), column_name(move(column_name)),
	      expression(move(new_default)) {
	}
	~SetDefaultInfo() override {
	}
    SetDefaultInfo(SetDefaultInfo const& sdi) : AlterTableInfo(sdi) {
        column_name = sdi.column_name;
        expression = sdi.expression->Copy();
	}

	//! The column name to alter
	string column_name;
	//! The expression used for data conversion
	unique_ptr<ParsedExpression> expression;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
    unique_ptr<ParseInfo> clone() const override;
};

//===--------------------------------------------------------------------===//
// Alter View
//===--------------------------------------------------------------------===//
enum class AlterViewType : uint8_t { INVALID = 0, RENAME_VIEW = 1 };

struct AlterViewInfo : public AlterInfo {
	AlterViewInfo(AlterViewType type, string schema, string view)
	    : AlterInfo(AlterType::ALTER_VIEW, schema, view), alter_view_type(type) {
	}
	~AlterViewInfo() override {
	}

	AlterViewType alter_view_type;

public:
	CatalogType GetCatalogType() override {
		return CatalogType::VIEW_ENTRY;
	}
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source);
};

//===--------------------------------------------------------------------===//
// RenameViewInfo
//===--------------------------------------------------------------------===//
struct RenameViewInfo : public AlterViewInfo {
	RenameViewInfo(string schema, string view, string new_name)
	    : AlterViewInfo(AlterViewType::RENAME_VIEW, schema, view), new_view_name(new_name) {
	}
	~RenameViewInfo() override {
	}
    RenameViewInfo(RenameViewInfo const& rvi) : AlterViewInfo(rvi) {
        new_view_name = rvi.new_view_name;
	}

	//! Relation new name
	string new_view_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
    unique_ptr<ParseInfo> clone() const override;
};

} // namespace duckdb
