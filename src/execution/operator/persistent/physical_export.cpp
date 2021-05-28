#include "duckdb/execution/operator/persistent/physical_export.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

using std::stringstream;

static void WriteCatalogEntries(stringstream &ss, vector<CatalogEntry *> &entries) {
	for (auto &entry : entries) {
		ss << entry->ToSQL() << std::endl;
	}
	ss << std::endl;
}

static void WriteStringStreamToFile(FileSystem &fs, stringstream &ss, string path) {
	auto ss_string = ss.str();
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW,
	                          FileLockType::WRITE_LOCK);
	fs.Write(*handle, (void *)ss_string.c_str(), ss_string.size());
	handle.reset();
}

static void WriteValueAsSQL(stringstream &ss, Value &val) {
	if (val.type().IsNumeric()) {
		ss << val.ToString();
	} else {
		ss << "'" << val.ToString() << "'";
	}
}

static void WriteCopyStatement(FileSystem &fs, stringstream &ss, TableCatalogEntry *table, CopyInfo &info,
                               CopyFunction &function) {
	string table_file_path;
	ss << "COPY ";
	if (table->schema->name != DEFAULT_SCHEMA) {
		table_file_path = fs.JoinPath(
		    info.file_path, StringUtil::Format("%s.%s.%s", table->schema->name, table->name, function.extension));
		ss << table->schema->name << ".";
	} else {
		table_file_path = fs.JoinPath(info.file_path, StringUtil::Format("%s.%s", table->name, function.extension));
	}
	ss << table->name << " FROM '" << table_file_path << "' (";
	// write the copy options
	ss << "FORMAT '" << info.format << "'";
	if (info.format == "csv") {
		// insert default csv options, if not specified
		if (info.options.find("header") == info.options.end()) {
			info.options["header"].push_back(Value::INTEGER(0));
		}
		if (info.options.find("delimiter") == info.options.end() && info.options.find("sep") == info.options.end() &&
		    info.options.find("delim") == info.options.end()) {
			info.options["delimiter"].push_back(Value(","));
		}
		if (info.options.find("quote") == info.options.end()) {
			info.options["quote"].push_back(Value("\""));
		}
	}
	for (auto &copy_option : info.options) {
		ss << ", " << copy_option.first << " ";
		if (copy_option.second.size() == 1) {
			WriteValueAsSQL(ss, copy_option.second[0]);
		} else {
			// FIXME handle multiple options
			throw NotImplementedException("FIXME: serialize list of options");
		}
	}
	ss << ");" << std::endl;
}

void PhysicalExport::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &ccontext = context.client;
	auto &fs = FileSystem::GetFileSystem(ccontext);

	// gather all catalog types to export
	vector<CatalogEntry *> schemas;
	vector<CatalogEntry *> sequences;
	vector<CatalogEntry *> tables;
	vector<CatalogEntry *> views;
	vector<CatalogEntry *> indexes;

	Catalog::GetCatalog(ccontext).schemas->Scan(context.client, [&](CatalogEntry *entry) {
		auto schema = (SchemaCatalogEntry *)entry;
		if (schema->name != DEFAULT_SCHEMA) {
			// export schema
			schemas.push_back(schema);
		}
		schema->Scan(context.client, CatalogType::TABLE_ENTRY, [&](CatalogEntry *entry) {
			if (entry->type == CatalogType::TABLE_ENTRY) {
				tables.push_back(entry);
			} else {
				views.push_back(entry);
			}
		});
		schema->Scan(context.client, CatalogType::SEQUENCE_ENTRY,
		             [&](CatalogEntry *entry) { sequences.push_back(entry); });
		schema->Scan(context.client, CatalogType::INDEX_ENTRY, [&](CatalogEntry *entry) { indexes.push_back(entry); });
	});

	// write the schema.sql file
	// export order is SCHEMA -> SEQUENCE -> TABLE -> VIEW -> INDEX

	stringstream ss;
	WriteCatalogEntries(ss, schemas);
	WriteCatalogEntries(ss, sequences);
	WriteCatalogEntries(ss, tables);
	WriteCatalogEntries(ss, views);
	WriteCatalogEntries(ss, indexes);

	WriteStringStreamToFile(fs, ss, fs.JoinPath(info->file_path, "schema.sql"));

	// write the load.sql file
	// for every table, we write COPY INTO statement with the specified options
	stringstream load_ss;
	for (auto &table : tables) {
		WriteCopyStatement(fs, load_ss, (TableCatalogEntry *)table, *info, function);
	}
	WriteStringStreamToFile(fs, load_ss, fs.JoinPath(info->file_path, "load.sql"));
	state->finished = true;
}

} // namespace duckdb
