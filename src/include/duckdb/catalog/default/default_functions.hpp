//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/default/default_generator.hpp"

namespace duckdb {
class SchemaCatalogEntry;

class DefaultFunctionGenerator : public DefaultGenerator {
public:
	DefaultFunctionGenerator(Catalog &catalog, SchemaCatalogEntry *schema);

	SchemaCatalogEntry *schema;

	unique_ptr<DefaultGenerator> Copy() const override {
        return make_unique<DefaultFunctionGenerator>(catalog, schema);
    }

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
};

} // namespace duckdb
