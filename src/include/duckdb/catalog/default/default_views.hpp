//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/default/default_views.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/default/default_generator.hpp"

namespace duckdb {
class SchemaCatalogEntry;

class DefaultViewGenerator : public DefaultGenerator {
public:
	DefaultViewGenerator(Catalog &catalog, SchemaCatalogEntry *schema);

    unique_ptr<DefaultGenerator> Copy() const override {
        return make_unique<DefaultViewGenerator>(catalog, schema);
    }

	SchemaCatalogEntry *schema;

public:
	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
};

} // namespace duckdb
