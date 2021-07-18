//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/atomic.hpp"

#include <memory>

namespace duckdb {
struct AlterInfo;
class Catalog;
class CatalogSet;
class ClientContext;

//! Abstract base class of an entry in the catalog
class CatalogEntry {
public:
	CatalogEntry(CatalogType type, Catalog *catalog, string name)
	    : type(type), catalog(catalog), set(nullptr), name(name), deleted(false), temporary(false), internal(false),
	      parent(nullptr) {
	}

    CatalogEntry(CatalogEntry const& ce) {
        type = ce.type;
        catalog = ce.catalog;
        set = ce.set;
        name = ce.name;
        deleted = ce.deleted;
        temporary = ce.temporary;
        internal = ce.internal;
        timestamp = ce.timestamp.load();
        child = ce.child->Copy();    //FIXME: Copy(context) throw exception
        parent = ce.parent;
	}

	virtual ~CatalogEntry();

	virtual unique_ptr<CatalogEntry> AlterEntry(ClientContext &context, AlterInfo *info) {
		throw CatalogException("Unsupported alter type for catalog entry!");
	}
    unique_ptr<CatalogEntry> Copy() {
	    return make_unique<CatalogEntry>(*this);
	}
	virtual unique_ptr<CatalogEntry> Copy(ClientContext &context) {
		throw CatalogException("Unsupported copy type for catalog entry!");
	}
	//! Sets the CatalogEntry as the new root entry (i.e. the newest entry) - this is called on a rollback to an
	//! AlterEntry
	virtual void SetAsRoot() {
	}
	//! Convert the catalog entry to a SQL string that can be used to re-construct the catalog entry
	virtual string ToSQL() {
		throw CatalogException("Unsupported catalog type for ToSQL()");
	}

	//! The type of this catalog entry
	CatalogType type;
	//! Reference to the catalog this entry belongs to
	Catalog *catalog;
	//! Reference to the catalog set this entry is stored in
	CatalogSet *set;
	//! The name of the entry
	string name;
	//! Whether or not the object is deleted
	bool deleted;
	//! Whether or not the object is temporary and should not be added to the WAL
	bool temporary;
	//! Whether or not the entry is an internal entry (cannot be deleted, not dumped, etc)
	bool internal;
	//! Timestamp at which the catalog entry was created
	atomic<transaction_t> timestamp;
	//! Child entry
	unique_ptr<CatalogEntry> child;
	//! Parent entry (the node that owns this node)
	CatalogEntry *parent;
};
} // namespace duckdb
