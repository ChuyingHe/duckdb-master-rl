//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"

#include <functional>
#include <memory>

namespace duckdb {
struct AlterInfo;

class ClientContext;

typedef unordered_map<CatalogSet *, unique_lock<mutex>> set_lock_map_t;

struct MappingValue {
	explicit MappingValue(idx_t index_) : index(index_), timestamp(0), deleted(false), parent(nullptr) {
	}

    unique_ptr<MappingValue> Copy() const {
	    MappingValue* parent_raw_ptr = parent->Copy().get();
	    auto copy = make_unique<MappingValue>(index);
        copy->timestamp = timestamp;
        copy->deleted = deleted;
        copy->parent = parent_raw_ptr;
        copy->child = child->Copy();
        return move(copy);
	}

	idx_t index;
	transaction_t timestamp;
	bool deleted;
	unique_ptr<MappingValue> child;
	MappingValue *parent;
};

//! The Catalog Set stores (key, value) map of a set of CatalogEntries
class CatalogSet {
	friend class DependencyManager;

public:
    CatalogSet(CatalogSet const& cs): catalog(cs.catalog) {
        for (auto const& elem: cs.mapping) {
            mapping[elem.first] = elem.second->Copy();
        }
        //FIXME:	unordered_map<idx_t, unique_ptr<CatalogEntry>> entries;
        for (auto const& elem: cs.entries) {
            entries[elem.first] = elem.second->Copy();    // Copy(context) throw exception, therefore wrote Copy()
        }
        current_entry = cs.current_entry;
        defaults = cs.defaults->Copy();
    }

public:
	explicit CatalogSet(Catalog &catalog, unique_ptr<DefaultGenerator> defaults = nullptr);

    unique_ptr<CatalogSet> Copy() const {
        return make_unique<CatalogSet>(*this);
    }

	//! Create an entry in the catalog set. Returns whether or not it was
	//! successful.
	bool CreateEntry(ClientContext &context, const string &name, unique_ptr<CatalogEntry> value,
	                 unordered_set<CatalogEntry *> &dependencies);

	bool AlterEntry(ClientContext &context, const string &name, AlterInfo *alter_info);

	bool DropEntry(ClientContext &context, const string &name, bool cascade);

	void CleanupEntry(CatalogEntry *catalog_entry);

	//! Returns the entry with the specified name
	CatalogEntry *GetEntry(ClientContext &context, const string &name);

	//! Gets the entry that is most similar to the given name (i.e. smallest levenshtein distance), or empty string if
	//! none is found
	string SimilarEntry(ClientContext &context, const string &name);

	//! Rollback <entry> to be the currently valid entry for a certain catalog
	//! entry
	void Undo(CatalogEntry *entry);

	//! Scan the catalog set, invoking the callback method for every entry
	template <class T>
	void Scan(ClientContext &context, T &&callback) {
		// lock the catalog set
		lock_guard<mutex> lock(catalog_lock);
		for (auto &kv : entries) {
			auto entry = kv.second.get();
			entry = GetEntryForTransaction(context, entry);
			if (!entry->deleted) {
				callback(entry);
			}
		}
	}

	template <class T>
	vector<T *> GetEntries(ClientContext &context) {
		vector<T *> result;
		Scan(context, [&](CatalogEntry *entry) { result.push_back((T *)entry); });
		return result;
	}

	//! Scan the catalog set, invoking the callback method for every committed entry
	template <class T>
	void Scan(T &&callback) {
		// lock the catalog set
		lock_guard<mutex> lock(catalog_lock);
		for (auto &kv : entries) {
			auto entry = kv.second.get();
			entry = GetCommittedEntry(entry);
			if (!entry->deleted) {
				callback(entry);
			}
		}
	}

	static bool HasConflict(ClientContext &context, transaction_t timestamp);
	static bool UseTimestamp(ClientContext &context, transaction_t timestamp);

	idx_t GetEntryIndex(CatalogEntry *entry);
	CatalogEntry *GetEntryFromIndex(idx_t index);
	void UpdateTimestamp(CatalogEntry *entry, transaction_t timestamp);

	//! Returns the root entry with the specified name regardless of transaction (or nullptr if there are none)
	CatalogEntry *GetRootEntry(const string &name);

private:
	//! Given a root entry, gets the entry valid for this transaction
	CatalogEntry *GetEntryForTransaction(ClientContext &context, CatalogEntry *current);
	CatalogEntry *GetCommittedEntry(CatalogEntry *current);
	bool GetEntryInternal(ClientContext &context, const string &name, idx_t &entry_index, CatalogEntry *&entry);
	bool GetEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry *&entry);
	//! Drops an entry from the catalog set; must hold the catalog_lock to safely call this
	void DropEntryInternal(ClientContext &context, idx_t entry_index, CatalogEntry &entry, bool cascade,
	                       set_lock_map_t &lock_set);
	MappingValue *GetMapping(ClientContext &context, const string &name, bool allow_lowercase_alias,
	                         bool get_latest = false);
	void PutMapping(ClientContext &context, const string &name, idx_t entry_index);
	void DeleteMapping(ClientContext &context, const string &name);

private:
	Catalog &catalog;
	//! The catalog lock is used to make changes to the data
	mutex catalog_lock;
	//! Mapping of string to catalog entry
	unordered_map<string, unique_ptr<MappingValue>> mapping;
	//! The set of catalog entries
	unordered_map<idx_t, unique_ptr<CatalogEntry>> entries;
	//! The current catalog entry index
	idx_t current_entry = 0;
	//! The generator used to generate default internal entries
	unique_ptr<DefaultGenerator> defaults;
};

} // namespace duckdb
