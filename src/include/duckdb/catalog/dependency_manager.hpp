//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/dependency_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/dependency.hpp"

namespace duckdb {
class Catalog;
class ClientContext;

//! The DependencyManager is in charge of managing dependencies between catalog entries
class DependencyManager {
	friend class CatalogSet;

public:
	explicit DependencyManager(Catalog &catalog);
    DependencyManager(DependencyManager const& dm) : catalog(dm.catalog) {

        for (auto const& elem: dm.dependents_map) {
            CatalogEntry elem_first_content = *elem.first;
            CatalogEntry* first = &elem_first_content;
            dependents_map[first] = elem.second;        //FIXME: dependency_set_t = unordered_set<Dependency, DependencyHashFunction, DependencyEquality>;
        }

        for (auto const& elem: dm.dependencies_map) {
            CatalogEntry elem_first_content = *elem.first;  //first
            CatalogEntry* first = &elem_first_content;

            unordered_set<CatalogEntry*> second;            //second
            for (auto const& s:elem.second) {
                CatalogEntry copy_content = *s;
                CatalogEntry* copy_ptr = &copy_content;
                second.insert(copy_ptr);
            }
            dependencies_map[first] = second;
        }
    }
	//! Erase the object from the DependencyManager; this should only happen when the object itself is destroyed
	void EraseObject(CatalogEntry *object);
	// //! Clear all the dependencies of all entries in the catalog set
	void ClearDependencies(CatalogSet &set);

    unique_ptr<DependencyManager> clone() {
        return make_unique<DependencyManager>(*this);
    }

private:
	Catalog &catalog;
	//! Map of objects that DEPEND on [object], i.e. [object] can only be deleted when all entries in the dependency map
	//! are deleted.
	unordered_map<CatalogEntry *, dependency_set_t> dependents_map;
	//! Map of objects that the source object DEPENDS on, i.e. when any of the entries in the vector perform a CASCADE
	//! drop then [object] is deleted as wel
	unordered_map<CatalogEntry *, unordered_set<CatalogEntry *>> dependencies_map;

private:
	void AddObject(ClientContext &context, CatalogEntry *object, unordered_set<CatalogEntry *> &dependencies);
	void DropObject(ClientContext &context, CatalogEntry *object, bool cascade, set_lock_map_t &lock_set);
	void AlterObject(ClientContext &context, CatalogEntry *old_obj, CatalogEntry *new_obj);
	void EraseObjectInternal(CatalogEntry *object);
};
} // namespace duckdb
