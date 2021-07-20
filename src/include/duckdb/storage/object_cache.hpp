//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/object_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
class ClientContext;

//! ObjectCache is the base class for objects caches in DuckDB
class ObjectCacheEntry {
public:
	virtual ~ObjectCacheEntry() {
	}
    shared_ptr<ObjectCacheEntry> clone() {
        return make_shared<ObjectCacheEntry>(*this);
	}
};

class ObjectCache {
public:
	shared_ptr<ObjectCacheEntry> Get(string key) {
		lock_guard<mutex> glock(lock);
		auto entry = cache.find(key);
		if (entry == cache.end()) {
			return nullptr;
		}
		return entry->second;
	}


	void Put(string key, shared_ptr<ObjectCacheEntry> value) {
		lock_guard<mutex> glock(lock);
		cache[key] = move(value);
	}

    ObjectCache() {
	}

    ObjectCache(ObjectCache const& oc) {
        for (auto const& elem: oc.cache) {
            cache[elem.first] = elem.second->clone();
        }
	}

    unique_ptr<ObjectCache> clone() {
        return make_unique<ObjectCache>(*this);
	}

	static ObjectCache &GetObjectCache(ClientContext &context);
	static bool ObjectCacheEnabled(ClientContext &context);

private:
	//! Object Cache
	unordered_map<string, shared_ptr<ObjectCacheEntry>> cache;
	mutex lock;
};

} // namespace duckdb
