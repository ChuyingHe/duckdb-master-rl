//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension.hpp"

namespace duckdb {
class StorageManager;       // StorageManager is responsible for managing the physical storage of the database on disk
class Catalog;              // The Catalog object represents the catalog of the database.
class TransactionManager;   // The Transaction Manager is responsible for creating and managing transactions
class ConnectionManager;    // Connect the DatabaseInstance and ClientContext
class FileSystem;
class TaskScheduler;        // The TaskScheduler is responsible for managing tasks and threads
class ObjectCache;          // ObjectCache is the base class for objects caches in DuckDB

class DatabaseInstance : public std::enable_shared_from_this<DatabaseInstance> {
	friend class DuckDB;

public:
	DUCKDB_API DatabaseInstance();
	DUCKDB_API ~DatabaseInstance();
    DatabaseInstance(DatabaseInstance const& di);

	DBConfig config;        // this is optional and only used in tests at the moment

public:
	StorageManager &GetStorageManager();
	Catalog &GetCatalog();
	FileSystem &GetFileSystem();
	TransactionManager &GetTransactionManager();
	TaskScheduler &GetScheduler();
	ObjectCache &GetObjectCache();
	ConnectionManager &GetConnectionManager();

	idx_t NumberOfThreads();

	static DatabaseInstance &GetDatabase(ClientContext &context);

private:
	void Initialize(const char *path, DBConfig *config);

	void Configure(DBConfig &config);

private:
	unique_ptr<StorageManager> storage;
	unique_ptr<Catalog> catalog;
	unique_ptr<TransactionManager> transaction_manager;
	unique_ptr<TaskScheduler> scheduler;
	unique_ptr<ObjectCache> object_cache;
	unique_ptr<ConnectionManager> connection_manager;
};

//! The database object. This object holds the catalog and all the
//! database-specific meta information.
class DuckDB {
public:
	DUCKDB_API explicit DuckDB(const char *path = nullptr, DBConfig *config = nullptr);
	DUCKDB_API explicit DuckDB(const string &path, DBConfig *config = nullptr);
	DUCKDB_API ~DuckDB();

	//! Reference to the actual database instance
	shared_ptr<DatabaseInstance> instance;

public:
	template <class T>
	void LoadExtension() {
		T extension;
		extension.Load(*this);
	}

	DUCKDB_API FileSystem &GetFileSystem();

	DUCKDB_API idx_t NumberOfThreads();
	DUCKDB_API static const char *SourceID();
	DUCKDB_API static const char *LibraryVersion();
};

} // namespace duckdb
