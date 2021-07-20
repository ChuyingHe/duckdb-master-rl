//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/local_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {
class DataTable;
class WriteAheadLog;
struct TableAppendState;

class LocalTableStorage {
public:
	explicit LocalTableStorage(DataTable &table);
	~LocalTableStorage();

    LocalTableStorage(LocalTableStorage const& lts):table(lts.table) {
        collection = lts.collection;

        indexes.reserve(lts.indexes.size());
        for (auto const& elem: lts.indexes) {
            indexes.push_back(elem->clone());
        }

        //FIXME: unordered_map<idx_t, unique_ptr<bool[]>> deleted_entries;

        deleted_rows = lts.deleted_rows;
        active_scans = lts.active_scans;
    }

    unique_ptr<LocalTableStorage> clone() {
        return make_unique<LocalTableStorage>(*this);
    }

	DataTable &table;
	//! The main chunk collection holding the data
	ChunkCollection collection;
	//! The set of unique indexes
	vector<unique_ptr<Index>> indexes;
	//! The set of deleted entries
	unordered_map<idx_t, unique_ptr<bool[]>> deleted_entries;
	//! The number of deleted rows
	idx_t deleted_rows;
	//! The number of active scans
	idx_t active_scans = 0;

public:
	void InitializeScan(LocalScanState &state, TableFilterSet *table_filters = nullptr);
	idx_t EstimatedSize();

	void Clear();
};

//! The LocalStorage class holds appends that have not been committed yet
class LocalStorage {
public:
	struct CommitState {
		unordered_map<DataTable *, unique_ptr<TableAppendState>> append_states;
	};

public:
	explicit LocalStorage(Transaction &transaction) : transaction(transaction) {
	}

    LocalStorage(LocalStorage const& ls): transaction(ls.transaction) {
        //	unordered_map<DataTable *, unique_ptr<LocalTableStorage>> table_storage;
        for (auto const& elem:ls.table_storage) {
            table_storage[elem.first] = elem.second->clone();
        }
	}

	//! Initialize a scan of the local storage
	void InitializeScan(DataTable *table, LocalScanState &state, TableFilterSet *table_filters);
	//! Scan
	void Scan(LocalScanState &state, const vector<column_t> &column_ids, DataChunk &result);

	//! Append a chunk to the local storage
	void Append(DataTable *table, DataChunk &chunk);
	//! Delete a set of rows from the local storage
	void Delete(DataTable *table, Vector &row_ids, idx_t count);
	//! Update a set of rows in the local storage
	void Update(DataTable *table, Vector &row_ids, const vector<column_t> &column_ids, DataChunk &data);

	//! Commits the local storage, writing it to the WAL and completing the commit
	void Commit(LocalStorage::CommitState &commit_state, Transaction &transaction, WriteAheadLog *log,
	            transaction_t commit_id);

	bool ChangesMade() noexcept {
		return table_storage.size() > 0;
	}
	idx_t EstimatedSize();

	bool Find(DataTable *table) {
		return table_storage.find(table) != table_storage.end();
	}

	idx_t AddedRows(DataTable *table) {
		auto entry = table_storage.find(table);
		if (entry == table_storage.end()) {
			return 0;
		}
		return entry->second->collection.Count() - entry->second->deleted_rows;
	}

	void AddColumn(DataTable *old_dt, DataTable *new_dt, ColumnDefinition &new_column, Expression *default_value);
	void ChangeType(DataTable *old_dt, DataTable *new_dt, idx_t changed_idx, const LogicalType &target_type,
	                const vector<column_t> &bound_columns, Expression &cast_expr);

private:
	LocalTableStorage *GetStorage(DataTable *table);

	template <class T>
	bool ScanTableStorage(DataTable &table, LocalTableStorage &storage, T &&fun);

private:
	Transaction &transaction;
	unordered_map<DataTable *, unique_ptr<LocalTableStorage>> table_storage;

	void Flush(DataTable &table, LocalTableStorage &storage);
};

} // namespace duckdb
