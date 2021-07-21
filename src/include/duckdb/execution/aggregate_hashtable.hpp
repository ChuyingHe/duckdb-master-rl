//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/aggregate_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/base_aggregate_hashtable.hpp"

namespace duckdb {
class BlockHandle;
class BufferHandle;

//! GroupedAggregateHashTable is a linear probing HT that is used for computing
//! aggregates
/*!
    GroupedAggregateHashTable is a HT that is used for computing aggregates. It takes
   as input the set of groups and the types of the aggregates to compute and
   stores them in the HT. It uses linear probing for collision resolution.
*/

// two part hash table
// hashes and payload
// hashes layout:
// [SALT][PAGE_NR][PAGE_OFFSET]
// [SALT] are the high bits of the hash value, e.g. 16 for 64 bit hashes
// [PAGE_NR] is the buffer managed payload page index
// [PAGE_OFFSET] is the logical entry offset into said payload page

// NOTE: PAGE_NR and PAGE_OFFSET are reversed for 64 bit HTs because struct packing

// payload layout
// [HASH][GROUPS][PADDING][PAYLOAD]
// [HASH] is the hash of the groups
// [GROUPS] is the group data, could be multiple values, fixed size, strings are elsewhere
// [PADDING] is gunk data to align payload properly
// [PAYLOAD] is the payload (i.e. the aggregate states)
struct aggr_ht_entry_64 {
	uint16_t salt;
	uint16_t page_offset;
	uint32_t page_nr; // this has to come last because alignment
};

struct aggr_ht_entry_32 {
	uint8_t salt;
	uint8_t page_nr;
	uint16_t page_offset;
};

enum HtEntryType { HT_WIDTH_32, HT_WIDTH_64 };

class GroupedAggregateHashTable : public BaseAggregateHashTable {
public:
	GroupedAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, const vector<BoundAggregateExpression *> &aggregates,
	                          HtEntryType entry_type = HtEntryType::HT_WIDTH_64);
	GroupedAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types,
	                          vector<LogicalType> payload_types, vector<AggregateObject> aggregates,
	                          HtEntryType entry_type = HtEntryType::HT_WIDTH_64);
	GroupedAggregateHashTable(BufferManager &buffer_manager, vector<LogicalType> group_types);
	~GroupedAggregateHashTable() override;

	//! Add the given data to the HT, computing the aggregates grouped by the
	//! data in the group chunk. When resize = true, aggregates will not be
	//! computed but instead just assigned.
	idx_t AddChunk(DataChunk &groups, DataChunk &payload);
	idx_t AddChunk(DataChunk &groups, Vector &group_hashes, DataChunk &payload);

	//! Scan the HT starting from the scan_position until the result and group
	//! chunks are filled. scan_position will be updated by this function.
	//! Returns the amount of elements found.
	idx_t Scan(idx_t &scan_position, DataChunk &result);

	//! Fetch the aggregates for specific groups from the HT and place them in the result
	void FetchAggregates(DataChunk &groups, DataChunk &result);

	//! Finds or creates groups in the hashtable using the specified group keys. The addresses vector will be filled
	//! with pointers to the groups in the hash table, and the new_groups selection vector will point to the newly
	//! created groups. The return value is the amount of newly created groups.
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &group_hashes, Vector &addresses_out,
	                         SelectionVector &new_groups_out);
	idx_t FindOrCreateGroups(DataChunk &groups, Vector &addresses_out, SelectionVector &new_groups_out);
	void FindOrCreateGroups(DataChunk &groups, Vector &addresses_out);

	//! Executes the filter(if any) and update the aggregates
	static void UpdateAggregate(AggregateObject &aggr, DataChunk &payload, Vector &distinct_addresses,
	                            idx_t input_count, idx_t payload_idx);
	void Combine(GroupedAggregateHashTable &other);

	idx_t Size() {
		return entries;
	}

	idx_t MaxCapacity();

	void Partition(vector<GroupedAggregateHashTable *> &partition_hts, hash_t mask, idx_t shift);

	void Finalize();

	//! The stringheap of the AggregateHashTable
	StringHeap string_heap;

	//! The hash table load factor, when a resize is triggered
	constexpr static float LOAD_FACTOR = 1.5;
	constexpr static uint8_t HASH_WIDTH = sizeof(hash_t);

    GroupedAggregateHashTable(GroupedAggregateHashTable const& gaht);

    unique_ptr<GroupedAggregateHashTable> clone() const {
        return make_unique<GroupedAggregateHashTable>(*this);
    }

private:
	HtEntryType entry_type;

	//! The total tuple size
	idx_t tuple_size;
	//! The amount of tuples that fit in a single block
	idx_t tuples_per_block;
	//! The capacity of the HT. This can be increased using
	//! GroupedAggregateHashTable::Resize
	idx_t capacity;
	//! The amount of entries stored in the HT currently
	idx_t entries;
	//! The data of the HT
	vector<unique_ptr<BufferHandle>> payload_hds;
	vector<data_ptr_t> payload_hds_ptrs;

	//! The hashes of the HT
	unique_ptr<BufferHandle> hashes_hdl;
	data_ptr_t hashes_hdl_ptr;
	data_ptr_t hashes_end_ptr; // of hashes

	idx_t hash_prefix_shift;
	idx_t payload_page_offset;

	//! Bitmask for getting relevant bits from the hashes to determine the position
	hash_t bitmask;

	//! Pointer vector for Scan()
	Vector addresses;

	vector<unique_ptr<GroupedAggregateHashTable>> distinct_hashes;

	bool is_finalized;

	// some stuff from FindOrCreateGroupsInternal() to avoid allocation there
	Vector ht_offsets;
	Vector hash_salts;
	SelectionVector group_compare_vector;
	SelectionVector no_match_vector;
	SelectionVector empty_vector;

private:
	//GroupedAggregateHashTable(const GroupedAggregateHashTable &) = delete;

	//! Resize the HT to the specified size. Must be larger than the current
	//! size.
	void Destroy();
	void ScatterGroups(DataChunk &groups, unique_ptr<VectorData[]> &group_data, Vector &addresses,
	                   const SelectionVector &sel, idx_t count);

	void Verify();

	void FlushMove(Vector &source_addresses, Vector &source_hashes, idx_t count);
	void NewBlock();

	template <class T>
	void VerifyInternal();
	template <class T>
	void Resize(idx_t size);
	template <class T>
	idx_t FindOrCreateGroupsInternal(DataChunk &groups, Vector &group_hashes, Vector &addresses,
	                                 SelectionVector &new_groups);

	template <class FUNC = std::function<void(idx_t, idx_t, data_ptr_t)>>
	void PayloadApply(FUNC fun);
};

} // namespace duckdb
