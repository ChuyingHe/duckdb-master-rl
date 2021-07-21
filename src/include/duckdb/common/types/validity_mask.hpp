//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/validity_mask.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {
struct ValidityMask;

using validity_t = uint64_t;

struct ValidityData {
	static constexpr const int BITS_PER_VALUE = sizeof(validity_t) * 8;
	static constexpr const validity_t MAX_ENTRY = ~validity_t(0);

public:
	DUCKDB_API explicit ValidityData(idx_t count);
	DUCKDB_API ValidityData(const ValidityMask &original, idx_t count);

    ValidityData(ValidityData const& vd) {
        //FIXME: how to copy unique_ptr<validity_t[]> owned_data;
        /*unique_ptr<validity_t[]> copy_ptr = make_unique<validity_t[]>(vd.owned_data_count);
        for (int i = 0 ;i<vd.owned_data_count; i++) {
            copy_ptr[i] = vd.owned_data[i];
        }*/
    }
    buffer_ptr<ValidityData> clone() {
        return make_shared<ValidityData>(*this);
    }

    unique_ptr<validity_t[]> owned_data;
    int owned_data_count;

public:
	static inline idx_t EntryCount(idx_t count) {
		return (count + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
	}
};

//! Type used for validity masks
struct ValidityMask {
	friend struct ValidityData;

public:
	static constexpr const int BITS_PER_VALUE = ValidityData::BITS_PER_VALUE;
	static constexpr const int STANDARD_ENTRY_COUNT = (STANDARD_VECTOR_SIZE + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
	static constexpr const int STANDARD_MASK_SIZE = STANDARD_ENTRY_COUNT * sizeof(validity_t);

public:
	ValidityMask() : validity_mask(nullptr) {
	}
	explicit ValidityMask(idx_t max_count) {
		Initialize(max_count);
	}
	explicit ValidityMask(validity_t *ptr) : validity_mask(ptr) {
	}
	explicit ValidityMask(data_ptr_t ptr) : ValidityMask((validity_t *)ptr) {
	}
	ValidityMask(const ValidityMask &original, idx_t count) {
		Copy(original, count);
	}
    ValidityMask(ValidityMask const& vm) {
	    validity_t vm_content = *vm.validity_mask;
	    validity_mask = &vm_content;
        validity_data = vm.validity_data->clone();
	}

	static inline idx_t ValidityMaskSize(idx_t count = STANDARD_VECTOR_SIZE) {
		return ValidityData::EntryCount(count) * sizeof(validity_t);
	}
	bool AllValid() const {
		return !validity_mask;
	}
	bool CheckAllValid(idx_t count) const {
		if (AllValid()) {
			return true;
		}
		idx_t entry_count = ValidityData::EntryCount(count);
		idx_t valid_count = 0;
		for (idx_t i = 0; i < entry_count; i++) {
			valid_count += validity_mask[i] == ValidityData::MAX_ENTRY;
		}
		return valid_count == entry_count;
	}
	validity_t *GetData() const {
		return validity_mask;
	}
	void Reset() {
		validity_mask = nullptr;
		validity_data.reset();
	}

	void Resize(idx_t old_size, idx_t new_size);

	static inline idx_t EntryCount(idx_t count) {
		return ValidityData::EntryCount(count);
	}
	validity_t GetValidityEntry(idx_t entry_idx) const {
		if (!validity_mask) {
			return ValidityData::MAX_ENTRY;
		}
		return validity_mask[entry_idx];
	}
	static inline bool AllValid(validity_t entry) {
		return entry == ValidityData::MAX_ENTRY;
	}
	static inline bool NoneValid(validity_t entry) {
		return entry == 0;
	}
	static inline bool RowIsValid(validity_t entry, idx_t idx_in_entry) {
		return entry & (validity_t(1) << validity_t(idx_in_entry));
	}
	inline void GetEntryIndex(idx_t row_idx, idx_t &entry_idx, idx_t &idx_in_entry) const {
		entry_idx = row_idx / BITS_PER_VALUE;
		idx_in_entry = row_idx % BITS_PER_VALUE;
	}

	//! RowIsValidUnsafe should only be used if AllValid() is false: it achieves the same as RowIsValid but skips a
	//! not-null check
	inline bool RowIsValidUnsafe(idx_t row_idx) const {
		D_ASSERT(validity_mask);
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		auto entry = GetValidityEntry(entry_idx);
		return RowIsValid(entry, idx_in_entry);
	}

	//! Returns true if a row is valid (i.e. not null), false otherwise
	inline bool RowIsValid(idx_t row_idx) const {
		if (!validity_mask) {
			return true;
		}
		return RowIsValidUnsafe(row_idx);
	}

	//! Same as SetValid, but skips a null check on validity_mask
	inline void SetValidUnsafe(idx_t row_idx) {
		D_ASSERT(validity_mask);
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		validity_mask[entry_idx] |= (validity_t(1) << validity_t(idx_in_entry));
	}

	//! Marks the entry at the specified row index as valid (i.e. not-null)
	inline void SetValid(idx_t row_idx) {
		if (!validity_mask) {
			// if AllValid() we don't need to do anything
			// the row is already valid
			return;
		}
		SetValidUnsafe(row_idx);
	}

	//! Marks the entry at the specified row index as invalid (i.e. null)
	inline void SetInvalidUnsafe(idx_t row_idx) {
		D_ASSERT(validity_mask);
		idx_t entry_idx, idx_in_entry;
		GetEntryIndex(row_idx, entry_idx, idx_in_entry);
		validity_mask[entry_idx] &= ~(validity_t(1) << validity_t(idx_in_entry));
	}

	//! Marks the entry at the specified row index as invalid (i.e. null)
	inline void SetInvalid(idx_t row_idx) {
		if (!validity_mask) {
			D_ASSERT(row_idx <= STANDARD_VECTOR_SIZE);
			Initialize(STANDARD_VECTOR_SIZE);
		}
		SetInvalidUnsafe(row_idx);
	}

	//! Mark the entrry at the specified index as either valid or invalid (non-null or null)
	inline void Set(idx_t row_idx, bool valid) {
		if (valid) {
			SetValid(row_idx);
		} else {
			SetInvalid(row_idx);
		}
	}

	//! Ensure the validity mask is writable, allocating space if it is not initialized
	inline void EnsureWritable() {
		if (!validity_mask) {
			Initialize();
		}
	}

	//! Marks "count" entries in the validity mask as invalid (null)
	inline void SetAllInvalid(idx_t count) {
		D_ASSERT(count <= STANDARD_VECTOR_SIZE);
		EnsureWritable();
		for (idx_t i = 0; i < ValidityData::EntryCount(count); i++) {
			validity_mask[i] = 0;
		}
	}

	//! Marks "count" entries in the validity mask as valid (not null)
	inline void SetAllValid(idx_t count) {
		D_ASSERT(count <= STANDARD_VECTOR_SIZE);
		EnsureWritable();
		for (idx_t i = 0; i < ValidityData::EntryCount(count); i++) {
			validity_mask[i] = ValidityData::MAX_ENTRY;
		}
	}

	void Slice(const ValidityMask &other, idx_t offset);
	void Combine(const ValidityMask &other, idx_t count);
	string ToString(idx_t count) const;

	bool IsMaskSet() const;

public:
	void Initialize(validity_t *validity) {
		validity_data.reset();
		validity_mask = validity;
	}
	void Initialize(const ValidityMask &other) {
		validity_mask = other.validity_mask;
		validity_data = other.validity_data;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		validity_data = make_buffer<ValidityData>(count);
		validity_mask = validity_data->owned_data.get();
	}
	void Copy(const ValidityMask &other, idx_t count) {
		if (other.AllValid()) {
			validity_data = nullptr;
			validity_mask = nullptr;
		} else {
			validity_data = make_buffer<ValidityData>(other, count);
			validity_mask = validity_data->owned_data.get();
		}
	}

private:
	validity_t *validity_mask;
	buffer_ptr<ValidityData> validity_data;
};

} // namespace duckdb
