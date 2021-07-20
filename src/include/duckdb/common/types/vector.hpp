//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bitset.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

struct VectorData {
	const SelectionVector *sel;
	data_ptr_t data;
	ValidityMask validity;
};

class VectorStructBuffer;
class VectorListBuffer;
class ChunkCollection;

struct SelCache;

//!  Vector of values of a specified PhysicalType.
class Vector {
	friend struct ConstantVector;
	friend struct DictionaryVector;
	friend struct FlatVector;
	friend struct ListVector;
	friend struct StringVector;
	friend struct StructVector;
	friend struct SequenceVector;

	friend class DataChunk;

public:
	Vector();
	//! Create a vector of size one holding the passed on value
	explicit Vector(const Value &value);
	//! Create an empty standard vector with a type, equivalent to calling Vector(type, true, false)
	explicit Vector(const LogicalType &type);
	//! Create a non-owning vector that references the specified data
	Vector(const LogicalType &type, data_ptr_t dataptr);
	//! Create an owning vector that holds at most STANDARD_VECTOR_SIZE entries.
	/*!
	    Create a new vector
	    If create_data is true, the vector will be an owning empty vector.
	    If zero_data is true, the allocated data will be zero-initialized.
	*/
	Vector(const LogicalType &type, bool create_data, bool zero_data);
	// implicit copying of Vectors is not allowed
	Vector(const Vector &) = delete; // from duckdb
    /*Vector(Vector const& vector) {
        data = vector.data;
        validity = vector.validity;
        buffer = vector.buffer->clone();
        auxiliary = vector.auxiliary->clone();
    }*/

    // but moving of vectors is allowed
	Vector(Vector &&other) noexcept;

public:
	//! Create a vector that references the specified value.
	void Reference(const Value &value);
	//! Causes this vector to reference the data held by the other vector.
	void Reference(Vector &other);

	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, idx_t offset);
	//! Creates a reference to a slice of the other vector
	void Slice(Vector &other, const SelectionVector &sel, idx_t count);
	//! Turns the vector into a dictionary vector with the specified dictionary
	void Slice(const SelectionVector &sel, idx_t count);
	//! Slice the vector, keeping the result around in a cache or potentially using the cache instead of slicing
	void Slice(const SelectionVector &sel, idx_t count, SelCache &cache);

	//! Creates the data of this vector with the specified type. Any data that
	//! is currently in the vector is destroyed.
	void Initialize(const LogicalType &new_type = LogicalType(LogicalTypeId::INVALID), bool zero_data = false);

	//! Converts this Vector to a printable string representation
	string ToString(idx_t count) const;
	void Print(idx_t count);

	string ToString() const;
	void Print();

	//! Flatten the vector, removing any compression and turning it into a FLAT_VECTOR
	DUCKDB_API void Normalify(idx_t count);
	DUCKDB_API void Normalify(const SelectionVector &sel, idx_t count);
	//! Obtains a selection vector and data pointer through which the data of this vector can be accessed
	DUCKDB_API void Orrify(idx_t count, VectorData &data);

	//! Turn the vector into a sequence vector
	void Sequence(int64_t start, int64_t increment);

	//! Verify that the Vector is in a consistent, not corrupt state. DEBUG
	//! FUNCTION ONLY!
	void Verify(idx_t count);
	void Verify(const SelectionVector &sel, idx_t count);
	void UTFVerify(idx_t count);
	void UTFVerify(const SelectionVector &sel, idx_t count);

	//! Returns the [index] element of the Vector as a Value.
	Value GetValue(idx_t index) const;
	//! Sets the [index] element of the Vector to the specified Value.
	void SetValue(idx_t index, const Value &val);

	void SetAuxiliary(buffer_ptr<VectorBuffer> new_buffer) {
		auxiliary = std::move(new_buffer);
	};

	//! This functions resizes the vector
	void Resize(idx_t cur_size, idx_t new_size);

	//! Serializes a Vector to a stand-alone binary blob
	void Serialize(idx_t count, Serializer &serializer);
	//! Deserializes a blob back into a Vector
	void Deserialize(idx_t count, Deserializer &source);

	// Getters
	inline VectorType GetVectorType() const {
		return buffer->GetVectorType();
	}
	inline const LogicalType &GetType() const {
		return buffer->GetType();
	}
	inline VectorBufferType GetBufferType() const {
		return buffer->GetBufferType();
	}
	inline data_ptr_t GetData() {
		return data;
	}

	buffer_ptr<VectorBuffer> GetAuxiliary() {
		return auxiliary;
	}

	buffer_ptr<VectorBuffer> GetBuffer() {
		return buffer;
	}

	// Setters
	inline void SetVectorType(VectorType vector_type) {
		buffer->SetVectorType(vector_type);
	}
	inline void SetType(const LogicalType &type) {
		buffer->SetType(type);
	}
	inline void SetBufferType(VectorBufferType buffer_type) {
		buffer->SetBufferType(buffer_type);
	}

protected:
	//! A pointer to the data.
	data_ptr_t data;
	//! The validity mask of the vector
	ValidityMask validity;
	//! The main buffer holding the data of the vector
	buffer_ptr<VectorBuffer> buffer;
	//! The buffer holding auxiliary data of the vector
	//! e.g. a string vector uses this to store strings
	buffer_ptr<VectorBuffer> auxiliary;
};

//! The DictionaryBuffer holds a selection vector
class VectorChildBuffer : public VectorBuffer {
public:
	VectorChildBuffer() : VectorBuffer(VectorBufferType::VECTOR_CHILD_BUFFER), data() {
	}

public:
	Vector data;
};

struct ConstantVector {
	static inline const_data_ptr_t GetData(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR ||
		         vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.data;
	}
	static inline data_ptr_t GetData(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR ||
		         vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.data;
	}
	template <class T>
	static inline const T *GetData(const Vector &vector) {
		return (const T *)ConstantVector::GetData(vector);
	}
	template <class T>
	static inline T *GetData(Vector &vector) {
		return (T *)ConstantVector::GetData(vector);
	}
	static inline bool IsNull(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		return !vector.validity.RowIsValid(0);
	}
	static inline void SetNull(Vector &vector, bool is_null) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		vector.validity.Set(0, !is_null);
	}
	static inline ValidityMask &Validity(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		return vector.validity;
	}

	static const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE];
	static const SelectionVector ZERO_SELECTION_VECTOR;
};

struct DictionaryVector {
	static inline const SelectionVector &SelVector(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		return ((const DictionaryBuffer &)*vector.buffer).GetSelVector();
	}
	static inline SelectionVector &SelVector(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		return ((DictionaryBuffer &)*vector.buffer).GetSelVector();
	}
	static inline const Vector &Child(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		return ((const VectorChildBuffer &)*vector.auxiliary).data;
	}
	static inline Vector &Child(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::DICTIONARY_VECTOR);
		return ((VectorChildBuffer &)*vector.auxiliary).data;
	}
};

struct FlatVector {
	static inline data_ptr_t GetData(Vector &vector) {
		return ConstantVector::GetData(vector);
	}
	template <class T>
	static inline const T *GetData(const Vector &vector) {
		return ConstantVector::GetData<T>(vector);
	}
	template <class T>
	static inline T *GetData(Vector &vector) {
		return ConstantVector::GetData<T>(vector);
	}
	static inline void SetData(Vector &vector, data_ptr_t data) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		vector.data = data;
	}
	template <class T>
	static inline T GetValue(Vector &vector, idx_t idx) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return FlatVector::GetData<T>(vector)[idx];
	}
	static inline const ValidityMask &Validity(const Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.validity;
	}
	static inline ValidityMask &Validity(Vector &vector) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return vector.validity;
	}
	static inline void SetValidity(Vector &vector, ValidityMask &new_validity) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		vector.validity.Initialize(new_validity);
	}
	static inline void SetNull(Vector &vector, idx_t idx, bool is_null) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		vector.validity.Set(idx, !is_null);
	}
	static inline bool IsNull(const Vector &vector, idx_t idx) {
		D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR);
		return !vector.validity.RowIsValid(idx);
	}

	static const sel_t INCREMENTAL_VECTOR[STANDARD_VECTOR_SIZE];
	static const SelectionVector INCREMENTAL_SELECTION_VECTOR;
};

struct ListVector {
	static const Vector &GetEntry(const Vector &vector);
	static Vector &GetEntry(Vector &vector);
	static idx_t GetListSize(const Vector &vector);
	static void SetListSize(Vector &vec, idx_t size);
	static bool HasEntry(const Vector &vector);
	static void SetEntry(Vector &vector, unique_ptr<Vector> entry);
	static void Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset = 0);
	static void Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
	                   idx_t source_offset = 0);
	static void PushBack(Vector &target, Value &insert);
	static void Initialize(Vector &vec);
	//! Share the entry of the other list vector
	static void ReferenceEntry(Vector &vector, Vector &other);
};

struct StringVector {
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const char *data, idx_t len);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const char *data);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, string_t data);
	//! Add a string to the string heap of the vector (auxiliary data)
	static string_t AddString(Vector &vector, const string &data);
	//! Add a string or a blob to the string heap of the vector (auxiliary data)
	//! This function is the same as ::AddString, except the added data does not need to be valid UTF8
	static string_t AddStringOrBlob(Vector &vector, string_t data);
	//! Allocates an empty string of the specified size, and returns a writable pointer that can be used to store the
	//! result of an operation
	static string_t EmptyString(Vector &vector, idx_t len);
	//! Adds a reference to a handle that stores strings of this vector
	static void AddHandle(Vector &vector, unique_ptr<BufferHandle> handle);
	//! Adds a reference to an unspecified vector buffer that stores strings of this vector
	static void AddBuffer(Vector &vector, buffer_ptr<VectorBuffer> buffer);
	//! Add a reference from this vector to the string heap of the provided vector
	static void AddHeapReference(Vector &vector, Vector &other);
};

struct StructVector {
	static bool HasEntries(const Vector &vector);
	static const child_list_t<unique_ptr<Vector>> &GetEntries(const Vector &vector);
	static void AddEntry(Vector &vector, const string &name, unique_ptr<Vector> entry);
};

struct SequenceVector {
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment) {
		D_ASSERT(vector.GetVectorType() == VectorType::SEQUENCE_VECTOR);
		auto data = (int64_t *)vector.buffer->GetData();
		start = data[0];
		increment = data[1];
	}
};

} // namespace duckdb
