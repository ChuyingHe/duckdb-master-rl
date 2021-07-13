//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/base_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class Vector;
class ValidityStatistics;

class BaseStatistics {
public:
	explicit BaseStatistics(LogicalType type);
	virtual ~BaseStatistics();

    BaseStatistics(BaseStatistics &bs) {
        type = bs.type;
        validity_stats = bs.validity_stats->Copy();
    }

	//! The type of the logical segment
	LogicalType type;
	//! The validity stats of the column (if any)
	unique_ptr<BaseStatistics> validity_stats;

public:
	bool CanHaveNull();

	static unique_ptr<BaseStatistics> CreateEmpty(LogicalType type);

	virtual void Merge(const BaseStatistics &other);
	virtual unique_ptr<BaseStatistics> Copy();
	virtual void Serialize(Serializer &serializer);
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, LogicalType type);
	//! Verify that a vector does not violate the statistics
	virtual void Verify(Vector &vector, idx_t count);

	virtual string ToString();
};

} // namespace duckdb
