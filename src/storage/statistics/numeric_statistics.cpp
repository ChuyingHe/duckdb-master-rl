#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

template <>
void NumericStatistics::Update<int8_t>(SegmentStatistics &stats, int8_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<int8_t>(new_value, nstats.min.value_.tinyint, nstats.max.value_.tinyint);
}

template <>
void NumericStatistics::Update<int16_t>(SegmentStatistics &stats, int16_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<int16_t>(new_value, nstats.min.value_.smallint, nstats.max.value_.smallint);
}

template <>
void NumericStatistics::Update<int32_t>(SegmentStatistics &stats, int32_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<int32_t>(new_value, nstats.min.value_.integer, nstats.max.value_.integer);
}

template <>
void NumericStatistics::Update<int64_t>(SegmentStatistics &stats, int64_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<int64_t>(new_value, nstats.min.value_.bigint, nstats.max.value_.bigint);
}

template <>
void NumericStatistics::Update<uint8_t>(SegmentStatistics &stats, uint8_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<uint8_t>(new_value, nstats.min.value_.utinyint, nstats.max.value_.utinyint);
}

template <>
void NumericStatistics::Update<uint16_t>(SegmentStatistics &stats, uint16_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<uint16_t>(new_value, nstats.min.value_.usmallint, nstats.max.value_.usmallint);
}

template <>
void NumericStatistics::Update<uint32_t>(SegmentStatistics &stats, uint32_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<uint32_t>(new_value, nstats.min.value_.uinteger, nstats.max.value_.uinteger);
}

template <>
void NumericStatistics::Update<uint64_t>(SegmentStatistics &stats, uint64_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<uint64_t>(new_value, nstats.min.value_.ubigint, nstats.max.value_.ubigint);
}

template <>
void NumericStatistics::Update<hugeint_t>(SegmentStatistics &stats, hugeint_t new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<hugeint_t>(new_value, nstats.min.value_.hugeint, nstats.max.value_.hugeint);
}

template <>
void NumericStatistics::Update<float>(SegmentStatistics &stats, float new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<float>(new_value, nstats.min.value_.float_, nstats.max.value_.float_);
}

template <>
void NumericStatistics::Update<double>(SegmentStatistics &stats, double new_value) {
	auto &nstats = (NumericStatistics &)*stats.statistics;
	UpdateValue<double>(new_value, nstats.min.value_.double_, nstats.max.value_.double_);
}

template <>
void NumericStatistics::Update<interval_t>(SegmentStatistics &stats, interval_t new_value) {
}

NumericStatistics::NumericStatistics(LogicalType type_p) : BaseStatistics(move(type_p)) {
	min = Value::MaximumValue(type);
	max = Value::MinimumValue(type);
}

NumericStatistics::NumericStatistics(LogicalType type_p, Value min_p, Value max_p)
    : BaseStatistics(move(type_p)), min(move(min_p)), max(move(max_p)) {
}

void NumericStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const NumericStatistics &)other_p;
	if (other.min < min) {
		min = other.min;
	}
	if (other.max > max) {
		max = other.max;
	}
}

bool NumericStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return constant >= min && constant <= max;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return constant <= max;
	case ExpressionType::COMPARE_GREATERTHAN:
		return constant < max;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return constant >= min;
	case ExpressionType::COMPARE_LESSTHAN:
		return constant > min;
	default:
		throw InternalException("Operation not implemented");
	}
}

unique_ptr<BaseStatistics> NumericStatistics::Copy() {
	auto stats = make_unique<NumericStatistics>(type, min, max);
	if (validity_stats) {
		stats->validity_stats = validity_stats->Copy();
	}
	return move(stats);
}

void NumericStatistics::Serialize(Serializer &serializer) {
	BaseStatistics::Serialize(serializer);
	min.Serialize(serializer);
	max.Serialize(serializer);
}

unique_ptr<BaseStatistics> NumericStatistics::Deserialize(Deserializer &source, LogicalType type) {
	auto min = Value::Deserialize(source);
	auto max = Value::Deserialize(source);
	return make_unique_base<BaseStatistics, NumericStatistics>(move(type), min, max);
}

string NumericStatistics::ToString() {
	return StringUtil::Format("Numeric Statistics<%s> %s[Min: %s, Max: %s]", type.ToString(),
	                          validity_stats ? validity_stats->ToString() : "", min.ToString(), max.ToString());
}

template <class T>
void NumericStatistics::TemplatedVerify(Vector &vector, idx_t count) {
	VectorData vdata;
	vector.Orrify(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto index = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		if (!min.is_null && LessThan::Operation(data[index], min.GetValueUnsafe<T>())) {
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
		if (!max.is_null && GreaterThan::Operation(data[index], max.GetValueUnsafe<T>())) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
	}
}

void NumericStatistics::Verify(Vector &vector, idx_t count) {
	BaseStatistics::Verify(vector, count);

	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		break;
	case PhysicalType::INT8:
		TemplatedVerify<int8_t>(vector, count);
		break;
	case PhysicalType::INT16:
		TemplatedVerify<int16_t>(vector, count);
		break;
	case PhysicalType::INT32:
		TemplatedVerify<int32_t>(vector, count);
		break;
	case PhysicalType::INT64:
		TemplatedVerify<int64_t>(vector, count);
		break;
	case PhysicalType::INT128:
		TemplatedVerify<hugeint_t>(vector, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedVerify<float>(vector, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedVerify<double>(vector, count);
		break;
	default:
		throw InternalException("Unsupported type %s for numeric statistics verify", type.ToString());
	}
}

} // namespace duckdb
