#include "duckdb/optimizer/statistics_propagator.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

static unique_ptr<BaseStatistics> StatisticsOperationsNumericNumericCast(const BaseStatistics *input_p,
                                                                         const LogicalType &target) {
	auto &input = (NumericStatistics &)*input_p;

	Value min = input.min, max = input.max;
	if (!min.TryCastAs(target) || !max.TryCastAs(target)) {
		// overflow in cast: bailout
		return nullptr;
	}
	auto stats = make_unique<NumericStatistics>(target, move(min), move(max));
	if (input.validity_stats) {
		stats->validity_stats = input.validity_stats->Copy();
	}
	return move(stats);
}

static unique_ptr<BaseStatistics> StatisticsNumericCastSwitch(const BaseStatistics *input, const LogicalType &target) {
	switch (target.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return StatisticsOperationsNumericNumericCast(input, target);
	default:
		return nullptr;
	}
}

unique_ptr<BaseStatistics> StatisticsPropagator::PropagateExpression(BoundCastExpression &cast,
                                                                     unique_ptr<Expression> *expr_ptr) {
	auto child_stats = PropagateExpression(cast.child);
	if (!child_stats) {
		return nullptr;
	}
	switch (cast.child->return_type.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return StatisticsNumericCastSwitch(child_stats.get(), cast.return_type);
	default:
		return nullptr;
	}
}

} // namespace duckdb
