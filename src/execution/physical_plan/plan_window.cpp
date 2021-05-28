#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

#include <numeric>

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalWindow &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);
#ifdef DEBUG
	for (auto &expr : op.expressions) {
		D_ASSERT(expr->IsWindow());
	}
#endif

	// Slice types
	auto types = op.types;
	const auto output_idx = types.size() - op.expressions.size();
	types.resize(output_idx);

	// Process the window functions by sharing the partition/order definitions
	vector<idx_t> evaluation_order;
	vector<idx_t> remaining(op.expressions.size());
	std::iota(remaining.begin(), remaining.end(), 0);
	while (!remaining.empty()) {
		// Find all functions that share the partitioning of the first remaining expression
		const auto over_idx = remaining[0];
		auto over_expr = reinterpret_cast<BoundWindowExpression *>(op.expressions[over_idx].get());

		vector<idx_t> matching;
		vector<idx_t> unprocessed;
		for (const auto &expr_idx : remaining) {
			D_ASSERT(op.expressions[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.expressions[expr_idx].get());
			if (over_expr->KeysAreCompatible(wexpr)) {
				matching.emplace_back(expr_idx);
			} else {
				unprocessed.emplace_back(expr_idx);
			}
		}
		remaining.swap(unprocessed);

		// Extract the matching expressions
		vector<unique_ptr<Expression>> select_list;
		for (const auto &expr_idx : matching) {
			select_list.emplace_back(move(op.expressions[expr_idx]));
			types.emplace_back(op.types[output_idx + expr_idx]);
		}

		// Chain the new window operator on top of the plan
		auto window = make_unique<PhysicalWindow>(types, move(select_list), op.estimated_cardinality);
		window->children.push_back(move(plan));
		plan = move(window);

		// Remember the projection order if we changed it
		if (!remaining.empty() || !evaluation_order.empty()) {
			evaluation_order.insert(evaluation_order.end(), matching.begin(), matching.end());
		}
	}

	// Put everything back into place if it moved
	if (!evaluation_order.empty()) {
		vector<unique_ptr<Expression>> select_list(op.types.size());
		// The inputs don't move
		for (idx_t i = 0; i < output_idx; ++i) {
			select_list[i] = make_unique<BoundReferenceExpression>(op.types[i], i);
		}
		// The outputs have been rearranged
		for (idx_t i = 0; i < evaluation_order.size(); ++i) {
			const auto expr_idx = evaluation_order[i] + output_idx;
			select_list[expr_idx] = make_unique<BoundReferenceExpression>(op.types[expr_idx], i + output_idx);
		}
		auto proj = make_unique<PhysicalProjection>(op.types, move(select_list), op.estimated_cardinality);
		proj->children.push_back(move(plan));
		plan = move(proj);
	}

	return plan;
}

} // namespace duckdb
