#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

class PhysicalUnionOperatorState : public PhysicalOperatorState {
public:
	explicit PhysicalUnionOperatorState(PhysicalOperator &op) : PhysicalOperatorState(op, nullptr), top_done(false) {
	}
	unique_ptr<PhysicalOperatorState> top_state;
	unique_ptr<PhysicalOperatorState> bottom_state;
	bool top_done = false;
};

PhysicalUnion::PhysicalUnion(vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                             unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNION, move(types), estimated_cardinality) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

// first exhaust top, then exhaust bottom. state to remember which.
void PhysicalUnion::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
    printf("PhysicalUnion::GetChunkInternal\n");
	auto state = reinterpret_cast<PhysicalUnionOperatorState *>(state_p);
	if (!state->top_done) {
		children[0]->GetChunk(context, chunk, state->top_state.get());
		if (chunk.size() == 0) {
			state->top_done = true;
		}
	}
	if (state->top_done) {
		children[1]->GetChunk(context, chunk, state->bottom_state.get());
	}
	if (chunk.size() == 0) {
		state->finished = true;
	}
}

unique_ptr<PhysicalOperatorState> PhysicalUnion::GetOperatorState() {
	auto state = make_unique<PhysicalUnionOperatorState>(*this);
	state->top_state = children[0]->GetOperatorState();
	state->bottom_state = children[1]->GetOperatorState();
	return (move(state));
}

void PhysicalUnion::FinalizeOperatorState(PhysicalOperatorState &state_p, ExecutionContext &context) {
	auto &state = reinterpret_cast<PhysicalUnionOperatorState &>(state_p);
	if (!children.empty() && state.top_state) {
		children[0]->FinalizeOperatorState(*state.top_state, context);
	}
	if (!children.empty() && state.bottom_state) {
		children[1]->FinalizeOperatorState(*state.bottom_state, context);
	}
}

} // namespace duckdb
