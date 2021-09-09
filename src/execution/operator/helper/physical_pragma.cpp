#include "duckdb/execution/operator/helper/physical_pragma.hpp"

namespace duckdb {

void PhysicalPragma::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
    //printf("PhysicalPragma::GetChunkInternal\n");
	auto &client = context.client;
	FunctionParameters parameters {info.parameters, info.named_parameters};
	function.function(client, parameters);
}

} // namespace duckdb
