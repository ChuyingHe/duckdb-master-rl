//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_unnest.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
class PhysicalTableInOutFunction : public PhysicalOperator {
public:
	PhysicalTableInOutFunction(vector<LogicalType> types, TableFunction function_p,
	                           unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
	                           idx_t estimated_cardinality);

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

    PhysicalTableInOutFunction(PhysicalTableInOutFunction const& ptiof) : PhysicalOperator(PhysicalOperatorType::INOUT_FUNCTION, ptiof.types, ptiof.estimated_cardinality),
    function(ptiof.function), bind_data(ptiof.bind_data->Copy()), column_ids(ptiof.column_ids) {
    }

public:
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalTableInOutFunction>(*this);
    }

private:
	//! The table function
	TableFunction function;
	//! Bind data of the function
	unique_ptr<FunctionData> bind_data;

	vector<column_t> column_ids;
};

} // namespace duckdb
