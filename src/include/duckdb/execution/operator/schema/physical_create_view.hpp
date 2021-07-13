//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/schema/physical_create_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

namespace duckdb {

//! PhysicalCreateView represents a CREATE VIEW command
class PhysicalCreateView : public PhysicalOperator {
public:
	explicit PhysicalCreateView(unique_ptr<CreateViewInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::CREATE_VIEW, {LogicalType::BIGINT}, estimated_cardinality),
	      info(move(info)) {
	}
    /*PhysicalCreateView(PhysicalCreateView const& pcv) : PhysicalOperator(PhysicalOperatorType::CREATE_VIEW, {LogicalType::BIGINT}, pcv.estimated_cardinality),
                                                        info(pcv.info) {
	}
*/
	unique_ptr<CreateViewInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    /*std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalCreateView>(*this);
    }*/
};

} // namespace duckdb
