//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parser/parsed_data/transaction_info.hpp"

namespace duckdb {

//! PhysicalTransaction represents a transaction operator (e.g. BEGIN or COMMIT)
class PhysicalTransaction : public PhysicalOperator {
public:
	explicit PhysicalTransaction(unique_ptr<TransactionInfo> info, idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::TRANSACTION, {LogicalType::BOOLEAN}, estimated_cardinality),
	      info(move(info)) {
	}
    PhysicalTransaction(PhysicalTransaction const& pt) : PhysicalOperator(PhysicalOperatorType::TRANSACTION, {LogicalType::BOOLEAN}, pt.estimated_cardinality),
                                                         info(pt.info->duplicate()) {
	}

	unique_ptr<TransactionInfo> info;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
    std::unique_ptr<PhysicalOperator> clone() const override {
        return make_unique<PhysicalTransaction>(*this);
    }
};

} // namespace duckdb
