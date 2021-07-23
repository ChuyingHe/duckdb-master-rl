//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/transaction_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

enum class TransactionType : uint8_t { INVALID, BEGIN_TRANSACTION, COMMIT, ROLLBACK };

struct TransactionInfo : public ParseInfo {
	explicit TransactionInfo(TransactionType type) : type(type) {
	}

	//! The type of transaction statement
	TransactionType type;

    // FOR IMPLEMENTATION
    TransactionInfo(TransactionInfo const& ti) {
        type = ti.type;
    }
    unique_ptr<ParseInfo> clone() const override {
        return make_unique<TransactionInfo>(*this);
    }
};

} // namespace duckdb
