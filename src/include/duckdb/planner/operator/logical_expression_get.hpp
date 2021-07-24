//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_expression_get.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalExpressionGet represents a scan operation over a set of to-be-executed expressions
class LogicalExpressionGet : public LogicalOperator {
public:
	LogicalExpressionGet(idx_t table_index, vector<LogicalType> types,
	                     vector<vector<unique_ptr<Expression>>> expressions)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXPRESSION_GET), table_index(table_index), expr_types(types),
	      expressions(move(expressions)) {
	}

	//! The table index in the current bind context
	idx_t table_index;
	//! The types of the expressions
	vector<LogicalType> expr_types;
	//! The set of expressions
	vector<vector<unique_ptr<Expression>>> expressions;

    // FOR DEBUG
    /*LogicalExpressionGet() : LogicalOperator(LogicalOperatorType::LOGICAL_EXPRESSION_GET) {}
    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalExpressionGet>();
    }*/
    // FOR IMPLEMENTATION
    LogicalExpressionGet(LogicalExpressionGet const &leg) : LogicalOperator(leg) {
        table_index = leg.table_index;
        expr_types = leg.expr_types;

        for(const auto& row: leg.expressions) {
            vector<unique_ptr<Expression>> tmp;
            tmp.reserve(row.size());
            for(const auto& exp: row) {
                tmp.push_back(exp->Copy());    // elem: unique_ptr<Expression> Copy()
            }
            expressions.push_back(tmp);
        }

    }

    unique_ptr<LogicalOperator> clone() const override {
        return make_unique<LogicalExpressionGet>(*this);
    }


public:
	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, expr_types.size());
	}

protected:
	void ResolveTypes() override {
		// types are resolved in the constructor
		this->types = expr_types;
	}
};
} // namespace duckdb
