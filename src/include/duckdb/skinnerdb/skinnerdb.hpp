//
// Created by Chuying He on 26/06/2021.
//

#ifndef DUCKDB_SKINNERDB_HPP
#define DUCKDB_SKINNERDB_HPP

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"

namespace duckdb {
    class SkinnerDB {
    public:
        SkinnerDB(QueryProfiler &profiler, ClientContext& context);

        QueryProfiler profiler;
        ClientContext& context;

        void runStatement(shared_ptr<PreparedStatementData> plan);
        unique_ptr<QueryResult> Execute(ClientContextLock &lock, const string &query, unique_ptr<SQLStatement> statement, bool allow_stream_result);


    private:
        bool finished = false;  //indicator: whether the whole query has been executed or not
        int state = 0;             //恢复执行状态?

        shared_ptr<PreparedStatementData> CreatePreparedStatement(ClientContextLock &lock, const string &query,
                                                                                 unique_ptr<SQLStatement> statement);
    };

}

#endif // DUCKDB_SKINNERDB_HPP
