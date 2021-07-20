//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/segment_base.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

class SegmentBase {
public:
	SegmentBase(idx_t start, idx_t count) : start(start), count(count) {
	}
	virtual ~SegmentBase() {
		// destroy the chain of segments iteratively (rather than recursively)
		while (next && next->next) {
			next = move(next->next);
		}
	}

    SegmentBase(SegmentBase const& sb): start(sb.start) {
	    count = sb.count.load();
        next = sb.next->clone();
	}

    unique_ptr<SegmentBase> clone() {
        return make_unique<SegmentBase>(*this);
	}

	//! The start row id of this chunk
	const idx_t start;
	//! The amount of entries in this storage chunk
	atomic<idx_t> count;
	//! The next segment after this one
	unique_ptr<SegmentBase> next;
};

} // namespace duckdb
