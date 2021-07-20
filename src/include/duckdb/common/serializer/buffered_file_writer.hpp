//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_file_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

#define FILE_BUFFER_SIZE 4096

class BufferedFileWriter : public Serializer {
public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	BufferedFileWriter(FileSystem &fs, string path,
	                   uint8_t open_flags = FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);

	FileSystem &fs;
	unique_ptr<data_t[]> data;
	idx_t offset;
	idx_t total_written;
	unique_ptr<FileHandle> handle;

    BufferedFileWriter(BufferedFileWriter const& bfw): fs(bfw.fs) {
        //FIXME:unique_ptr<data_t[]> data;
        offset = bfw.offset;
        total_written = bfw.total_written;
        handle = bfw.handle->clone();
    }

    unique_ptr<BufferedFileWriter> clone() const {
        return make_unique<BufferedFileWriter>(*this);
    }

public:
	void WriteData(const_data_ptr_t buffer, uint64_t write_size) override;
	//! Flush the buffer to disk and sync the file to ensure writing is completed
	void Sync();
	//! Flush the buffer to the file (without sync)
	void Flush();
	//! Returns the current size of the file
	int64_t GetFileSize();
	//! Truncate the size to a previous size (given that size <= GetFileSize())
	void Truncate(int64_t size);

	idx_t GetTotalWritten();
};

} // namespace duckdb
