#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"

#include <cstring>

namespace duckdb {

FileBuffer::FileBuffer(FileBufferType type, uint64_t bufsiz) : type(type) {
	const int sector_size = Storage::SECTOR_SIZE;
	// round up to the nearest sector_size, this is only really necessary if the file buffer will be used for Direct IO
	if (bufsiz % sector_size != 0) {
		bufsiz += sector_size - (bufsiz % sector_size);
	}
	D_ASSERT(bufsiz % sector_size == 0);
	D_ASSERT(bufsiz >= sector_size);
	// we add (sector_size - 1) to ensure that we can align the buffer to sector_size
	malloced_buffer = (data_ptr_t)malloc(bufsiz + (sector_size - 1));
	if (!malloced_buffer) {
		throw std::bad_alloc();
	}
	// round to multiple of sector_size
	uint64_t num = (uint64_t)malloced_buffer;
	uint64_t remainder = num % sector_size;
	if (remainder != 0) {
		num = num + sector_size - remainder;
	}
	D_ASSERT(num % sector_size == 0);
	D_ASSERT(num + bufsiz <= ((uint64_t)malloced_buffer + bufsiz + (sector_size - 1)));
	D_ASSERT(num >= (uint64_t)malloced_buffer);
	// construct the FileBuffer object
	internal_buffer = (data_ptr_t)num;
	internal_size = bufsiz;
	buffer = internal_buffer + Storage::BLOCK_HEADER_SIZE;
	size = internal_size - Storage::BLOCK_HEADER_SIZE;
}

FileBuffer::~FileBuffer() {
	free(malloced_buffer);
}

void FileBuffer::Read(FileHandle &handle, uint64_t location) {
	// read the buffer from disk
	handle.Read(internal_buffer, internal_size, location);
	// compute the checksum
	auto stored_checksum = Load<uint64_t>(internal_buffer);
	uint64_t computed_checksum = Checksum(buffer, size);
	// verify the checksum
	if (stored_checksum != computed_checksum) {
		throw IOException("Corrupt database file: computed checksum %llu does not match stored checksum %llu in block",
		                  computed_checksum, stored_checksum);
	}
}

void FileBuffer::Write(FileHandle &handle, uint64_t location) {
	// compute the checksum and write it to the start of the buffer
	uint64_t checksum = Checksum(buffer, size);
	Store<uint64_t>(checksum, internal_buffer);
	// now write the buffer
	handle.Write(internal_buffer, internal_size, location);
}

void FileBuffer::Clear() {
	memset(internal_buffer, 0, internal_size);
}
} // namespace duckdb
