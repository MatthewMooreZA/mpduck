#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

//! Schema extracted from an mpfile 'VARIABLE_TYPES' row.
//! column_types contains DuckDB type-name strings aligned 1:1 with column_names.
struct MPFileSchema {
	bool found = false;
	vector<string> column_names;
	vector<string> column_types;
};

//! Parse the header ('!') and schema ('VARIABLE_TYPES') rows from an mpfile.
//! Reads only until the first data ('*') row, 'VARIABLE_TYPES' row, or unrecognised line.
MPFileSchema ParseMPFileSchema(const string &path, FileSystem &fs);

//! Merge a collection of schemas into one, widening types where they differ.
//! Numeric widening order: SMALLINT < INTEGER < DOUBLE. Any non-numeric mismatch
//! falls back to VARCHAR. Columns present in only some files are included as-is.
MPFileSchema MergeSchemas(const vector<MPFileSchema> &schemas);

class MPFileHandle : public FileHandle {
public:
	MPFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, string header_p,
	             unique_ptr<FileSystem> owned_fs_p, FileSystem *raw_fs_p, unique_ptr<FileHandle> raw_handle_p,
	             idx_t data_start_p, idx_t data_end_p);
	void Close() override {
	}

	string header;                     //! normalised '!' rows; served from position 0
	unique_ptr<FileSystem> owned_fs;   //! owns raw_fs when there is no DB context
	FileSystem *raw_fs;                //! underlying filesystem for data-block I/O
	unique_ptr<FileHandle> raw_handle; //! handle into the original .rpt/.prn file
	idx_t data_start;                  //! raw-file offset of first '*' row
	idx_t data_end;                    //! raw-file offset one-past the last '*' row
	idx_t position;                    //! current position in the filtered stream
};

class MPFileSystem : public FileSystem {
public:
	string GetName() const override {
		return "MPFileSystem";
	}

	bool CanHandleFile(const string &path) override;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	int64_t GetFileSize(FileHandle &handle) override;

	bool CanSeek() override {
		return true;
	}
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;

	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

	// Give MPFileSystem absolute priority for .rpt/.prn files so it always wins
	// over other sub-systems (e.g. httpfs) regardless of extension load order.
	bool IsManuallySet() override;

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;

	timestamp_t GetLastModifiedTime(FileHandle &handle) override;

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
};

} // namespace duckdb
