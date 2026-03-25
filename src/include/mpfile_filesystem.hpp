#pragma once

#include "duckdb/common/file_system.hpp"

namespace duckdb {

//! Schema extracted from an mpfile '&' row.
//! column_types contains DuckDB type-name strings aligned 1:1 with column_names.
struct MPFileSchema {
	bool found = false;
	vector<string> column_names;
	vector<string> column_types;
};

//! Parse the header ('!') and schema ('&') rows from raw mpfile content.
//! Throws if a '&' row is found after the first '*' (data) row.
MPFileSchema ParseMPFileSchema(const string &raw_content);

//! Merge a collection of schemas into one, widening types where they differ.
//! Numeric widening order: SMALLINT < INTEGER < DOUBLE. Any non-numeric mismatch
//! falls back to VARCHAR. Columns present in only some files are included as-is.
MPFileSchema MergeSchemas(const vector<MPFileSchema> &schemas);

class MPFileHandle : public FileHandle {
public:
	MPFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, string content);
	void Close() override {
	}

	string content;
	idx_t position;
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

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
};

} // namespace duckdb
