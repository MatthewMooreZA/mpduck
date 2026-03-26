#include "mpfile_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"

#include <cstring>
#include <unordered_map>

namespace duckdb {

// Guard that prevents MPFileSystem from intercepting its own internal raw-file opens.
// Set to true while MPFileSystem (or ParseMPFileSchema) is opening the underlying file
// so that CanHandleFile returns false and the VFS routes to the correct sub-system
// (e.g. httpfs for https://) instead of recursing back into MPFileSystem.
static thread_local bool t_mpfs_opening = false;

MPFileHandle::MPFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, string content_p)
    : FileHandle(fs, path, flags), content(std::move(content_p)), position(0) {
}

static vector<string> SplitFields(const string &raw, size_t start, size_t end) {
	vector<string> fields;
	size_t field_start = start;
	for (size_t i = start; i <= end; i++) {
		if (i == end || raw[i] == ',') {
			fields.push_back(raw.substr(field_start, i - field_start));
			field_start = i + 1;
		}
	}
	return fields;
}

static string MapMPType(const string &mp_type, size_t col_idx) {
	if (mp_type == "N") {
		return "DOUBLE";
	}
	if (mp_type == "I") {
		return "INTEGER";
	}
	if (mp_type == "S") {
		return "SMALLINT";
	}
	if (!mp_type.empty() && mp_type[0] == 'T') {
		// T{n} = fixed-length text; n is ignored for reads
		return "VARCHAR";
	}
	throw IOException("Unknown mpfile type specifier '%s' at column %llu", mp_type, (unsigned long long)col_idx);
}

MPFileSchema ParseMPFileSchema(const string &path, FileSystem &fs) {
	MPFileSchema schema;

	t_mpfs_opening = true;
	unique_ptr<FileHandle> handle;
	try {
		handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
		t_mpfs_opening = false;
	} catch (...) {
		t_mpfs_opening = false;
		return schema;
	}

	const idx_t CHUNK_SIZE = 65536;
	string buf(CHUNK_SIZE, '\0');
	string pending;

	while (true) {
		int64_t n = handle->file_system.Read(*handle, &buf[0], CHUNK_SIZE);
		if (n <= 0) {
			break;
		}

		size_t chunk_pos = 0;
		bool done = false;

		while (chunk_pos < static_cast<size_t>(n)) {
			// Find the next newline within the current chunk
			size_t nl = chunk_pos;
			while (nl < static_cast<size_t>(n) && buf[nl] != '\n') {
				nl++;
			}

			if (nl == static_cast<size_t>(n)) {
				// No newline — carry the remainder forward into pending
				pending.append(buf.data() + chunk_pos, n - chunk_pos);
				chunk_pos = n;
				continue;
			}

			// Complete line: pending prefix + chunk[chunk_pos..nl)
			string line = pending + string(buf.data() + chunk_pos, nl - chunk_pos);
			pending.clear();
			chunk_pos = nl + 1;

			if (!line.empty() && line.back() == '\r') {
				line.pop_back();
			}
			if (line.empty()) {
				continue;
			}

			char indicator = line[0];
			size_t comma = line.find(',');
			bool has_comma = (comma != string::npos);

			if (indicator == '*') {
				// First data row — nothing more to learn
				done = true;
			} else if (indicator == '!' && has_comma) {
				schema.column_names = SplitFields(line, comma + 1, line.size());
			} else if (indicator == '&') {
				// Always skip '&' rows
			} else {
				// Check for VARIABLE_TYPES (case-insensitive)
				size_t field_end = has_comma ? comma : line.size();
				string first_field(line, 0, field_end);
				for (auto &c : first_field) {
					c = (char)toupper((unsigned char)c);
				}
				if (first_field == "VARIABLE_TYPES") {
					if (has_comma) {
						auto fields = SplitFields(line, comma + 1, line.size());
						// fields[0] is the type for the indicator column itself — skip it to align with column names
						for (size_t i = 1; i < fields.size(); i++) {
							schema.column_types.push_back(MapMPType(fields[i], i - 1));
						}
						schema.found = true;
					}
				} else {
					// Unrecognised line — pre-data metadata (e.g. "Output_Format", "NUMLINES").
					// Schema parsing always stops at the first '*' row so it never reaches
					// the real footer; skip these lines rather than stopping early.
				}
			}

			if (done) {
				break;
			}
		}

		if (done) {
			break;
		}
	}

	// Handle a final partial line if the file does not end with '\n'
	if (!pending.empty()) {
		if (pending.back() == '\r') {
			pending.pop_back();
		}
		if (!pending.empty()) {
			char indicator = pending[0];
			size_t comma = pending.find(',');
			bool has_comma = (comma != string::npos);
			if (indicator == '!' && has_comma) {
				schema.column_names = SplitFields(pending, comma + 1, pending.size());
			}
			// VARIABLE_TYPES or footer at EOF: leave schema as-is
		}
	}

	return schema;
}

static string MergeTypes(const string &a, const string &b) {
	if (a == b) {
		return a;
	}
	// Numeric widening: SMALLINT < INTEGER < DOUBLE
	static const vector<string> numeric_order = {"SMALLINT", "INTEGER", "DOUBLE"};
	int idx_a = -1, idx_b = -1;
	for (int i = 0; i < static_cast<int>(numeric_order.size()); i++) {
		if (numeric_order[i] == a) {
			idx_a = i;
		}
		if (numeric_order[i] == b) {
			idx_b = i;
		}
	}
	if (idx_a >= 0 && idx_b >= 0) {
		return numeric_order[static_cast<size_t>(std::max(idx_a, idx_b))];
	}
	// Any other mismatch (e.g. numeric vs VARCHAR) — fall back to VARCHAR
	return "VARCHAR";
}

MPFileSchema MergeSchemas(const vector<MPFileSchema> &schemas) {
	// Collect all columns in first-seen order, widening types on conflict.
	vector<string> col_order;
	std::unordered_map<string, string> merged_types;

	for (const auto &s : schemas) {
		if (!s.found) {
			continue;
		}
		size_t n = MinValue(s.column_names.size(), s.column_types.size());
		for (size_t i = 0; i < n; i++) {
			const auto &name = s.column_names[i];
			const auto &type = s.column_types[i];
			auto it = merged_types.find(name);
			if (it == merged_types.end()) {
				col_order.push_back(name);
				merged_types[name] = type;
			} else {
				it->second = MergeTypes(it->second, type);
			}
		}
	}

	if (col_order.empty()) {
		return {};
	}

	MPFileSchema result;
	result.found = true;
	for (const auto &name : col_order) {
		result.column_names.push_back(name);
		result.column_types.push_back(merged_types.at(name));
	}
	return result;
}

bool MPFileSystem::IsManuallySet() {
	// Return true (absolute priority) except during the internal raw-file open
	// where we need other sub-systems (httpfs, local FS) to handle I/O.
	return !t_mpfs_opening;
}

bool MPFileSystem::CanHandleFile(const string &path) {
	if (t_mpfs_opening) {
		return false;
	}
	auto lower = StringUtil::Lower(path);
	return StringUtil::EndsWith(lower, ".rpt") || StringUtil::EndsWith(lower, ".prn");
}

//! Process one complete line for CSV output. Strips the indicator field and appends
//! the remainder to filtered for '!' and '*' rows. Returns false when the line marks
//! the start of a footer and streaming should stop.
//! seen_data tracks whether any '*' row has been emitted. Unrecognised lines are only
//! treated as the footer start once at least one data row has been seen; before that
//! they are pre-data metadata (e.g. "Output_Format", "NUMLINES") and are silently skipped.
static bool ProcessFilterLine(const string &line, string &filtered, bool &seen_data) {
	if (line.empty()) {
		return true;
	}
	char indicator = line[0];
	if (indicator == '*') {
		seen_data = true;
		size_t comma = line.find(',');
		if (comma != string::npos) {
			filtered.append(line.data() + comma + 1, line.size() - comma - 1);
			filtered += '\n';
		}
		return true;
	}
	if (indicator == '!') {
		size_t comma = line.find(',');
		if (comma != string::npos) {
			filtered.append(line.data() + comma + 1, line.size() - comma - 1);
			filtered += '\n';
		}
		return true;
	}
	if (indicator == '&') {
		return true; // always skip
	}
	// Check for VARIABLE_TYPES (case-insensitive) — skip it
	size_t comma = line.find(',');
	size_t field_end = (comma != string::npos) ? comma : line.size();
	string first_field(line, 0, field_end);
	for (auto &c : first_field) {
		c = (char)toupper((unsigned char)c);
	}
	if (first_field == "VARIABLE_TYPES") {
		return true;
	}
	// Unrecognised line: footer only if we have already emitted data rows.
	// Before the first '*' row these are pre-data metadata lines — skip them.
	return !seen_data;
}

//! Stream an mpfile, returning a filtered string containing only the content of
//! '!' and '*' rows with the indicator field stripped. Stops at the first
//! unrecognised line that follows at least one '*' data row (i.e. the footer).
//! Unrecognised lines before any '*' row are pre-data metadata and are skipped.
static string FilterMPFileContent(FileHandle &raw_handle) {
	const idx_t CHUNK_SIZE = 65536;
	string buf(CHUNK_SIZE, '\0');
	string pending;
	string filtered;
	bool seen_data = false;

	while (true) {
		int64_t n = raw_handle.file_system.Read(raw_handle, &buf[0], CHUNK_SIZE);
		if (n <= 0) {
			break;
		}

		size_t chunk_pos = 0;
		bool done = false;

		while (chunk_pos < static_cast<size_t>(n)) {
			size_t nl = chunk_pos;
			while (nl < static_cast<size_t>(n) && buf[nl] != '\n') {
				nl++;
			}

			if (nl == static_cast<size_t>(n)) {
				pending.append(buf.data() + chunk_pos, n - chunk_pos);
				chunk_pos = n;
				continue;
			}

			string line = pending + string(buf.data() + chunk_pos, nl - chunk_pos);
			pending.clear();
			chunk_pos = nl + 1;

			if (!line.empty() && line.back() == '\r') {
				line.pop_back();
			}

			if (!ProcessFilterLine(line, filtered, seen_data)) {
				done = true;
				break;
			}
		}

		if (done) {
			break;
		}
	}

	// Handle a final partial line if the file does not end with '\n'
	if (!pending.empty()) {
		if (pending.back() == '\r') {
			pending.pop_back();
		}
		ProcessFilterLine(pending, filtered, seen_data);
	}

	return filtered;
}

unique_ptr<FileHandle> MPFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                              optional_ptr<FileOpener> opener) {
	auto db = FileOpener::TryGetDatabase(opener);
	unique_ptr<FileSystem> owned_local;
	FileSystem *raw_fs;
	if (db) {
		raw_fs = &db->GetFileSystem();
	} else {
		owned_local = FileSystem::CreateLocal();
		raw_fs = owned_local.get();
	}

	t_mpfs_opening = true;
	unique_ptr<FileHandle> raw_handle;
	try {
		// When raw_fs is the database's OpenerFileSystem the opener is injected
		// automatically, so we must not pass it explicitly.
		if (db) {
			raw_handle = raw_fs->OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
		} else {
			raw_handle = raw_fs->OpenFile(path, FileOpenFlags::FILE_FLAGS_READ, opener);
		}
		t_mpfs_opening = false;
	} catch (...) {
		t_mpfs_opening = false;
		throw;
	}

	auto filtered = FilterMPFileContent(*raw_handle);
	return make_uniq<MPFileHandle>(*this, path, flags, std::move(filtered));
}

int64_t MPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &mph = handle.Cast<MPFileHandle>();
	auto available = static_cast<int64_t>(mph.content.size()) - static_cast<int64_t>(mph.position);
	auto to_read = MinValue<int64_t>(nr_bytes, MaxValue<int64_t>(available, 0));
	if (to_read > 0) {
		memcpy(buffer, mph.content.data() + mph.position, to_read);
		mph.position += to_read;
	}
	return to_read;
}

void MPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &mph = handle.Cast<MPFileHandle>();
	if (location + static_cast<idx_t>(nr_bytes) > mph.content.size()) {
		throw IOException("MPFileSystem: read past end of filtered file content");
	}
	memcpy(buffer, mph.content.data() + location, nr_bytes);
}

int64_t MPFileSystem::GetFileSize(FileHandle &handle) {
	auto &mph = handle.Cast<MPFileHandle>();
	return static_cast<int64_t>(mph.content.size());
}

void MPFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &mph = handle.Cast<MPFileHandle>();
	mph.position = location;
}

idx_t MPFileSystem::SeekPosition(FileHandle &handle) {
	auto &mph = handle.Cast<MPFileHandle>();
	return mph.position;
}

vector<OpenFileInfo> MPFileSystem::Glob(const string &path, FileOpener *opener) {
	// If the path contains no glob characters, treat it as a single file path
	// and return it directly without expanding. This avoids the local filesystem
	// glob failing on paths that don't need expansion.
	if (path.find_first_of("*?[") == string::npos) {
		return {OpenFileInfo(path)};
	}
	auto local_fs = FileSystem::CreateLocal();
	return local_fs->Glob(path, opener);
}

bool MPFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto local_fs = FileSystem::CreateLocal();
	return local_fs->FileExists(filename, opener);
}

} // namespace duckdb
