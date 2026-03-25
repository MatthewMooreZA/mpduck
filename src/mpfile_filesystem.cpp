#include "mpfile_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstring>
#include <unordered_map>

namespace duckdb {

MPFileHandle::MPFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, string content_p)
    : FileHandle(fs, path, flags), content(std::move(content_p)), position(0) {
}

//! Read raw file content and return a filtered version containing only the
//! data from header rows ('!') and data rows ('*'), with the first-column
//! indicator field stripped. Schema rows ('&') and any other rows are dropped.
static string FilterMPFileContent(const string &raw) {
	string filtered;
	filtered.reserve(raw.size());

	size_t pos = 0;
	while (pos < raw.size()) {
		size_t line_end = raw.find('\n', pos);
		size_t next_pos = (line_end != string::npos) ? line_end + 1 : raw.size();
		size_t line_len = (line_end != string::npos) ? line_end - pos : raw.size() - pos;

		// Strip Windows carriage return
		if (line_len > 0 && raw[pos + line_len - 1] == '\r') {
			line_len--;
		}

		if (line_len > 0) {
			char indicator = raw[pos];
			if (indicator == '!' || indicator == '*') {
				size_t comma_pos = raw.find(',', pos);
				if (comma_pos != string::npos && comma_pos < pos + line_len) {
					size_t field_start = comma_pos + 1;
					size_t field_len = (pos + line_len) - field_start;
					filtered.append(raw.data() + field_start, field_len);
					filtered += '\n';
				}
			}
			// Drop '&' rows and any unrecognised rows
		}

		pos = next_pos;
	}

	return filtered;
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

MPFileSchema ParseMPFileSchema(const string &raw) {
	MPFileSchema schema;
	bool found_data = false;

	size_t pos = 0;
	while (pos < raw.size()) {
		size_t line_end = raw.find('\n', pos);
		size_t next_pos = (line_end != string::npos) ? line_end + 1 : raw.size();
		size_t line_len = (line_end != string::npos) ? line_end - pos : raw.size() - pos;

		if (line_len > 0 && raw[pos + line_len - 1] == '\r') {
			line_len--;
		}

		if (line_len > 0) {
			char indicator = raw[pos];
			size_t line_end_pos = pos + line_len;
			size_t comma = raw.find(',', pos);
			bool has_comma = (comma != string::npos && comma < line_end_pos);

			if (indicator == '*') {
				found_data = true;
			} else if (indicator == '!' && has_comma) {
				schema.column_names = SplitFields(raw, comma + 1, line_end_pos);
			} else if (indicator == '&') {
				if (found_data) {
					throw IOException(
					    "mpfile schema row ('&') found after data row ('*'); schema must precede all data rows");
				}
				if (has_comma) {
					auto fields = SplitFields(raw, comma + 1, line_end_pos);
					// fields[0] is the type for the indicator column itself — skip it to align with column names
					for (size_t i = 1; i < fields.size(); i++) {
						schema.column_types.push_back(MapMPType(fields[i], i - 1));
					}
					schema.found = true;
				}
			}
		}

		pos = next_pos;
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

bool MPFileSystem::CanHandleFile(const string &path) {
	auto lower = StringUtil::Lower(path);
	return StringUtil::EndsWith(lower, ".rpt") || StringUtil::EndsWith(lower, ".prn");
}

unique_ptr<FileHandle> MPFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                              optional_ptr<FileOpener> opener) {
	auto local_fs = FileSystem::CreateLocal();
	auto raw_handle = local_fs->OpenFile(path, FileOpenFlags::FILE_FLAGS_READ, opener);

	auto file_size = local_fs->GetFileSize(*raw_handle);
	string raw_content(file_size, '\0');
	if (file_size > 0) {
		local_fs->Read(*raw_handle, &raw_content[0], file_size, 0);
	}

	auto filtered = FilterMPFileContent(raw_content);
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
