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

struct ScopedMPFSBypass {
	bool prev;
	ScopedMPFSBypass() : prev(t_mpfs_opening) {
		t_mpfs_opening = true;
	}
	~ScopedMPFSBypass() {
		t_mpfs_opening = prev;
	}
};

MPFileHandle::MPFileHandle(FileSystem &fs, const string &path, FileOpenFlags flags, string header_p,
                           unique_ptr<FileSystem> owned_fs_p, FileSystem *raw_fs_p, unique_ptr<FileHandle> raw_handle_p,
                           idx_t data_start_p, idx_t data_end_p)
    : FileHandle(fs, path, flags), header(std::move(header_p)), owned_fs(std::move(owned_fs_p)), raw_fs(raw_fs_p),
      raw_handle(std::move(raw_handle_p)), data_start(data_start_p), data_end(data_end_p), position(0) {
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

static string GetFirstFieldUpper(const string &line) {
	size_t end = line.find(',');
	if (end == string::npos) {
		end = line.size();
	}
	string field(line, 0, end);
	for (auto &c : field) {
		c = (char)toupper((unsigned char)c);
	}
	return field;
}

//! Calls callback(line) for each complete, CR-stripped line in handle.
//! Returns false from the callback to stop iteration early.
template <typename F>
static void ForEachLine(FileHandle &handle, F &&callback) {
	const idx_t CHUNK_SIZE = 65536;
	string buf(CHUNK_SIZE, '\0');
	string pending;

	while (true) {
		int64_t n = handle.file_system.Read(handle, &buf[0], CHUNK_SIZE);
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

			if (!callback(line)) {
				done = true;
				break;
			}
		}

		if (done) {
			break;
		}
	}

	// Flush a final partial line if the file does not end with '\n'
	if (!pending.empty()) {
		if (pending.back() == '\r') {
			pending.pop_back();
		}
		if (!pending.empty()) {
			callback(pending);
		}
	}
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

	unique_ptr<FileHandle> handle;
	try {
		ScopedMPFSBypass bypass;
		handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
	} catch (...) {
		return schema;
	}

	ForEachLine(*handle, [&](const string &line) -> bool {
		if (line.empty()) {
			return true;
		}

		char indicator = line[0];
		size_t comma = line.find(',');
		bool has_comma = (comma != string::npos);

		if (indicator == '*') {
			// First data row — nothing more to learn
			return false;
		} else if (indicator == '!' && has_comma) {
			schema.column_names.clear();
			schema.column_names.push_back("!");
			auto names = SplitFields(line, comma + 1, line.size());
			schema.column_names.insert(schema.column_names.end(), names.begin(), names.end());
		} else if (indicator == '&') {
			// Always skip '&' rows
		} else if (GetFirstFieldUpper(line) == "VARIABLE_TYPES") {
			if (has_comma) {
				auto fields = SplitFields(line, comma + 1, line.size());
				for (size_t i = 0; i < fields.size(); i++) {
					schema.column_types.push_back(MapMPType(fields[i], i));
				}
				schema.found = true;
			}
		}
		// Unrecognised lines — pre-data metadata (e.g. "Output_Format", "NUMLINES").
		// Schema parsing always stops at the first '*' row so it never reaches
		// the real footer; skip these lines rather than stopping early.

		return true;
	});

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
	return StringUtil::EndsWith(lower, ".rpt") || StringUtil::EndsWith(lower, ".prn") ||
	       StringUtil::EndsWith(lower, ".fac");
}

//! Returns the byte offset of the first character of the line that contains pos.
static size_t LineStart(const char *data, size_t pos) {
	while (pos > 0 && data[pos - 1] != '\n') {
		pos--;
	}
	return pos;
}

//! Binary-searches [data_start, n] from the end to find the first byte after the last
//! '*' row (i.e. the start of the footer, or n if there is no footer).
//! The footer is assumed to be very small so convergence is fast.
//! The indicator column is never inside quotes, so plain '\n' scanning suffices here.
static size_t FindDataBlockEnd(const char *data, size_t n, size_t data_start) {
	if (data_start >= n) {
		return n;
	}

	const size_t LINEAR_THRESHOLD = 256;
	size_t lo = data_start;
	size_t hi = n;

	while (hi - lo > LINEAR_THRESHOLD) {
		size_t mid = lo + (hi - lo) / 2;
		size_t ls = LineStart(data, mid);
		if (ls < data_start) {
			ls = data_start;
		}
		if (data[ls] == '*') {
			lo = mid;
		} else {
			hi = ls;
		}
	}

	// Linear scan to pinpoint the exact boundary.
	size_t pos = LineStart(data, lo);
	if (pos < data_start) {
		pos = data_start;
	}
	while (pos < hi) {
		if (data[pos] != '*') {
			return pos;
		}
		const char *nl = static_cast<const char *>(memchr(data + pos, '\n', hi - pos));
		if (!nl) {
			break;
		}
		pos = static_cast<size_t>(nl - data) + 1;
	}
	return hi;
}

struct PreDataScan {
	string header;    //! normalised '!' rows ready for CSV consumption
	idx_t data_start; //! absolute file offset of the first '*' row
	bool uses_crlf;   //! true if the source file uses \r\n line endings
};

//! Reads the pre-data section of raw_handle sequentially, building the normalised
//! CSV header from '!' rows and returning the file offset of the first '*' row.
//! After this call the handle position is somewhere past data_start due to chunk
//! over-read; callers must use random-access reads for all subsequent I/O.
static PreDataScan ScanPreDataSection(FileHandle &raw_handle, FileSystem &raw_fs) {
	const idx_t CHUNK_SIZE = 65536;
	vector<char> buf(CHUNK_SIZE);

	PreDataScan result;
	result.data_start = 0;
	result.uses_crlf = true; // well-formed mpfiles use CRLF; overridden for pure-LF files

	string pending;       // incomplete line spanning a chunk boundary
	idx_t file_pos = 0;   // absolute offset of the first byte of the current chunk
	idx_t line_start = 0; // absolute offset of the first byte of the current pending line

	while (true) {
		int64_t n = raw_fs.Read(raw_handle, buf.data(), CHUNK_SIZE);
		if (n <= 0) {
			// Flush a final partial line (file does not end with '\n').
			if (!pending.empty()) {
				if (pending.back() == '\r') {
					pending.pop_back();
				}
				if (!pending.empty() && pending[0] == '*') {
					result.data_start = line_start;
					return result;
				}
			}
			result.data_start = file_pos; // no data block found
			return result;
		}

		idx_t chunk_pos = 0;
		while (chunk_pos < static_cast<idx_t>(n)) {
			// Track where the current pending line starts in the file.
			if (pending.empty()) {
				line_start = file_pos + chunk_pos;
			}

			// Find the next '\n' within this chunk.
			const char *nl_p = static_cast<const char *>(memchr(buf.data() + chunk_pos, '\n', n - chunk_pos));
			if (!nl_p) {
				// No newline — buffer the remainder and fetch the next chunk.
				pending.append(buf.data() + chunk_pos, n - chunk_pos);
				break;
			}

			idx_t nl_idx = static_cast<idx_t>(nl_p - buf.data());

			// Assemble the complete line (may span two chunks via pending).
			const char *line_data;
			idx_t line_len;
			string assembled;
			if (pending.empty()) {
				line_data = buf.data() + chunk_pos;
				line_len = nl_idx - chunk_pos;
			} else {
				assembled = pending + string(buf.data() + chunk_pos, nl_idx - chunk_pos);
				line_data = assembled.data();
				line_len = assembled.size();
				pending.clear();
			}
			// Strip a trailing \r if present; a line without \r means the file uses
			// pure LF endings rather than the standard Windows CRLF.
			if (line_len > 0 && line_data[line_len - 1] == '\r') {
				line_len--;
			} else {
				result.uses_crlf = false;
			}

			// Process the line.
			if (line_len > 0) {
				char indicator = line_data[0];
				if (indicator == '*') {
					result.data_start = line_start;
					return result;
				}
				if (indicator == '!') {
					const char *comma = static_cast<const char *>(memchr(line_data, ',', line_len));
					if (comma) {
						idx_t comma_idx = static_cast<idx_t>(comma - line_data);
						result.header += '!';
						result.header.append(line_data + comma_idx, line_len - comma_idx);
						// Use the same line ending as the data rows so the CSV sniffer
						// sees a consistent stream (all LF or all CRLF).
						result.header += (result.uses_crlf ? "\r\n" : "\n");
					}
				}
			}

			chunk_pos = nl_idx + 1;
		}

		file_pos += n;
	}
}

//! Returns true if buf[search_from .. n) contains at least one line whose first character is '*'.
static bool ChunkContainsStarRow(const char *buf, size_t n, size_t search_from) {
	size_t pos = search_from;
	while (pos < n) {
		if (buf[pos] == '*') {
			return true;
		}
		const char *nl = static_cast<const char *>(memchr(buf + pos, '\n', n - pos));
		if (!nl) {
			break;
		}
		pos = static_cast<size_t>(nl - buf) + 1;
	}
	return false;
}

//! Returns the file offset one past the last '*' row (= start of footer, or file_size).
//! Reads the tail in exponentially growing chunks starting at INITIAL_CHUNK to minimise
//! I/O when the footer is small (the common case).
static idx_t FindDataEnd(FileHandle &raw_handle, FileSystem &raw_fs, idx_t file_size, idx_t data_start) {
	if (data_start >= file_size) {
		return data_start;
	}

	const idx_t INITIAL_CHUNK = 4096;
	const idx_t MAX_CHUNK = 1048576;

	idx_t chunk_size = INITIAL_CHUNK;

	while (true) {
		idx_t chunk_offset = (file_size > chunk_size) ? (file_size - chunk_size) : 0;
		if (chunk_offset < data_start) {
			chunk_offset = data_start;
		}
		idx_t chunk_len = file_size - chunk_offset;

		// Allocate without zero-initialising: every byte is overwritten by the Read call below.
		auto buf = unique_ptr<char[]>(new char[chunk_len]);
		raw_fs.Read(raw_handle, buf.get(), static_cast<int64_t>(chunk_len), chunk_offset);

		// chunk_offset >= data_start is invariant (enforced by the clamp above), so
		// data_start always falls at or before the start of the buffer.  Search from 0.
		// (Computing data_start - chunk_offset when chunk_offset > data_start would
		// silently underflow the unsigned arithmetic and skip all '*' detection.)
		size_t rel_data_start = 0;

		if (ChunkContainsStarRow(buf.get(), static_cast<size_t>(chunk_len), rel_data_start)) {
			size_t rel_end = FindDataBlockEnd(buf.get(), static_cast<size_t>(chunk_len), rel_data_start);
			return chunk_offset + static_cast<idx_t>(rel_end);
		}

		// Entire window is footer — if we have reached data_start there are no '*' rows at all.
		if (chunk_offset == data_start) {
			return data_start;
		}

		// Double chunk size, capped at the full data section so chunk_offset eventually
		// reaches data_start and the loop is guaranteed to terminate regardless of file size.
		idx_t max_chunk = file_size - data_start;
		chunk_size = (chunk_size <= max_chunk / 2) ? (chunk_size * 2) : max_chunk;
	}
}

//! Copies len bytes of the filtered stream starting at from into dst.
//! Positions in [0, header.size()) come from the in-memory header buffer;
//! positions in [header.size(), total_size) are piped directly from the raw file.
static void DoRead(MPFileHandle &mph, char *dst, idx_t len, idx_t from) {
	idx_t header_size = mph.header.size();

	// Part 1: serve from the in-memory header.
	if (from < header_size && len > 0) {
		idx_t n = MinValue(header_size - from, len);
		memcpy(dst, mph.header.data() + from, n);
		dst += n;
		len -= n;
		from += n;
	}

	// Part 2: pipe directly from the raw file (no copy into an intermediate buffer).
	if (len > 0) {
		idx_t raw_offset = mph.data_start + (from - header_size);
		mph.raw_fs->Read(*mph.raw_handle, dst, static_cast<int64_t>(len), raw_offset);
	}
}

unique_ptr<FileHandle> MPFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                              optional_ptr<FileOpener> opener) {
	auto db = FileOpener::TryGetDatabase(opener);
	unique_ptr<FileSystem> owned_fs;
	FileSystem *raw_fs;
	if (db) {
		// Use the raw VirtualFileSystem directly rather than db->GetFileSystem()
		// (which returns DatabaseFileSystem, an OpenerFileSystem that injects
		// DatabaseFileOpener and rejects explicit openers via VerifyNoOpener).
		// We need to pass the original opener so that sub-systems like httpfs
		// receive the ClientContextFileOpener with its S3 credentials.
		raw_fs = DBConfig::GetConfig(*db).file_system.get();
	} else {
		owned_fs = FileSystem::CreateLocal();
		raw_fs = owned_fs.get();
	}

	// Delegate write-mode opens to the underlying FS without any mpfile processing
	// (there is nothing to filter when creating or overwriting a file).
	if (flags.OpenForWriting()) {
		ScopedMPFSBypass bypass;
		return raw_fs->OpenFile(path, flags, opener);
	}

	unique_ptr<FileHandle> raw_handle;
	{
		ScopedMPFSBypass bypass;
		raw_handle = raw_fs->OpenFile(path, FileOpenFlags::FILE_FLAGS_READ, opener);
	}

	idx_t file_size = static_cast<idx_t>(raw_fs->GetFileSize(*raw_handle));

	// Sequential scan of the tiny pre-data section to build the normalised header.
	auto scan = ScanPreDataSection(*raw_handle, *raw_fs);

	// Single random-access read of the tail to locate the end of the data block.
	idx_t data_end = FindDataEnd(*raw_handle, *raw_fs, file_size, scan.data_start);

	return make_uniq<MPFileHandle>(*this, path, flags, std::move(scan.header), std::move(owned_fs), raw_fs,
	                               std::move(raw_handle), scan.data_start, data_end);
}

int64_t MPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &mph = handle.Cast<MPFileHandle>();
	idx_t total_size = mph.header.size() + (mph.data_end - mph.data_start);
	auto available = static_cast<int64_t>(total_size) - static_cast<int64_t>(mph.position);
	auto to_read = MinValue<int64_t>(nr_bytes, MaxValue<int64_t>(available, 0));
	if (to_read > 0) {
		DoRead(mph, static_cast<char *>(buffer), static_cast<idx_t>(to_read), mph.position);
		mph.position += static_cast<idx_t>(to_read);
	}
	return to_read;
}

void MPFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &mph = handle.Cast<MPFileHandle>();
	idx_t total_size = mph.header.size() + (mph.data_end - mph.data_start);
	if (location + static_cast<idx_t>(nr_bytes) > total_size) {
		throw IOException("MPFileSystem: read past end of filtered file content");
	}
	DoRead(mph, static_cast<char *>(buffer), static_cast<idx_t>(nr_bytes), location);
}

int64_t MPFileSystem::GetFileSize(FileHandle &handle) {
	auto &mph = handle.Cast<MPFileHandle>();
	return static_cast<int64_t>(mph.header.size() + (mph.data_end - mph.data_start));
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
	auto db = FileOpener::TryGetDatabase(opener);
	if (db) {
		// Delegate to the underlying VirtualFileSystem with bypass active so
		// MPFileSystem does not intercept its own Glob call.
		ScopedMPFSBypass bypass;
		return DBConfig::GetConfig(*db).file_system->Glob(path, opener);
	}
	auto local_fs = FileSystem::CreateLocal();
	return local_fs->Glob(path, opener);
}

void MPFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	auto db = FileOpener::TryGetDatabase(opener);
	if (db) {
		ScopedMPFSBypass bypass;
		DBConfig::GetConfig(*db).file_system->MoveFile(source, target, opener);
		return;
	}
	auto local_fs = FileSystem::CreateLocal();
	local_fs->MoveFile(source, target, opener);
}

bool MPFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto db = FileOpener::TryGetDatabase(opener);
	if (db) {
		ScopedMPFSBypass bypass;
		return DBConfig::GetConfig(*db).file_system->FileExists(filename, opener);
	}
	auto local_fs = FileSystem::CreateLocal();
	return local_fs->FileExists(filename, opener);
}

} // namespace duckdb
