#include "mpfile_filesystem.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include <cstring>

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
	auto local_fs = FileSystem::CreateLocal();
	return local_fs->Glob(path, opener);
}

bool MPFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto local_fs = FileSystem::CreateLocal();
	return local_fs->FileExists(filename, opener);
}

} // namespace duckdb
