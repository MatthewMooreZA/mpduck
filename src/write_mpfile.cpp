#include "write_mpfile.hpp"
#include "mpfile_filesystem.hpp"

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <mutex>

namespace duckdb {

//! Map a DuckDB column type to the corresponding mpfile VARIABLE_TYPES code.
//! T = text (VARCHAR and anything else), S = smallint, I = integer, N = numeric.
static string DuckTypeToMPCode(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::SMALLINT:
		return "S";
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return "I";
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return "N";
	default:
		return "T";
	}
}

//! Append val wrapped in double-quotes (escaping embedded double-quotes by doubling) directly into out.
static void AppendQuotedMPString(const char *data, size_t len, string &out) {
	out += '"';
	for (size_t i = 0; i < len; ++i) {
		if (data[i] == '"') {
			out += '"';
		}
		out += data[i];
	}
	out += '"';
}

struct WriteMPFileData : FunctionData {
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> mp_type_codes;             //! one per data column: T, S, N, or I
	vector<pair<string, string>> metadata_kv; //! from KV_METADATA option
	bool include_types = true;                //! from INCLUDE_TYPES option (default true)
	idx_t bytes_per_row_estimate = 0;         //! pre-computed buffer size hint per row

	bool IsStringCol(idx_t i) const {
		return mp_type_codes[i] == "T";
	}

	static idx_t EstimateBytesPerRow(const vector<string> &type_codes) {
		// '*' row marker + "\r\n" + ',' per column
		idx_t estimate = 3 + type_codes.size();
		for (const auto &code : type_codes) {
			if (code == "T") {
				estimate += 22; // avg quoted VARCHAR
			} else if (code == "S") {
				estimate += 7; // max int16: sign + 5 digits
			} else if (code == "I") {
				estimate += 12; // max int64: sign + 19 digits (typical much shorter)
			} else {
				estimate += 15; // N: std::to_string double gives ~12 chars + headroom
			}
		}
		return estimate;
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<WriteMPFileData>();
		copy->column_names = column_names;
		copy->column_types = column_types;
		copy->mp_type_codes = mp_type_codes;
		copy->metadata_kv = metadata_kv;
		copy->include_types = include_types;
		copy->bytes_per_row_estimate = bytes_per_row_estimate;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<WriteMPFileData>();
		return column_names == o.column_names && column_types == o.column_types;
	}
};

struct WriteMPFileGlobalState : GlobalFunctionData {
	unique_ptr<FileHandle> handle;
	mutex write_lock;
};

struct WriteMPFileLocalState : LocalFunctionData {
	string buffer;         //! accumulates formatted rows across chunks
	idx_t flush_threshold; //! computed from row width: target ~8 chunks per flush, clamped to [1 MB, 64 MB]

	explicit WriteMPFileLocalState(idx_t threshold) : flush_threshold(threshold) {
	}
};

static unique_ptr<FunctionData> WriteMPFileBind(ClientContext &context, CopyFunctionBindInput &input,
                                                const vector<string> &names, const vector<LogicalType> &sql_types) {
	auto data = make_uniq<WriteMPFileData>();
	data->column_names = names;
	data->column_types = sql_types;
	data->mp_type_codes.reserve(sql_types.size());
	for (const auto &type : sql_types) {
		data->mp_type_codes.push_back(DuckTypeToMPCode(type));
	}

	for (auto &option : input.info.options) {
		auto loption = StringUtil::Lower(option.first);
		if (loption == "kv_metadata") {
			auto &kv_struct = option.second[0];
			if (kv_struct.type().id() != LogicalTypeId::STRUCT) {
				throw BinderException("KV_METADATA must be a struct, e.g. {key: 'value'}");
			}
			auto &values = StructValue::GetChildren(kv_struct);
			for (idx_t i = 0; i < values.size(); i++) {
				string key = StructType::GetChildName(kv_struct.type(), i);
				data->metadata_kv.emplace_back(key, values[i].ToString());
			}
		} else if (loption == "include_types") {
			auto &val = option.second[0];
			if (val.type().id() != LogicalTypeId::BOOLEAN) {
				throw BinderException("INCLUDE_TYPES must be a boolean (true or false)");
			}
			data->include_types = BooleanValue::Get(val);
		} else {
			throw NotImplementedException("Unrecognised mpfile COPY option: %s", option.first);
		}
	}

	data->bytes_per_row_estimate = WriteMPFileData::EstimateBytesPerRow(data->mp_type_codes);
	return std::move(data);
}

static unique_ptr<GlobalFunctionData> WriteMPFileInitGlobal(ClientContext &context, FunctionData &bind_data,
                                                            const string &file_path) {
	auto &data = bind_data.Cast<WriteMPFileData>();
	auto gstate = make_uniq<WriteMPFileGlobalState>();

	auto &fs = FileSystem::GetFileSystem(context);
	gstate->handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);

	// VARIABLE_TYPES row: T1 for the indicator column, then one code per data column.
	string vt_row = "VARIABLE_TYPES,T1";
	for (const auto &code : data.mp_type_codes) {
		vt_row += ',';
		vt_row += code;
	}
	vt_row += "\r\n";

	// Header (!) row: unquoted column names (SplitFields in the reader does not
	// do CSV unquoting, so quoting here would embed the quotes in the name).
	string header_row = "!";
	for (const auto &name : data.column_names) {
		header_row += ',';
		header_row += name;
	}
	header_row += "\r\n";

	string metadata_rows;
	for (const auto &kv : data.metadata_kv) {
		metadata_rows += kv.first;
		metadata_rows += ',';
		metadata_rows += kv.second;
		metadata_rows += "\r\n";
	}

	string preamble = metadata_rows;
	if (data.include_types) {
		preamble += vt_row;
	}
	preamble += header_row;
	gstate->handle->Write((void *)preamble.data(), (int64_t)preamble.size());

	return std::move(gstate);
}

static void WriteMPFileSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                            LocalFunctionData &lstate_ref, DataChunk &input) {
	auto &data = bind_data.Cast<WriteMPFileData>();
	auto &state = gstate.Cast<WriteMPFileGlobalState>();
	auto &lstate = lstate_ref.Cast<WriteMPFileLocalState>();

	idx_t ncols = data.column_names.size();
	idx_t nrows = input.size();

	// Flatten all vectors once up front so FlatVector accessors are valid.
	for (idx_t col = 0; col < ncols; col++) {
		input.data[col].Flatten(nrows);
	}

	auto &output = lstate.buffer; //! accumulates formatted rows; flushed when threshold is reached

	for (idx_t row = 0; row < nrows; row++) {
		output += '*';
		for (idx_t col = 0; col < ncols; col++) {
			output += ',';
			auto &vec = input.data[col];
			if (!FlatVector::Validity(vec).RowIsValid(row)) {
				if (data.IsStringCol(col)) {
					output += "\"\"";
				}
				// numeric NULLs produce an empty field
				continue;
			}
			switch (data.column_types[col].id()) {
			case LogicalTypeId::VARCHAR: {
				auto sv = FlatVector::GetData<string_t>(vec)[row];
				AppendQuotedMPString(sv.GetData(), sv.GetSize(), output);
				break;
			}
			case LogicalTypeId::SMALLINT: {
				char tmp[8];
				char *end = tmp + sizeof(tmp);
				int16_t val = FlatVector::GetData<int16_t>(vec)[row];
				bool neg = val < 0;
				uint16_t uval = neg ? (uint16_t(-(val + 1)) + 1) : uint16_t(val);
				char *start = NumericHelper::FormatUnsigned<uint16_t>(uval, end);
				if (neg) {
					*--start = '-';
				}
				output.append(start, UnsafeNumericCast<size_t>(end - start));
				break;
			}
			case LogicalTypeId::INTEGER: {
				char tmp[12];
				char *end = tmp + sizeof(tmp);
				int32_t val = FlatVector::GetData<int32_t>(vec)[row];
				bool neg = val < 0;
				uint32_t uval = neg ? (uint32_t(-(val + 1)) + 1) : uint32_t(val);
				char *start = NumericHelper::FormatUnsigned<uint32_t>(uval, end);
				if (neg) {
					*--start = '-';
				}
				output.append(start, UnsafeNumericCast<size_t>(end - start));
				break;
			}
			case LogicalTypeId::BIGINT: {
				char tmp[21];
				char *end = tmp + sizeof(tmp);
				int64_t val = FlatVector::GetData<int64_t>(vec)[row];
				bool neg = val < 0;
				uint64_t uval = neg ? (uint64_t(-(val + 1)) + 1) : uint64_t(val);
				char *start = NumericHelper::FormatUnsigned<uint64_t>(uval, end);
				if (neg) {
					*--start = '-';
				}
				output.append(start, UnsafeNumericCast<size_t>(end - start));
				break;
			}
			case LogicalTypeId::FLOAT:
				output += std::to_string(FlatVector::GetData<float>(vec)[row]);
				break;
			case LogicalTypeId::DOUBLE:
				output += std::to_string(FlatVector::GetData<double>(vec)[row]);
				break;
			case LogicalTypeId::DECIMAL: {
				auto &col_type = data.column_types[col];
				uint8_t width = DecimalType::GetWidth(col_type);
				uint8_t scale = DecimalType::GetScale(col_type);
				char tmp[40];
				switch (col_type.InternalType()) {
				case PhysicalType::INT16: {
					auto v = FlatVector::GetData<int16_t>(vec)[row];
					idx_t len = UnsafeNumericCast<idx_t>(DecimalToString::DecimalLength<int16_t>(v, width, scale));
					DecimalToString::FormatDecimal<int16_t>(v, width, scale, tmp, len);
					output.append(tmp, len);
					break;
				}
				case PhysicalType::INT32: {
					auto v = FlatVector::GetData<int32_t>(vec)[row];
					idx_t len = UnsafeNumericCast<idx_t>(DecimalToString::DecimalLength<int32_t>(v, width, scale));
					DecimalToString::FormatDecimal<int32_t>(v, width, scale, tmp, len);
					output.append(tmp, len);
					break;
				}
				case PhysicalType::INT64: {
					auto v = FlatVector::GetData<int64_t>(vec)[row];
					idx_t len = UnsafeNumericCast<idx_t>(DecimalToString::DecimalLength<int64_t>(v, width, scale));
					DecimalToString::FormatDecimal<int64_t>(v, width, scale, tmp, len);
					output.append(tmp, len);
					break;
				}
				default:
					// hugeint_t decimal — fallback to generic path
					output += input.GetValue(col, row).ToString();
				}
				break;
			}
			default:
				// Fallback for any remaining types — correct but not maximally fast.
				output += input.GetValue(col, row).ToString();
				break;
			}
		}
		output += "\r\n";
	}

	if (output.size() >= lstate.flush_threshold) {
		lock_guard<mutex> lock(state.write_lock);
		state.handle->Write((void *)output.data(), (int64_t)output.size());
		output.clear();
	}
}

static unique_ptr<LocalFunctionData> WriteMPFileInitLocal(ExecutionContext & /*context*/, FunctionData &bind_data) {
	auto &data = bind_data.Cast<WriteMPFileData>();
	// Target ~8 chunks per flush so that both narrow and wide files amortise syscall overhead equally.
	// DuckDB's standard vector size is 2048 rows; clamp to [1 MB, 64 MB] to bound memory per thread.
	static constexpr idx_t CHUNK_ROWS = 2048;
	static constexpr idx_t TARGET_CHUNKS = 8;
	static constexpr idx_t MIN_THRESHOLD = 1 * 1024 * 1024;
	static constexpr idx_t MAX_THRESHOLD = 64 * 1024 * 1024;
	idx_t threshold = CHUNK_ROWS * data.bytes_per_row_estimate * TARGET_CHUNKS;
	threshold = MaxValue(threshold, MIN_THRESHOLD);
	threshold = MinValue(threshold, MAX_THRESHOLD);
	return make_uniq<WriteMPFileLocalState>(threshold);
}

static void WriteMPFileCombine(ExecutionContext & /*context*/, FunctionData & /*bind_data*/,
                               GlobalFunctionData &gstate_ref, LocalFunctionData &lstate_ref) {
	auto &state = gstate_ref.Cast<WriteMPFileGlobalState>();
	auto &lstate = lstate_ref.Cast<WriteMPFileLocalState>();
	if (!lstate.buffer.empty()) {
		lock_guard<mutex> lock(state.write_lock);
		state.handle->Write((void *)lstate.buffer.data(), (int64_t)lstate.buffer.size());
		lstate.buffer.clear();
	}
}

static void WriteMPFileFinalize(ClientContext & /*context*/, FunctionData & /*bind_data*/,
                                GlobalFunctionData & /*gstate*/) {
	// FileHandle destructor closes the file.
}

static void WriteMPFileCopyOptions(ClientContext & /*context*/, CopyOptionsInput &input) {
	input.options["kv_metadata"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
	input.options["include_types"] = CopyOption(LogicalType::BOOLEAN, CopyOptionMode::WRITE_ONLY);
}

void RegisterWriteMPFile(ExtensionLoader &loader) {
	CopyFunction copy_func("mpfile");
	copy_func.extension = "rpt";
	copy_func.copy_to_bind = WriteMPFileBind;
	copy_func.copy_to_initialize_local = WriteMPFileInitLocal;
	copy_func.copy_to_initialize_global = WriteMPFileInitGlobal;
	copy_func.copy_to_sink = WriteMPFileSink;
	copy_func.copy_to_combine = WriteMPFileCombine;
	copy_func.copy_to_finalize = WriteMPFileFinalize;
	copy_func.copy_options = WriteMPFileCopyOptions;
	loader.RegisterFunction(copy_func);

	// Register extension aliases so FORMAT is optional when writing to .rpt or .prn files.
	// DuckDB's auto-detection extracts the file extension and looks it up as a format name.
	for (auto &ext : {"rpt", "prn", "fac"}) {
		CopyFunction alias(ext);
		alias.copy_to_bind = WriteMPFileBind;
		alias.copy_to_initialize_local = WriteMPFileInitLocal;
		alias.copy_to_initialize_global = WriteMPFileInitGlobal;
		alias.copy_to_sink = WriteMPFileSink;
		alias.copy_to_combine = WriteMPFileCombine;
		alias.copy_to_finalize = WriteMPFileFinalize;
		alias.copy_options = WriteMPFileCopyOptions;
		loader.RegisterFunction(std::move(alias));
	}
}

} // namespace duckdb
