#include "write_mpfile.hpp"
#include "mpfile_filesystem.hpp"

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
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

//! Wrap val in double-quotes, escaping any embedded double-quotes by doubling them.
static string QuoteMPString(const string &val) {
	string result;
	result.reserve(val.size() + 2);
	result += '"';
	for (char c : val) {
		if (c == '"') {
			result += "\"\"";
		} else {
			result += c;
		}
	}
	result += '"';
	return result;
}

struct WriteMPFileData : FunctionData {
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<string> mp_type_codes;             //! one per data column: T, S, N, or I
	vector<pair<string, string>> metadata_kv; //! from KV_METADATA option

	bool IsStringCol(idx_t i) const {
		return mp_type_codes[i] == "T";
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<WriteMPFileData>();
		copy->column_names = column_names;
		copy->column_types = column_types;
		copy->mp_type_codes = mp_type_codes;
		copy->metadata_kv = metadata_kv;
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
		} else {
			throw NotImplementedException("Unrecognised mpfile COPY option: %s", option.first);
		}
	}

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

	string preamble = metadata_rows + vt_row + header_row;
	gstate->handle->Write((void *)preamble.data(), (int64_t)preamble.size());

	return std::move(gstate);
}

static void WriteMPFileSink(ExecutionContext &context, FunctionData &bind_data, GlobalFunctionData &gstate,
                            LocalFunctionData & /*lstate*/, DataChunk &input) {
	auto &data = bind_data.Cast<WriteMPFileData>();
	auto &state = gstate.Cast<WriteMPFileGlobalState>();

	idx_t ncols = data.column_names.size();
	idx_t nrows = input.size();

	string output;
	output.reserve(nrows * (ncols + 1) * 8);

	for (idx_t row = 0; row < nrows; row++) {
		output += '*';
		for (idx_t col = 0; col < ncols; col++) {
			output += ',';
			Value val = input.GetValue(col, row);
			if (val.IsNull()) {
				if (data.IsStringCol(col)) {
					output += "\"\"";
				}
				// numeric NULLs produce an empty field
			} else {
				string str = val.ToString();
				if (data.IsStringCol(col)) {
					output += QuoteMPString(str);
				} else {
					output += str;
				}
			}
		}
		output += "\r\n";
	}

	lock_guard<mutex> lock(state.write_lock);
	state.handle->Write((void *)output.data(), (int64_t)output.size());
}

static unique_ptr<LocalFunctionData> WriteMPFileInitLocal(ExecutionContext & /*context*/,
                                                          FunctionData & /*bind_data*/) {
	return make_uniq<LocalFunctionData>();
}

static void WriteMPFileCombine(ExecutionContext & /*context*/, FunctionData & /*bind_data*/,
                               GlobalFunctionData & /*gstate*/, LocalFunctionData & /*lstate*/) {
	// All writes go directly to the global handle under its mutex; nothing to combine.
}

static void WriteMPFileFinalize(ClientContext & /*context*/, FunctionData & /*bind_data*/,
                                GlobalFunctionData & /*gstate*/) {
	// FileHandle destructor closes the file.
}

static void WriteMPFileCopyOptions(ClientContext & /*context*/, CopyOptionsInput &input) {
	input.options["kv_metadata"] = CopyOption(LogicalType::ANY, CopyOptionMode::WRITE_ONLY);
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
