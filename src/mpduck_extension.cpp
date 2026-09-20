#define DUCKDB_EXTENSION_MAIN

#include "mpduck_extension.hpp"
#include "mpfile_filesystem.hpp"
#include "write_mpfile.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

//! The read_mpfile named parameters that are forwarded to read_csv_auto, with read_csv's defaults.
struct MPReadOptions {
	//! Bind and sniff every file up front so files may differ in columns and types (also reports a placeholder
	//! cardinality to the planner).
	bool union_by_name = false;
	//! Skip rows that fail to parse instead of raising an error.
	bool ignore_errors = false;
	//! When non-null, forwarded as the filename= parameter (true, or the name of the column to add).
	unique_ptr<Value> filename;
};

//! Build a read_csv_auto ref for the given list of paths, wrapped in SELECT * EXCLUDE ("!").
//! When a schema is provided its column types are forwarded via the dtypes parameter. Each option that differs
//! from read_csv's default is forwarded as the read_csv_auto named parameter of the same name.
static unique_ptr<TableRef> MakeReadCSVRefFromPaths(const vector<string> &paths, const MPFileSchema &schema,
                                                    MPReadOptions options) {
	vector<unique_ptr<ParsedExpression>> children;
	vector<Value> path_values;
	path_values.reserve(paths.size());
	for (const auto &p : paths) {
		path_values.push_back(Value(p));
	}
	children.push_back(make_uniq<ConstantExpression>(Value::LIST(LogicalType::VARCHAR, std::move(path_values))));

	if (options.union_by_name) {
		auto union_by_name_expr = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
		union_by_name_expr->SetAlias("union_by_name");
		children.push_back(std::move(union_by_name_expr));
	}

	if (options.ignore_errors) {
		auto ignore_errors_expr = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
		ignore_errors_expr->SetAlias("ignore_errors");
		children.push_back(std::move(ignore_errors_expr));
	}

	if (schema.found && !schema.column_names.empty() && !schema.column_types.empty()) {
		child_list_t<Value> dtypes_fields;
		size_t n = MinValue(schema.column_names.size(), schema.column_types.size());
		for (size_t i = 0; i < n; i++) {
			dtypes_fields.push_back({schema.column_names[i], Value(schema.column_types[i])});
		}
		auto dtypes_expr = make_uniq<ConstantExpression>(Value::STRUCT(std::move(dtypes_fields)));
		dtypes_expr->SetAlias("dtypes");
		children.push_back(std::move(dtypes_expr));
	}

	if (options.filename) {
		auto filename_expr = make_uniq<ConstantExpression>(*options.filename);
		filename_expr->SetAlias("filename");
		children.push_back(std::move(filename_expr));
	}

	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));

	// Wrap in SELECT * EXCLUDE ("!") to hide the indicator column from callers.
	auto select_node = make_uniq<SelectNode>();
	auto star = make_uniq<StarExpression>();
	star->exclude_list.insert(QualifiedColumnName("!"));
	select_node->select_list.push_back(std::move(star));
	select_node->from_table = std::move(table_function);

	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);

	return make_uniq<SubqueryRef>(std::move(select_stmt));
}

//! Build a read_csv_auto ref for a single path (wraps MakeReadCSVRefFromPaths). One file is trivially
//! consistent with itself, so union_by_name is never needed.
static unique_ptr<TableRef> MakeReadCSVRef(const string &path, const MPFileSchema &schema,
                                           MPReadOptions options = MPReadOptions()) {
	options.union_by_name = false;
	return MakeReadCSVRefFromPaths({path}, schema, std::move(options));
}

//! Read a BOOLEAN named parameter. Absent or NULL means false, like read_csv.
static bool ExtractBool(TableFunctionBindInput &input, const string &name) {
	auto it = input.named_parameters.find(name);
	return it != input.named_parameters.end() && !it->second.IsNull() && it->second.GetValue<bool>();
}

//! Collect the named parameters that are forwarded to read_csv_auto.
static MPReadOptions ExtractReadOptions(TableFunctionBindInput &input) {
	MPReadOptions options;
	options.union_by_name = ExtractBool(input, "union_by_name");
	options.ignore_errors = ExtractBool(input, "ignore_errors");
	auto it = input.named_parameters.find("filename");
	if (it != input.named_parameters.end()) {
		options.filename = make_uniq<Value>(it->second);
	}
	return options;
}

//! Concrete file paths for entries that are each either a literal path or a glob, in order.
static vector<string> ExpandEntries(FileSystem &fs, const vector<string> &entries) {
	vector<string> paths;
	for (const auto &entry : entries) {
		if (FileSystem::HasGlob(entry)) {
			for (const auto &info : fs.Glob(entry)) {
				paths.push_back(info.path);
			}
		} else {
			paths.push_back(entry);
		}
	}
	return paths;
}

//! Shared by the glob and list forms of read_mpfile. Files are never combined by name unless asked to.
//!
//! union_by_name = false (default): the entries are only expanded. The schema for the dtypes parameter comes from
//! the first matched file that has data; every other file is left for read_csv_auto to open lazily, so bind
//! cost does not grow with the number of files (which matters most on remote storage). All files must have the
//! same columns, as with read_csv.
//!
//! union_by_name = true: every file's header and VARIABLE_TYPES rows are read up front, files with no data are
//! dropped, the schemas are merged (numeric types widen, other mismatches become VARCHAR) and read_csv_auto is
//! asked to union the files by column name. This costs one extra open and read per file.
static unique_ptr<TableRef> ReadMPFileMultiRef(FileSystem &fs, const vector<string> &entries, MPReadOptions options,
                                               const string &no_match_message) {
	auto paths = ExpandEntries(fs, entries);

	if (!options.union_by_name) {
		MPFileSchema schema;
		for (const auto &path : paths) {
			auto s = ParseMPFileSchema(path, fs);
			if (s.column_names.size() >= 2) { // "!" indicator + at least one column
				schema = std::move(s);
				break;
			}
		}
		if (schema.column_names.empty()) {
			throw InvalidInputException("%s", no_match_message);
		}
		return MakeReadCSVRefFromPaths(paths, schema, std::move(options));
	}

	vector<string> data_paths;
	vector<MPFileSchema> schemas;
	for (const auto &path : paths) {
		auto s = ParseMPFileSchema(path, fs);
		if (s.column_names.size() >= 2) {
			data_paths.push_back(path);
			schemas.push_back(std::move(s));
		}
	}
	if (data_paths.empty()) {
		throw InvalidInputException("%s", no_match_message);
	}
	auto schema = MergeSchemas(schemas);
	return MakeReadCSVRefFromPaths(data_paths, schema, std::move(options));
}

//! Redirect read_mpfile('path') to read_csv_auto(['path'][, dtypes=...]). A glob is handled by
//! ReadMPFileMultiRef; a literal path is a single file and needs no union.
//! The registered MPFileSystem intercepts each file open and returns filtered content.
static unique_ptr<TableRef> ReadMPFileBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto path = input.inputs[0].GetValue<string>();
	auto &fs = FileSystem::GetFileSystem(context);
	auto options = ExtractReadOptions(input);

	if (FileSystem::HasGlob(path)) {
		return ReadMPFileMultiRef(fs, {path}, std::move(options),
		                          StringUtil::Format("read_mpfile: no files with data matched '%s'", path));
	}

	auto schema = ParseMPFileSchema(path, fs);
	auto table_function = MakeReadCSVRef(path, schema, std::move(options));
	table_function->alias = fs.ExtractBaseName(path);
	return std::move(table_function);
}

//! Allow direct FROM 'file.rpt' / FROM 'file.prn' syntax by redirecting to read_csv_auto.
static unique_ptr<TableRef> ReadMPFileReplacement(ClientContext &context, ReplacementScanInput &input,
                                                  optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!ReplacementScan::CanReplace(table_name, {"rpt", "prn", "fac"})) {
		return nullptr;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	if (FileSystem::HasGlob(table_name)) {
		// A glob pattern is not an openable path: expand it like read_mpfile does (union_by_name = false).
		return ReadMPFileMultiRef(fs, {table_name}, MPReadOptions(),
		                          StringUtil::Format("read_mpfile: no files with data matched '%s'", table_name));
	}
	auto schema = ParseMPFileSchema(table_name, fs);
	auto table_function = MakeReadCSVRef(table_name, schema);
	table_function->alias = fs.ExtractBaseName(table_name);
	return std::move(table_function);
}

//! Redirect read_mpfile(['path1', 'path2', ...]) — each entry may be a literal path or glob.
//! See ReadMPFileMultiRef for how the files are combined. No alias — list input spans multiple files.
static unique_ptr<TableRef> ReadMPFileListBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto &fs = FileSystem::GetFileSystem(context);
	vector<string> entries;
	for (const auto &entry : ListValue::GetChildren(input.inputs[0])) {
		entries.push_back(entry.GetValue<string>());
	}
	return ReadMPFileMultiRef(fs, entries, ExtractReadOptions(input),
	                          "read_mpfile: no files matched the provided paths");
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register the custom filesystem so .rpt and .prn file opens are intercepted
	// and the mpfile format is transparently filtered before CSV parsing.
	auto &db_instance = loader.GetDatabaseInstance();
	db_instance.GetFileSystem().RegisterSubSystem(make_uniq<MPFileSystem>());

	// Explicit table function: SELECT * FROM read_mpfile('file.rpt')
	TableFunction read_mpfile("read_mpfile", {LogicalType::VARCHAR}, nullptr, nullptr);
	read_mpfile.bind_replace = ReadMPFileBindReplace;
	read_mpfile.named_parameters["filename"] = LogicalType::ANY;
	read_mpfile.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
	read_mpfile.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(read_mpfile);

	// List overload: SELECT * FROM read_mpfile(['file1.rpt', 'glob_*.rpt', ...])
	TableFunction read_mpfile_list("read_mpfile", {LogicalType::LIST(LogicalType::VARCHAR)}, nullptr, nullptr);
	read_mpfile_list.bind_replace = ReadMPFileListBindReplace;
	read_mpfile_list.named_parameters["filename"] = LogicalType::ANY;
	read_mpfile_list.named_parameters["union_by_name"] = LogicalType::BOOLEAN;
	read_mpfile_list.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(read_mpfile_list);

	// Implicit scan: SELECT * FROM 'file.rpt'
	DBConfig::GetConfig(db_instance).replacement_scans.emplace_back(ReadMPFileReplacement);

	// COPY TO with FORMAT mpfile
	RegisterWriteMPFile(loader);
}

void MpduckExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string MpduckExtension::Name() {
	return "mpduck";
}

std::string MpduckExtension::Version() const {
#ifdef EXT_VERSION_MPDUCK
	return EXT_VERSION_MPDUCK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(mpduck, loader) {
	duckdb::LoadInternal(loader);
}
}
