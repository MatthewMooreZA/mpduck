#define DUCKDB_EXTENSION_MAIN

#include "mpduck_extension.hpp"
#include "mpfile_filesystem.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

//! Build a read_csv_auto TableFunctionRef for the given path.
//! When a schema is provided its column types are forwarded via the dtypes parameter.
static unique_ptr<TableFunctionRef> MakeReadCSVRef(const string &path, const MPFileSchema &schema) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(path)));

	auto union_by_name = make_uniq<ConstantExpression>(Value::BOOLEAN(true));
	union_by_name->SetAlias("union_by_name");
	children.push_back(std::move(union_by_name));

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

	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));
	return table_function;
}

//! Redirect read_mpfile('path') to read_csv_auto('path', union_by_name=true[, dtypes=...]).
//! The registered MPFileSystem intercepts each file open and returns filtered content.
static unique_ptr<TableRef> ReadMPFileBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto path = input.inputs[0].GetValue<string>();
	auto &fs = FileSystem::GetFileSystem(context);

	MPFileSchema schema;
	if (FileSystem::HasGlob(path)) {
		// Expand the glob, parse each file's schema, and merge.
		auto files = fs.Glob(path);
		vector<MPFileSchema> schemas;
		schemas.reserve(files.size());
		for (const auto &info : files) {
			schemas.push_back(ParseMPFileSchema(info.path, fs));
		}
		schema = MergeSchemas(schemas);
	} else {
		schema = ParseMPFileSchema(path, fs);
	}

	auto table_function = MakeReadCSVRef(path, schema);
	if (!FileSystem::HasGlob(path)) {
		table_function->alias = fs.ExtractBaseName(path);
	}
	return std::move(table_function);
}

//! Allow direct FROM 'file.rpt' / FROM 'file.prn' syntax by redirecting to read_csv_auto.
static unique_ptr<TableRef> ReadMPFileReplacement(ClientContext &context, ReplacementScanInput &input,
                                                  optional_ptr<ReplacementScanData> data) {
	auto table_name = ReplacementScan::GetFullPath(input);
	if (!ReplacementScan::CanReplace(table_name, {"rpt", "prn"})) {
		return nullptr;
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto schema = ParseMPFileSchema(table_name, fs);
	auto table_function = MakeReadCSVRef(table_name, schema);
	table_function->alias = fs.ExtractBaseName(table_name);
	return std::move(table_function);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register the custom filesystem so .rpt and .prn file opens are intercepted
	// and the mpfile format is transparently filtered before CSV parsing.
	auto &db_instance = loader.GetDatabaseInstance();
	db_instance.GetFileSystem().RegisterSubSystem(make_uniq<MPFileSystem>());

	// Explicit table function: SELECT * FROM read_mpfile('file.rpt')
	TableFunction read_mpfile("read_mpfile", {LogicalType::VARCHAR}, nullptr, nullptr);
	read_mpfile.bind_replace = ReadMPFileBindReplace;
	loader.RegisterFunction(read_mpfile);

	// Implicit scan: SELECT * FROM 'file.rpt'
	DBConfig::GetConfig(db_instance).replacement_scans.emplace_back(ReadMPFileReplacement);
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
