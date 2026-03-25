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

//! Redirect read_mpfile('path') to read_csv_auto('path').
//! The registered MPFileSystem intercepts the file open and returns filtered content.
static unique_ptr<TableRef> ReadMPFileBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto path = input.inputs[0].GetValue<string>();

	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(path)));
	table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));

	auto &fs = FileSystem::GetFileSystem(context);
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

	auto table_function = make_uniq<TableFunctionRef>();
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
	table_function->function = make_uniq<FunctionExpression>("read_csv_auto", std::move(children));

	auto &fs = FileSystem::GetFileSystem(context);
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
