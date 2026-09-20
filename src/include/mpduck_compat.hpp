#pragma once

// Lets one source tree build against both DuckDB 1.5.x and DuckDB 2.0.
//
// DuckDB's C++ API is internal and not stable across releases. The differences mpduck is exposed to are kept
// here so the rest of the code reads the same for both versions:
//
//   - 2.0 introduced Identifier for SQL names (column names, aliases, COPY option keys); 1.5 uses plain string.
//   - 2.0 deleted ConstantExpression(const Value &) in favour of ConstantExpression::FromValue.
//   - 2.0 made StarExpression::exclude_list private (ExcludeListMutable()).
//   - 2.0 replaced Vector::Flatten(count) with Flatten().
//
// The switch is the presence of duckdb/common/identifier.hpp, which only exists from 2.0.

#include "duckdb.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"

#if __has_include("duckdb/common/identifier.hpp")
#define MPDUCK_DUCKDB_V2 1
#endif

namespace duckdb {

#ifdef MPDUCK_DUCKDB_V2

//! The type DuckDB uses for SQL names: what a name in a COPY bind callback or a struct field looks like.
using MPName = Identifier;

inline Identifier MPMakeName(const string &name) {
	return Identifier(name);
}
inline const string &MPNameString(const Identifier &name) {
	return name.GetIdentifierName();
}
inline unique_ptr<ParsedExpression> MPConstant(const Value &value) {
	return ConstantExpression::FromValue(value);
}
inline void MPExcludeColumn(StarExpression &star, const string &column) {
	star.ExcludeListMutable().insert(QualifiedColumnName(Identifier(column)));
}
inline void MPFlatten(Vector &vector, idx_t) {
	vector.Flatten();
}

#else

using MPName = string;

inline string MPMakeName(const string &name) {
	return name;
}
inline const string &MPNameString(const string &name) {
	return name;
}
inline unique_ptr<ParsedExpression> MPConstant(const Value &value) {
	return make_uniq<ConstantExpression>(value);
}
inline void MPExcludeColumn(StarExpression &star, const string &column) {
	star.exclude_list.insert(QualifiedColumnName(column));
}
inline void MPFlatten(Vector &vector, idx_t count) {
	vector.Flatten(count);
}

#endif

} // namespace duckdb
