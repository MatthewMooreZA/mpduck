# mpduck

A DuckDB extension for reading and writing FIS Prophet model point files (`.rpt`, `.prn`, `.fac`).

## Installation

```sql
INSTALL mpduck FROM community;
LOAD mpduck;
```

## Versions

mpduck follows [semantic versioning](https://semver.org). The mpduck version is independent of the DuckDB version:
the same source builds against DuckDB 2.0 and DuckDB 1.5.x (CI builds v2.0.0 and v1.5.6), and an extension binary
has to match the DuckDB version it is loaded into.

## Reading files

Reference a `.rpt`, `.prn`, or `.fac` file directly in a `FROM` clause:

```sql
SELECT * FROM 'model_points.rpt';
SELECT * FROM 'data/*.rpt';
SELECT * FROM 'data/*.fac';
```

Or use `read_mpfile()` explicitly:

```sql
-- Single file or glob
SELECT * FROM read_mpfile('model_points.rpt');
SELECT * FROM read_mpfile('data/*.rpt');

-- Multiple files or globs
SELECT * FROM read_mpfile(['mp_2024.rpt', 'mp_2025.rpt']);
SELECT * FROM read_mpfile(['region_a*.rpt', 'region_b*.rpt']);
```

### union_by_name

> **Breaking change in mpduck 1.0.** Versions before 1.0 always combined multiple files as if
> `union_by_name = true`. From 1.0, `read_mpfile` takes a `union_by_name` parameter that **defaults to `false`**,
> matching `read_csv`, so reads that relied on the old behaviour now need `union_by_name = true`.
> See [Upgrading to mpduck 1.0](#upgrading-to-mpduck-10).

Like `read_csv`, `read_mpfile` takes `union_by_name` (from mpduck 1.0), which defaults to `false`.

- **`false` (default):** the files are expected to have the same columns. The column types come from the
  `VARIABLE_TYPES` row of the first file that has data, and the remaining files are opened lazily by the scan, so
  the cost of binding does not grow with the number of files. This matters most on remote storage such as S3.
  Files with different columns fail with an error. Zero-byte files are skipped (see
  [skip_empty_files](#skip_empty_files)), but a non-empty file with no header row is not.
- **`true`:** every file's header and `VARIABLE_TYPES` rows are read up front. Files with no data are skipped,
  columns are matched by name (missing columns become `NULL`), and types that differ between files are widened
  (`SMALLINT` < `INTEGER` < `DOUBLE`, anything else becomes `VARCHAR`). This costs an extra open and read per file.

In both modes a file that cannot be opened (missing, permission denied, network or credential errors) is an
error; it is never silently skipped.

```sql
SELECT * FROM read_mpfile('data/*.rpt', union_by_name = true);
```

`FROM 'data/*.rpt'` behaves like the default (`union_by_name = false`).

#### Upgrading to mpduck 1.0

When moving from a version before 1.0, add `union_by_name = true` if a multi-file read (a glob, or a list of files) does any of these:

| Your files | Before 1.0 | 1.0, by default | To keep the old behaviour |
|---|---|---|---|
| have different columns | combined by name, missing columns `NULL` | error: `COLUMN_TYPES error: Columns with names ... do not exist in the CSV File` | `union_by_name = true` |
| have the same columns with different types (e.g. `SMALLINT` in one, `INTEGER` in another) | types widened | types taken from the first file that has data (a value that does not fit is an error) | `union_by_name = true` |
| include a zero-byte file | file skipped | file skipped (`skip_empty_files`, on by default) | nothing to change |
| include a non-empty file with no header row | file skipped | error | `union_by_name = true` |
| are read with `FROM 'data/*.rpt'` | combined by name | same as `read_mpfile` with the default | use `read_mpfile('data/*.rpt', union_by_name = true)` |

Reads of a single file, and of several files that share the same columns and types, are unaffected.

Two further changes in 1.0 apply in both modes:

- An unreadable file (for example a missing path, or a permission or credentials error) used to be silently dropped
  from a multi-file read, returning partial results. It is now an error.
- The CSV dialect is no longer sniffed. `read_mpfile` now reads with `delim = ','`, `quote = '"'` and
  `escape = '"'`, which is faster (see [delim, quote, escape and new_line](#delim-quote-escape-and-new_line)). A file
  that uses a different quote or escape character, which the sniffer used to detect, needs `quote` and `escape` passed
  explicitly. Line endings are still detected.

### ignore_errors

*Available from mpduck 1.0.* Like `read_csv`, `read_mpfile` takes `ignore_errors`, which defaults to `false`. When
it is `true`, rows that fail to parse (for example a non-numeric value in a `S`, `I` or `N` column) are skipped
instead of raising an error.

```sql
SELECT * FROM read_mpfile('model_points.rpt', ignore_errors = true);
```

It behaves exactly as it does in `read_csv`: it skips rows, not files, so a file that cannot be opened is still an
error, and only the columns a query actually reads are converted, so a bad value in a column the query does not
use does not drop the row.

### delim, quote, escape and new_line

`read_mpfile` passes the CSV dialect of an mpfile to `read_csv` instead of leaving it to be sniffed. The defaults are
the standard CSV conventions (RFC 4180), which is also what `COPY ... TO` writes. Each can be overridden with the
parameter of the same name that `read_csv` takes:

| Parameter | Default | Notes |
|---|---|---|
| `delim` (alias `sep`) | `','` | Only `','` is accepted, see below. Give `delim` or `sep`, not both. |
| `quote` | `'"'` | Quoting is optional: unquoted fields read as they are. `quote = ''` turns quote handling off. |
| `escape` | `'"'` | A quote inside a quoted field is written doubled. |
| `new_line` | not set | Detected from the file, so CRLF and LF files both read without options. |

```sql
-- Single-quoted fields, as in 'O''Brien'
SELECT * FROM read_mpfile('model_points.rpt', quote = '''', escape = '''');

-- Force a line ending (the text \r\n, \n or \r, as in read_csv, not the control characters)
SELECT * FROM read_mpfile('lf_endings.rpt', new_line = '\n');
```

A `new_line` that does not match the file is an error. A NULL value leaves the default in place. The settings apply to
every file in a glob or list, and `FROM 'file.rpt'` always uses the defaults, since it takes no parameters.

`delim` and `sep` are accepted for parity with `read_csv`, but only `','` is valid: the header (`!`) rows are split and
rebuilt on commas, so any other delimiter is rejected with an error.

Fixing the dialect makes reads faster, most of all for very wide files and for globs of many files, because the CSV
sniffer no longer has to search for it. Measurements are in [`bench/RESULTS.md`](bench/RESULTS.md).

### skip_empty_files

`read_mpfile` takes `skip_empty_files`, which defaults to `true`. When it is `true`,
zero-byte files in a glob or a list of files are dropped before anything is read, so one empty file does not fail
the whole read. This applies with `union_by_name` either `false` or `true`, and to `FROM 'data/*.rpt'`.

```sql
-- Fail on empty files instead of skipping them
SELECT * FROM read_mpfile('data/*.rpt', skip_empty_files = false);
```

- It costs one open and size lookup per file at bind time, with no reading. On remote storage that is one extra
  request per file, so set `skip_empty_files = false` if you read a very large number of files you know are not empty.
- It only covers files of exactly zero bytes. A non-empty file with no header row is still an error with the default
  `union_by_name = false`, and is skipped with `union_by_name = true`.
- A single literal path, `read_mpfile('file.rpt')`, is never filtered.
- With `skip_empty_files = false` and `union_by_name = false`, an empty file fails the read with
  `Columns with names ... do not exist in the CSV File`. With `union_by_name = true` it is still skipped, because
  every file's header is checked.
- If every matched file is empty, the read fails with a `no files ... matched` error rather than returning no rows.

## Writing files

```sql
-- The format is auto-detected from the .rpt, .prn, or .fac extension
COPY (SELECT * FROM my_table) TO 'output.rpt';
COPY (SELECT * FROM my_table) TO 'output.prn';
COPY (SELECT * FROM my_table) TO 'output.fac';

-- FORMAT mpfile can also be specified explicitly
COPY (SELECT * FROM my_table) TO 'output.rpt' (FORMAT mpfile);
```

### KV_METADATA

Use `KV_METADATA` to embed key-value metadata rows at the top of the file (before the schema row). The value is a struct of string key-value pairs:

```sql
COPY (SELECT * FROM my_table) TO 'output.rpt' (
    KV_METADATA {run_date: '2024-01-01', version: '1.0'}
);
```

### INCLUDE_TYPES

Controls whether the `VARIABLE_TYPES` schema row is written. Defaults to `true`.

Set to `false` to omit the `VARIABLE_TYPES` row — useful when the consuming application does not expect it:

```sql
COPY (SELECT * FROM my_table) TO 'output.rpt' (INCLUDE_TYPES false);
```

## Building from source

```bash
git clone --recurse-submodules https://github.com/your-org/mpduck
cd mpduck
make
```

Run tests:
```bash
make test
```
