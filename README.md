# mpduck

A DuckDB extension for reading and writing FIS Prophet model point files (`.rpt`, `.prn`, `.fac`).

## Installation

```sql
INSTALL mpduck FROM community;
LOAD mpduck;
```

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

> **Breaking change in mpduck 2.0.** Versions before 2.0 always combined multiple files as if
> `union_by_name = true`. From 2.0, `read_mpfile` takes a `union_by_name` parameter that **defaults to `false`**,
> matching `read_csv`, so reads that relied on the old behaviour now need `union_by_name = true`.
> See [Upgrading to mpduck 2.0](#upgrading-to-mpduck-20).

Like `read_csv`, `read_mpfile` takes `union_by_name` (from mpduck 2.0), which defaults to `false`.

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

#### Upgrading to mpduck 2.0

When moving from a version before 2.0, add `union_by_name = true` if a multi-file read (a glob, or a list of files) does any of these:

| Your files | Before 2.0 | 2.0, by default | To keep the old behaviour |
|---|---|---|---|
| have different columns | combined by name, missing columns `NULL` | error: `COLUMN_TYPES error: Columns with names ... do not exist in the CSV File` | `union_by_name = true` |
| have the same columns with different types (e.g. `SMALLINT` in one, `INTEGER` in another) | types widened | types taken from the first file that has data (a value that does not fit is an error) | `union_by_name = true` |
| include a zero-byte file | file skipped | file skipped (`skip_empty_files`, on by default) | nothing to change |
| include a non-empty file with no header row | file skipped | error | `union_by_name = true` |
| are read with `FROM 'data/*.rpt'` | combined by name | same as `read_mpfile` with the default | use `read_mpfile('data/*.rpt', union_by_name = true)` |

Reads of a single file, and of several files that share the same columns and types, are unaffected.

One further change in 2.0 applies in both modes: an unreadable file (for example a missing path, or a permission or
credentials error) used to be silently dropped from a multi-file read, returning partial results. It is now an error.

### ignore_errors

*Available from mpduck 2.0.* Like `read_csv`, `read_mpfile` takes `ignore_errors`, which defaults to `false`. When
it is `true`, rows that fail to parse (for example a non-numeric value in a `S`, `I` or `N` column) are skipped
instead of raising an error.

```sql
SELECT * FROM read_mpfile('model_points.rpt', ignore_errors = true);
```

It behaves exactly as it does in `read_csv`: it skips rows, not files, so a file that cannot be opened is still an
error, and only the columns a query actually reads are converted, so a bad value in a column the query does not
use does not drop the row.

### skip_empty_files

*Available from mpduck 2.0.* `read_mpfile` takes `skip_empty_files`, which defaults to `true`. When it is `true`,
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
