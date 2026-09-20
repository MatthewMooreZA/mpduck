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

Like `read_csv`, `read_mpfile` takes `union_by_name`, which defaults to `false`.

- **`false` (default):** the files are expected to have the same columns. The column types come from the
  `VARIABLE_TYPES` row of the first file that has data, and the remaining files are opened lazily by the scan, so
  the cost of binding does not grow with the number of files. This matters most on remote storage such as S3.
  Files with different columns fail with an error, and files with no data are not skipped.
- **`true`:** every file's header and `VARIABLE_TYPES` rows are read up front. Files with no data are skipped,
  columns are matched by name (missing columns become `NULL`), and types that differ between files are widened
  (`SMALLINT` < `INTEGER` < `DOUBLE`, anything else becomes `VARCHAR`). This costs an extra open and read per file.

In both modes a file that cannot be opened (missing, permission denied, network or credential errors) is an
error; it is never silently skipped.

```sql
SELECT * FROM read_mpfile('data/*.rpt', union_by_name = true);
```

`FROM 'data/*.rpt'` behaves like the default (`union_by_name = false`).

### ignore_errors

Like `read_csv`, `read_mpfile` takes `ignore_errors`, which defaults to `false`. When it is `true`, rows that fail
to parse (for example a non-numeric value in a `S`, `I` or `N` column) are skipped instead of raising an error.

```sql
SELECT * FROM read_mpfile('model_points.rpt', ignore_errors = true);
```

It behaves exactly as it does in `read_csv`: it skips rows, not files, so a file that cannot be opened is still an
error, and only the columns a query actually reads are converted, so a bad value in a column the query does not
use does not drop the row.

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
