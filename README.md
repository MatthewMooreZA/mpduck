# mpduck

A DuckDB extension for reading and writing FIS Prophet model point files (`.rpt`, `.prn`).

## Installation

```sql
INSTALL mpduck;
LOAD mpduck;
```

## Reading files

Reference a `.rpt` or `.prn` file directly in a `FROM` clause:

```sql
SELECT * FROM 'model_points.rpt';
SELECT * FROM 'data/*.rpt';
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

## Writing files

```sql
COPY (SELECT * FROM my_table) TO 'output.rpt' (FORMAT mpfile);
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
