# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`mpduck` is a DuckDB extension for reading and writing **model point files (mpfiles)** produced by the FIS Prophet actuarial application. These files use `.rpt`, `.prn` or `.fac` extensions and are CSV-like, but use a first-column indicator to distinguish between header rows (`!`), data rows (`*`), comment rows (`&`) and schema rows (`VARIABLE_TYPES`) — which standard CSV readers cannot handle.

## Build commands

```bash
git submodule update --init --recursive   # first time only
make                # Build release
make test           # Run SQL logic tests (release)
make test_debug     # Run SQL logic tests (debug)
```

Build outputs:
- `./build/release/duckdb` — DuckDB shell with extension preloaded
- `./build/release/test/unittest` — Test runner with extension linked
- `./build/release/extension/mpduck/mpduck.duckdb_extension` — Loadable extension binary

To run a single test:
```bash
./build/release/test/unittest "test/sql/mpduck.test"
```

## Architecture

The extension is built against DuckDB's internal C++ API (not the stable C API).

- **`src/mpduck_extension.cpp`** — `LoadInternal()` registers everything: the `MPFileSystem` sub-system, the `read_mpfile` table function (VARCHAR and LIST(VARCHAR) overloads, both via `bind_replace`), a replacement scan for `FROM 'x.rpt'`, and the COPY function. Reading works by building a parser AST that rewrites to `SELECT * EXCLUDE ("!") FROM read_csv_auto([...], dtypes=..., filename=...)`. `union_by_name` defaults to false like `read_csv`: only the first file with data is parsed for its schema and the rest are opened lazily. With `union_by_name=true` every file's schema is parsed, empty files are dropped, schemas are merged (`MergeSchemas`) and `union_by_name=true` is forwarded (`ReadMPFileMultiRef`).
- **`src/mpfile_filesystem.cpp` / `src/include/mpfile_filesystem.hpp`** — `MPFileSystem` claims `.rpt`/`.prn`/`.fac` paths (`IsManuallySet` gives it priority) and serves a filtered stream: a normalised in-memory header built from the `!` rows, followed by the raw `*` data block, with any footer cut off (`ScanPreDataSection`, `FindDataEnd`). A thread-local bypass flag (`ScopedMPFSBypass`) stops it from intercepting its own raw opens. Also holds `ParseMPFileSchema` (header + `VARIABLE_TYPES` → DuckDB types) and `MergeSchemas` (type widening across multiple files).
- **`src/write_mpfile.cpp`** — `COPY ... TO 'x.rpt'` / `(FORMAT mpfile)` with `KV_METADATA` and `INCLUDE_TYPES` options. Registered under the names `mpfile`, `rpt`, `prn`, `fac`.
- **`extension_config.cmake`** — Registers the extension with DuckDB's build system and enables test loading.
- **`CMakeLists.txt`** — Builds the static and loadable extension targets. No external dependencies (`vcpkg.json` has none).

## Testing

Tests use DuckDB's SQLLogicTest format (`test/sql/mpduck.test`, fixtures in `test/data/`). The test file uses `require mpduck` to load the extension.

## Updating DuckDB version

See `docs/UPDATING.md` — involves bumping the `duckdb` and `extension-ci-tools` submodules and updating version references in `.github/workflows/MainDistributionPipeline.yml`.
