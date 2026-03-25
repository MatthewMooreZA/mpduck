# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`mpduck` is a DuckDB extension for reading and writing **model point files (mpfiles)** produced by the FIS Prophet actuarial application. These files use `.rpt` or `.prn` extensions and are CSV-like, but use a first-column indicator to distinguish between header rows, data rows, and schema definition rows — which standard CSV readers cannot handle.

The current scalar functions (`mpduck`, `mpduck_openssl_version`) are placeholders from the extension template. The real functionality will be table functions or COPY support for reading/writing `.rpt`/`.prn` files.

## Build commands

```bash
make                # Build release
make test           # Run SQL logic tests (release)
make test_debug     # Run SQL logic tests (debug)
```

Build outputs:
- `./build/release/duckdb` — DuckDB shell with extension preloaded
- `./build/release/test/unittest` — Test runner with extension linked
- `./build/release/extension/mpduck/mpduck.duckdb_extension` — Loadable extension binary

To run the extension interactively:
```bash
./build/release/duckdb
```

To run a single test:
```bash
./build/release/test/unittest "test/sql/mpduck.test"
```

## Architecture

The extension follows the standard DuckDB extension pattern:

- **`src/include/mpduck_extension.hpp`** — Declares `MpduckExtension` inheriting from `Extension`, implementing `Load()`, `Name()`, and `Version()`.
- **`src/mpduck_extension.cpp`** — Registers scalar functions in `Load()` using `UnaryExecutor` and `StringVector::AddString()` for result strings. Exports the C entry point via `DUCKDB_CPP_EXTENSION_ENTRY`.
- **`extension_config.cmake`** — Registers the extension with DuckDB's build system and enables test loading.
- **`CMakeLists.txt`** — Links OpenSSL (SSL + Crypto) to both the static and loadable extension targets. OpenSSL is sourced via vcpkg (`vcpkg.json`).

## Testing

Tests use DuckDB's SQLLogicTest format (`.test` files in `test/sql/`). Each test file uses `require mpduck` to load the extension before testing functions.

## Updating DuckDB version

See `docs/UPDATING.md` — involves bumping the `duckdb` and `extension-ci-tools` submodules and updating version references in GitHub workflow files.
