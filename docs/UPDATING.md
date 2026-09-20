# Extension updating 
When cloning this template, the target version of DuckDB should be the latest stable release of DuckDB. However, there 
will inevitably come a time when a new DuckDB is released and the extension repository needs updating. This process goes
as follows:

- Bump submodules
  - `./duckdb` should be set to latest tagged release
  - `./extension-ci-tools` should be set to updated branch corresponding to latest DuckDB release. So if you're building for DuckDB `v1.1.0` there will be a branch in `extension-ci-tools` named `v1.1.0` to which you should check out. 
- Bump versions in `./.github/workflows/MainDistributionPipeline.yml`
  - `duckdb_version` and `ci_tools_version` inputs in the `duckdb-stable-build` job should be set to latest tagged release
  - `duckdb_version` and `ci_tools_version` inputs in the `code-quality-check` job should be set to latest tagged release
  - the `@vX.Y.Z` ref on the reusable workflows (`_extension_distribution.yml` and `_extension_code_quality.yml`) should be set to latest tagged release
  - the `duckdb-next-build` job intentionally tracks DuckDB `main` (non-blocking) to give early warning of API breakage; leave it alone

# Supporting more than one DuckDB version
mpduck builds against DuckDB 1.5.x and 2.0 from the same source. Differences in DuckDB's internal C++ API are isolated
in `src/include/mpduck_compat.hpp` (see the comment at the top of that file). When a DuckDB bump breaks the build:

- If the break is a rename or a type change, add a helper to `mpduck_compat.hpp` with one implementation per version
  and call it from the sources, rather than sprinkling `#if` around.
- To try a new DuckDB, point the `duckdb` submodule at it (or use a `git worktree`, so the existing build is kept)
  and build. The `duckdb-next-build` CI job does the same against DuckDB `main`.
- Once an old version is no longer supported, delete its half of the `#if` in the compat header.

# API changes
DuckDB extensions built with this extension template are built against the internal C++ API of DuckDB. This API is not guaranteed to be stable.
What this means for extension development is that when updating your extensions DuckDB target version using the above steps, you may run into the fact that your extension no longer builds properly.

Currently, DuckDB does not (yet) provide a specific change log for these API changes, but it is generally not too hard to figure out what has changed.

For figuring out how and why the C++ API changed, we recommend using the following resources:
- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ Header file of the API that has changed