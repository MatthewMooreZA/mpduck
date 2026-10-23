# Extension updating 
When cloning this template, the target version of DuckDB should be the latest stable release of DuckDB. However, there 
will inevitably come a time when a new DuckDB is released and the extension repository needs updating. This process goes
as follows:

- Bump submodules
  - `./duckdb` should be set to latest tagged release (currently `v2.0.0`)
  - `./extension-ci-tools` tracks `main`; the workflow itself uses the `v2.0-cyanoptera` branch of extension-ci-tools
- Bump versions in `./.github/workflows/MainDistributionPipeline.yml`
  - `duckdb_version` in the `duckdb-stable-build` and `code-quality-check` jobs should be set to latest tagged release
  - `duckdb-1-5-build` keeps building the 1.5.x line from the same source; its `duckdb_version`, `ci_tools_version`
    and `@vX.Y.Z` ref move together with the latest 1.5 patch release
  - the `duckdb-stable-build` and `code-quality-check` jobs (and their `ci_tools_version`) use `v2.0-cyanoptera`, as in
    duckdb/extension-template; move them to a release-named branch once one exists for 2.0.x

# Supporting more than one DuckDB version
mpduck builds against DuckDB 1.5.x and 2.0 from the same source. Differences in DuckDB's internal C++ API are isolated
in `src/include/mpduck_compat.hpp` (see the comment at the top of that file). When a DuckDB bump breaks the build:

- If the break is a rename or a type change, add a helper to `mpduck_compat.hpp` with one implementation per version
  and call it from the sources, rather than sprinkling `#if` around.
- To try a new DuckDB, point the `duckdb` submodule at it (or use a `git worktree`, so the existing build is kept)
  and build.
- Once an old version is no longer supported, delete its half of the `#if` in the compat header.

# API changes
DuckDB extensions built with this extension template are built against the internal C++ API of DuckDB. This API is not guaranteed to be stable.
What this means for extension development is that when updating your extensions DuckDB target version using the above steps, you may run into the fact that your extension no longer builds properly.

Currently, DuckDB does not (yet) provide a specific change log for these API changes, but it is generally not too hard to figure out what has changed.

For figuring out how and why the C++ API changed, we recommend using the following resources:
- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ Header file of the API that has changed