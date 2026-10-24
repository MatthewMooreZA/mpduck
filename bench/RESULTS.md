# read_mpfile vs read_csv: benchmark results

Speed and memory of `read_mpfile` against a `read_csv` that skips the metadata rows, across mpfile shapes, query
kinds and thread counts. Produced by `bench/perf_matrix.py`; this page records one run and what it showed.

**Summary.** With the current defaults (`delim`, `quote` and `escape` pinned, `new_line` left to auto-detection),
`read_mpfile` is level with `read_csv` on ordinary files, 20-30% faster on very wide ones, and 10-20x faster on a glob
of many small files. On a glob of 100 files of about 100 MB each, `read_csv` is killed by the OOM killer on every
query while `read_mpfile` completes in about 2 GB.

## Setup

- **Machine:** Linux 6.18, 4 vCPU, about 16 GB RAM, virtual disk. One machine, no isolation from other load.
- **Build:** release build of DuckDB (submodule `d54cbbe3`, the 2.0 line) with the extension loaded explicitly
  (`LOAD '.../mpduck.duckdb_extension'`; the shell does not preload it). Results below are commit `564a51a`.
- **Each measurement is a fresh DuckDB process**, so peak RSS belongs to one query. Wall time and CPU time are the
  best of N runs, RSS is the median (`ru_maxrss` from `wait4`). N is 3, and 2 for the 10 GB sets. Differences under
  about 5-10% are within run-to-run noise on this machine.
- **Queries:** `count(*)`, `agg` (count plus sums over up to 40 numeric columns), `proj1` (one column), and
  `materialize` (`CREATE TEMP TABLE AS SELECT *`, skipped for the 10 GB sets because 180M rows would not fit in RAM).
  All at 1 and 4 threads.
- **Baseline:** `read_csv` reading the same bytes through a symlink with a `.txt` extension (so `MPFileSystem` does
  not intercept it), `header=true`, with `skip=3` (two KV metadata rows plus `VARIABLE_TYPES`) or `skip=2` when the
  file has no `VARIABLE_TYPES` row. For files with types there are two baselines: `csv_auto` (types sniffed) and
  `csv_typed` (explicit `dtypes` equal to what `read_mpfile` derives). The tables use `csv_typed`, or `csv_auto` for
  files without types; the two were within noise of each other everywhere except where noted.
- **Every dataset exists twice**: once with a `VARIABLE_TYPES` row ("typed"), and once written with
  `INCLUDE_TYPES false` ("no types", `_notypes` in the harness). Most real mpfiles carry no types. Without them
  `read_mpfile` passes no `dtypes` and `read_csv_auto` sniffs the types.

| Dataset | Shape |
|---|---|
| `narrow_100k`, `narrow_1m`, `narrow_5m` | 8 columns (2 text, SMALLINT, INTEGER, 4 DOUBLE); 5 MB, 54 MB, 270 MB |
| `narrow_footer_1m` | as `narrow_1m` plus a footer (no `read_csv` baseline: it cannot cope with the footer) |
| `wide_300c_100k` | 300 columns (100 each of DOUBLE, INTEGER, text), 100k rows, 271 MB |
| `strings_1m` | 4 columns, long text containing commas and quotes, 153 MB |
| `sparse_1m` | 8 columns, 90% empty, 18 MB |
| `files_500x2k` | glob of 500 files of 2,000 rows each, 54 MB total |
| `files_100x100mb` | glob of 100 files of 1.8M rows each, 180M rows, 9.75 GB |

## Results: current defaults

Wall seconds, with peak RSS in MB in brackets. `agg` query. mpfile / baseline.

### Files with a `VARIABLE_TYPES` row (baseline `csv_typed`)

| Dataset | 1 thread | 4 threads |
|---|---|---|
| narrow_100k | 0.10 (48) / 0.10 (48) | 0.09 (49) / 0.10 (48) |
| narrow_1m | 0.38 (97) / 0.41 (97) | 0.18 (99) / 0.17 (98) |
| narrow_5m | 1.61 (166) / 1.75 (165) | 0.54 (258) / 0.55 (285) |
| narrow_footer_1m | 0.39 (97) / n/a | 0.17 (99) / n/a |
| wide_300c_100k | **1.07 (207) / 1.39 (207)** | **0.69 (272) / 0.95 (301)** |
| strings_1m | 0.32 (165) / 0.31 (164) | 0.19 (197) / 0.19 (197) |
| sparse_1m | 0.24 (61) / 0.25 (61) | 0.14 (62) / 0.15 (62) |
| files_500x2k | **0.89 (67) / 8.92 (397)** | **0.60 (76) / 8.65 (463)** |

### Files without types (baseline `csv_auto`)

| Dataset | 1 thread | 4 threads |
|---|---|---|
| narrow_100k | 0.09 (49) / 0.10 (48) | 0.09 (49) / 0.10 (48) |
| narrow_1m | 0.40 (98) / 0.40 (97) | 0.18 (99) / 0.19 (98) |
| narrow_5m | 1.59 (165) / 1.57 (165) | 0.51 (286) / 0.52 (284) |
| narrow_footer_1m | 0.38 (97) / n/a | 0.17 (99) / n/a |
| wide_300c_100k | **1.12 (206) / 1.42 (209)** | **0.67 (274) / 0.88 (272)** |
| strings_1m | 0.35 (165) / 0.30 (164) | 0.18 (197) / 0.20 (196) |
| sparse_1m | 0.23 (61) / 0.24 (61) | 0.13 (62) / 0.14 (62) |
| files_500x2k | **0.76 (67) / 9.01 (399)** | **0.39 (77) / 8.93 (461)** |

### Materialising into a table (`materialize`, typed, 1 thread)

| Dataset | mpfile | csv_typed |
|---|---|---|
| narrow_1m | 0.49 (141) | 0.53 (139) |
| narrow_5m | 2.28 (484) | 2.57 (549) |
| wide_300c_100k | 2.20 (373) | 2.46 (374) |
| strings_1m | 0.72 (335) | 0.86 (335) |
| sparse_1m | 0.30 (111) | 0.33 (116) |
| files_500x2k | 0.96 (127) | 8.81 (466) |

`read_csv` also materialises the `!` indicator column, which `read_mpfile` drops.

### 100 files of about 100 MB (9.75 GB, 180M rows)

Each set was read into the page cache before its run. `read_mpfile` wall seconds (RSS in MB). `read_csv` was
OOM-killed on **all 12 query/thread/typing combinations**, after 18-25 s at a peak RSS of about 13.6 GB.

| Query | Typed, 1 thread | Typed, 4 threads | No types, 1 thread | No types, 4 threads |
|---|---|---|---|---|
| count | 33.6 (1588) | 9.1 (2015) | 34.4 (1738) | 9.3 (1999) |
| agg | 59.6 (1374) | 15.5 (2094) | 58.6 (1476) | 15.2 (2104) |
| proj1 | 37.1 (1601) | 10.0 (1955) | 36.6 (1600) | 9.7 (1982) |

## Before and after pinning the dialect

The first run used the extension as originally written, where `read_mpfile` left dialect detection to the CSV
sniffer. These are `read_mpfile` only, `count(*)`.

| Scenario | Before | After |
|---|---|---|
| wide, typed, 1 / 4 threads | 1.94 / 1.63 s | 0.97 / 0.74 s |
| wide, no types, 1 / 4 threads | 1.81 / 1.41 s | 1.01 / 0.64 s |
| 500 small files, typed, 1 / 4 threads | 3.51 / 3.28 s | 0.77 / 0.61 s |
| 500 small files, no types, 1 / 4 threads | 0.69 / 0.42 s | 0.49 / 0.33 s |
| narrow 100k, 1 thread | 0.13 s | 0.08 s |
| 100 x 100 MB, typed, 1 / 4 threads | 66.9 / 15.0 s | 33.6 / 9.1 s |
| 100 x 100 MB, no types, 1 / 4 threads | 37.1 / 13.9 s | 34.4 / 9.3 s |

Before the change, wide files were 30-60% slower than `read_csv` and the typed glob was 3x faster. The 10 GB
"before" runs were single runs that were not pre-read into the page cache, so part of that improvement is cache
warmth and not the change. The other rows are not affected by that.

## What the investigation found

**Wide-file slowness (fixed).** `callgrind` on the bind step showed 14.6 G instructions for `read_mpfile` against
7.2 G for `read_csv`; the extra 7.3 G was all in `CSVSniffer::DetectDialect -> RefineCandidates`. The sniffer
enumerates delimiter x quote x escape candidates, and when they tie it re-scans later chunks for each one, at a
cost proportional to row width. Pinning any one of the three options helps on its own (wide bind: 1.20 s unpinned,
1.00 s `delim`, 0.86 s `quote`, 0.65 s `escape`); all three together give 0.41 s. A plain file holding exactly the
stream `MPFileSystem` produces is just as slow under default `read_csv`, so the wrapper itself is not the cause.
I/O is identical (about 64 MB read at bind in both).

**Passing `dtypes` makes globs slower (fixed by the same change).** `dtypes` replaces detected types but leaves
sniffing on. Later files in a glob normally take a cheap column-count sniff (`AdaptiveSniff`/`MinimalSniff`) and fall
back to the full sniffer if the schema does not match. On plain-CSV copies of the 500 files, adding `dtypes` took the
read from 0.71 s to 3.0 s, and pinning the dialect brought it back to 0.8 s. Why the fallback is taken when types are
set was not traced in DuckDB's source; this is inferred from the measurements.

**`read_csv`'s `skip` is what blows up multi-file reads.** On 20 of the large files, `read_csv(skip=2,
header=true)` was OOM-killed at 13.6 GB, with or without the dialect pinned. On clean copies of the same files
(metadata rows removed) `read_csv` used 759 MB, matching `read_mpfile` at 755 MB in about the same time. So the glob
advantage of `read_mpfile` comes from `MPFileSystem` removing the metadata rows, which means `read_csv` never needs
`skip`. Anyone reading mpfiles with plain `read_csv` has to use `skip`, so the comparison reflects real use, but
the baseline is penalised by an option that `read_mpfile` avoids. The reason `skip` costs so much was not traced.

**`new_line` is not worth pinning.** With the other three options pinned, adding `new_line='\r\n'` changed bind and
`count` times by -8% to +9%, in both directions, across six dataset/typing combinations (7 runs each). It gives no
measurable gain and would make pure-LF files fail, so it is only forwarded when the user sets it.

**Quotes are not required.** Pinning `quote='"'` only names the character that opens a quoted field. Fully
unquoted files, files with quoted strings and unquoted numbers, and unquoted fields with a stray `"` mid-value all
read correctly with the defaults.

## Caveats

- One machine; noise is about 5-10%, and a handful of narrow 1M-row cells favour `read_csv` by up to 15%
  (for example 0.30 s against 0.26 s for `count`, typed, 1 thread) while others favour `read_mpfile`. I treat these
  as noise but cannot rule out a few percent of real cost.
- The data is machine-generated by `COPY ... (FORMAT mpfile)`. No real Prophet output was available; files that
  use another quote or escape convention would need `quote`/`escape` overrides.
- CPU time exceeds wall time on globs even at `threads=1` (for example 2.2 s CPU in 0.77 s wall for the typed
  500-file glob, after the fix; before it was about 3.5x). Some work runs outside the `threads` limit. This was
  not explained.
- The 10 GB results are best-of-2 and depend on the page cache: a cold read of the same glob took 444 s.
- With no types, `read_mpfile` and `read_csv` sniff types independently; the sniffed types were not compared.

## Reproducing

```bash
make                                                      # builds build/release/duckdb and the extension
python3 bench/perf_matrix.py --runs 3 --threads "1 4"     # whole matrix; --only name,name picks datasets
python3 bench/perf_matrix.py --only wide_300c_100k,wide_300c_100k_notypes --json out.json
```

Datasets are generated on first use under `/tmp/mpduck-perf` (`--dir` to change): about 21 GB for the two 10 GB
sets plus about 2 GB for the rest. Each dataset has a `_notypes` twin, e.g. `--only narrow_1m_notypes`. The 10 GB
sets need `--only files_100x100mb` (and the `_notypes` twin); read them into the page cache first
(`cat dir/*.rpt > /dev/null`) for stable timings. `bench/memory_bench.sh` is an earlier, single-shape version of
the same comparison.
