#!/usr/bin/env bash
# Compare peak memory of read_mpfile('dir/*.rpt') against equivalent read_csv globs as the file count grows.
#
# Two layouts: by default total data is held constant (TOTAL_ROWS) and split across N files. Set PER_FILE_ROWS
# instead to give every file the same size (total grows with N), which shows whether memory is retained per file.
# Baselines read footerless copies (.txt, so MPFileSystem does not intercept) with skip=3 to drop the
# KV_METADATA/VARIABLE_TYPES rows; the '!' row is the header. All files share one schema, so read_csv
# both with and without union_by_name is valid (read_mpfile always uses union_by_name).
#
# Usage: bench/glob_bench.sh [duckdb-binary]
# Environment: TOTAL_ROWS (default 1000000), COUNTS (default "2 5 10 20"), THREADS (default "1 2"),
#              PER_FILE_ROWS (unset), RUNS (default 3), BENCH_DIR (default /tmp/mpduck-bench),
#              MEM_LIMIT (default 2GB), INCLUDE_PLAIN=1 to also run read_csv without union_by_name.
# WARNING: the plain (no union_by_name) read_csv glob with skip= was observed using 4-7 GB of RAM and 13-26 s
# on just 55 MB of data, so it is excluded unless INCLUDE_PLAIN=1. Do not enable it with large files.
# Deliberately gentle: everything runs one process at a time under nice/ionice with a capped thread count
# and memory_limit, so it is safe to run on a shared or interactive machine.
set -euo pipefail

DUCKDB="${1:-./build/release/duckdb}"
TOTAL_ROWS="${TOTAL_ROWS:-1000000}"
COUNTS="${COUNTS:-2 5 10 20}"
THREADS="${THREADS:-1 2}"
MEM_LIMIT="${MEM_LIMIT:-2GB}"
LOW="nice -n 19 ionice -c3"
RUNS="${RUNS:-3}"
BENCH_DIR="${BENCH_DIR:-/tmp/mpduck-bench}"
SKIP=3

DT="dtypes={'!': 'VARCHAR', POLICY: 'VARCHAR', PRODUCT: 'VARCHAR', AGE: 'SMALLINT', TERM: 'INTEGER', SA: 'DOUBLE', PREM: 'DOUBLE', RATE: 'DOUBLE', DUR: 'DOUBLE'}"
AGG="count(*) AS n, max(length(POLICY)) AS l, sum(AGE) AS a, sum(TERM) AS t, sum(SA) AS sa, sum(PREM) AS p, sum(RATE) AS r, sum(DUR) AS d"

gen_files() { # n total_rows
	local n=$1 total=$2 dir="$BENCH_DIR/glob_${2}_x$1"
	if [[ -f "$dir/.done" ]]; then return; fi
	rm -rf "$dir"
	mkdir -p "$dir"
	local per=$((total / n))
	for ((k = 0; k < n; k++)); do
		local lo=$((k * per)) hi=$(((k + 1) * per))
		local f
		f=$(printf '%s/part_%03d' "$dir" "$k")
		$LOW "$DUCKDB" -c "
SET threads=2; SET memory_limit='$MEM_LIMIT';
COPY (
  SELECT 'P' || lpad(i::VARCHAR, 9, '0') AS POLICY, 'PROD' || (i % 50)::VARCHAR AS PRODUCT,
         (18 + i % 60)::SMALLINT AS AGE, (5 + i % 30)::INTEGER AS TERM, (i % 1000) * 1000.5 AS SA,
         (i % 777) * 12.25 AS PREM, (i % 97) / 100.0 AS RATE, (i % 41) * 0.5 AS DUR
  FROM range($lo, $hi) t(i)
) TO '$f.rpt' (FORMAT mpfile, KV_METADATA {run: 'bench', version: '1'});"
		cp "$f.rpt" "$f.txt"                                                # footerless CSV copy
		printf 'This is a footer line\r\nMore footer, with, commas\r\n' >>"$f.rpt" # real-looking mpfile
	done
	touch "$dir/.done"
}

measure() { # threads sql -> "min/med/max secs result"
	local th=$1 sql=$2 rss=() best_t=999999 result=""
	for _ in $(seq "$RUNS"); do
		local out tm
		out=$(mktemp)
		tm=$(mktemp)
		$LOW /usr/bin/time -f '%M %e' -o "$tm" "$DUCKDB" -csv -noheader -c "SET threads=$th; SET memory_limit='$MEM_LIMIT'; $sql;" >"$out" 2>"$out.err" || {
			echo "FAILED: $sql"; cat "$out.err"; exit 1
		}
		read -r kb secs <"$tm"
		result=$(tail -1 "$out" | cut -c1-34)
		rss+=("$((kb / 1024))")
		awk -v a="$secs" -v b="$best_t" 'BEGIN{exit !(a<b)}' && best_t=$secs
		rm -f "$out" "$out.err" "$tm"
	done
	local sorted
	sorted=($(printf '%s\n' "${rss[@]}" | sort -n))
	echo "${sorted[0]}/${sorted[$((${#sorted[@]} / 2))]}/${sorted[-1]} $best_t $result"
}

printf '%-6s %-3s %-26s %18s %7s  %s\n' files thr query "rss_MB min/med/max" wall_s result
for n in $COUNTS; do
	total=$TOTAL_ROWS
	[[ -n "${PER_FILE_ROWS:-}" ]] && total=$((PER_FILE_ROWS * n))
	gen_files "$n" "$total"
	dir="$BENCH_DIR/glob_${total}_x$n"
	for th in $THREADS; do
		declare -A Q=(
			[mpfile_glob]="SELECT $AGG FROM read_mpfile('$dir/*.rpt')"
			[csv_glob_union]="SELECT $AGG FROM read_csv('$dir/*.txt', skip=$SKIP, header=true, union_by_name=true, $DT)"
			[csv_glob_plain]="SELECT $AGG FROM read_csv('$dir/*.txt', skip=$SKIP, header=true, $DT)"
		)
		names="mpfile_glob csv_glob_union"
		[[ "${INCLUDE_PLAIN:-0}" == 1 ]] && names="$names csv_glob_plain"
		for name in $names; do
			read -r rss secs result <<<"$(measure "$th" "${Q[$name]}")"
			printf '%-6s %-3s %-26s %18s %7s  %s\n' "$n" "$th" "$name" "$rss" "$secs" "$result"
		done
	done
done
