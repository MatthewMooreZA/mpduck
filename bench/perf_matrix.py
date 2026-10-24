#!/usr/bin/env python3
"""read_mpfile vs read_csv across a matrix of mpfile shapes, query kinds and thread counts.

For each dataset an mpfile is generated with COPY (KV_METADATA + VARIABLE_TYPES + '!' header + data). The read_csv
baseline reads the same bytes through a symlink with a non-mpfile extension (so MPFileSystem does not intercept it)
with skip=3 (2 KV rows + VARIABLE_TYPES) and the '!' row as header. Two baselines:
  csv_auto   - read_csv(skip=3, header=true), types sniffed
  csv_typed  - same plus explicit dtypes identical to what read_mpfile derives from VARIABLE_TYPES

Each measurement is a fresh DuckDB process (so peak RSS is attributable to one query); wall = best of RUNS,
rss = median of RUNS (ru_maxrss from wait4).

Usage: bench/perf_matrix.py [--duckdb PATH] [--dir DIR] [--runs N] [--threads "1 4"] [--only name,name] [--json OUT]
"""
import argparse, json, os, statistics, subprocess, sys, tempfile

ap = argparse.ArgumentParser()
ap.add_argument("--duckdb", default="./build/release/duckdb")
ap.add_argument("--ext", default="build/release/extension/mpduck/mpduck.duckdb_extension")
ap.add_argument("--dir", default="/tmp/mpduck-perf")
ap.add_argument("--runs", type=int, default=3)
ap.add_argument("--threads", default="1 4")
ap.add_argument("--only", default="")
ap.add_argument("--json", default="")
a = ap.parse_args()
os.makedirs(a.dir, exist_ok=True)
SKIP = 3

# ---- dataset definitions: name -> (rows, select-expression list [(name, expr, duckdb_type)], files) ----
NARROW = [("POLICY", "'P' || lpad(i::VARCHAR, 9, '0')", "VARCHAR"),
          ("PRODUCT", "'PROD' || (i % 50)::VARCHAR", "VARCHAR"),
          ("AGE", "(18 + i % 60)::SMALLINT", "SMALLINT"),
          ("TERM", "(5 + i % 30)::INTEGER", "INTEGER"),
          ("SA", "(i % 1000) * 1000.5", "DOUBLE"),
          ("PREM", "(i % 777) * 12.25", "DOUBLE"),
          ("RATE", "(i % 97) / 100.0", "DOUBLE"),
          ("DUR", "(i % 41) * 0.5", "DOUBLE")]
WIDE = []
for k in range(300):
    if k % 3 == 0:
        WIDE.append((f"D{k}", f"((i * {k + 7}) % 100003) / 7.0", "DOUBLE"))
    elif k % 3 == 1:
        WIDE.append((f"I{k}", f"((i * {k + 3}) % 9973)::INTEGER", "INTEGER"))
    else:
        WIDE.append((f"S{k}", f"'v' || ((i + {k}) % 500)::VARCHAR", "VARCHAR"))
STRINGS = [("ID", "i::BIGINT", "BIGINT"),
           ("NOTE", "'note, with comma and \"quote\" ' || md5(i::VARCHAR) || md5((i+1)::VARCHAR)", "VARCHAR"),
           ("DESC", "repeat('x', 20 + i % 40) || i::VARCHAR", "VARCHAR"),
           ("CAT", "'C' || (i % 5)::VARCHAR", "VARCHAR")]
SPARSE = [(n, f"CASE WHEN i % 10 = 0 THEN {e} END", t) for n, e, t in NARROW]  # 90% NULL (empty fields)

# name: (rows, cols, nfiles, footer)
DATASETS = {
    "narrow_100k":  (100_000, NARROW, 1, False),
    "narrow_1m":    (1_000_000, NARROW, 1, False),
    "narrow_5m":    (5_000_000, NARROW, 1, False),
    "narrow_footer_1m": (1_000_000, NARROW, 1, True),
    "wide_300c_100k": (100_000, WIDE, 1, False),
    "strings_1m":   (1_000_000, STRINGS, 1, False),
    "sparse_1m":    (1_000_000, SPARSE, 1, False),
    "files_500x2k": (2_000, NARROW, 500, False),
    # ~100 MB per file (8 narrow cols, ~58 bytes/row): 100 files, ~10 GB total. Check the printed size.
    "files_100x100mb": (1_800_000, NARROW, 100, False),
}
# Most real mpfiles carry no VARIABLE_TYPES row: every dataset gets a "<name>_notypes" twin written with
# INCLUDE_TYPES false, where read_mpfile cannot pass dtypes and read_csv_auto sniffs the types.
NOTYPES = set()
for _n in list(DATASETS):
    DATASETS[_n + "_notypes"] = DATASETS[_n]
    NOTYPES.add(_n + "_notypes")
# Queries skipped per dataset: materialising ~180M rows would exceed RAM.
SKIP_QUERIES = {"files_100x100mb": {"materialize"}, "files_100x100mb_notypes": {"materialize"}}


def sh(sql, threads=None, timed=False):
    pre = f"LOAD '{a.ext}'; " + (f"SET threads={threads}; " if threads else "")
    cmd = [a.duckdb, "-unsigned", "-csv", "-noheader", "-c", pre + sql]
    if not timed:
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode:
            sys.exit(f"duckdb failed: {r.stderr[:500]}\n{sql[:300]}")
        return r.stdout
    import time
    with tempfile.TemporaryFile() as so, tempfile.TemporaryFile() as se:
        t0 = time.time()
        p = subprocess.Popen(cmd, stdout=so, stderr=se)
        _, status, ru = os.wait4(p.pid, 0)
        wall = time.time() - t0
        se.seek(0)
        so.seek(0)
        err, out = se.read().decode(errors="replace"), so.read().decode(errors="replace")
    rc = os.waitstatus_to_exitcode(status)
    if rc == -9:  # SIGKILL: almost certainly the OOM killer; record it as a result rather than aborting the matrix
        return int(ru.ru_maxrss) // 1024, wall, ru.ru_utime + ru.ru_stime, "OOM-KILLED"
    if rc:
        sys.exit(f"duckdb failed: {err[:500]}\n{sql[:300]}")
    return int(ru.ru_maxrss) // 1024, wall, ru.ru_utime + ru.ru_stime, out.strip().splitlines()[-1][:60]


def gen(name):
    rows, cols, nfiles, footer = DATASETS[name]
    d = os.path.join(a.dir, name)
    marker = os.path.join(d, ".done")
    if not os.path.exists(marker):
        os.makedirs(d, exist_ok=True)
        sel = ", ".join(f"{e} AS {n}" for n, e, _ in cols)
        for f in range(nfiles):
            out = os.path.join(d, f"f{f:04d}.rpt")
            sh(f"SET threads=4; COPY (SELECT {sel} FROM range({rows}) t(i)) TO '{out}' "
               f"(FORMAT mpfile, KV_METADATA {{run: 'bench', version: '1'}}"
               f"{', INCLUDE_TYPES false' if name in NOTYPES else ''});")
            os.symlink(out, os.path.join(d, f"f{f:04d}.txt"))
            if footer:
                with open(out, "ab") as fh:
                    fh.write(b"This is a footer line\r\nMore footer, with, commas\r\n")
        open(marker, "w").close()
    return d


def queries(name):
    rows, cols, nfiles, footer = DATASETS[name]
    d = os.path.join(a.dir, name)
    dt = ", ".join(f"'{n}': '{t}'" for n, _, t in cols)
    dt = f"{{'!': 'VARCHAR', {dt}}}"
    num = [n for n, _, t in cols if t != "VARCHAR"]
    # footer files: csv baseline cannot cope with the footer, so it reads the symlink to the same (footer-bearing) bytes
    # only when there is none; for the footer case fall back to ignore_errors-free comparison on mpfile only.
    mp = f"read_mpfile('{d}/*.rpt')" if nfiles > 1 else f"read_mpfile('{d}/f0000.rpt')"
    csvp = f"'{d}/*.txt'" if nfiles > 1 else f"'{d}/f0000.txt'"
    skip = SKIP - 1 if name in NOTYPES else SKIP  # no VARIABLE_TYPES row
    sources = {"mpfile": mp}
    if not footer:
        sources["csv_auto"] = f"read_csv({csvp}, skip={skip}, header=true)"
        if name not in NOTYPES:
            sources["csv_typed"] = f"read_csv({csvp}, skip={skip}, header=true, dtypes={dt})"
    first, last = cols[0][0], cols[-1][0]
    agg_cols = ", ".join(f"sum(\"{n}\")" for n in num[:40]) + (f", max(length(\"{cols[0][0]}\"))" if cols[0][2] == "VARCHAR" else "")
    qs = {
        "count": "SELECT count(*) FROM {s}",
        "agg": f"SELECT count(*), {agg_cols} FROM {{s}}",
        "proj1": f"SELECT count(\"{last}\"), min(\"{last}\") FROM {{s}}",
        "materialize": "CREATE TEMP TABLE t AS SELECT * FROM {s}; SELECT count(*) FROM t",
    }
    return sources, qs


results = []
only = set(filter(None, a.only.split(",")))
for name in DATASETS:
    if only and name not in only:
        continue
    d = gen(name)
    size = sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d) if f.endswith(".rpt")) // 2**20
    sources, qs = queries(name)
    print(f"\n=== {name}  ({size} MB on disk, {DATASETS[name][0] * DATASETS[name][2]} rows, {len(DATASETS[name][1])} cols) ===", flush=True)
    print(f"{'query':<12}{'thr':>4}  " + "".join(f"{s:>26}" for s in sources), flush=True)
    print(f"{'':<12}{'':>4}  " + "".join(f"{'wall_s / cpu_s / rssMB':>26}" for _ in sources), flush=True)
    for q, tmpl in qs.items():
        if q in SKIP_QUERIES.get(name, ()):
            continue
        for th in a.threads.split():
            row = []
            for s, src in sources.items():
                runs = [sh(tmpl.format(s=src), threads=th, timed=True) for _ in range(a.runs)]
                wall = min(r[1] for r in runs)
                cpu = min(r[2] for r in runs)
                rss = int(statistics.median(r[0] for r in runs))
                if any(r[3] == "OOM-KILLED" for r in runs):
                    row.append(f"OOM @ {max(r[0] for r in runs)}MB/{wall:.0f}s")
                else:
                    row.append(f"{wall:6.2f} / {cpu:6.2f} / {rss:5d}")
                results.append(dict(dataset=name, query=q, threads=int(th), source=s, wall=wall, cpu=cpu, rss_mb=rss,
                                    result=runs[0][3], size_mb=size))
            print(f"{q:<12}{th:>4}  " + "".join(f"{x:>26}" for x in row), flush=True)
if a.json:
    json.dump(results, open(a.json, "w"), indent=1)
