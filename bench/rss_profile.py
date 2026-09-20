#!/usr/bin/env python3
"""Sample a DuckDB shell's RSS every 20 ms while it runs a query and print the memory profile.

Usage: bench/rss_profile.py <duckdb-binary> <threads> <sql> [label]
Prints peak RSS, time-to-peak, and a compact timeline (one sample per ~0.25 s).
"""
import subprocess, sys, time

duckdb, threads, sql = sys.argv[1], sys.argv[2], sys.argv[3]
label = sys.argv[4] if len(sys.argv) > 4 else ""
p = subprocess.Popen([duckdb, "-csv", "-noheader", "-c", f"SET threads={threads}; {sql};"],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
samples = []
t0 = time.time()
while p.poll() is None:
    try:
        with open(f"/proc/{p.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    samples.append((time.time() - t0, int(line.split()[1]) // 1024))
                    break
    except FileNotFoundError:
        break
    time.sleep(0.02)
if not samples:
    sys.exit("no samples")
peak_t, peak = max(((t, r) for t, r in samples), key=lambda x: x[1])[0], max(r for _, r in samples)
timeline, next_t = [], 0.0
for t, r in samples:
    if t >= next_t:
        timeline.append(str(r))
        next_t += 0.25
print(f"{label:<28} peak {peak:4d} MB @ {peak_t:5.2f}s  total {samples[-1][0]:5.2f}s | " + " ".join(timeline))
