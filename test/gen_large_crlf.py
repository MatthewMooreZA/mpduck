#!/usr/bin/env python3
"""Generate test/data/large_crlf.rpt — a >1 MB mpfile with CRLF line endings.

This file is used to reproduce the infinite loop in FindDataEnd that occurs
when file_size - data_start > MAX_CHUNK (1 MB).  Run this script once to
regenerate the file; the output is committed so CI does not need Python.

Usage:
    python3 test/gen_large_crlf.py
"""

import os

ROWS = 56000  # ~1.1 MB total with CRLF (each data row ~20 bytes)
OUT = os.path.join(os.path.dirname(__file__), "data", "large_crlf.rpt")

with open(OUT, "wb") as f:
    f.write(b"VARIABLE_TYPES,T1,T20,N\r\n")
    f.write(b"!,SPCODE,VALUE\r\n")
    for i in range(ROWS):
        line = f"*,SP{i:06d},{i * 1.5:.1f}\r\n"
        f.write(line.encode())

print(f"Wrote {ROWS} rows to {OUT}  ({os.path.getsize(OUT):,} bytes)")
