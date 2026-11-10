#!/usr/bin/env python3
"""Summarize measured I/O diagnostics without inferring Spark query latency."""
import argparse
import json
from pathlib import Path
import statistics

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("root")
args = parser.parse_args()
root = Path(args.root)
rows = []
for path in sorted(root.glob("**/*.jsonl")):
    if "trace" in path.name:
        continue
    records = [json.loads(line) for line in path.read_text().splitlines()]
    provenance = next((r for r in records if r.get("kind") == "provenance"), None)
    measured = [r for r in records if r.get("kind") == "measurement"]
    if provenance is None or not measured:
        continue
    assert all(r["all_bytes_equal"] for r in measured)
    steady = measured[1:] or measured
    seconds = statistics.mean(r["seconds"] for r in steady)
    rows.append({"case": str(path.relative_to(root)), "backend": provenance["backend"],
                 "file_workers": provenance["file_workers"],
                 "threads": provenance.get("kvikio_threads") or
                 provenance.get("posix_pool_threads"),
                 "window": provenance["range_window"], "task_bytes": provenance["task_bytes"],
                 "cache_policy": provenance["cache_policy"],
                 "bytes": provenance["native_buffer_bytes"],
                 "steady_seconds": seconds,
                 "steady_GiB_per_second": provenance["native_buffer_bytes"] / 2 ** 30 / seconds,
                 "iterations": len(measured), "all_bytes_equal": True})
result = {"scope": "Independent native-host I/O diagnostic; no Spark, H2D, GPU decode, or GDS",
          "steady_definition": "Mean of iterations 2 and 3 when available",
          "cache_caveat": "DONTNEED is advisory; neither case establishes remote physical I/O",
          "cases": rows}
print(json.dumps(result, indent=2))
