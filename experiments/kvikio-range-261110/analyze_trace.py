#!/usr/bin/env python3
"""Check actual syscall overlap; queued futures are not evidence of concurrent I/O."""
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("trace")
parser.add_argument("--require-same-fd-overlap", action="store_true")
parser.add_argument("--max-same-fd", type=int)
args = parser.parse_args()
calls = [json.loads(line) for line in Path(args.trace).read_text().splitlines()]
events = []
for call in calls:
    if call["returned"] != call["requested"]:
        raise AssertionError(f"Incomplete observed read: {call}")
    events.extend([(call["start_ns"], 1, call["pid"], call["fd"]),
                   (call["end_ns"], -1, call["pid"], call["fd"])])
active = peak = peak_fd = 0
per_fd = {}
for _, delta, pid, fd in sorted(events):
    key = (pid, fd)
    active += delta
    per_fd[key] = per_fd.get(key, 0) + delta
    peak = max(peak, active)
    peak_fd = max(peak_fd, per_fd[key])
result = {"calls": len(calls), "actual_peak_overlapping_syscalls": peak,
          "actual_peak_same_fd_syscalls": peak_fd,
          "native_thread_ids": sorted({c["tid"] for c in calls}),
          "note": "Diagnostic run only; logging overhead excludes it from performance results"}
print(json.dumps(result, indent=2))
if args.require_same_fd_overlap and peak_fd < 2:
    raise SystemExit("No same-file overlap demonstrated")
if args.max_same_fd is not None and peak_fd > args.max_same_fd:
    raise SystemExit("Serial control unexpectedly overlapped reads")
