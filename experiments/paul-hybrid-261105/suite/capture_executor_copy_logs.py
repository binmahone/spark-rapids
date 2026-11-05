#!/usr/bin/env python3
"""Persist UCX receive-copy diagnostics from transient Spark executor logs."""

import argparse
import glob
import os
import time


MARKER = "UCX_RECEIVE_D2D_COPY_"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--done-file", required=True)
    parser.add_argument("--poll-seconds", type=float, default=0.2)
    return parser.parse_args()


def main():
    args = parse_args()
    positions = {}
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "a", encoding="utf-8", buffering=1) as output:
        while not os.path.exists(args.done_file):
            pattern = os.path.join(args.worker_root, "worker-*", "work", "app-*", "*", "stderr")
            for path in glob.glob(pattern):
                try:
                    stat = os.stat(path)
                    key = (path, stat.st_ino)
                    position = positions.get(key, 0)
                    if stat.st_size < position:
                        position = 0
                    with open(path, "r", encoding="utf-8", errors="replace") as source:
                        source.seek(position)
                        for line in source:
                            if MARKER in line:
                                output.write(f"path={path} {line}")
                        positions[key] = source.tell()
                except FileNotFoundError:
                    continue
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
