#!/usr/bin/env python3
"""Read identical projected Parquet ranges with POSIX or KvikIO into native host memory.

This is an I/O diagnostic, not a Spark query benchmark. Timings exclude allocation,
footer parsing, and full-byte verification. Optional file-local cache eviction is
advisory; it does not establish physical storage traffic.
"""

import argparse
import concurrent.futures
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import sys
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def emit(value):
    print(json.dumps(value, sort_keys=True), flush=True)


def coalesce(ranges):
    merged = []
    for offset, length in sorted(ranges):
        if merged and offset < merged[-1][0] + merged[-1][1]:
            raise ValueError("Overlapping column chunks")
        if merged and offset == merged[-1][0] + merged[-1][1]:
            merged[-1][1] += length
        else:
            merged.append([offset, length])
    return merged


def plan(args):
    files = []
    for pathname in args.files:
        path = Path(pathname).resolve(strict=True)
        metadata = pq.read_metadata(path)
        columns = set(args.columns.split(","))
        found = {metadata.schema.column(i).path for i in range(metadata.num_columns)}
        if columns - found:
            raise ValueError(f"Missing columns in {path}: {columns - found}")
        ranges = []
        for group_id in range(metadata.num_row_groups):
            group = metadata.row_group(group_id)
            for i in range(group.num_columns):
                column = group.column(i)
                if column.path_in_schema not in columns:
                    continue
                offsets = [column.data_page_offset]
                if column.has_dictionary_page:
                    offsets.append(column.dictionary_page_offset)
                offset = min(offsets)
                length = column.total_compressed_size
                if offset < 4 or length <= 0 or offset + length > path.stat().st_size:
                    raise ValueError(f"Invalid column range: {path}, {offset}, {length}")
                ranges.append([offset, length])
        files.append({"path": str(path), "size": path.stat().st_size,
                      "mtime_ns": path.stat().st_mtime_ns,
                      "row_groups": metadata.num_row_groups,
                      "column_chunk_count": len(ranges), "ranges": coalesce(ranges)})
    document = {"format": 1, "columns": args.columns.split(","), "files": files,
                "selection": "Projected column chunks from all row groups; no predicate pruning"}
    Path(args.output).write_text(json.dumps(document, indent=2) + "\n")
    emit({"kind": "plan", "output": args.output, "files": len(files),
          "ranges": sum(len(f["ranges"]) for f in files),
          "bytes": sum(n for f in files for _, n in f["ranges"])})


def fill_posix(fd, target, file_offset):
    view = memoryview(target)
    done = 0
    while done < len(view):
        count = os.preadv(fd, [view[done:]], file_offset + done)
        if count == 0:
            raise EOFError(f"Short POSIX read at {file_offset + done}")
        done += count
    return done


def target_slices(file, buffer):
    output_offset = 0
    for file_offset, length in file["ranges"]:
        yield file_offset, length, buffer[output_offset:output_offset + length]
        output_offset += length


def read_file_posix(file, buffer):
    total = 0
    fd = os.open(file["path"], os.O_RDONLY)
    try:
        for offset, _, target in target_slices(file, buffer):
            total += fill_posix(fd, target, offset)
    finally:
        os.close(fd)
    return total


def read_file_kvikio(file, buffer, kvikio, window, task_bytes):
    pending = []
    total = 0
    first_error = None
    # Every submitted future is drained before closing the file or releasing its buffer.
    with kvikio.CuFile(file["path"], "r") as source:
        try:
            for offset, length, target in target_slices(file, buffer):
                future = source.pread(target, size=length, file_offset=offset,
                                      task_size=task_bytes or length)
                pending.append((future, length))
                if len(pending) >= window:
                    future, expected = pending.pop(0)
                    count = future.get()
                    if count != expected:
                        raise EOFError(f"Short KvikIO read: {count} != {expected}")
                    total += count
        except BaseException as error:
            first_error = error
        finally:
            for future, expected in pending:
                try:
                    count = future.get()
                    if count != expected:
                        raise EOFError(f"Short KvikIO read: {count} != {expected}")
                    total += count
                except BaseException as error:
                    if first_error is None:
                        first_error = error
        if first_error is not None:
            raise first_error
    return total


def read_file_posix_parallel(file, buffer, pool, window):
    pending = []
    total = 0
    first_error = None
    fd = os.open(file["path"], os.O_RDONLY)
    try:
        try:
            for offset, length, target in target_slices(file, buffer):
                pending.append((pool.submit(fill_posix, fd, target, offset), length))
                if len(pending) >= window:
                    future, expected = pending.pop(0)
                    count = future.result()
                    if count != expected:
                        raise EOFError("Short concurrent POSIX read")
                    total += count
        except BaseException as error:
            first_error = error
        finally:
            for future, expected in pending:
                try:
                    count = future.result()
                    if count != expected:
                        raise EOFError("Short concurrent POSIX read")
                    total += count
                except BaseException as error:
                    if first_error is None:
                        first_error = error
        if first_error is not None:
            raise first_error
    finally:
        os.close(fd)
    return total


def digest(buffer):
    return hashlib.sha256(memoryview(buffer)).hexdigest()


def run(args):
    document_bytes = Path(args.manifest).read_bytes()
    document = json.loads(document_bytes)
    files = document["files"]
    if not files:
        raise ValueError("No files")
    total_bytes = sum(n for f in files for _, n in f["ranges"])
    if total_bytes > args.max_buffer_gib * 2 ** 30:
        raise ValueError("Requested native buffers exceed --max-buffer-gib")
    for f in files:
        stat = Path(f["path"]).stat()
        if stat.st_size != f["size"] or stat.st_mtime_ns != f["mtime_ns"]:
            raise ValueError(f"Input changed since manifest creation: {f['path']}")
    buffers = [np.full(sum(n for _, n in f["ranges"]), 0xA5, dtype=np.uint8) for f in files]
    versions = {p: importlib.metadata.version(p) for p in ["numpy", "pyarrow", "kvikio-cu13",
                                                         "libkvikio-cu13"]}
    kvikio = None
    if args.backend == "kvikio":
        os.environ["KVIKIO_COMPAT_MODE"] = "ON"
        os.environ["KVIKIO_NTHREADS"] = str(args.threads)
        os.environ["KVIKIO_AUTO_DIRECT_IO_READ"] = "OFF"
        import kvikio
        import kvikio.defaults
        kvikio.defaults.set({"compat_mode": kvikio.CompatMode.ON, "num_threads": args.threads,
                             "auto_direct_io_read": False})
        versions["kvikio_git_commit"] = kvikio.__git_commit__
    emit({"kind": "provenance", "host": platform.node(), "machine": platform.machine(),
          "backend": args.backend, "versions": versions, "native_buffer_bytes": total_bytes,
          "manifest_sha256": hashlib.sha256(document_bytes).hexdigest(),
          "file_workers": args.file_workers,
          "kvikio_threads": args.threads if args.backend == "kvikio" else None,
          "posix_pool_threads": args.threads if args.backend == "posix-parallel" else None,
          "range_window": args.window, "task_bytes": args.task_bytes,
          "cache_policy": args.cache_policy,
          "transport": "POSIX to native host memory; no GDS or H2D",
          "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
          "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
    expected_digests = []
    # Independent byte reference, outside the measured region. This warms the selected ranges.
    for file, buffer in zip(files, buffers):
        read_file_posix(file, buffer)
        expected_digests.append(digest(buffer))
        buffer.fill(0xA5)
    for iteration in range(args.repeats):
        for buffer in buffers:
            buffer.fill(0xA5)
        if args.cache_policy == "advise-drop":
            for file in files:
                fd = os.open(file["path"], os.O_RDONLY)
                try:
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                finally:
                    os.close(fd)
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as posix_pool, \
                concurrent.futures.ThreadPoolExecutor(max_workers=args.file_workers) as scheduler:
            start = time.perf_counter_ns()
            if args.backend == "kvikio":
                futures = [scheduler.submit(read_file_kvikio, f, b, kvikio,
                                            args.window, args.task_bytes)
                           for f, b in zip(files, buffers)]
            elif args.backend == "posix-parallel":
                futures = [scheduler.submit(read_file_posix_parallel, f, b, posix_pool, args.window)
                           for f, b in zip(files, buffers)]
            else:
                futures = [scheduler.submit(read_file_posix, f, b) for f, b in zip(files, buffers)]
            # The scheduler waits for all file readers on failure before buffers leave scope.
            counts = [future.result() for future in futures]
            elapsed = time.perf_counter_ns() - start
        actual_digests = [digest(buffer) for buffer in buffers]
        if actual_digests != expected_digests or sum(counts) != total_bytes:
            raise AssertionError("Native-host bytes differ from independent POSIX reference")
        emit({"kind": "measurement", "iteration": iteration + 1,
              "seconds": elapsed / 1e9, "GiB_per_second": total_bytes / 2 ** 30 / (elapsed / 1e9),
              "bytes": total_bytes, "all_bytes_equal": True,
              "sha256": actual_digests})


def fixture(args):
    output = Path(args.directory)
    output.mkdir(parents=True, exist_ok=False)
    random = np.random.default_rng(261110)
    for i in range(args.files):
        table = pa.table({
            "l_quantity": random.uniform(1, 50, args.rows),
            "l_extendedprice": random.uniform(1, 100000, args.rows),
            "l_discount": random.uniform(0, 0.1, args.rows),
            "l_shipdate": pa.array(random.integers(8000, 11000, args.rows, dtype=np.int32),
                                   type=pa.date32()),
            "unused": random.integers(0, 2 ** 30, args.rows, dtype=np.int64)})
        pq.write_table(table, output / f"part-{i}.parquet", row_group_size=125000,
                       compression="snappy", use_dictionary=False)
    emit({"kind": "fixture", "path": str(output), "files": args.files, "rows": args.rows})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    p = commands.add_parser("plan")
    p.add_argument("--files", nargs="+", required=True)
    p.add_argument("--columns", default="l_quantity,l_extendedprice,l_discount,l_shipdate")
    p.add_argument("--output", required=True)
    p.set_defaults(action=plan)
    p = commands.add_parser("run")
    p.add_argument("--manifest", required=True)
    p.add_argument("--backend", choices=["posix", "posix-parallel", "kvikio"], required=True)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--window", type=int, default=4)
    p.add_argument("--task-bytes", type=int, default=0,
                   help="0 keeps each column range as one KvikIO task")
    p.add_argument("--file-workers", type=int, default=1)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--max-buffer-gib", type=float, default=8)
    p.add_argument("--cache-policy", choices=["warm", "advise-drop"], default="warm",
                   help="File-local DONTNEED is advisory, never a claim of cold physical I/O")
    p.set_defaults(action=run)
    p = commands.add_parser("fixture")
    p.add_argument("--directory", required=True)
    p.add_argument("--files", type=int, default=2)
    p.add_argument("--rows", type=int, default=1000000)
    p.set_defaults(action=fixture)
    args = parser.parse_args()
    for name in ["threads", "window", "file_workers", "repeats", "rows", "files"]:
        value = getattr(args, name, None)
        if isinstance(value, int) and value <= 0:
            parser.error(f"{name} must be positive")
    if getattr(args, "task_bytes", 0) < 0:
        parser.error("task-bytes must be nonnegative")
    args.action(args)


if __name__ == "__main__":
    main()
