#!/usr/bin/env python3

import argparse
import csv
import ctypes
import glob
import os
import socket
import time


class NvmlUtilization(ctypes.Structure):
    _fields_ = [("gpu", ctypes.c_uint), ("memory", ctypes.c_uint)]


class NvmlMemory(ctypes.Structure):
    _fields_ = [
        ("total", ctypes.c_ulonglong),
        ("free", ctypes.c_ulonglong),
        ("used", ctypes.c_ulonglong),
    ]


class NvmlSampler:
    def __init__(self):
        self.library = ctypes.CDLL("libnvidia-ml.so.1")
        self.library.nvmlInit_v2.restype = ctypes.c_int
        self.library.nvmlDeviceGetCount_v2.argtypes = [ctypes.POINTER(ctypes.c_uint)]
        self.library.nvmlDeviceGetCount_v2.restype = ctypes.c_int
        self.library.nvmlDeviceGetHandleByIndex_v2.argtypes = [
            ctypes.c_uint,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        self.library.nvmlDeviceGetHandleByIndex_v2.restype = ctypes.c_int
        self.library.nvmlDeviceGetUtilizationRates.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(NvmlUtilization),
        ]
        self.library.nvmlDeviceGetUtilizationRates.restype = ctypes.c_int
        self.library.nvmlDeviceGetMemoryInfo.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(NvmlMemory),
        ]
        self.library.nvmlDeviceGetMemoryInfo.restype = ctypes.c_int
        self.library.nvmlDeviceGetPowerUsage.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint),
        ]
        self.library.nvmlDeviceGetPowerUsage.restype = ctypes.c_int
        self.library.nvmlShutdown.restype = ctypes.c_int

        self._check(self.library.nvmlInit_v2(), "nvmlInit_v2")
        count = ctypes.c_uint()
        self._check(self.library.nvmlDeviceGetCount_v2(ctypes.byref(count)),
                    "nvmlDeviceGetCount_v2")
        self.handles = []
        for index in range(count.value):
            handle = ctypes.c_void_p()
            self._check(
                self.library.nvmlDeviceGetHandleByIndex_v2(index, ctypes.byref(handle)),
                "nvmlDeviceGetHandleByIndex_v2",
            )
            self.handles.append(handle)

    @staticmethod
    def _check(code, operation):
        if code != 0:
            raise RuntimeError(f"{operation} failed with NVML status {code}")

    def sample(self):
        samples = []
        for index, handle in enumerate(self.handles):
            utilization = NvmlUtilization()
            memory = NvmlMemory()
            power_mw = ctypes.c_uint()
            self._check(
                self.library.nvmlDeviceGetUtilizationRates(handle, ctypes.byref(utilization)),
                "nvmlDeviceGetUtilizationRates",
            )
            self._check(
                self.library.nvmlDeviceGetMemoryInfo(handle, ctypes.byref(memory)),
                "nvmlDeviceGetMemoryInfo",
            )
            power_status = self.library.nvmlDeviceGetPowerUsage(handle, ctypes.byref(power_mw))
            samples.append((
                index,
                utilization.gpu,
                utilization.memory,
                memory.used,
                power_mw.value if power_status == 0 else "",
            ))
        return samples

    def close(self):
        self.library.nvmlShutdown()


def read_integer(path):
    try:
        with open(path, encoding="ascii") as source:
            return int(source.read().strip())
    except (FileNotFoundError, PermissionError, ValueError):
        return ""


def read_meminfo():
    values = {}
    try:
        with open("/proc/meminfo", encoding="ascii") as source:
            for line in source:
                key, raw_value = line.split(":", 1)
                fields = raw_value.split()
                if fields:
                    values[key] = int(fields[0]) * 1024
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return values


def executor_memory():
    rss_values = []
    virtual_values = []
    for process_dir in glob.glob("/proc/[0-9]*"):
        try:
            with open(os.path.join(process_dir, "cmdline"), "rb") as source:
                command = source.read().replace(b"\0", b" ")
            if b"CoarseGrainedExecutorBackend" not in command:
                continue
            status = {}
            with open(os.path.join(process_dir, "status"), encoding="ascii") as source:
                for line in source:
                    if line.startswith(("VmRSS:", "VmSize:")):
                        key, raw_value = line.split(":", 1)
                        status[key] = int(raw_value.split()[0]) * 1024
            rss_values.append(status.get("VmRSS", 0))
            virtual_values.append(status.get("VmSize", 0))
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
            continue
    return (
        len(rss_values),
        sum(rss_values),
        max(rss_values, default=0),
        sum(virtual_values),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--done-file", required=True)
    parser.add_argument("--interval-seconds", type=float, default=0.5)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    node = socket.gethostname()
    gpu_path = os.path.join(args.output_dir, f"gpu-telemetry-{node}.csv")
    rdma_path = os.path.join(args.output_dir, f"rdma-telemetry-{node}.csv")
    host_path = os.path.join(args.output_dir, f"host-memory-telemetry-{node}.csv")
    counter_paths = sorted(glob.glob(
        "/sys/class/infiniband/*/ports/*/counters/port_*_data"))
    nvml = NvmlSampler()

    try:
        with open(gpu_path, "w", newline="", encoding="utf-8") as gpu_file, \
                open(rdma_path, "w", newline="", encoding="utf-8") as rdma_file, \
                open(host_path, "w", newline="", encoding="utf-8") as host_file:
            gpu_writer = csv.writer(gpu_file)
            rdma_writer = csv.writer(rdma_file)
            host_writer = csv.writer(host_file)
            gpu_writer.writerow([
                "epoch_ns", "node", "gpu_index", "gpu_util_pct", "memory_util_pct",
                "memory_used_bytes", "power_mw",
            ])
            rdma_writer.writerow([
                "epoch_ns", "node", "device", "port", "counter", "counter_units",
            ])
            host_writer.writerow([
                "epoch_ns", "node", "mem_available_bytes", "mem_free_bytes",
                "cached_bytes", "anon_pages_bytes", "committed_as_bytes",
                "executor_count", "executor_rss_bytes", "max_executor_rss_bytes",
                "executor_virtual_bytes",
            ])
            while not os.path.exists(args.done_file):
                epoch_ns = time.time_ns()
                for sample in nvml.sample():
                    gpu_writer.writerow((epoch_ns, node, *sample))
                for path in counter_paths:
                    parts = path.split("/")
                    rdma_writer.writerow((
                        epoch_ns,
                        node,
                        parts[4],
                        parts[6],
                        parts[-1],
                        read_integer(path),
                    ))
                memory = read_meminfo()
                host_writer.writerow((
                    epoch_ns,
                    node,
                    memory.get("MemAvailable", ""),
                    memory.get("MemFree", ""),
                    memory.get("Cached", ""),
                    memory.get("AnonPages", ""),
                    memory.get("Committed_AS", ""),
                    *executor_memory(),
                ))
                gpu_file.flush()
                rdma_file.flush()
                host_file.flush()
                time.sleep(args.interval_seconds)
    finally:
        nvml.close()


if __name__ == "__main__":
    main()
