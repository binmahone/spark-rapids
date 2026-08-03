#!/usr/bin/env python3
"""Run one frozen V8 workload with the v017 Serverless image."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


PROJECT = "rapids-spark"
REGION = "us-central1"
RUNTIME_REQUEST = "2.2"
EXPECTED_RUNTIME = "2.2.82"
STAGING_BUCKET = "rapids-spark-industry4-ideal-uc1-260731"
RUN_ROOT = "gs://rapids-spark-industry4-ideal-uc1-260731/runs/v017-suite-v001"
CONTAINER = (
    "us-central1-docker.pkg.dev/rapids-spark/rapids-serverless/"
    "snap-adaptive-v017-spark351-260803@"
    "sha256:0a9986cde640cb8a9d266a27ffaaead17509b6274d52c263f809af068bbdd4af"
)
BASE_WRAPPER = (
    "gs://rapids-spark-industry4-ideal-uc1-260731/assets/wrapper/"
    "d6a4f27540b2773ae6faf15093f4ea17aecbdfedc7efadf59a4a945e83405c6e/"
    "ideal_schema_candidate_wrapper.py"
)
REPEATED_WRAPPER = (
    "gs://rapids-spark-industry4-ideal-uc1-260731/assets/repeated-wrapper/"
    "74669e730d5e34e9606557423cef64ce93d3fc937aa27c87a02cb6ac5709e304/"
    "repeated_workload_wrapper.py"
)
TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELLED"}


WORKLOADS = {
    "paypal": {
        "module": "frozen_paypal_candidate",
        "sha256": "d76c8e6eb12a302e61196dd7a2aee16a3cf192df387c078d94142f0602b102c4",
        "uri": (
            "gs://rapids-spark-industry4-ideal-uc1-260731/assets/frozen/paypal/"
            "d76c8e6eb12a302e61196dd7a2aee16a3cf192df387c078d94142f0602b102c4/"
            "frozen_paypal_candidate.py"
        ),
        "repetitions": 3,
        "arguments": [
            "--input-uri",
            "gs://industry-usecase-uscentral1/paypal-v8-small/shared-input-v001/data",
            "--output-uri",
            "{run_root}/output",
            "--successor-partitions",
            "640",
            "--fanout",
            "5",
            "--application-name",
            "{run_id}",
        ],
        "properties": {
            "spark.sql.files.maxPartitionBytes": "536870912",
            "spark.sql.objectHashAggregate.sortBased.fallbackThreshold": "128",
            "spark.sql.shuffle.partitions": "640",
        },
        "output_prefixes": ["output"],
    },
    "walmart": {
        "module": "frozen_walmart_candidate",
        "sha256": "4e8c4dd55be24cc29f0064e5f4a744b6d203cb106ac9486fb159292460d815af",
        "uri": (
            "gs://rapids-spark-industry4-ideal-uc1-260731/assets/frozen/walmart/"
            "4e8c4dd55be24cc29f0064e5f4a744b6d203cb106ac9486fb159292460d815af/"
            "frozen_walmart_candidate.py"
        ),
        "repetitions": 3,
        "arguments": [
            "--input-uri",
            "gs://industry-usecase-uscentral1/walmart-impressions-v8/worker-a/shared/input-v001",
            "--first-output-uri",
            "{run_root}/first-material",
            "--second-output-uri",
            "{run_root}/second-material",
            "--final-output-uri",
            "{run_root}/final-output",
            "--base-rows",
            "2374150",
            "--extra-third-rows",
            "966754",
            "--first-material-output-files",
            "96",
            "--second-material-output-partitions",
            "552",
            "--exchange-partitions",
            "38",
            "--application-name",
            "{run_id}",
        ],
        "properties": {
            "spark.sql.adaptive.coalescePartitions.enabled": "false",
            "spark.sql.files.maxPartitionBytes": "536870912",
            "spark.sql.shuffle.partitions": "38",
        },
        "output_prefixes": ["first-material", "second-material", "final-output"],
    },
    "finra": {
        "module": "frozen_finra_candidate",
        "sha256": "99da8061c4e3a1644bb84b3cbddf55d354357b4afd49f0b78d60eaed47e62033",
        "uri": (
            "gs://rapids-spark-industry4-ideal-uc1-260731/assets/frozen/finra/"
            "99da8061c4e3a1644bb84b3cbddf55d354357b4afd49f0b78d60eaed47e62033/"
            "frozen_finra_candidate.py"
        ),
        "repetitions": 1,
        "arguments": [
            "--left-input-uri",
            "gs://industry-usecase-uscentral1/finra-v8/worker-a/shared_upstream/small-v001/left-orc",
            "--right-input-uri",
            "gs://industry-usecase-uscentral1/finra-v8/worker-a/shared_upstream/small-v001/right-parquet",
            "--first-output-uri",
            "{run_root}/first-material",
            "--second-output-uri",
            "{run_root}/second-material",
            "--final-output-uri",
            "{run_root}/final-output",
            "--result-uri",
            "{run_root}/candidate-result.json",
            "--shuffle-partitions",
            "64",
            "--second-output-partitions",
            "64",
            "--application-name",
            "{run_id}",
        ],
        "properties": {
            "spark.executor.memory": "12g",
            "spark.executorEnv.MALLOC_TRIM_THRESHOLD_": "131072",
            "spark.rapids.memory.host.offHeapLimit.size": "32G",
            "spark.rapids.shuffle.multiThreaded.writer.threads": "16",
            "spark.rapids.sql.batchSizeBytes": "256MB",
            "spark.sql.join.preferSortMergeJoin": "true",
            "spark.sql.shuffle.partitions": "64",
        },
        "output_prefixes": [
            "first-material",
            "second-material",
            "final-output",
            "candidate-result.json",
        ],
    },
}


BASE_PROPERTIES = {
    "dataproc.artifacts.remove": "rapids",
    "dataproc.sparkRapids.useDefaultJars": "false",
    "dataproc.tier": "premium",
    "spark.dataproc.driver.compute.tier": "premium",
    "spark.dataproc.driver.disk.size": "1500G",
    "spark.dataproc.driver.disk.tier": "premium",
    "spark.dataproc.engine": "default",
    "spark.dataproc.executor.compute.tier": "premium",
    "spark.dataproc.executor.disk.tier": "premium",
    "spark.dataproc.executor.resource.accelerator.type": "l4",
    "spark.driver.cores": "4",
    "spark.driver.memory": "8g",
    "spark.dynamicAllocation.enabled": "false",
    "spark.eventLog.enabled": "true",
    "spark.executor.cores": "16",
    "spark.executor.instances": "4",
    "spark.executor.memory": "8g",
    "spark.hadoop.fs.gs.perfio.transport": "http",
    "spark.io.compression.codec": "zstd",
    "spark.plugins": "com.nvidia.spark.SQLPlugin",
    "spark.rapids.filecache.enabled": "false",
    "spark.rapids.memory.host.offHeapLimit.enabled": "true",
    "spark.rapids.memory.host.offHeapLimit.size": "40G",
    "spark.rapids.memory.host.partialFileBufferMemoryThreshold": "0.6",
    "spark.rapids.memory.host.spillStorageSize": "32G",
    "spark.rapids.memory.pinnedPool.setCuioDefault": "true",
    "spark.rapids.memory.pinnedPool.size": "8g",
    "spark.rapids.perfio.gcs.enabled": "true",
    "spark.rapids.shuffle.kudo.serializer.enabled": "true",
    "spark.rapids.shuffle.kudo.serializer.read.mode": "GPU",
    "spark.rapids.shuffle.kudo.serializer.write.mode": "GPU",
    "spark.rapids.shuffle.mode": "MULTITHREADED",
    "spark.rapids.shuffle.multiThreaded.reader.threads": "32",
    "spark.rapids.shuffle.multiThreaded.writer.threads": "32",
    "spark.rapids.shuffle.multithreaded.adaptiveGpuCompression.maxConcurrentTasks": "16",
    "spark.rapids.shuffle.multithreaded.adaptiveGpuCompression.maxGpuSemaphoreWaiters": "16",
    "spark.rapids.shuffle.multithreaded.adaptiveGpuCompression.releaseAfterGpuPhase": "true",
    "spark.rapids.shuffle.multithreaded.skipMerge": "true",
    "spark.rapids.sql.batchSizeBytes": "1GB",
    "spark.rapids.sql.concurrentGpuTasks": "2",
    "spark.rapids.sql.concurrentGpuTasks.dynamic": "true",
    "spark.rapids.sql.enabled": "true",
    "spark.rapids.sql.exec.opTimeTrackingRDD.enabled": "true",
    "spark.rapids.sql.explain": "ALL",
    "spark.rapids.sql.maxConcurrentGpuTasks": "2",
    "spark.rapids.sql.metrics.level": "DEBUG",
    "spark.shuffle.compress": "true",
    "spark.shuffle.manager": "com.nvidia.spark.rapids.RapidsShuffleManager",
    "spark.shuffle.spill.compress": "true",
    "spark.speculation": "false",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.exchange.reuse": "false",
    "spark.task.resource.gpu.amount": "0.0625",
}


def run(command: list[str], check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if check and result.returncode:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def describe(batch: str) -> dict | None:
    result = run(
        [
            "gcloud", "dataproc", "batches", "describe", batch,
            "--project", PROJECT, "--region", REGION, "--format=json",
        ],
        check=False,
    )
    return json.loads(result.stdout) if result.returncode == 0 else None


def path_exists(uri: str) -> bool:
    return run(["gcloud", "storage", "ls", uri], check=False).returncode == 0


def properties(workload: str, adaptive: bool, run_id: str, run_root: str) -> dict:
    result = dict(BASE_PROPERTIES)
    result.update(WORKLOADS[workload]["properties"])
    result["spark.app.name"] = run_id
    result["spark.eventLog.dir"] = f"{run_root}/eventlogs/"
    result[
        "spark.rapids.shuffle.multithreaded.adaptiveGpuCompression.enabled"
    ] = str(adaptive).lower()
    return result


def wait_for_terminal(batch: str, evidence: Path) -> dict:
    history = []
    while True:
        status = describe(batch)
        if status is None:
            raise RuntimeError(f"batch disappeared: {batch}")
        history.append(
            {
                "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                "state": status.get("state"),
                "state_message": status.get("stateMessage"),
            }
        )
        status["_poll_history"] = history
        write_json(evidence / "terminal-status.json", status)
        if status.get("state") in TERMINAL_STATES:
            break
        time.sleep(5)
    deadline = time.monotonic() + 600
    while (
        status.get("state") == "SUCCEEDED"
        and not status.get("runtimeInfo", {}).get("approximateUsage")
        and time.monotonic() < deadline
    ):
        time.sleep(5)
        status = describe(batch) or status
        write_json(evidence / "terminal-status.json", status)
    return status


def calculate_cost(status: dict) -> dict:
    usage = status.get("runtimeInfo", {}).get("approximateUsage", {})
    required = {
        "milliDcuSeconds",
        "shuffleStorageGbSeconds",
        "milliAcceleratorSeconds",
    }
    if not required.issubset(usage):
        return {
            "usage": usage,
            "total_cost_usd_estimate": None,
            "reason": "approximate usage is incomplete",
        }
    dcu = int(usage["milliDcuSeconds"]) / 1000 / 3600 * 0.089
    shuffle = int(usage["shuffleStorageGbSeconds"]) / 3600 * 0.000136986
    accelerator = int(usage["milliAcceleratorSeconds"]) / 1000 / 3600 * 0.672048287
    return {
        "usage": usage,
        "dcu_cost_usd": dcu,
        "shuffle_cost_usd": shuffle,
        "accelerator_cost_usd": accelerator,
        "total_cost_usd_estimate": dcu + shuffle + accelerator,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", required=True, choices=tuple(WORKLOADS))
    parser.add_argument("--adaptive", required=True, choices=("on", "off"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--evidence-root", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    adaptive = args.adaptive == "on"
    workload = WORKLOADS[args.workload]
    run_root = f"{RUN_ROOT}/{args.workload}/{args.adaptive}/{args.run_id}"
    evidence = args.evidence_root / args.workload / args.adaptive / args.run_id
    if describe(args.run_id) is not None:
        raise ValueError(f"batch identity already exists: {args.run_id}")
    if path_exists(f"{run_root}/**"):
        raise ValueError(f"run root is not empty: {run_root}")

    repetitions = int(workload["repetitions"])
    wrapper = REPEATED_WRAPPER if repetitions > 1 else BASE_WRAPPER
    candidate_args = [
        "--workload", args.workload,
        "--frozen-module", workload["module"],
        "--frozen-sha256", workload["sha256"],
    ]
    if repetitions > 1:
        candidate_args.extend(["--repetitions", str(repetitions)])
    candidate_args.extend(
        value.format(run_id=args.run_id, run_root=run_root)
        for value in workload["arguments"]
    )
    spark_properties = properties(args.workload, adaptive, args.run_id, run_root)
    py_files = [workload["uri"]]
    if repetitions > 1:
        py_files.append(BASE_WRAPPER)
    command = [
        "gcloud", "dataproc", "batches", "submit", "pyspark", wrapper,
        "--project", PROJECT,
        "--region", REGION,
        "--version", RUNTIME_REQUEST,
        "--batch", args.run_id,
        "--request-id", str(uuid.uuid4()),
        "--subnet", "default",
        "--staging-bucket", STAGING_BUCKET,
        "--ttl", "3h",
        "--labels", f"arm=rapids,owner=industry4v17,study=v017-suite,workload={args.workload}",
        "--properties", ",".join(
            f"{key}={value}" for key, value in sorted(spark_properties.items())
        ),
        "--py-files", ",".join(py_files),
        "--container-image", CONTAINER,
        "--async", "--format=json", "--", *candidate_args,
    ]
    write_json(
        evidence / "submission-plan.json",
        {
            "workload": args.workload,
            "adaptive": adaptive,
            "run_id": args.run_id,
            "run_root": run_root,
            "runtime_request": RUNTIME_REQUEST,
            "expected_resolved_runtime": EXPECTED_RUNTIME,
            "container": CONTAINER,
            "repetitions": repetitions,
            "candidate_uri": workload["uri"],
            "candidate_sha256": workload["sha256"],
            "properties": spark_properties,
            "arguments": candidate_args,
            "command_sha256": hashlib.sha256("\0".join(command).encode()).hexdigest(),
        },
    )
    response = run(command)
    write_json(evidence / "submission-response.json", json.loads(response.stdout))
    status = wait_for_terminal(args.run_id, evidence)
    resolved_runtime = status.get("runtimeConfig", {}).get("version")
    write_json(
        evidence / "runtime-verification.json",
        {
            "expected": EXPECTED_RUNTIME,
            "observed": resolved_runtime,
            "matches": resolved_runtime == EXPECTED_RUNTIME,
        },
    )
    if resolved_runtime != EXPECTED_RUNTIME:
        raise RuntimeError(
            f"runtime resolution drift: expected {EXPECTED_RUNTIME}, "
            f"observed {resolved_runtime}"
        )
    write_json(evidence / "cost.json", calculate_cost(status))

    eventlog_inventory = run(
        ["gcloud", "storage", "ls", "--long", f"{run_root}/eventlogs/**"],
        check=False,
    )
    (evidence / "eventlog-inventory.txt").write_text(
        eventlog_inventory.stdout + eventlog_inventory.stderr
    )
    output_inventory = run(
        ["gcloud", "storage", "du", "--summarize", f"{run_root}/**"],
        check=False,
    )
    (evidence / "output-inventory.txt").write_text(
        output_inventory.stdout + output_inventory.stderr
    )

    if status.get("state") == "SUCCEEDED":
        for prefix in workload["output_prefixes"]:
            run(
                ["gcloud", "storage", "rm", "--recursive", f"{run_root}/{prefix}"],
                check=False,
            )
        write_json(
            evidence / "cleanup.json",
            {
                "owned_outputs_deleted": workload["output_prefixes"],
                "eventlogs_retained": True,
                "shared_inputs_modified": False,
            },
        )
    if status.get("state") != "SUCCEEDED":
        raise SystemExit(4)


if __name__ == "__main__":
    main()
