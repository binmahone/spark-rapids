#!/usr/bin/env python3
"""Run selected TPC-H queries with bounded collection and fail-closed evidence."""

import csv
import gc
import hashlib
import json
import os
import sys
import threading
import time
import traceback
from pathlib import Path

from pyspark.sql import SparkSession


TABLES = (
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"
)
LARGE_RESULT_QUERIES = {11, 20}


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_query_profile():
    raw_path = os.environ.get("TPCH_QUERY_PROFILE_PATH", "").strip()
    if not raw_path:
        return None, None, None
    path = Path(raw_path)
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise ValueError(f"Unsupported query profile schema in {path}")
    return path, document, sha256_file(path)


def load_policy_manifest():
    raw_path = os.environ.get("TPCH_POLICY_MANIFEST_PATH", "").strip()
    if not raw_path:
        return None, None, None
    path = Path(raw_path)
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("schema_version") != 1:
        raise ValueError(f"Unsupported policy manifest schema in {path}")
    return path, document, sha256_file(path)


def load_catalog_statistics():
    raw_path = os.environ.get("TPCH_CATALOG_STATS_PATH", "").strip()
    if not raw_path:
        return None, None, None
    path = Path(raw_path)
    tables = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = (part.strip() for part in line.split("=", 1))
        parts = key.split(".")
        if len(parts) == 3 and parts[0] == "table" and parts[2] == "rowCount":
            tables.setdefault(parts[1], {"columns": {}})["row_count"] = int(value)
        elif len(parts) == 4 and parts[0] == "column":
            column = tables.setdefault(parts[1], {"columns": {}})["columns"].setdefault(
                parts[2], {}
            )
            if parts[3] == "distinctCount":
                column["distinct_count"] = int(value)
            elif parts[3] == "min":
                column["minimum"] = value
            elif parts[3] == "max":
                column["maximum"] = value
        elif len(parts) == 2 and parts[0] == "notNull":
            table = tables.setdefault(parts[1], {"columns": {}})
            for column_name in value.split(","):
                table["columns"].setdefault(column_name.strip(), {})["null_count"] = 0
    for table, statistics in tables.items():
        if statistics.get("row_count", -1) < 0:
            raise ValueError(f"Missing or invalid row count for {table} in {path}")
        for column, column_statistics in statistics["columns"].items():
            ndv = column_statistics.get("distinct_count")
            if ndv is not None and (ndv < 0 or ndv > statistics["row_count"]):
                raise ValueError(
                    f"Invalid distinct count for {table}.{column} in {path}: {ndv}"
                )
    return path, tables, sha256_file(path)


def install_catalog_statistics(spark, table, statistics, verify_plan_statistics):
    jvm = spark._jvm
    column_stats = jvm.java.util.HashMap()
    minimum_values = jvm.java.util.HashMap()
    maximum_values = jvm.java.util.HashMap()
    null_counts = jvm.java.util.HashMap()
    for column, column_statistics in sorted(statistics["columns"].items()):
        if "distinct_count" in column_statistics:
            column_stats.put(column, str(column_statistics["distinct_count"]))
        if "minimum" in column_statistics:
            minimum_values.put(column, column_statistics["minimum"])
        if "maximum" in column_statistics:
            maximum_values.put(column, column_statistics["maximum"])
        if "null_count" in column_statistics:
            null_counts.put(column, str(column_statistics["null_count"]))
    jvm.com.nvidia.spark.rapids.bench.CatalogStatsBridge.install(
        spark._jsparkSession,
        table,
        str(statistics["row_count"]),
        column_stats,
        minimum_values,
        maximum_values,
        null_counts,
    )

    installed_plan = spark.table(table)._jdf.queryExecution().optimizedPlan()
    installed = installed_plan.stats()
    if not verify_plan_statistics:
        return {
            "catalog_row_count": statistics["row_count"],
            "catalog_column_statistics": dict(sorted(statistics["columns"].items())),
            "plan_statistics_visible": False,
            "size_in_bytes": str(installed.sizeInBytes()),
        }
    if not installed.rowCount().isDefined():
        raise RuntimeError(f"Catalog row count is not visible in Catalyst for {table}")
    actual_rows = int(str(installed.rowCount().get()))
    if actual_rows != statistics["row_count"]:
        raise RuntimeError(
            f"Catalog row-count mismatch for {table}: "
            f"expected={statistics['row_count']} actual={actual_rows}"
        )
    actual_ndvs = {}
    output = installed_plan.output()
    for index in range(output.size()):
        attribute = output.apply(index)
        column = attribute.name()
        if column not in statistics["columns"]:
            continue
        column_stat = installed.attributeStats().get(attribute)
        expected_column = statistics["columns"][column]
        if not column_stat.isDefined():
            raise RuntimeError(
                f"Catalog column statistics are not visible in Catalyst for {table}.{column}"
            )
        actual_column = column_stat.get()
        if "distinct_count" in expected_column:
            if not actual_column.distinctCount().isDefined():
                raise RuntimeError(
                    f"Catalog distinct count is not visible in Catalyst for {table}.{column}"
                )
            actual_ndv = int(str(actual_column.distinctCount().get()))
            expected_ndv = expected_column["distinct_count"]
            if actual_ndv != expected_ndv:
                raise RuntimeError(
                    f"Catalog distinct-count mismatch for {table}.{column}: "
                    f"expected={expected_ndv} actual={actual_ndv}"
                )
            actual_ndvs[column] = actual_ndv
        if "null_count" in expected_column:
            if not actual_column.nullCount().isDefined():
                raise RuntimeError(
                    f"Catalog null count is not visible in Catalyst for {table}.{column}"
                )
            actual_null_count = int(str(actual_column.nullCount().get()))
            if actual_null_count != expected_column["null_count"]:
                raise RuntimeError(
                    f"Catalog null-count mismatch for {table}.{column}: "
                    f"expected={expected_column['null_count']} actual={actual_null_count}"
                )
    return {
        "row_count": actual_rows,
        "size_in_bytes": str(installed.sizeInBytes()),
        "column_ndvs": dict(sorted(actual_ndvs.items())),
        "plan_statistics_visible": True,
    }


def register_tables(spark, data_dir, catalog_statistics):
    installed = {}
    verify_plan_statistics = (
        spark.conf.get("spark.sql.cbo.enabled").strip().lower() == "true"
        or spark.conf.get("spark.sql.cbo.planStats.enabled").strip().lower() == "true"
    )
    for table in TABLES:
        table_path = data_dir / table
        if not table_path.is_dir():
            raise FileNotFoundError(table_path)
        if catalog_statistics is None:
            spark.read.parquet(str(table_path)).createOrReplaceTempView(table)
            continue
        if table not in catalog_statistics:
            raise ValueError(f"Catalog statistics are missing table {table}")
        escaped_path = str(table_path).replace("'", "''")
        spark.sql(f"DROP TABLE IF EXISTS `{table}`")
        spark.sql(f"CREATE TABLE `{table}` USING PARQUET LOCATION '{escaped_path}'")
        installed[table] = install_catalog_statistics(
            spark, table, catalog_statistics[table], verify_plan_statistics
        )
    return installed


def validate_uniform_configuration(spark, policy_path, policy_document):
    if policy_document is None:
        return
    expected = {
        key: str(value)
        for key, value in policy_document.get("uniform_configuration", {}).items()
        if key.startswith("spark.")
    }
    mismatches = []
    for key, expected_value in sorted(expected.items()):
        try:
            actual_value = spark.conf.get(key)
        except Exception:
            actual_value = "<unset>"
        if actual_value != expected_value:
            mismatches.append(
                f"{key}: expected={expected_value!r}, actual={actual_value!r}"
            )
    if mismatches:
        details = "\n".join(mismatches)
        raise RuntimeError(
            f"Runtime SparkConf does not match policy manifest {policy_path}:\n{details}"
        )


def selected_queries():
    raw = os.environ.get("TPCH_QUERIES", "all").strip().lower()
    if raw == "all":
        return list(range(1, 23))
    result = set()
    for value in raw.split(","):
        value = value.strip()
        if not value:
            continue
        if "-" in value:
            start, end = (int(part) for part in value.split("-", 1))
            result.update(range(start, end + 1))
        else:
            result.add(int(value))
    ordered = sorted(result)
    if not ordered or ordered[0] < 1 or ordered[-1] > 22:
        raise ValueError(f"Invalid TPCH_QUERIES={raw!r}")
    return ordered


def executor_count(spark):
    statuses = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
    return max(0, statuses.size() - 1)


def json_rows(rows):
    return [[None if value is None else str(value) for value in row] for row in rows]


def write_summary(path, records):
    if not records:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)


def logical_plan_stats(plan):
    """Return a recursive snapshot of Catalyst estimates for one optimized plan."""
    statistics = plan.stats()
    row_count = None
    if statistics.rowCount().isDefined():
        row_count = str(statistics.rowCount().get())
    attribute_statistics = {}
    output = plan.output()
    for index in range(output.size()):
        attribute = output.apply(index)
        column_stat = statistics.attributeStats().get(attribute)
        if not column_stat.isDefined():
            continue
        value = column_stat.get()
        attribute_statistics[f"{attribute.name()}#{attribute.exprId().id()}"] = {
            "distinct_count": (
                str(value.distinctCount().get())
                if value.distinctCount().isDefined()
                else None
            ),
            "null_count": (
                str(value.nullCount().get())
                if value.nullCount().isDefined()
                else None
            ),
            "minimum": str(value.min().get()) if value.min().isDefined() else None,
            "maximum": str(value.max().get()) if value.max().isDefined() else None,
        }
    children = plan.children()
    return {
        "node": plan.nodeName(),
        "summary": plan.simpleString(1000),
        "row_count": row_count,
        "size_in_bytes": str(statistics.sizeInBytes()),
        "statistics": statistics.toString(),
        "attribute_statistics": attribute_statistics,
        "children": [
            logical_plan_stats(children.apply(index))
            for index in range(children.size())
        ],
    }


def main():
    data_dir = Path(os.environ["TPCH_DATA_DIR"])
    query_dir = Path(os.environ["TPCH_QUERY_DIR"])
    output_dir = Path(os.environ["TPCH_OUTPUT_DIR"])
    expected_executors = int(os.environ["TPCH_EXPECTED_EXECUTORS"])
    iterations = int(os.environ.get("TPCH_QUERY_ITERATIONS", "1"))
    timeout_seconds = int(os.environ.get("TPCH_QUERY_TIMEOUT_SECONDS", "1800"))
    stop_query_after_failure = os.environ.get(
        "TPCH_STOP_QUERY_AFTER_FAILURE", "false"
    ).strip().lower() == "true"
    abort_suite_after_failure = os.environ.get(
        "TPCH_ABORT_SUITE_AFTER_FAILURE", "false"
    ).strip().lower() == "true"
    queries = selected_queries()
    profile_path, query_profile, query_profile_sha256 = load_query_profile()
    policy_path, policy_document, policy_sha256 = load_policy_manifest()
    catalog_stats_path, catalog_statistics, catalog_stats_sha256 = (
        load_catalog_statistics()
    )
    result_sink_policy = os.environ.get("TPCH_RESULT_SINK_POLICY", "legacy").strip()
    persist_full_result = os.environ.get(
        "TPCH_PERSIST_FULL_RESULT", "false"
    ).strip().lower() == "true"
    result_digest_enabled = os.environ.get(
        "TPCH_RESULT_DIGEST_ENABLED", "false"
    ).strip().lower() == "true"
    capture_logical_stats = os.environ.get(
        "TPCH_CAPTURE_LOGICAL_STATS", "false"
    ).strip().lower() == "true"
    full_result_max_rows = int(os.environ.get("TPCH_FULL_RESULT_MAX_ROWS", "1000"))
    if full_result_max_rows < 1:
        raise ValueError("TPCH_FULL_RESULT_MAX_ROWS must be positive")
    if result_sink_policy not in {
        "legacy",
        "jvm-driver-collect-all",
        "distributed-jvm-drain-all",
    }:
        raise ValueError(
            "TPCH_RESULT_SINK_POLICY must be legacy, jvm-driver-collect-all, "
            "or distributed-jvm-drain-all"
        )
    output_dir.mkdir(parents=True, exist_ok=True)

    if query_profile is not None:
        missing = [query for query in queries if str(query) not in query_profile["queries"]]
        if missing:
            raise ValueError(f"Query profile {profile_path} is missing queries {missing}")

    spark = SparkSession.builder.appName("mahonem-cudf-spark-ucx-tpch-sf30000").getOrCreate()
    validate_uniform_configuration(spark, policy_path, policy_document)
    spark.sparkContext.setLogLevel("WARN")
    spark._jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
        "org.apache.spark.sql.rapids.GpuCatalogCleanupListener",
        spark._jvm.org.apache.logging.log4j.Level.INFO,
    )

    deadline = time.time() + 300
    observed = executor_count(spark)
    while observed < expected_executors and time.time() < deadline:
        print(f"Waiting for executors: {observed}/{expected_executors}", flush=True)
        time.sleep(5)
        observed = executor_count(spark)
    if observed != expected_executors:
        raise RuntimeError(f"Expected {expected_executors} executors, observed {observed}")

    installed_catalog_statistics = register_tables(spark, data_dir, catalog_statistics)

    metadata = {
        "application_id": spark.sparkContext.applicationId,
        "artifact": os.environ["TPCH_ENGINE_ARTIFACT"],
        "artifact_sha256": os.environ["TPCH_ENGINE_ARTIFACT_SHA256"],
        "data_dir": str(data_dir),
        "engine": os.environ["TPCH_ENGINE"],
        "engine_version": os.environ["TPCH_ENGINE_VERSION"],
        "experiment_profile": os.environ.get("TPCH_EXPERIMENT_PROFILE", "baseline"),
        "executors": observed,
        "queries": queries,
        "query_dir": str(query_dir),
        "query_iterations": iterations,
        "query_timeout_seconds": timeout_seconds,
        "stop_query_after_failure": stop_query_after_failure,
        "abort_suite_after_failure": abort_suite_after_failure,
        "query_profile": str(profile_path) if profile_path is not None else None,
        "query_profile_sha256": query_profile_sha256,
        "query_profile_document": query_profile,
        "policy_manifest": str(policy_path) if policy_path is not None else None,
        "policy_manifest_sha256": policy_sha256,
        "policy_manifest_document": policy_document,
        "catalog_stats_path": (
            str(catalog_stats_path) if catalog_stats_path is not None else None
        ),
        "catalog_stats_sha256": catalog_stats_sha256,
        "installed_catalog_statistics": installed_catalog_statistics,
        "result_sink_policy": result_sink_policy,
        "persist_full_result": persist_full_result,
        "result_digest_enabled": result_digest_enabled,
        "capture_logical_stats": capture_logical_stats,
        "full_result_max_rows": full_result_max_rows,
        "canonical_query_sha256": {
            str(query): sha256_file(query_dir / f"q{query}.sql") for query in queries
        },
        "slurm_job_id": os.environ["SLURM_JOB_ID"],
        "slurm_job_nodelist": os.environ["SLURM_JOB_NODELIST"],
        "spark_conf": dict(spark.sparkContext.getConf().getAll()),
        "spark_version": spark.version,
        "strict_gpu_only": True,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8"
    )

    records = []
    failures = 0
    abort_suite = False
    for query in queries:
        source_job = ""
        active_profile = {}
        if query_profile is None:
            sql_path = query_dir / f"q{query}.sql"
        else:
            query_entry = query_profile["queries"][str(query)]
            sql_path = Path(query_entry["sql_path"])
            expected_sql_sha256 = query_entry["sql_sha256"]
            actual_sql_sha256 = sha256_file(sql_path)
            if actual_sql_sha256 != expected_sql_sha256:
                raise RuntimeError(
                    f"Q{query} SQL hash mismatch: expected={expected_sql_sha256} "
                    f"actual={actual_sql_sha256} path={sql_path}"
                )
            source_job = str(query_entry["source_job"])
            for key, value in query_entry["spark_conf"].items():
                spark.conf.set(key, value)
            active_profile = {
                key: spark.conf.get(key) for key in query_entry["spark_conf"]
            }
            (output_dir / f"q{query}.active-profile.json").write_text(
                json.dumps(
                    {
                        "source_job": source_job,
                        "sql_path": str(sql_path),
                        "sql_sha256": actual_sql_sha256,
                        "spark_conf": active_profile,
                    },
                    indent=2,
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            print(
                f"[TPC-H] Q{query} source_job={source_job} profile={active_profile}",
                flush=True,
            )
        sql_text = sql_path.read_text(encoding="utf-8")
        for iteration in range(1, iterations + 1):
            print(f"[TPC-H] Q{query} iteration={iteration} START", flush=True)
            started_epoch_ns = time.time_ns()
            started = time.perf_counter()
            status = "PASS"
            error = ""
            initial_plan = ""
            plan = ""
            optimized_plan = ""
            optimized_plan_statistics = None
            rows = []
            row_count = None
            sink = "collect"
            result_document = None
            query_execution_elapsed = None
            timed_out = threading.Event()
            job_group = f"tpch-q{query}-i{iteration}-{started_epoch_ns}"

            def cancel_query():
                timed_out.set()
                print(
                    f"[TPC-H] Q{query} iteration={iteration} TIMEOUT after {timeout_seconds}s",
                    file=sys.stderr,
                    flush=True,
                )
                spark.sparkContext.cancelJobGroup(job_group)

            spark.sparkContext.setJobGroup(
                job_group, f"TPC-H Q{query} iteration {iteration}", interruptOnCancel=True
            )
            timer = threading.Timer(timeout_seconds, cancel_query)
            timer.daemon = True
            timer.start()
            try:
                frame = spark.sql(sql_text)
                if capture_logical_stats:
                    catalyst_plan = frame._jdf.queryExecution().optimizedPlan()
                    optimized_plan = catalyst_plan.treeString()
                    optimized_plan_statistics = logical_plan_stats(catalyst_plan)
                initial_plan = frame._jdf.queryExecution().executedPlan().toString()
                is_adaptive = "AdaptiveSparkPlan" in initial_plan
                if not is_adaptive and "Gpu" not in initial_plan:
                    raise RuntimeError(
                        f"Q{query} produced no GPU operators:\n{initial_plan}"
                    )
                if result_sink_policy == "distributed-jvm-drain-all":
                    sink = "distributed-jvm-drain"
                    row_count = frame._jdf.queryExecution().toRdd().count()
                    query_execution_elapsed = time.perf_counter() - started
                    result_document = {
                        "sink": sink,
                        "rows": row_count,
                        "full_result_requested": False,
                        "full_result_persisted": False,
                        "diagnostic_only": True,
                    }
                elif result_sink_policy == "jvm-driver-collect-all" or query in LARGE_RESULT_QUERIES:
                    sink = "jvm-driver-collect"
                    java_rows = frame._jdf.collect()
                    query_execution_elapsed = time.perf_counter() - started
                    row_count = len(java_rows)
                    sample_count = min(10, row_count)
                    full_result_fits = persist_full_result and row_count <= full_result_max_rows
                    result_document = {
                        "sink": sink,
                        "rows": row_count,
                        "first_rows": [str(java_rows[i]) for i in range(sample_count)],
                        "last_rows": [
                            str(java_rows[row_count - sample_count + i])
                            for i in range(sample_count)
                        ],
                        "full_result_requested": persist_full_result,
                        "full_result_max_rows": full_result_max_rows,
                        "full_result_persisted": full_result_fits,
                    }
                    if result_digest_enabled:
                        digest_started = time.perf_counter()
                        digests = spark._jvm.com.nvidia.spark.rapids.benchmark.ResultDigest.compute(
                            java_rows
                        )
                        if int(digests[0]) != row_count:
                            raise RuntimeError(
                                f"Q{query} result digest row count mismatch: "
                                f"collected={row_count} digested={digests[0]}"
                            )
                        result_document.update({
                            "ordered_sha256": str(digests[1]),
                            "multiset_sha256_sum": str(digests[2]),
                            "multiset_sha256_xor": str(digests[3]),
                            "digest_seconds": round(
                                time.perf_counter() - digest_started, 6
                            ),
                        })
                        if len(digests) >= 7:
                            result_document.update({
                                "normalized_ordered_sha256": str(digests[4]),
                                "normalized_multiset_sha256_sum": str(digests[5]),
                                "normalized_multiset_sha256_xor": str(digests[6]),
                            })
                    if full_result_fits:
                        result_document["all_rows"] = [str(row) for row in java_rows]
                    elif persist_full_result:
                        result_document["full_result_omitted_reason"] = (
                            f"row count {row_count} exceeds limit {full_result_max_rows}"
                        )
                    del java_rows
                else:
                    rows = frame.collect()
                    query_execution_elapsed = time.perf_counter() - started
                    row_count = len(rows)
                plan = frame._jdf.queryExecution().executedPlan().toString()
                if "Gpu" not in plan:
                    raise RuntimeError(
                        f"Q{query} final plan produced no GPU operators:\n{plan}"
                    )
            except Exception:
                status = "TIMEOUT" if timed_out.is_set() else "FAIL"
                failures += 1
                error = traceback.format_exc()
                print(error, file=sys.stderr, flush=True)
            finally:
                timer.cancel()
                spark.sparkContext._jsc.setLocalProperty("spark.jobGroup.id", None)

            elapsed = query_execution_elapsed
            if elapsed is None:
                elapsed = time.perf_counter() - started
            prefix = f"q{query}.iter{iteration}"
            (output_dir / f"{prefix}.initial-plan.txt").write_text(
                initial_plan, encoding="utf-8"
            )
            if capture_logical_stats:
                (output_dir / f"{prefix}.optimized-plan.txt").write_text(
                    optimized_plan, encoding="utf-8"
                )
                (output_dir / f"{prefix}.logical-plan-stats.json").write_text(
                    json.dumps(optimized_plan_statistics, indent=2), encoding="utf-8"
                )
            (output_dir / f"{prefix}.plan.txt").write_text(plan, encoding="utf-8")
            (output_dir / f"{prefix}.result.json").write_text(
                json.dumps(result_document if result_document is not None else json_rows(rows), indent=2),
                encoding="utf-8",
            )
            if error:
                (output_dir / f"{prefix}.error.txt").write_text(error, encoding="utf-8")
            record = {
                "query": query,
                "iteration": iteration,
                "source_job": source_job,
                "status": status,
                "seconds": round(elapsed, 6),
                "started_epoch_ns": started_epoch_ns,
                "ended_epoch_ns": time.time_ns(),
                "rows": row_count if row_count is not None else "",
                "sink": sink,
                "gpu_plan": "Gpu" in plan,
                "error": error.splitlines()[-1] if error else "",
            }
            records.append(record)
            write_summary(output_dir / "summary.csv", records)
            print(
                f"[TPC-H] Q{query} iteration={iteration} {status} "
                f"{elapsed:.3f}s rows={record['rows']} sink={sink} gpu={record['gpu_plan']}",
                flush=True,
            )
            del rows
            gc.collect()
            spark._jvm.System.gc()
            if status != "PASS" and abort_suite_after_failure:
                print(
                    f"[TPC-H] Q{query} aborting suite after {status}; "
                    "the next query requires a fresh Spark application",
                    flush=True,
                )
                abort_suite = True
                break
            if status != "PASS" and stop_query_after_failure:
                print(
                    f"[TPC-H] Q{query} skipping remaining iterations after {status}",
                    flush=True,
                )
                break
        if abort_suite:
            break

    spark.stop()
    print(f"Completed {len(records)} executions; failures={failures}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
