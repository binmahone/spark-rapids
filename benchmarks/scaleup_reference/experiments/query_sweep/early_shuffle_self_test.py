#!/usr/bin/env python3
"""Check result parity and branch reachability for the separate action helper."""
import json


def verify(spark, app):
    collector = spark._jvm.com.nvidia.spark.rapids.benchmark.EarlyShuffleCollector
    digest = spark._jvm.com.nvidia.spark.rapids.benchmark.ResultDigest
    keys = ['spark.rapids.sql.enabled', 'spark.sql.shuffle.partitions',
            'spark.sql.autoBroadcastJoinThreshold']
    saved = {}
    for key in keys:
        value = spark._jsparkSession.conf().getOption(key)
        saved[key] = value.get() if value.isDefined() else None
    statements = [
        '''SELECT /*+ SHUFFLE_HASH(a,b) */ a.k, SUM(a.v*b.v) AS amount, COUNT(*) AS n
           FROM (SELECT CASE WHEN id % 7=0 THEN NULL ELSE id % 23 END k,
                 CAST(id % 19 AS DECIMAL(15,2)) v FROM range(0,8192,1,8)) a
           JOIN (SELECT CASE WHEN id % 11=0 THEN NULL ELSE id % 23 END k,
                 CAST(id % 13 AS DECIMAL(15,2)) v FROM range(0,1024,1,8)) b
           ON a.k=b.k GROUP BY a.k ORDER BY a.k''',
        '''SELECT CAST(NULL AS DECIMAL(38,4)) amount, DATE '2026-11-26' d,
                  array(1, NULL, 3) a, named_struct('s', 'x', 'n', 5L) b''',
        'SELECT id FROM range(0,0,1,8)']
    records = []
    try:
        spark.conf.set('spark.sql.shuffle.partitions', '8')
        spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')
        for index, sql in enumerate(statements):
            spark.conf.set('spark.rapids.sql.enabled', 'false')
            cpu = spark.sql(sql)._jdf.collect()
            expected = list(digest.compute(cpu))[2:4]
            spark.conf.set('spark.rapids.sql.enabled', 'true')
            for mode in (False, True):
                frame = spark.sql(sql)
                receipt = app / f'collector-test-{index}-{mode}.json'
                receipt.with_suffix('.plan.txt').write_text(
                    str(frame._jdf.queryExecution().executedPlan()))
                rows = collector.collect(frame._jdf, mode, str(receipt))
                assert list(digest.compute(rows))[2:4] == expected
                evidence = json.loads(receipt.read_text())
                assert evidence['status'] == 'passed'
                if index == 0 and mode:
                    assert 'Gpu' in str(frame._jdf.queryExecution().executedPlan())
                    assert len(evidence['submissions']) >= 2, evidence
                if not mode:
                    assert not evidence['submissions']
                records.append({'case': index, 'eager': mode, 'rows': len(rows),
                                'submissions': len(evidence['submissions']), 'passed': True})
    finally:
        for key, value in saved.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
    (app / 'collector-self-test.json').write_text(json.dumps({
        'status': 'passed', 'checks': records,
        'coverage': 'CPU/original/helper parity; null and duplicate join keys; decimals; nested rows; empty output; actual leaf submissions'}, indent=2) + '\n')
