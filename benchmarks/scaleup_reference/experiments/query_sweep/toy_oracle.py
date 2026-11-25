#!/usr/bin/env python3
"""Try duplicate/null counterexamples against the SQL probes on Spark CPU."""
import argparse
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
import json
from pathlib import Path

from pyspark.sql import SparkSession
from variants import ORDER, original, targeted

SCHEMAS = {
    'region': 'r_regionkey BIGINT, r_name STRING, r_comment STRING',
    'nation': 'n_nationkey BIGINT, n_name STRING, n_regionkey BIGINT, n_comment STRING',
    'customer': 'c_custkey BIGINT, c_name STRING, c_address STRING, c_nationkey BIGINT, c_phone STRING, c_acctbal DECIMAL(15,2), c_mktsegment STRING, c_comment STRING',
    'supplier': 's_suppkey BIGINT, s_name STRING, s_address STRING, s_nationkey BIGINT, s_phone STRING, s_acctbal DECIMAL(15,2), s_comment STRING',
    'part': 'p_partkey BIGINT, p_name STRING, p_mfgr STRING, p_brand STRING, p_type STRING, p_size INT, p_container STRING, p_retailprice DECIMAL(15,2), p_comment STRING',
    'partsupp': 'ps_partkey BIGINT, ps_suppkey BIGINT, ps_availqty INT, ps_supplycost DECIMAL(15,2), ps_comment STRING',
    'orders': 'o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus STRING, o_totalprice DECIMAL(15,2), o_orderdate DATE, o_orderpriority STRING, o_clerk STRING, o_shippriority INT, o_comment STRING',
    'lineitem': 'l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT, l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), l_returnflag STRING, l_linestatus STRING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct STRING, l_shipmode STRING, l_comment STRING'}


def fixture():
    result = {t: [] for t in SCHEMAS}
    result['region'] = [(0, 'EUROPE', ''), (1, 'ASIA', ''), (2, 'AMERICA', ''),
                        (0, 'EUROPE', ''), (None, 'ASIA', '')]
    countries = ['FRANCE', 'GERMANY', 'CANADA', 'BRAZIL', 'SAUDI ARABIA', 'CHINA']
    result['nation'] = [(i, name, [0, 0, 2, 2, 1, 1][i], '')
                        for i, name in enumerate(countries)]
    result['nation'] += [(0, 'FRANCE', 0, ''), (None, None, None, '')]
    for i in range(1, 7):
        result['customer'].append((i, f'c{i}', 'address', i - 1, '13-123',
            Decimal(10 * i), 'BUILDING', 'customer'))
        result['supplier'].append((i, f's{i}', 'address', i - 1, '13-456',
            Decimal(100 * i), 'normal'))
    result['customer'] += [result['customer'][0],
        (1, 'other customer', 'other', 1, '31-123', Decimal(80), 'BUILDING', 'duplicate key'),
        (99, 'no orders', 'address', 0, '13-999', Decimal(999), 'BUILDING', 'no match'),
        (None, 'null key', None, None, '13-777', Decimal(1000), None, None)]
    result['supplier'] += [result['supplier'][0],
        (None, None, None, None, None, None, 'normal')]
    parts = [('Brand#23', 'LARGE BRASS', 15, 'MED BOX'),
             ('Brand#12', 'PROMO BRASS', 3, 'SM CASE'),
             ('Brand#23', 'ECONOMY ANODIZED STEEL', 14, 'MED BOX'),
             ('Brand#34', 'PROMO STEEL', 9, 'LG CASE'),
             ('Brand#45', 'MEDIUM POLISHED STEEL', 49, 'MED PACK'),
             (None, None, None, None)]
    for i, (brand, kind, size, container) in enumerate(parts, 1):
        result['part'].append((i, 'forest green part', 'mfgr', brand, kind, size,
                               container, Decimal(100), 'part'))
        for supplier in range(1, 7):
            result['partsupp'].append((i, supplier, 1000, Decimal(supplier), 'partsupp'))
    result['part'] += [result['part'][0],
        (1, 'forest green other', 'mfgr2', 'Brand#34', 'PROMO OTHER', 15, 'LG BOX',
         Decimal(200), 'duplicate key'), (None, None, None, None, None, None, None, None, None)]
    result['partsupp'] += [result['partsupp'][0], (None, None, 1, None, None)]
    dates = [date(1993, 8, 1), date(1993, 11, 1), date(1994, 5, 1),
             date(1995, 2, 1), date(1995, 11, 1), date(1996, 2, 1)]
    for key in range(1, 37):
        when = dates[(key - 1) % len(dates)]
        result['orders'].append((key, (key // 6 + key) % 6 + 1, 'F', Decimal(1000 + key), when,
            ['1-URGENT', '2-HIGH', '3-MEDIUM', None][key % 4], 'clerk', 0,
            'special x requests' if key % 5 == 0 else 'ordinary'))
        for number in range(1, 8):
            ship = [date(1994, 6, 1), date(1995, 9, 15), date(1996, 1, 15)][key % 3]
            result['lineitem'].append((key, number % 6 + 1, number % 6 + 1, number,
                Decimal(50 if key % 3 == 0 else [5, 15, 25, 1, 10, 30, 20][number - 1]),
                Decimal(100 + key * 100 + number), Decimal('0.06'), Decimal('0.02'),
                'R', 'O', ship, ship + timedelta(days=10),
                ship + timedelta(days=20 if number % 3 else 5),
                'DELIVER IN PERSON', ['AIR', 'MAIL', 'SHIP'][(key + number) % 3], 'line'))
    result['orders'] += [result['orders'][0],
        (1, 1, 'F', Decimal(1001), date(1993, 11, 1), '1-URGENT', 'clerk', 0, 'ordinary'),
        (None, 1, 'F', Decimal(1), date(1993, 8, 1), None, None, 0, 'ordinary'),
        (100, None, 'F', Decimal(1), date(1993, 8, 1), None, None, 0, None)]
    result['lineitem'] += [result['lineitem'][0],
        (None, None, None, 1, None, None, None, None, None, None, None, None, None, None, None, None)]
    return result


def value(spark, sql):
    frame = spark.sql(sql)
    rows = frame.collect()
    values = Counter(tuple(None if v is None else str(v) for v in row) for row in rows)
    return values, frame.schema.jsonValue()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    spark = (SparkSession.builder.master('local[2]').appName('query-sweep-cpu-counterexamples')
        .config('spark.plugins', '').config('spark.sql.shuffle.partitions', '2')
        .config('spark.sql.adaptive.enabled', 'false').config('spark.sql.ansi.enabled', 'false')
        .config('spark.sql.session.timeZone', 'UTC').getOrCreate())
    report = {'backend': 'Spark CPU, no SQLPlugin', 'status': 'running',
              'checks': [], 'counterexample_mutations': []}
    output = Path(args.output)
    try:
        data = fixture()
        for table, rows in data.items():
            spark.createDataFrame(rows, SCHEMAS[table]).createOrReplaceTempView(table)
        for query in ORDER:
            candidate = targeted(query)
            if candidate is None:
                continue
            expected, expected_schema = value(spark, original(query))
            observed, observed_schema = value(spark, candidate.sql)
            assert expected == observed, (query, candidate.name, expected - observed, observed - expected)
            assert expected_schema == observed_schema, (query, expected_schema, observed_schema)
            report['checks'].append({'query': query, 'variant': candidate.name,
                'fixture': 'duplicate keys, duplicate rows, nullable keys and payloads',
                'output_rows': sum(expected.values()), 'status': 'passed'})
        for empty_table, queries in [('orders', [13, 22]), ('lineitem', [4])]:
            spark.createDataFrame([], SCHEMAS[empty_table]).createOrReplaceTempView(empty_table)
            for query in queries:
                assert value(spark, original(query)) == value(spark, targeted(query).sql)
                report['checks'].append({'query': query, 'fixture': 'empty ' + empty_table,
                                         'status': 'passed'})
            spark.createDataFrame(data[empty_table], SCHEMAS[empty_table]).createOrReplaceTempView(empty_table)
        mutant = targeted(13).sql.replace('SUM(COALESCE(probe_count, 0L))', 'COUNT(probe_count)')
        assert value(spark, original(13))[0] != value(spark, mutant)[0]
        report['counterexample_mutations'].append({
            'query': 13, 'mutation': 'Count matches instead of summing right-side multiplicities',
            'detected': True})
        report['status'] = 'passed'
    except BaseException as error:
        report.update(status='failed', error=repr(error))
        raise
    finally:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + '\n')
        spark.stop()


if __name__ == '__main__':
    main()
