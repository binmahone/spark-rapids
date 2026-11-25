# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Bounded relational experiments; each arm changes only its stated query transformation."""
import re


def transform(query, original, arm):
    if arm == 'baseline':
        return original
    if arm == 'prefilter' and query in (18,20):
        column = 'l_orderkey' if query == 18 else 'p_partkey'
        result,count = re.subn(r'(\bselect\s+)(' + column + r'\b)',
            r'\1/*+ BROADCAST */ \2',original,count=1,flags=re.I)
        assert count == 1 and result.replace('/*+ BROADCAST */ ', '') == original
        return result
    if arm == 'dimension-broadcast' and query == 3:
        result,count = re.subn(r'(\bselect\s+)', r'\1/*+ BROADCAST(customer) */ ', original,count=1,flags=re.I)
        assert count == 1 and result.replace('/*+ BROADCAST(customer) */ ', '') == original
        return result
    if arm == 'preaggregate' and query == 3:
        return """
WITH line_revenue AS (
    SELECT l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM lineitem
    WHERE l_shipdate > date '1995-03-15'
    GROUP BY l_orderkey
)
SELECT l_orderkey, sum(revenue) AS revenue, o_orderdate, o_shippriority
FROM customer JOIN orders ON c_custkey = o_custkey
JOIN line_revenue ON l_orderkey = o_orderkey
WHERE c_mktsegment = 'BUILDING' AND o_orderdate < date '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
"""
    if arm == 'row-propagation' and query == 5:
        clause = """AND c_nationkey IN (
            SELECT /*+ BROADCAST */ n_nationkey FROM nation JOIN region
              ON n_regionkey = r_regionkey WHERE r_name = 'ASIA')
"""
        result,count = re.subn(r'(\bgroup\s+by\b)', clause+r'\1', original,count=1,flags=re.I)
        assert count == 1
        return result
    if arm == 'row-propagation' and query == 9:
        clause = """AND ps_partkey IN (
            SELECT /*+ BROADCAST */ p_partkey FROM part WHERE p_name LIKE '%green%')
"""
        result,count = re.subn(r'(\n\s*\)\s+as\s+profit\b)', '\n'+clause+r'\1', original,count=1,flags=re.I)
        assert count == 1
        return result
    if arm == 'preaggregate' and query == 4:
        return """
WITH eligible_orders AS (
    SELECT l_orderkey FROM lineitem
    WHERE l_commitdate < l_receiptdate
    GROUP BY l_orderkey
)
SELECT o_orderpriority, count(*) AS order_count
FROM orders JOIN eligible_orders ON o_orderkey = l_orderkey
WHERE o_orderdate >= date '1993-07-01'
  AND o_orderdate < date '1993-07-01' + interval '3' month
GROUP BY o_orderpriority
ORDER BY o_orderpriority
"""
    if arm == 'preaggregate' and query == 18:
        return """
WITH line_quantity AS (
    SELECT l_orderkey, sum(l_quantity) AS sum_quantity
    FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300
)
SELECT /*+ BROADCAST(line_quantity) */ c_name, c_custkey, o_orderkey,
       o_orderdate, o_totalprice, sum(sum_quantity) AS `sum(l_quantity)`
FROM customer JOIN orders ON c_custkey = o_custkey
JOIN line_quantity ON o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
"""
    if arm == 'preaggregate' and query == 13:
        return """
WITH order_counts AS (
    SELECT o_custkey, count(o_orderkey) AS n
    FROM orders
    WHERE o_comment NOT LIKE '%special%requests%'
    GROUP BY o_custkey
), c_orders AS (
    SELECT c_custkey, coalesce(sum(coalesce(n, cast(0 AS bigint))), cast(0 AS bigint)) AS c_count
    FROM customer LEFT OUTER JOIN order_counts ON c_custkey = o_custkey
    GROUP BY c_custkey
)
SELECT c_count, count(*) AS custdist
FROM c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
"""
    if arm == 'preaggregate' and query == 10:
        return """
WITH fact_revenue AS (
    SELECT o_custkey, sum(l_extendedprice * (1 - l_discount)) AS revenue
    FROM orders JOIN lineitem ON l_orderkey = o_orderkey
    WHERE o_orderdate >= date '1993-10-01'
      AND o_orderdate < date '1993-10-01' + interval '3' month
      AND l_returnflag = 'R'
    GROUP BY o_custkey
)
SELECT c_custkey, c_name, sum(revenue) AS revenue, c_acctbal, n_name,
       c_address, c_phone, c_comment
FROM fact_revenue JOIN customer ON c_custkey = o_custkey
JOIN nation ON c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
"""
    raise ValueError(f'No audited transformation for Q{query}, arm={arm}')


def check_semantics(spark, suite, requested, arm, require_gpu=True):
    """Try duplicate, NULL, unmatched and empty inputs on CPU and on the GPU rewrite."""
    from datetime import date
    from collections import Counter
    cases = {
        13: {
            'customer': ('c_custkey long', [(1,), (1,), (2,), (None,), (3,)]),
            'orders': ('o_custkey long, o_orderkey long, o_comment string', [
                (1,11,'ordinary'), (1,12,'ordinary'), (1,None,'ordinary'),
                (2,21,'special other requests'), (2,22,None), (None,23,'ordinary'),
                (4,41,'ordinary')])},
        10: {
            'customer': ('c_custkey long, c_name string, c_acctbal double, c_nationkey long, '
                         'c_address string, c_phone string, c_comment string', [
                (1,'A',1.5,1,'addr','phone','comment'),
                (1,'A',1.5,1,'addr','phone','comment'),
                (1,'B',None,2,None,'phone','other'),
                (2,'C',2.,1,'addr','phone','comment'),
                (None,'N',3.,1,'addr','phone','comment')]),
            'orders': ('o_orderkey long, o_custkey long, o_orderdate date', [
                (11,1,date(1993,10,2)), (11,1,date(1993,10,2)),
                (12,1,date(1993,12,31)), (13,2,date(1994,1,1)),
                (14,None,date(1993,11,1)), (15,3,date(1993,11,1))]),
            'lineitem': ('l_orderkey long, l_extendedprice double, l_discount double, l_returnflag string', [
                (11,10.,.1,'R'), (11,20.,.2,'R'), (11,999.,.0,'N'),
                (12,None,.2,'R'), (12,9.,None,'R'), (13,40.,0.,'R'),
                (14,50.,0.,'R'), (15,60.,0.,'R'), (None,80.,0.,'R')]),
            'nation': ('n_nationkey long, n_name string', [(1,'X'),(1,'X'),(2,'Y')])}}
    cases[3] = {
        'customer': ('c_custkey long, c_mktsegment string', [(1,'BUILDING'),(1,'BUILDING'),(2,None),(3,'OTHER')]),
        'orders': ('o_orderkey long, o_custkey long, o_orderdate date, o_shippriority int', [
            (11,1,date(1995,3,1),0),(11,1,date(1995,3,1),0),
            (11,1,date(1995,2,1),1),(12,1,date(1995,1,1),0),
            (13,1,date(1995,4,1),0),(14,None,date(1995,1,1),0)]),
        'lineitem': ('l_orderkey long, l_extendedprice double, l_discount double, l_shipdate date', [
            (11,10.,.1,date(1995,4,1)),(11,20.,.2,date(1995,4,1)),
            (11,99.,0.,date(1995,1,1)),(12,None,.2,date(1995,4,1)),
            (12,9.,None,date(1995,4,1)),(13,40.,0.,date(1995,4,1)),
            (None,50.,0.,date(1995,4,1))])}
    cases[4] = {
        'orders': ('o_orderkey long, o_orderpriority string, o_orderdate date', [
            (11,'A',date(1993,8,1)),(11,'A',date(1993,8,1)),
            (12,'B',date(1993,8,1)),(13,'A',date(1993,8,1)),
            (14,None,date(1993,8,1)),(None,'A',date(1993,8,1)),
            (15,'A',date(1993,10,1))]),
        'lineitem': ('l_orderkey long, l_commitdate date, l_receiptdate date', [
            (11,date(1993,8,1),date(1993,8,2)),(11,date(1993,8,1),date(1993,8,2)),
            (12,date(1993,8,2),date(1993,8,1)),(13,None,date(1993,8,2)),
            (14,date(1993,8,1),date(1993,8,2)),(None,date(1993,8,1),date(1993,8,2)),
            (15,date(1993,8,1),date(1993,8,2))])}
    cases[18] = {
        'customer': ('c_custkey long, c_name string', [(1,'A'),(1,'A'),(2,'B'),(None,'N')]),
        'orders': ('o_orderkey long, o_custkey long, o_orderdate date, o_totalprice double', [
            (11,1,date(1995,1,1),500.),(11,1,date(1995,1,1),500.),
            (12,2,date(1995,1,1),500.),(13,1,date(1995,2,1),None),
            (None,1,date(1995,1,1),100.)]),
        'lineitem': ('l_orderkey long, l_quantity double', [
            (11,200.),(11,150.),(11,None),(12,180.),(12,180.),
            (13,200.),(13,150.),(14,400.),(None,400.)])}
    graph_tables = {
        'customer': ('c_custkey long, c_nationkey int', [(1,1),(1,1),(2,2),(3,None)]),
        'orders': ('o_orderkey long, o_custkey long, o_orderdate date', [
            (11,1,date(1994,2,1)),(11,1,date(1994,2,1)),(12,2,date(1994,2,1)),
            (13,1,date(1996,1,1)),(14,3,date(1994,2,1))]),
        'lineitem': ('l_orderkey long, l_partkey long, l_suppkey long, l_extendedprice double, l_discount double, l_quantity double', [
            (11,1,1,10.,0.,2.),(11,2,2,20.,0.,3.),(12,1,1,30.,0.,4.),
            (13,3,1,40.,0.,5.),(14,None,1,50.,0.,6.),(None,1,1,None,None,None)]),
        'supplier': ('s_suppkey long, s_nationkey int', [(1,1),(1,1),(2,2),(3,None)]),
        'nation': ('n_nationkey int, n_name string, n_regionkey int', [(1,'A',1),(1,'A',1),(2,'B',2),(None,'N',1)]),
        'region': ('r_regionkey int, r_name string', [(1,'ASIA'),(1,'ASIA'),(2,'OTHER'),(None,'ASIA')]),
        'part': ('p_partkey long, p_name string', [(1,'green'),(1,'green'),(2,'green item'),(3,'red'),(None,'green')]),
        'partsupp': ('ps_partkey long, ps_suppkey long, ps_supplycost double', [(1,1,2.),(1,1,2.),(2,2,3.),(3,1,4.),(None,1,5.)])}
    cases[5] = {k:v for k,v in graph_tables.items() if k not in ('part','partsupp')}
    cases[9] = {k:v for k,v in graph_tables.items() if k not in ('customer','region')}
    results=[]
    previous = spark.conf.get('spark.rapids.sql.enabled', 'true')
    try:
        for query,tables in cases.items():
            if query not in requested:
                continue
            original=(suite/'inputs/aws48x/sql'/f'q{query}.sql').read_text()
            for empty in (False,True):
                for table,(schema,rows) in tables.items():
                    spark.createDataFrame([] if empty else rows,schema).createOrReplaceTempView('__probe_'+table)
                def renamed(sql):
                    for table in tables:
                        sql=re.sub(r'\b'+table+r'\b','__probe_'+table,sql,flags=re.I)
                    return sql
                spark.conf.set('spark.rapids.sql.enabled','false')
                reference=Counter(tuple(row) for row in spark.sql(renamed(original)).collect())
                cpu=Counter(tuple(row) for row in spark.sql(renamed(transform(query,original,arm))).collect())
                if reference != cpu:
                    raise AssertionError(f'Q{query}: CPU counterexample failed')
                if not require_gpu:
                    results.append({'query':query,'empty':empty,'status':'passed',
                                    'reference':'CPU original SQL','cpu_rewrite_equal':True,
                                    'gpu_rewrite_checked':False})
                    continue
                spark.conf.set('spark.rapids.sql.enabled','true')
                frame=spark.sql(renamed(transform(query,original,arm)))
                gpu=Counter(tuple(row) for row in frame.collect())
                plan=str(frame._jdf.queryExecution().executedPlan().toString())
                if reference != gpu or 'GpuHashAggregate' not in plan:
                    raise AssertionError(f'Q{query}: GPU counterexample failed or aggregate did not run on GPU')
                results.append({'query':query,'empty':empty,'status':'passed',
                                'reference':'CPU original SQL', 'cpu_rewrite_equal':True,
                                'gpu_rewrite_equal':True,'gpu_plan':plan})
    finally:
        spark.conf.set('spark.rapids.sql.enabled',previous)
        for table in set(t for tables in cases.values() for t in tables):
            spark.catalog.dropTempView('__probe_'+table)
    return results
