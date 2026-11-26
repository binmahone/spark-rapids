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


def selected_arm(query, arm):
    if arm == 'screen-extra':
        return {8: 'row-propagation', 11: 'aggregate-total-reuse'}.get(query, 'baseline')
    if arm == 'combined':
        return {3: 'preaggregate-broadcast', 4: 'preaggregate', 9: 'row-propagation-reuse', 10: 'preaggregate',
                15: 'aggregate-reuse', 17: 'window-average',
                18: 'prefilter-broadcast', 20: 'prefilter'}.get(query, 'baseline')
    if arm == 'screen':
        return {4: 'preaggregate', 5: 'row-propagation', 13: 'preaggregate',
                17: 'window-average'}.get(query, 'baseline')
    return arm


def transform(query, original, arm):
    arm = selected_arm(query, arm)
    if arm == 'baseline':
        return original
    if arm == 'preaggregate-prefilter-reuse' and query == 18:
        return transform(query, original, 'preaggregate-prefilter')
    if arm == 'aggregate-total-reuse' and query == 11:
        return """
WITH part_values AS (
    SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
    FROM partsupp JOIN supplier ON ps_suppkey = s_suppkey
    JOIN nation ON s_nationkey = n_nationkey
    WHERE n_name = 'GERMANY'
    GROUP BY ps_partkey
), marked AS (
    SELECT ps_partkey, value, sum(value) OVER () AS total_value FROM part_values
)
SELECT ps_partkey, value FROM marked
WHERE value > total_value * 0.00000001
ORDER BY value DESC
"""
    if arm == 'row-propagation' and query == 8:
        replaced, count = re.subn(r'\borders\b', 'filtered_orders', original, flags=re.I)
        assert count == 1
        return """WITH filtered_orders AS (
    SELECT o.* FROM orders o WHERE o_custkey IN (
        SELECT /*+ BROADCAST */ c_custkey FROM customer WHERE c_nationkey IN (
            SELECT n_nationkey FROM nation JOIN region ON n_regionkey = r_regionkey
            WHERE r_name = 'AMERICA')))
""" + replaced
    if arm == 'prefilter-broadcast' and query == 18:
        value = transform(query, original, 'prefilter')
        result, count = re.subn(r'(\bselect\s+)', r'\1/*+ BROADCAST(orders) */ ', value,
                                count=1, flags=re.I)
        assert count == 1
        return result
    if arm == 'preaggregate-broadcast' and query == 3:
        value = transform(query, original, 'preaggregate')
        before = 'SELECT l_orderkey, sum(revenue) AS revenue'
        assert value.count(before) == 1
        return value.replace(before, 'SELECT /*+ BROADCAST(customer) */ l_orderkey, sum(revenue) AS revenue')
    if arm == 'preaggregate-prefilter' and query == 18:
        value = transform(query, original, 'preaggregate')
        value = value.replace('FROM lineitem GROUP BY l_orderkey',
                              'FROM lineitem WHERE l_orderkey IS NOT NULL GROUP BY l_orderkey')
        before = 'FROM customer JOIN orders ON c_custkey = o_custkey'
        after = '''FROM customer JOIN (
    SELECT * FROM orders WHERE o_orderkey IN (
        SELECT /*+ BROADCAST */ l_orderkey FROM line_quantity)
) orders ON c_custkey = o_custkey'''
        assert value.count(before) == 1
        return value.replace(before, after)
    if arm == 'window-average' and query == 17:
        return """
WITH marked AS (
    SELECT l_partkey, l_quantity, l_extendedprice,
           0.2 * avg(l_quantity) OVER (PARTITION BY l_partkey) AS probe_limit
    FROM lineitem
    WHERE l_partkey IN (
        SELECT /*+ BROADCAST */ p_partkey FROM part
        WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
          AND p_partkey IS NOT NULL)
)
SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM marked JOIN part ON p_partkey = l_partkey
WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
  AND l_quantity < probe_limit
"""
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
    if arm == 'aggregate-reuse' and query == 15:
        return """
WITH revenue0 AS (
    SELECT l_suppkey AS supplier_no,
           sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM lineitem
    WHERE l_shipdate >= date '1996-01-01'
      AND l_shipdate < date '1996-01-01' + interval '3' month
    GROUP BY l_suppkey
), ranked AS (
    SELECT supplier_no, total_revenue, max(total_revenue) OVER () AS max_revenue
    FROM revenue0
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM supplier JOIN ranked ON s_suppkey = supplier_no
WHERE total_revenue = max_revenue
ORDER BY s_suppkey
"""
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
    if arm == 'row-propagation-reuse' and query == 9:
        result=transform(query,original,'row-propagation')
        before="WHERE p_name LIKE '%green%')"
        after="WHERE p_name LIKE '%green%' AND p_partkey IS NOT NULL)"
        assert result.count(before)==1
        return result.replace(before,after)
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
    SELECT l_orderkey, sum(l_quantity) AS l_quantity
    FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 300
)
SELECT /*+ BROADCAST(line_quantity) */ c_name, c_custkey, o_orderkey,
       o_orderdate, o_totalprice, sum(l_quantity)
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
    cases[15] = {
        'supplier': ('s_suppkey long, s_name string, s_address string, s_phone string', [
            (1,'A','addr','phone'),(1,'A','addr','phone'),(2,'B',None,'phone'),(None,'N','addr','phone')]),
        'lineitem': ('l_suppkey long, l_extendedprice double, l_discount double, l_shipdate date', [
            (1,100.,0.,date(1996,1,2)),(1,100.,0.,date(1996,2,2)),
            (2,150.,0.,date(1996,1,2)),(None,50.,0.,date(1996,1,2)),
            (2,999.,0.,date(1996,5,1)),(2,None,0.,date(1996,1,2))])}
    cases[17] = {
        'part': ('p_partkey long, p_brand string, p_container string', [
            (1,'Brand#23','MED BOX'),(1,'Brand#23','MED BOX'),
            (1,'Other','MED BOX'),(2,'Brand#23','MED BOX'),
            (3,'Other','MED BOX'),(None,'Brand#23','MED BOX')]),
        'lineitem': ('l_partkey long, l_quantity double, l_extendedprice double', [
            (1,1.,7.),(1,19.,70.),(1,None,700.),(1,0.,None),
            (2,2.,14.),(2,18.,140.),(2,None,1400.),
            (3,0.,7000.),(3,100.,70000.),(None,0.,700000.)])}
    cases[11] = {
        'partsupp': ('ps_partkey long, ps_suppkey long, ps_supplycost double, ps_availqty long', [
            (1,1,2.,2),(1,1,3.,3),(2,2,100.,1),(None,1,1024.,1),
            (3,1,None,1),(4,1,2.,None),(5,None,2.,2),(6,4,2.,2)]),
        'supplier': ('s_suppkey long, s_nationkey int', [(1,1),(1,1),(2,2),(None,1)]),
        'nation': ('n_nationkey int, n_name string', [(1,'GERMANY'),(1,'GERMANY'),(2,'OTHER'),(None,'GERMANY')])}
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
    cases[8] = {k:v for k,v in graph_tables.items() if k != 'partsupp'}
    cases[8]['orders'] = ('o_orderkey long, o_custkey long, o_orderdate date', [
        (11,1,date(1995,2,1)),(11,1,date(1995,2,1)),(12,2,date(1995,2,1)),
        (13,1,date(1996,1,1)),(14,3,date(1997,1,1))])
    cases[8]['region'] = ('r_regionkey int, r_name string', [(1,'AMERICA'),(1,'AMERICA'),(2,'OTHER')])
    cases[8]['nation'] = ('n_nationkey int, n_name string, n_regionkey int', [
        (1,'BRAZIL',1),(1,'BRAZIL',1),(2,'OTHER',2),(None,'N',1)])
    cases[8]['part'] = ('p_partkey long, p_type string', [
        (1,'ECONOMY ANODIZED STEEL'),(1,'ECONOMY ANODIZED STEEL'),
        (2,'OTHER'),(3,'ECONOMY ANODIZED STEEL'),(None,'ECONOMY ANODIZED STEEL')])
    results=[]
    previous = spark.conf.get('spark.rapids.sql.enabled', 'true')
    prefilter_key = 'spark.rapids.sql.optimizer.aggregatePrefilter.enabled'
    previous_prefilter = spark.conf.get(prefilter_key, 'true')
    try:
        for query,tables in cases.items():
            if query not in requested or selected_arm(query, arm) in ('baseline', 'prefilter'):
                continue
            spark.conf.set(prefilter_key, 'false' if selected_arm(query, arm) ==
                           'preaggregate-prefilter-reuse' else previous_prefilter)
            original=(suite/'inputs/aws48x/sql'/f'q{query}.sql').read_text()
            for empty in ((False,True,'null-max') if query==15 else (False,True)):
                for table,(schema,rows) in tables.items():
                    data=[] if empty is True else rows
                    if empty == 'null-max' and table == 'lineitem':
                        data=rows+[(None,1000.,0.,date(1996,1,2))]
                    spark.createDataFrame(data,schema).createOrReplaceTempView('__probe_'+table)
                def renamed(sql):
                    for table in tables:
                        sql=re.sub(r'\b'+table+r'\b','__probe_'+table,sql,flags=re.I)
                    return sql
                spark.conf.set('spark.rapids.sql.enabled','false')
                reference_frame=spark.sql(renamed(original))
                reference=Counter(tuple(row) for row in reference_frame.collect())
                cpu_frame=spark.sql(renamed(transform(query,original,arm)))
                cpu=Counter(tuple(row) for row in cpu_frame.collect())
                if reference != cpu or reference_frame.schema != cpu_frame.schema:
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
                if reference != gpu or reference_frame.schema != frame.schema or 'GpuHashAggregate' not in plan:
                    raise AssertionError(f'Q{query}: GPU counterexample failed or aggregate did not run on GPU')
                results.append({'query':query,'empty':empty,'status':'passed',
                                'reference':'CPU original SQL', 'cpu_rewrite_equal':True,
                                'gpu_rewrite_equal':True,'gpu_plan':plan})
    finally:
        spark.conf.set('spark.rapids.sql.enabled',previous)
        spark.conf.set(prefilter_key,previous_prefilter)
        for table in set(t for tables in cases.values() for t in tables):
            spark.catalog.dropTempView('__probe_'+table)
    return results
