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

"""Additional work-reduction candidates; integer casts require a verified dataset proof."""
import re


def planner_overrides(arm):
    if arm in ('counted-wide', 'compact-counted-state'):
        return {'spark.rapids.sql.optimizer.aggregatePrefilter.enabled': 'false'}
    if arm in ('profit-first', 'eligible-customer-join'):
        return {'spark.rapids.sql.optimizer.joinReorder.enabled': 'false',
                'spark.rapids.sql.optimizer.joinGraphReorder.enabled': 'false'}
    return {}


def transform_next(query, original, arm):
    if query == 21 and arm in ('counted-wide', 'compact-counted-state'):
        supplier = 'CAST(l_suppkey AS INT)' if arm == 'compact-counted-state' else 'l_suppkey'
        return f"""
WITH typed_line AS (
    SELECT l_orderkey, {supplier} AS __supplier, l_commitdate, l_receiptdate FROM lineitem
), order_state AS (
    SELECT l_orderkey, min(__supplier) AS __state_all_min, max(__supplier) AS __state_all_max,
           min(CASE WHEN l_receiptdate > l_commitdate THEN __supplier END) AS __state_late_min,
           max(CASE WHEN l_receiptdate > l_commitdate THEN __supplier END) AS __state_late_max,
           count(CASE WHEN l_receiptdate > l_commitdate THEN __supplier END) AS __state_late_count
    FROM typed_line GROUP BY l_orderkey
)
SELECT s_name, coalesce(sum(__state_late_count), cast(0 AS bigint)) AS numwait
FROM order_state JOIN supplier ON s_suppkey = __state_late_min
JOIN orders ON o_orderkey = l_orderkey
JOIN nation ON s_nationkey = n_nationkey
WHERE __state_all_min <> __state_all_max AND __state_late_min = __state_late_max
  AND o_orderstatus = 'F' AND n_name = 'SAUDI ARABIA'
GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100
"""
    if query == 5 and arm == 'revenue-first':
        value, count = re.subn(r'l_extendedprice\s*\*\s*\(\s*1\s*-\s*l_discount\s*\)',
                               '__line_revenue', original, flags=re.I)
        assert count == 1
        value, count = re.subn(r'\blineitem\b', '__measure_lineitem', value, flags=re.I)
        assert count == 1
        return '''WITH __measure_lineitem AS (
    SELECT l_orderkey, l_suppkey, l_extendedprice * (1 - l_discount) AS __line_revenue
    FROM lineitem)
''' + value
    if query == 21 and arm == 'compact-supplier':
        value, count = re.subn(r'\blineitem\b', '__narrow_lineitem', original, flags=re.I)
        assert count == 3
        return '''WITH __narrow_lineitem AS (
    SELECT l_orderkey, CAST(l_suppkey AS INT) AS l_suppkey, l_commitdate, l_receiptdate
    FROM lineitem)
''' + value
    if query == 9 and arm == 'profit-first':
        return """
WITH green AS (
    SELECT p_partkey FROM part WHERE p_name LIKE '%green%' AND p_partkey IS NOT NULL
), ps_green AS (
    SELECT ps_partkey, ps_suppkey, ps_supplycost FROM partsupp
    WHERE ps_partkey IN (SELECT /*+ BROADCAST */ p_partkey FROM green)
), fact_profit AS (
    SELECT /*+ BROADCAST(green) */ l_orderkey, l_suppkey,
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM lineitem JOIN green ON p_partkey = l_partkey
    JOIN ps_green ON ps_partkey = l_partkey AND ps_suppkey = l_suppkey
)
SELECT n_name AS nation, year(o_orderdate) AS o_year, sum(amount) AS sum_profit
FROM fact_profit JOIN supplier ON s_suppkey = l_suppkey
JOIN nation ON s_nationkey = n_nationkey
JOIN orders ON o_orderkey = l_orderkey
GROUP BY n_name, year(o_orderdate)
ORDER BY nation, o_year DESC
"""
    if query == 7 and arm == 'country-flags':
        return """
WITH s_country AS (
    SELECT s_suppkey, n_name = 'FRANCE' AS s_fr
    FROM supplier JOIN nation ON s_nationkey = n_nationkey
    WHERE n_name IN ('FRANCE', 'GERMANY')
), c_country AS (
    SELECT c_custkey, n_name = 'FRANCE' AS c_fr
    FROM customer JOIN nation ON c_nationkey = n_nationkey
    WHERE n_name IN ('FRANCE', 'GERMANY')
), shipping AS (
    SELECT s_fr, c_fr, year(l_shipdate) AS l_year,
           l_extendedprice * (1 - l_discount) AS volume
    FROM lineitem JOIN s_country ON s_suppkey = l_suppkey
    JOIN orders ON o_orderkey = l_orderkey
    JOIN c_country ON c_custkey = o_custkey
    WHERE l_shipdate BETWEEN date '1995-01-01' AND date '1996-12-31'
      AND s_fr <> c_fr
)
SELECT CASE WHEN s_fr THEN 'FRANCE' WHEN NOT s_fr THEN 'GERMANY' END AS supp_nation,
       CASE WHEN c_fr THEN 'FRANCE' WHEN NOT c_fr THEN 'GERMANY' END AS cust_nation,
       l_year, sum(volume) AS revenue
FROM shipping GROUP BY s_fr, c_fr, l_year
ORDER BY supp_nation, cust_nation, l_year
"""
    if query == 8 and arm == 'eligible-customer-join':
        return """
WITH eligible_customers AS (
    SELECT c_custkey FROM customer JOIN nation ON c_nationkey = n_nationkey
    JOIN region ON n_regionkey = r_regionkey WHERE r_name = 'AMERICA'
), eligible_orders AS (
    SELECT /*+ BROADCAST(eligible_customers) */ o_orderkey, o_orderdate
    FROM orders JOIN eligible_customers ON o_custkey = c_custkey
    WHERE o_orderdate BETWEEN date '1995-01-01' AND date '1996-12-31'
), fact AS (
    SELECT /*+ BROADCAST(part, supplier) */ l_orderkey, s_nationkey,
           l_extendedprice * (1 - l_discount) AS volume
    FROM lineitem JOIN part ON p_partkey = l_partkey
    JOIN supplier ON s_suppkey = l_suppkey
    WHERE p_type = 'ECONOMY ANODIZED STEEL'
)
SELECT year(o_orderdate) AS o_year,
       sum(CASE WHEN n_name = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share
FROM eligible_orders JOIN fact ON o_orderkey = l_orderkey
JOIN nation ON s_nationkey = n_nationkey
GROUP BY year(o_orderdate) ORDER BY o_year
"""
    raise ValueError('No next-stage candidate for ' + str((query, arm)))
