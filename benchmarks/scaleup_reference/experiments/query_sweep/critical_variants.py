"""Focused work-reduction probes for the most expensive retained queries."""
from variants import Variant


def witness_sql(narrow):
    key = 'CAST(l_suppkey AS INT)' if narrow else 'l_suppkey'
    return f"""WITH witness AS (
        SELECT l_orderkey, MIN({key}) AS probe_all_min,
            MAX({key}) AS probe_all_max,
            MIN(CASE WHEN l_receiptdate > l_commitdate THEN {key} END) AS probe_late_min,
            MAX(CASE WHEN l_receiptdate > l_commitdate THEN {key} END) AS probe_late_max,
            COUNT(CASE WHEN l_receiptdate > l_commitdate THEN l_suppkey END) AS probe_late_count
        FROM lineitem GROUP BY l_orderkey)
        SELECT s_name, COALESCE(SUM(probe_late_count), 0L) AS numwait
        FROM witness JOIN orders ON o_orderkey = l_orderkey
        JOIN supplier ON s_suppkey = probe_late_min
        JOIN nation ON s_nationkey = n_nationkey
        WHERE probe_all_min <> probe_all_max AND probe_late_min = probe_late_max
          AND o_orderstatus = 'F' AND n_name = 'SAUDI ARABIA'
        GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"""


def cases():
    q9 = """WITH eligible_parts AS (
        SELECT p_partkey FROM part WHERE p_name LIKE '%green%' AND p_partkey IS NOT NULL),
        supply AS (
        SELECT /*+ BROADCAST(eligible_parts) */ ps_partkey, ps_suppkey, ps_supplycost, s_nationkey
        FROM partsupp JOIN eligible_parts ON ps_partkey = p_partkey
        JOIN supplier ON ps_suppkey = s_suppkey),
        selected_lines AS (
        SELECT * FROM lineitem WHERE l_partkey IN (
            SELECT /*+ BROADCAST */ p_partkey FROM eligible_parts))
        SELECT /*+ SHUFFLE_HASH(supply, orders) BROADCAST(nation) */
            n_name AS nation, YEAR(o_orderdate) AS o_year,
            SUM(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
        FROM selected_lines JOIN supply ON l_partkey = ps_partkey AND l_suppkey = ps_suppkey
        JOIN orders ON l_orderkey = o_orderkey
        JOIN nation ON s_nationkey = n_nationkey
        GROUP BY n_name, YEAR(o_orderdate) ORDER BY nation, o_year DESC"""
    q8 = """WITH region_customers AS (
        SELECT c_custkey FROM customer JOIN nation ON c_nationkey = n_nationkey
        JOIN region ON n_regionkey = r_regionkey WHERE r_name = 'AMERICA'),
        region_orders AS (
        SELECT /*+ BROADCAST(region_customers) */ o_orderkey, YEAR(o_orderdate) AS o_year
        FROM orders JOIN region_customers ON o_custkey = c_custkey
        WHERE o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'),
        supplier_flags AS (
        SELECT s_suppkey, n_name = 'BRAZIL' AS probe_brazil
        FROM supplier JOIN nation ON s_nationkey = n_nationkey)
        SELECT /*+ BROADCAST(part, supplier_flags) SHUFFLE_HASH(region_orders) */ o_year,
            SUM(CASE WHEN probe_brazil THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
                / SUM(l_extendedprice * (1 - l_discount)) AS mkt_share
        FROM lineitem JOIN part ON l_partkey = p_partkey
        JOIN region_orders ON l_orderkey = o_orderkey
        JOIN supplier_flags ON l_suppkey = s_suppkey
        WHERE p_type = 'ECONOMY ANODIZED STEEL'
        GROUP BY o_year ORDER BY o_year"""
    fixed_order = {'spark.rapids.sql.optimizer.joinGraphReorder.enabled': 'false',
                   'spark.rapids.sql.optimizer.joinReorder.enabled': 'false'}
    return [
        Variant(9, 'critical_supply_before_fact', q9,
            'Attach supplier geography to selective partsupp before joining fact rows',
            overrides=dict(fixed_order),
            evidence='Supplier joins partsupp below the fact join; part-key semi filter remains'),
        Variant(8, 'critical_region_orders_flags', q8,
            'Fuse region eligibility into the orders join and carry a Boolean supplier payload',
            overrides=dict(fixed_order),
            evidence='Customer-region join below the fact exchange; supplier payload is probe_brazil'),
        Variant(21, 'critical_witness_int32', witness_sql(True),
            'Narrow four witness extrema after verifying every supplier-key footer bound fits int32',
            evidence='One lineitem scan and four INT extrema; order key and count retain BIGINT'),
        Variant(21, 'critical_witness_wide', witness_sql(False),
            'Control for the explicit witness SQL with all extrema retained as BIGINT',
            evidence='One lineitem scan and the same complete min/max/count witness semantics')]
