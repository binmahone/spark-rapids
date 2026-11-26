#!/usr/bin/env python3
"""Explicit SQL mechanism probes; these are not query-specific production optimizer rules."""
from dataclasses import dataclass, field
from pathlib import Path
import re

SUITE = Path(__file__).resolve().parents[2]
ORDER = [3, 1, 2, 4, 5, 6, 9, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]


@dataclass
class Variant:
    query: int
    name: str
    sql: str
    purpose: str
    overrides: dict = field(default_factory=dict)
    evidence: str = ''


def original(query):
    return (SUITE / 'inputs/aws48x/sql' / f'q{query}.sql').read_text()


def wrap(sql, table, selection):
    value, count = re.subn(r'\b' + table + r'\b', '(' + selection + ') AS ' + table,
                           sql, flags=re.I)
    if not count:
        raise AssertionError('Source relation was not found: ' + table)
    return value


def first_select_hint(sql, hint):
    value, count = re.subn(r'\bselect\b', 'SELECT /*+ ' + hint + ' */', sql,
                           count=1, flags=re.I)
    assert count == 1
    return value


def targeted(query):
    sql = original(query)
    if query == 2:
        value = wrap(sql, 'partsupp', """SELECT ps.* FROM partsupp ps
            WHERE ps_partkey IN (SELECT /*+ BROADCAST */ p_partkey FROM part
                WHERE p_size = 15 AND p_type LIKE '%BRASS')""")
        return Variant(query, 'early_partsupp_keys', value,
            'Propagate an existing selective part predicate to both partsupp inputs',
            evidence='A broadcast semi join filters partsupp before its shuffle or aggregate')
    if query == 3:
        return Variant(query, 'broadcast_customer', first_select_hint(sql, 'BROADCAST(customer)'),
            'Broadcast the already filtered, key-only customer build instead of shuffling orders',
            evidence='Customer-key broadcast join; build size must remain within runtime limits')
    if query == 4:
        value = """SELECT o_orderpriority, COUNT(*) AS order_count FROM orders
            WHERE o_orderdate >= DATE '1993-07-01'
              AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
              AND o_orderkey IN (SELECT DISTINCT l_orderkey FROM lineitem
                                WHERE l_commitdate < l_receiptdate)
            GROUP BY o_orderpriority ORDER BY o_orderpriority"""
        return Variant(query, 'deduplicate_existence_keys', value,
            'Remove duplicate membership keys before building an existence join',
            evidence='Partial key aggregate below the lineitem exchange; exact semi join retained')
    if query == 5:
        value = wrap(sql, 'customer', """SELECT c.* FROM customer c
            WHERE c_nationkey IN (SELECT /*+ BROADCAST */ n_nationkey
                FROM nation JOIN region ON n_regionkey = r_regionkey WHERE r_name = 'ASIA')""")
        return Variant(query, 'early_customer_region', value,
            'Propagate an existing inner-join geography predicate to customer before joining orders',
            evidence='Customer rows are reduced before the customer/orders join')
    if query == 7:
        return Variant(query, 'join_graph_control', sql,
            'Check whether the current join-graph search carries avoidable intermediate work',
            overrides={'spark.rapids.sql.optimizer.joinGraphReorder.enabled': 'false'},
            evidence='Executed join tree differs; this is a planner diagnostic, not a fixed-query policy')
    if query == 8:
        value = wrap(sql, 'orders', """SELECT o.* FROM orders o WHERE o_custkey IN (
            SELECT /*+ BROADCAST */ c_custkey FROM customer WHERE c_nationkey IN (
                SELECT n_nationkey FROM nation JOIN region ON n_regionkey = r_regionkey
                WHERE r_name = 'AMERICA'))""")
        return Variant(query, 'early_orders_region', value,
            'Filter orders with existing customer-region eligibility before the fact join',
            evidence='Broadcast semi join on o_custkey before its exchange; original joins retained')
    if query == 9:
        value = wrap(sql, 'partsupp', """SELECT ps.* FROM partsupp ps
            WHERE ps_partkey IN (SELECT /*+ BROADCAST */ p_partkey FROM part
                                WHERE p_name LIKE '%green%')""")
        return Variant(query, 'early_partsupp_keys', value,
            'Propagate the existing part filter to partsupp before its large shuffle',
            evidence='Broadcast semi join reduces partsupp rows before its exchange')
    if query == 10:
        value = """SELECT /*+ SHUFFLE_HASH(fact) */ c_custkey, c_name,
                   SUM(l_extendedprice * (1 - l_discount)) AS revenue,
                   c_acctbal, n_name, c_address, c_phone, c_comment
            FROM customer JOIN (
                SELECT o_custkey, l_extendedprice, l_discount FROM orders
                JOIN lineitem ON l_orderkey = o_orderkey
                WHERE o_orderdate >= DATE '1993-10-01'
                  AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH
                  AND l_returnflag = 'R'
            ) fact ON c_custkey = o_custkey
            JOIN nation ON c_nationkey = n_nationkey
            GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
            ORDER BY revenue DESC LIMIT 20"""
        return Variant(query, 'delay_customer_payload', value,
            'Join the narrow fact inputs before attaching wide customer strings; no sum reassociation',
            evidence='Customer strings do not cross the orders/lineitem exchange')
    if query == 12:
        high = "CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END"
        low = "CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END"
        value = f"""SELECT l_shipmode, SUM(probe_high) AS high_line_count,
                SUM(probe_low) AS low_line_count
            FROM (SELECT o_orderkey, {high} AS probe_high, {low} AS probe_low FROM orders) orders,
                 lineitem
            WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
              AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
              AND l_receiptdate >= DATE '1994-01-01'
              AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
            GROUP BY l_shipmode ORDER BY l_shipmode"""
        return Variant(query, 'project_priority_flags', value,
            'Replace order-priority strings with the exact two aggregate inputs before shuffle',
            evidence='Orders exchange carries integers instead of o_orderpriority strings')
    if query == 13:
        value = """SELECT c_count, COUNT(*) AS custdist FROM (
            SELECT c_custkey, COALESCE(SUM(COALESCE(probe_count, 0L)), 0L) AS c_count
            FROM customer LEFT OUTER JOIN (
                SELECT o_custkey, COUNT(o_orderkey) AS probe_count FROM orders
                WHERE o_comment NOT LIKE '%special%requests%' GROUP BY o_custkey
            ) counts ON c_custkey = o_custkey GROUP BY c_custkey
            ) c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC"""
        return Variant(query, 'count_before_outer_join', value,
            'Represent right-side multiplicities as counts; preserve duplicate left keys and null keys',
            evidence='Orders partial count precedes shuffle; join output avoids order-level expansion')
    if query == 14:
        value = sql.replace("p_type like 'PROMO%'", 'probe_promo')
        value = wrap(value, 'part', "SELECT p_partkey, p_type LIKE 'PROMO%' AS probe_promo FROM part")
        return Variant(query, 'project_promo_flag', value,
            'Carry a nullable Boolean through the join instead of the full part-type string',
            evidence='Part exchange schema contains probe_promo and no p_type payload')
    if query == 15:
        value = """WITH revenue0 AS (
                SELECT l_suppkey AS supplier_no,
                       SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
                FROM lineitem WHERE l_shipdate >= DATE '1996-01-01'
                  AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH
                GROUP BY l_suppkey),
            marked AS (SELECT *, MAX(total_revenue) OVER () AS probe_max FROM revenue0)
            SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
            FROM supplier JOIN marked ON s_suppkey = supplier_no
            WHERE total_revenue = probe_max ORDER BY s_suppkey"""
        return Variant(query, 'reuse_revenue_with_window', value,
            'Read and aggregate revenue once; trade recomputation for gathering compact supplier state',
            evidence='One lineitem scan; global max window operates on per-supplier state, including null key')
    if query == 16:
        value = wrap(sql, 'partsupp', """SELECT ps.* FROM partsupp ps WHERE ps_partkey IN (
            SELECT /*+ BROADCAST */ p_partkey FROM part WHERE p_brand <> 'Brand#45'
              AND p_type NOT LIKE 'MEDIUM POLISHED%'
              AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9))""")
        return Variant(query, 'early_partsupp_keys', value,
            'Filter partsupp with a narrow key set before joining the wider part attributes',
            evidence='Broadcast semi join precedes partsupp shuffle; count-distinct semantics unchanged')
    if query == 17:
        value = """SELECT /*+ BROADCAST(avg_state) */ SUM(l_extendedprice) / 7.0 AS avg_yearly
            FROM lineitem JOIN part ON p_partkey = l_partkey
            JOIN (
                SELECT l_partkey AS probe_partkey, 0.2 * AVG(l_quantity) AS probe_limit
                FROM lineitem WHERE l_partkey IN (
                    SELECT /*+ BROADCAST */ p_partkey FROM part
                    WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX')
                GROUP BY l_partkey
            ) avg_state ON l_partkey = probe_partkey
            WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
              AND l_quantity < probe_limit"""
        return Variant(query, 'broadcast_average_state', value,
            'Broadcast narrow per-key aggregate state rather than the expanded price/quantity probe',
            evidence='Average state is the broadcast build; selective part filtering remains on both scans')
    if query in (18, 20):
        column = 'l_orderkey' if query == 18 else 'p_partkey'
        value, count = re.subn(r'(\bselect\s+)(' + column + r'\b)',
                               r'\1/*+ BROADCAST */ \2', sql, count=1, flags=re.I)
        assert count == 1
        return Variant(query, 'broadcast_membership_keys', value,
            'Apply an existing small membership set before shuffling the fact inputs',
            evidence='Two target semi joins execute as broadcast joins')
    if query == 19:
        groups = [
            ("p_brand = 'Brand#12' AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG') AND p_size BETWEEN 1 AND 5", 'l_quantity >= 1 AND l_quantity <= 1 + 10'),
            ("p_brand = 'Brand#23' AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK') AND p_size BETWEEN 1 AND 10", 'l_quantity >= 10 AND l_quantity <= 10 + 10'),
            ("p_brand = 'Brand#34' AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG') AND p_size BETWEEN 1 AND 15", 'l_quantity >= 20 AND l_quantity <= 20 + 10')]
        part_flags = ', '.join(f'({p}) AS probe_p{i}' for i, (p, _) in enumerate(groups))
        line_flags = ', '.join(f'({l}) AS probe_l{i}' for i, (_, l) in enumerate(groups))
        condition = ' OR '.join(f'(probe_p{i} AND probe_l{i})' for i in range(3))
        value = f"""SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM
            (SELECT p_partkey, {part_flags} FROM part) part JOIN
            (SELECT l_partkey, l_extendedprice, l_discount, {line_flags} FROM lineitem
             WHERE l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON') lineitem
            ON p_partkey = l_partkey WHERE {condition}"""
        return Variant(query, 'project_join_predicate_flags', value,
            'Factor independent predicates into Boolean payloads before the join; preserve the full OR',
            evidence='Broadcast build and probe payloads carry Boolean flags instead of source strings')
    if query == 22:
        value = wrap(sql, 'orders', 'SELECT DISTINCT o_custkey FROM orders')
        return Variant(query, 'deduplicate_anti_keys', value,
            'Compact existence-only build keys while retaining NOT EXISTS and its null semantics',
            evidence='Key aggregate before anti join; no conversion from NOT EXISTS to NOT IN')
    if query == 21:
        value = wrap(sql, 'supplier', """SELECT s.* FROM supplier s WHERE s_nationkey IN (
            SELECT /*+ BROADCAST */ n_nationkey FROM nation WHERE n_name = 'SAUDI ARABIA')""")
        value = first_select_hint(value, 'BROADCAST(supplier)')
        return Variant(query, 'broadcast_eligible_suppliers', value,
            'Broadcast eligible suppliers to filter completed witness state; keep all witness rows',
            evidence='Supplier-key broadcast join follows the complete counted-existence aggregate')
    return None


def extra_cases():
    sql = """WITH marked AS (
            SELECT l_partkey, l_quantity, l_extendedprice,
                   0.2 * AVG(l_quantity) OVER (PARTITION BY l_partkey) AS probe_limit
            FROM lineitem WHERE l_partkey IN (
                SELECT /*+ BROADCAST */ p_partkey FROM part
                WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'))
        SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
        FROM marked JOIN part ON p_partkey = l_partkey
        WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
          AND l_quantity < probe_limit"""
    result = [Variant(17, 'single_scan_average_window', sql,
        'Retain selected lineitem payload for a per-key window rather than rereading lineitem',
        evidence='One lineitem scan, selective semi join, and GPU per-key average window')]
    return result


def followup_cases():
    result = []
    selection = """SELECT li.* FROM lineitem li WHERE l_orderkey IN (
        SELECT /*+ BROADCAST */ o_orderkey FROM orders
        WHERE o_orderdate >= DATE '1994-01-01'
          AND o_orderdate < DATE '1994-01-01' + INTERVAL '1' YEAR
          AND o_custkey IN (SELECT c_custkey FROM customer WHERE c_nationkey IN (
            SELECT n_nationkey FROM nation JOIN region ON n_regionkey = r_regionkey
            WHERE r_name = 'ASIA')))"""
    result.append(Variant(5, 'prefilter_lineitem_order_keys', wrap(original(5), 'lineitem', selection),
        'Propagate complete existing order/customer eligibility to the fact input',
        evidence='Order-key broadcast semi join filters lineitem before its largest exchange'))
    for reuse in [False, True]:
        result.append(Variant(3, 'ordinary_customer_' + ('reuse' if reuse else 'no_reuse'),
            targeted(3).sql, 'Separate ordinary broadcast transport from hash-table reuse',
            overrides={'spark.rapids.sql.driverlessBroadcast.enabled': 'false',
                       'spark.rapids.sql.join.hashTable.reuse': str(reuse).lower()},
            evidence='Ordinary GPU customer broadcast; DEBUG reuse counters prove cached hash reuse'))
    state = """WITH witness AS (
            SELECT l_orderkey, MIN(l_suppkey) AS probe_all_min,
                MAX(l_suppkey) AS probe_all_max,
                MIN(CASE WHEN l_receiptdate > l_commitdate THEN l_suppkey END) AS probe_late_min,
                MAX(CASE WHEN l_receiptdate > l_commitdate THEN l_suppkey END) AS probe_late_max,
                COUNT(CASE WHEN l_receiptdate > l_commitdate THEN l_suppkey END) AS probe_late_count
            FROM lineitem GROUP BY l_orderkey)
        SELECT s_name,
               COALESCE(SUM(probe_late_count), 0L) AS numwait
        FROM witness JOIN orders ON o_orderkey = l_orderkey
        JOIN supplier ON s_suppkey = probe_late_min
        JOIN nation ON s_nationkey = n_nationkey
        WHERE probe_all_min <> probe_all_max AND probe_late_min = probe_late_max
          AND o_orderstatus = 'F' AND n_name = 'SAUDI ARABIA'
        GROUP BY s_name ORDER BY numwait DESC, s_name LIMIT 100"""
    value = wrap(state, 'supplier', """SELECT s.* FROM supplier s WHERE s_nationkey IN (
        SELECT /*+ BROADCAST */ n_nationkey FROM nation WHERE n_name = 'SAUDI ARABIA')""")
    value = value.replace('SELECT s_name,', 'SELECT /*+ BROADCAST(supplier) */ s_name,', 1)
    result.append(Variant(21, 'preserve_witness_broadcast_suppliers', value,
        'Preserve the complete one-scan witness state while broadcasting eligible suppliers',
        evidence='One lineitem scan, explicit min/max/count witness state, and supplier broadcast'))
    value, count = re.subn(r'\bs_suppkey\s*=\s*supplier_no\b',
        '(s_suppkey <=> supplier_no) AND s_suppkey IS NOT NULL', original(15), count=1)
    assert count == 1
    result.append(Variant(15, 'reuse_revenue_exchange', value,
        'Preserve matching revenue inputs for exchange reuse without a global window',
        evidence='One lineitem scan and reused aggregate exchange; null suppliers cannot join'))
    return result


def catalog(include_followups=False):
    result = []
    for query in ORDER:
        result.append(Variant(query, 'batch_512m', original(query),
            'Auxiliary bounded-batch configuration control; not an implementation of late materialization',
            overrides={'spark.rapids.sql.batchSizeBytes': '536870912',
                       'spark.rapids.sql.reader.batchSizeBytes': '536870912'},
            evidence='Effective reader and operator batch targets are 512 MiB'))
        if query in (1, 6, 11):
            result.append(Variant(query, 'scan_split_512m', original(query),
                'Auxiliary scan scheduling control for queries with little removable join payload',
                overrides={'spark.sql.files.maxPartitionBytes': '512m',
                           'spark.sql.files.maxPartitionNum': '16384'},
                evidence='Smaller scan tasks; input bytes and aggregate semantics remain unchanged'))
        candidate = targeted(query)
        if candidate:
            result.append(candidate)
    from ceiling_variants import cases, refinements, q11_total_reuse
    return result + extra_cases() + (followup_cases() + cases() + refinements() +
                                    [q11_total_reuse()] if include_followups else [])
