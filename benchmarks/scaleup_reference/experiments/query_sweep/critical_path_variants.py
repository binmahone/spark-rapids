# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# Licensed under the Apache License, Version 2.0.
"""Work-reduction probes selected from retained critical-stage evidence."""
from dataclasses import replace
import hashlib
import json
from pathlib import Path

from variants import ORDER, Variant, original

HERE = Path(__file__).resolve().parent


def cases():
    sql = """WITH country_customers AS (
        SELECT c_custkey, n_name AS cust_nation
        FROM customer JOIN nation ON c_nationkey = n_nationkey
        WHERE n_name IN ('FRANCE', 'GERMANY')),
        country_orders AS (
        SELECT /*+ BROADCAST(country_customers) */ o_orderkey, cust_nation
        FROM orders JOIN country_customers ON o_custkey = c_custkey),
        country_suppliers AS (
        SELECT s_suppkey, n_name AS supp_nation
        FROM supplier JOIN nation ON s_nationkey = n_nationkey
        WHERE n_name IN ('FRANCE', 'GERMANY'))
        SELECT /*+ BROADCAST(country_suppliers) SHUFFLE_HASH(country_orders) */
            supp_nation, cust_nation, YEAR(l_shipdate) AS l_year,
            SUM(l_extendedprice * (1 - l_discount)) AS revenue
        FROM lineitem JOIN country_suppliers ON l_suppkey = s_suppkey
        JOIN country_orders ON l_orderkey = o_orderkey
        WHERE l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
          AND ((supp_nation = 'FRANCE' AND cust_nation = 'GERMANY')
            OR (supp_nation = 'GERMANY' AND cust_nation = 'FRANCE'))
        GROUP BY supp_nation, cust_nation, YEAR(l_shipdate)
        ORDER BY supp_nation, cust_nation, l_year"""
    return [Variant(7, 'country_customer_broadcast', sql,
        'Join orders to filtered customer-country payload before exchanging order keys',
        overrides={'spark.rapids.sql.optimizer.joinGraphReorder.enabled': 'false',
                   'spark.rapids.sql.optimizer.joinReorder.enabled': 'false'},
        evidence='GPU customer-country broadcast removes the full orders customer-key exchange')]


def controls():
    from variants import catalog
    from critical_variants import cases as critical_cases
    selection = json.loads((HERE / 'results/261126-aws-speed-of-light/selected-cases-before-q11.json').read_text())
    assert selection['common_configuration_sha256'] == hashlib.sha256(
        (HERE.parents[1] / 'configs/aws48x.json').read_bytes()).hexdigest()
    available = {(c.query, c.name): c for c in catalog(include_followups=True)}
    names = {c['query']: c['variant'] for c in selection['candidates']}
    values = {}
    for q in ORDER:
        c = available[(q, names[q])] if q in names else Variant(q, 'baseline', original(q), 'Retained reference')
        assert hashlib.sha256(c.sql.encode()).hexdigest() == selection['sql_sha256_by_query'][str(q)]
        values[q] = replace(c, overrides=selection['runtime_overrides_by_query'].get(str(q), {}))
    choice = json.loads((HERE / 'results/261126-aws-critical-queries/selection.json').read_text())
    assert choice['confirmed'] and choice['queries'] == [8, 21]
    for c in critical_cases():
        if c.query in choice['queries'] and c.name != 'critical_witness_wide':
            values[c.query] = replace(c, overrides={**values[c.query].overrides, **c.overrides})
    return values
