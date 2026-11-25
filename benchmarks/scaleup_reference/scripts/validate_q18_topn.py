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

"""Validate canonical Q18 results against an equal complete relation, allowing ties."""
from collections import Counter
from decimal import Decimal
import json


def validate_topn(record, complete_rows, schema, limit=100):
    assert record['schema'] == schema, 'Output schema differs'
    assert record['all_rows_retained'], 'Complete result rows required'
    rows = record['rows']
    assert record['result_rows'] == len(rows) == min(limit, len(complete_rows))
    encode = lambda row: json.dumps(row)
    complete_counts = Counter(encode(row) for row in complete_rows)
    result_counts = Counter(encode(row) for row in rows)
    assert not result_counts - complete_counts, 'Returned row or multiplicity not in complete relation'
    key = lambda row: (-Decimal(row[4]), row[3])
    expected_keys = sorted(key(row) for row in complete_rows)[:limit]
    assert [key(row) for row in rows] == expected_keys, 'Incorrect top-N ranking or ordering'
    return {'full_relation_membership': True, 'topn_sort_keys_equal': True,
            'schema_equal': True, 'ties_allowed_only_within_equal_order_by_keys': True}
