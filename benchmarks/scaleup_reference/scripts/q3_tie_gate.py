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

"""Validate complete-row top-K membership against a complete cutoff tie group."""
from datetime import date
from decimal import Decimal


def rank(row):
    assert len(row) == 4 and all(value is not None for value in row)
    return -Decimal(row[1]), date.fromisoformat(row[2])


def validate(reference, actual, limit=10):
    assert len(reference) > limit
    reference = [tuple(row) for row in reference]
    actual = [tuple(row) for row in actual]
    assert len(reference) == len(set(reference))
    reference_ranks = [rank(row) for row in reference]
    assert reference_ranks == sorted(reference_ranks)
    cutoff = reference_ranks[limit - 1]
    assert reference_ranks[-1] > cutoff, 'Reference truncates the cutoff tie group'
    required = {row for row in reference if rank(row) < cutoff}
    boundary = {row for row in reference if rank(row) == cutoff}
    assert len(actual) == limit and len(set(actual)) == limit
    assert required <= set(actual), 'A mandatory better-ranked row is missing'
    assert set(actual) <= required | boundary, 'A row is outside the eligible cutoff set'
    assert [rank(row) for row in actual] == sorted(rank(row) for row in actual)
    return dict(required_rows=len(required), cutoff_tie_rows=len(boundary),
        cutoff_slots=limit - len(required), cutoff_revenue=str(-cutoff[0]),
        cutoff_date=cutoff[1].isoformat(), all_rows_valid=True)
