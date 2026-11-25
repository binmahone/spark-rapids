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
    raise ValueError(f'No audited transformation for Q{query}, arm={arm}')
