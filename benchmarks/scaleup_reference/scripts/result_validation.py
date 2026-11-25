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

"""Compare retained result samples while respecting declared SQL ordering."""
import math
import re


def similar(values,right):
    fields=[];floating=[]
    for cell in values:
        value=cell['value'];kind=cell['type']
        if value is None:fields.append('null')
        elif kind in ['float','double'] or kind.startswith('decimal('):
            fields.append(r'([-+]?\d+(?:\.\d*)?(?:[Ee][-+]?\d+)?)')
            floating.append(float(value))
        else:fields.append(re.escape(value))
    match=re.fullmatch(r'\['+','.join(fields)+r'\]',right)
    return bool(match) and all(math.isclose(value,float(reference),rel_tol=1e-8,abs_tol=0.01)
                               for value,reference in zip(floating,match.groups()))


def compare_rows(actual,expected):
    return len(actual)==len(expected) and all(similar(x,y) for x,y in zip(actual,expected))


def compare_q11_window(actual,expected,side):
    """Check retained rows inside a wider sample, allowing extra ties only at its cut edge."""
    if side not in ['first','last'] or len(actual)<len(expected):return False
    parsed=[re.fullmatch(r'\[(-?\d+),([-+]?\d+(?:\.\d*)?(?:[Ee][-+]?\d+)?)\]',row) for row in expected]
    if not parsed or not all(parsed) or any(len(row)!=2 for row in actual):return False
    keys=[match[1] for match in parsed];values=[float(match[2]) for match in parsed]
    actual_keys=[row[0]['value'] for row in actual]
    actual_values=[float(row[1]['value']) for row in actual]
    if len(set(keys))!=len(keys) or len(set(actual_keys))!=len(actual_keys):return False
    if any(left<right for left,right in zip(values,values[1:])):return False
    if any(left<right for left,right in zip(actual_values,actual_values[1:])):return False
    near=lambda left,right:math.isclose(left,right,rel_tol=0,abs_tol=1e-7)
    high,low=values[0],values[-1]
    retained=[row for row,value in zip(actual,actual_values)
              if (value<=high or near(value,high)) and (value>=low or near(value,low))]
    actual_by_key={row[0]['value']:row for row in retained}
    if not set(keys)<=set(actual_by_key):return False
    expected_by_key=dict(zip(keys,expected))
    if not all(similar(actual_by_key[key],expected_by_key[key]) for key in keys):return False
    cut=low if side=='first' else high
    if any(not near(float(row[1]['value']),cut) for key,row in actual_by_key.items() if key not in expected_by_key):return False
    return True
