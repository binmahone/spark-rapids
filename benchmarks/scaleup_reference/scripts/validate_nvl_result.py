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

"""Require complete-result evidence before accepting a benchmark sample."""
from result_validation import compare_q11_window, compare_rows


class InsufficientValidationEvidence(AssertionError):
    """The available references cannot establish full result equivalence."""


def _same_complete_multiset(result, reference):
    keys = ('multiset_sha256_sum', 'multiset_sha256_xor')
    return all(isinstance(result.get(key), str) and len(result[key]) == 64
               and isinstance(reference.get(key), str) and len(reference[key]) == 64
               and result[key] == reference[key] for key in keys)


def validate_result(query, result, references):
    sample_only_match = False
    for reference in references:
        if result['rows'] != reference['rows']:
            continue
        if reference.get('all_rows') is not None:
            if compare_rows(result.get('typed_all_rows', []), reference['all_rows']):
                return {'status': 'passed', 'full_result_equivalence_proven': True,
                        'scope': 'All rows; integer/text exact, floating rel_tol=1e-8 abs_tol=0.01',
                        'independent_cpu_reference': False}
            continue
        if query == 11:
            samples_match = (
                compare_q11_window(result['typed_validation_first_rows'], reference['first_rows'], 'first')
                and compare_q11_window(result['typed_validation_last_rows'], reference['last_rows'], 'last'))
        else:
            samples_match = (compare_rows(result['typed_first_rows'], reference['first_rows'])
                             and compare_rows(result['typed_last_rows'], reference['last_rows']))
        if not samples_match:
            continue
        if _same_complete_multiset(result, reference):
            return {'status': 'passed', 'full_result_equivalence_proven': True,
                    'scope': 'Complete-result multiset fingerprints and retained ordering samples',
                    'independent_cpu_reference': False}
        sample_only_match = True
    if sample_only_match:
        raise InsufficientValidationEvidence(
            f'Q{query}: samples match but complete-result fingerprints differ or are missing. '
            'Do not publish this sample as validated. Floating differences require a complete '
            'key/value reference comparison; the retained edge samples cannot establish parity.')
    raise AssertionError('Result did not match the retained complete-result reference')
