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

"""CPU checks for configuration boundaries and rejection of incorrect reference results."""
from copy import deepcopy
from pathlib import Path
import os
import subprocess
import tempfile
import time
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from common import expand, load_config, read, write
import services
from q3_tie_gate import validate as validate_q3
from validate_q18_topn import validate_topn
from validate_nvl_result import validate_result, InsufficientValidationEvidence


class ContractTests(unittest.TestCase):
    def test_query_resources_cannot_change_inside_one_application(self):
        config = load_config('aws48x')
        profiles = expand(config, [1, 13, 18])
        self.assertEqual(profiles['1']['spark.task.resource.gpu.amount'], '0.25')
        self.assertEqual(profiles['18']['spark.task.resource.gpu.amount'], '0.5')
        config['application_mode'] = 'suite'
        with self.assertRaisesRegex(ValueError, 'startup-only'):
            expand(config, [1, 18])

    def test_override_does_not_mutate_base_and_nvl_has_no_scan_cap(self):
        config = load_config('nvl72')
        before = deepcopy(config)
        profiles = expand(config, [1, 13])
        self.assertEqual(config, before)
        self.assertNotIn('spark.sql.files.maxPartitionNum', profiles['13'])
        self.assertEqual(profiles['1']['spark.sql.files.maxPartitionBytes'], '4g')
        self.assertEqual(profiles['13']['spark.sql.files.maxPartitionBytes'], '1g')

    def test_duplicate_or_invalid_queries_are_rejected(self):
        for queries in ([], [1, 1], [0], [23]):
            with self.assertRaises(ValueError):
                expand(load_config('nvl72'), queries)

    def test_edge_samples_without_full_result_evidence_are_rejected(self):
        typed = [[{'type': 'long', 'value': '1'}]]
        result = {'rows': 1001, 'typed_first_rows': typed, 'typed_last_rows': typed}
        reference = {'rows': 1001, 'first_rows': ['[1]'], 'last_rows': ['[1]']}
        with self.assertRaises(InsufficientValidationEvidence):
            validate_result(20, result, [reference])

    def test_complete_rows_detect_wrong_value_and_missing_row(self):
        result = {'rows': 1, 'typed_all_rows': [[{'type': 'long', 'value': '3'}]]}
        reference = {'rows': 1, 'all_rows': ['[3]']}
        self.assertEqual(validate_result(2, result, [reference])['status'], 'passed')
        result['typed_all_rows'][0][0]['value'] = '4'
        with self.assertRaises(AssertionError):
            validate_result(2, result, [reference])

    def test_stop_reaps_children_after_the_recorded_leader_exits(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            child_file = directory / 'child.pid'
            script = ("import subprocess,sys; from pathlib import Path; "
                      "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(120)']); "
                      "Path(sys.argv[1]).write_text(str(p.pid))")
            process, receipt = services.launch([sys.executable, '-c', script, str(child_file)],
                                                directory / 'output.log', os.environ)
            state = directory / 'services.json'
            write(state, [receipt])
            try:
                self.assertEqual(process.wait(timeout=10), 0)
                child = int(child_file.read_text())
                self.assertIn(child, services.group_members(receipt))
                services.stop(state)
                self.assertEqual(services.group_members(receipt), [])
                self.assertEqual(read(str(state) + '.stopped.json')['remaining_recorded_processes'], [])
            finally:
                services.stop(state)
                process.wait(timeout=10)

    def test_topn_rejects_wrong_membership_and_wrong_order(self):
        schema = {'fields': []}
        rows = [['a', '1', '1', '1995-01-01', '20', '300'],
                ['b', '2', '2', '1995-01-01', '10', '300']]
        record = {'schema': schema, 'all_rows_retained': True, 'result_rows': 1,
                  'rows': [rows[0]]}
        self.assertTrue(validate_topn(record, rows, schema, limit=1)['topn_sort_keys_equal'])
        record['rows'] = [rows[1]]
        with self.assertRaises(AssertionError):
            validate_topn(record, rows, schema, limit=1)
        record['rows'] = [['invented', *rows[0][1:]]]
        with self.assertRaises(AssertionError):
            validate_topn(record, rows, schema, limit=1)


if __name__ == '__main__':
    unittest.main()
