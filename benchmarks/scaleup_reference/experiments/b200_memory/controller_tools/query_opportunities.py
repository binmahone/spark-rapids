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

#!/usr/bin/env python3
"""Build a query-by-direction inventory with explicit measurement and semantic limits."""
import json
import os
from pathlib import Path

ROOT=Path(os.environ['B200_PROBE_ROOT']).resolve()
raw=json.loads((ROOT/'evidence/all-query-results.json').read_text())
# E: active mechanism; C: candidate; L: low priority; R: reverse lifetime control.
status_names={'E':'active_in_baseline','C':'unmeasured_candidate','L':'no_priority_candidate','R':'retain_vs_recompute_reverse_control','W':'measured_work_reduction_in_prior_variant_without_qualified_speedup'}
patterns={
1: ('ELLLL','Scan and aggregate; the date filter is weakly selective and all measure columns are used. No new join-related opportunity.'),
2: ('CCER C'.replace(' ',''),'An aggregate-input semi filter is active. Also consider propagating the EUROPE key set to the wide outer supplier branch. Late output payload and stripping have narrower, layout-dependent opportunities.'),
3: ('CLC LL'.replace(' ',''),'Filtered customer still shuffles with orders. Compare a broadcast key set and, separately, preaggregating line revenue by order before joins.'),
4: ('CLCLL','The semi join avoids result multiplication but still shuffles 37.9B qualifying lineitem keys. A compact distinct key set or early order-date key filter needs a cost comparison.'),
5: ('CLLLL','The customer-orders branch is not restricted early to the ASIA nation set. Propagate that necessary key condition without changing final joins or multiplicity.'),
6: ('ELLLL','Filtering and scalar aggregation are active; no meaningful shuffle. Late I/O requires evidence that qualifying rows avoid payload pages, not just high row rejection.'),
7: ('ELLLL','Country key filters and broadcasts already precede the large final join. No additional high-priority candidate from the five directions.'),
8: ('CCLLL','Selective part filtering is active. Consider propagating the filtered customer region to orders before its shuffle. Late payload depends on selection/page locality.'),
9: ('CCLLC','The green-part filter reaches lineitem, but 8B partsupp rows still shuffle. Propagate the same necessary part-key set. Numeric payload stripping has an interval across later joins, but store/restore cost is unmeasured.'),
10:('LCCLC','Customer strings expand before the lineitem join. Compare fact revenue preaggregation before attaching attributes. This is not evidence for late physical reading or a strip/restore implementation.'),
11:('ELERL','Supplier-country filtering and grouping are active. Two partsupp scans remain. Reusing a smaller aggregate is a reverse control for recomputing versus retaining state.'),
12:('ELLLL','Date/mode filtering is active; the wide orders-side shuffle remains. Early keys are a conditional candidate. Replacing priority strings with flags is an adjacent expression/width optimization, not strip/restore.'),
13:('ELCLL','The outer join emits 15.34B rows before counting. Preaggregate order counts, while retaining a final customer-key aggregate to preserve duplicate customers and NULL behavior.'),
14:('ELLLL','The date-filtered join remains. Computing a promo flag before joining is an adjacent width tradeoff; it is not already-expanded-column strip/restore. Part-key preaggregation has limited expected row reduction.'),
15:('ECERL','Two lineitem scans and separate decimal aggregates execute. Retaining/reusing the aggregate may beat recomputation. Preserve the NULL supplier group when finding the maximum; one-sided inferred not-null filtering prevents direct exchange reuse.'),
16:('CL ELL'.replace(' ',''),'NULL-aware anti filtering and distinct supplier state are active. A selective part-key prefilter is a candidate. Grouping attributes are used immediately after the join, so this is not a strong temporary-unused-column stripping case.'),
17:('ECERL','A broadcast semi filter already limits the average input; only about 1.5GB shuffles but about 923GB is read. Reusing filtered lineitem is a reverse lifetime control; late reading requires reader/page evidence.'),
18:('WCCRL','The previous broadcast semi diagnostic greatly reduced shuffled rows. Sharing the grouped quantity state could remove the detail scan and repeated aggregate; compare against the hinted control, not just the slower shuffled baseline.'),
19:('ECLLL','The selective part join and scan filters already avoid a large shuffle. Only about 1.14M join rows survive; late measure-column reading coupled to join selection merits a page-locality feasibility check.'),
20:('WLELL','The previous broadcast semi diagnostic reduced shuffle with zero spill in both arms. Retain it as row-filter evidence; no new high-priority lifetime or stripping candidate.'),
21:('EL ELC'.replace(' ',''),'Counted existence state is proven by the multiplicity SUM and one lineitem scan. Do not count it as a new candidate. State width/admission and temporary supplier-name payload are conditional followups, not established wins.'),
22:('ELCRL','The anti join still consumes 15B order keys. Compare compact existence keys or a costed eligible-customer prefilter. Customer is scanned twice, making reuse a separate reverse lifetime control.')}
keys=['filter_rows','delay_reading_columns','delay_expanding_relationships','release_or_recompute','strip_store_restore_expanded_columns']
selected=[]
for q in range(1,23):
 label='q3-checked' if q==3 else 'all22-baseline' if q in (1,2) else 'all22-remainder'
 rows=[r for r in raw['rows'] if r['query']==q and r['label']==label]
 assert len(rows)==2,(q,len(rows))
 pattern,note=patterns[q];assert len(pattern)==5,(q,pattern)
 records=[]
 for r in rows:
  assert r['validation']['status'] in ('passed','reference-generated')
  records.append({k:r[k] for k in ['arm','iteration','sql_seconds','validation','task_metrics','reader_metrics_all_branches']})
 selected.append({'query':q,'directions':dict(zip(keys,[status_names[x] for x in pattern])),
                  'assessment':note,'selected_baseline_label':label,'samples':records,
                  'performance_qualified':False})
q21=next(r for r in raw['rows'] if r['query']==21)
assert '__rapids_corragg_counted_existence_multiplicity' in q21['plan_text']
report={'classification':'all-query mechanism inventory; timings affected by thermal throttling',
 'gpu_indices':[0,1,2,3],'extra_rmm_alloc_size_limit':False,
 'performance_qualified':False,'healthy_node_performance_benefit':'NOT MEASURED',
 'new_gpu_variant_benefits':'NOT MEASURED; additional GPU timing comparisons deferred after thermal finding',
 'directions':{
  keys[0]:'Reject unnecessary rows before expensive joins, aggregation and movement.',
  keys[1]:'Delay physical reading/materialization of currently unneeded columns; streaming an already-read payload is distinct.',
  keys[2]:'Delay expanding join matches/relationships. Preaggregation and compact existence states are adjacent algebraic probes, not a general factorized execution engine.',
  keys[3]:'Release at last use or recompute instead of retaining. R marks the opposite control: retain a small useful state to avoid recomputation. No new last-use release implementation was measured.',
  keys[4]:'Strip already-expanded temporarily unused columns, compress/store and restore later. Static projection and pre-join payload deferral are distinct.'},
 'status_definitions':status_names,
 'validation_scope':'Two selected normal executions per query against retained GPU baselines; no independent SF10K CPU oracle. Q3/Q10 use complete cutoff prefixes. An earlier Q3 validation attempt failed and is retained separately.',
 'cpu_counterexamples':json.loads((ROOT/'evidence/cpu-variant-checks.json').read_text()),
 'reader_counter_scope':'SQL event plans including scalar subqueries; shared SQL accumulator IDs counted once. Native read bytes are not cold physical disk traffic.',
 'measurement_limits':['All selected GPUs use reference RMM fractions; physical NVML usage includes the reserved pool.',
  'Thermal throttling affects timing qualification. Spill observations characterize this environment.',
  'Shuffle/spill task counters include cleanup tasks, which have zero shuffle/spill; no suite timing is constructed.'],
 'rows':selected,
 'priority_next_checks':['Costed necessary-key propagation: Q9, Q5, Q16, Q2; Q3 broadcast as a separate placement control.',
  'Compact state and preaggregation: Q10, Q13, Q18; Q4/Q22 distinct existence keys as lower-priority controls.',
  'Retain-versus-recompute controls: Q15 and Q17, then Q11/Q22. These do not validate a universal early-release/recompute policy.',
  'Reader feasibility: Q19 or sparse wide-dimension payload; establish page locality and bytes avoided before a larger implementation.',
  'Strip/restore feasibility: Q10, then Q9/Q2; compare incremental value after simpler preaggregation/filtering.'],
 'evidence':{'raw_results':'all-query-results.json','thermal':'thermal-qualification.json',
             'q3_retrospective':'q3-retrospective-cutoff-check.json'}}
(ROOT/'evidence/query-opportunities.json').write_text(json.dumps(report,indent=2)+'\n')
print('queries',len(selected),'selected_normal_actions',sum(len(r['samples']) for r in selected))
