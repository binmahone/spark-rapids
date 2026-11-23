# UCX 1.22 interval-tree patch

`ucx-1.22-interval-search.patch` changes overlap search to follow one branch at
each node of the disjoint interval tree. It includes randomized comparison with
a reference map and a large disjoint insertion regression. The equivalent
upstream change is [UCX #12367](https://github.com/openucx/ucx/pull/12367), commit
`ffbb1aa4c8ffdf2a93628b222f1594a70f76625c`. That commit uses the newer generic
red-black tree; this patch targets the original `v1.22.0` node representation
and its `check_rb_invariant` test helper. It does not upgrade UCX to 1.24.

The search implementation matches the previously retained 1.22 benchmark fix.
The build compiles the full installation with this patch and runs the interval
tree tests. There is no separately loaded replacement `libucs` file.

The patch and the upstream source retain the UCX project's license:
https://github.com/openucx/ucx/blob/v1.22.0/LICENSE
