# #4327 retrieval qualification decision

## Decision

The historical broad-text retrieval gap does not reproduce on the current text-v2 path. The retained 100k and 1M text rows preserve exact score-only pruning, zero document fetches, zero fail-closed outcomes, and complete reopen guardrails. No text product optimization target is activated.

The expensive 1M hybrid rows are directional fixed-budget measurements, not text optimization gates. Their reports record `budget_policy=fixed` and `budget_stop=exact_bound_insufficient`; they deliberately consume the configured 65,536-candidate boundary rather than claim adaptive exact termination. Differential allocation profiles localize their current costs without changing product behavior.

## Retained evidence

- `retrieval_100k/`: three serialized 100k repetitions, eight rows, 25 samples per row. Exact harness commit and base are recorded in its context and summary.
- `retrieval_1m/`: three serialized 1M repetitions, eight rows, 25 samples per row. Exact harness commit and base are recorded in its context and summary.
- `profile_1m_accepted/scalar/`: isolated 1M `hybrid_text_scalar_no_docs` allocation profile and baseline.
- `profile_1m_accepted/vector/`: isolated 1M `hybrid_text_vector_no_docs` allocation profile and baseline.
- `profile_1m/`: earlier profiling evidence retained for provenance; the accepted isolated profiles above supersede it for allocation attribution.

The accepted profiles were produced from clean commit `c11afbf0486a51083d0ec2c0efb46e597e61330b`, base `4a5448e22`, on Darwin 25.2.0 arm64 / Apple M3 with Go 1.26.0. Each uses 1,000,000 rows, 25 warm samples, isolated CPU-or-allocation capture, and a differential `alloc_space` baseline. The profiling harness from that commit was subsequently merged by #4351.

## Allocation attribution

The exact command for each retained profile is:

```sh
go tool pprof -top \
  -base allocs.pprof.base \
  -ignore=runtime/pprof \
  -focus='SearchHybrid|searchHybridVectorCandidatesWithAllowSetBudget' \
  allocs.pprof
```

Scalar differential allocation: 5,043.04 MiB total in focus. Largest flat owners are norm-block decode 1,406.67 MiB (27.89%), docmap-block decode 1,215.93 MiB (24.11%), leaf lookup 702.93 MiB (13.94%), OR block-max execution 499.23 MiB flat / 3,925.16 MiB cumulative, and hybrid fusion 488.84 MiB (9.69%).

Vector differential allocation: 6,839.82 MiB total in focus. Largest flat owners are norm-block decode 1,406.67 MiB (20.57%), docmap-block decode 1,215.93 MiB (17.78%), hybrid fusion 1,009.79 MiB (14.76%), leaf lookup 691.01 MiB (10.10%), candidate append 605.27 MiB (8.85%), OR block-max execution 512.50 MiB, and native vector candidate scratch 406.64 MiB.

These profiles classify the cost boundary; they do not authorize speculative optimization. Any follow-through belongs to a separately owned, prospectively targeted hybrid-budget or allocation issue.

`SHA256SUMS` binds every retained file except itself.
