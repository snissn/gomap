# Draft tracker: recover graph/router locality at the V1 quarter-probe budget

Parent: #4012  
Evidence source: #4015 / PR #4021  
Blocks: #4019

## Authoritative gate

This issue owns the **routing/locality** gate only: graph-overlap `.20` must
reach recall@10 >= `.90` at <=4 of 16 probes under the frozen FP32 canonical
truth contract. Exact partition-union parity, all-partition local-HNSW quality,
and router primary-partition coverage remain separately reported; a higher EF
does not satisfy this gate by itself.

| Control, 100k embedding calibration | p4 recall@10 | p16 recall@10 | Interpretation |
| --- | ---: | ---: | --- |
| stable-ID-hash / disjoint, EF64 | .2578 | 1.0000 | generic balanced baseline |
| graph / disjoint, EF64 | .6211 | .9971 | graph locality helps |
| graph / overlap .20, EF64 | .7201 | .9924 | current canonical candidate |
| graph / overlap .20, EF256 | .7289 | .9998 | local HNSW nearly eliminated |
| exact representative routing | .7291 | 1.0000 | current routing ceiling |
| required target | >= .9000 | >= .9000 | V1 acceptance threshold |

The 100k row is an attribution input, not a V1 pass. The required 1M
high-entropy qualification remains deferred until explicitly budgeted and then
must be rerun as a completion gate.

## Milestones and test-first plan

1. Add a deterministic unit/property test that demonstrates the candidate
   router/assignment change raises primary-home truth coverage at four probes;
   preserve canonical score/tie semantics and deterministic fixtures.
2. Build graph/disjoint, graph/overlap `.20`, stable-ID-hash/disjoint, and
   all-partition controls from the same fixture/index definition; retain M2/M3
   manifests and storage/balance evidence.
3. Run M8 with EF64/128/256 for p4/p16, explicitly separating exact routing,
   partition-local HNSW, and end-to-end recall; repeat p4/p16 three times.
4. When the 100k gate is credibly met, run the declared committed 1M
   high-entropy row with explicit scalar-work, M3 visit, exact-truth visit,
   time, and resource bounds. A timeout or resource refusal is a recorded
   failure/deferred result, never a substituted 100k pass.

## Performance classification

This is a correctness-relevant performance gate: it does not alter the frozen
FP32 oracle, but it changes graph/router construction and must prove both
quarter-probe recall and the coupled QPS/tail behavior. Use retained raw JSON,
not chat summaries, for comparisons. Keep overlap assets <=1.35x disjoint,
balance within the persisted capacity, exact-union ID/score parity passing,
and resource caps explicitly enforced.

## Non-goals

- Do not relax the `.90` target, use approximate all-partition HNSW as exact
  truth, or treat EF-only gain as a routing fix.
- Do not substitute the 100k embedding calibration for the committed 1M
  high-entropy qualification, or infer a semantic-corpus claim.
- Do not enable V1, migrate formats, or change public APIs from this tracker.

## PR, review, and CI policy

Use an issue-scoped branch and one reviewable PR per coherent construction
change. The PR must bind its artifact paths/digests, exact commands, fixture
identity, base/head SHA, and a gate ledger. Before merge: latest-head targeted
tests, affected package tests, required CI, review-thread resolution, and a
human check that the report says experimental/off unless every gate passes.
Performance evidence must be fresh for the exact head; do not reuse a result
after changing score, router, assignment, index, fixture, or topology identity.

## Completion criteria

- [ ] Test-first coverage proves the intended locality invariant.
- [ ] p4 graph-overlap reaches >=.90 on each declared corpus/control with
      three-run median/spread and exact routing reported separately.
- [ ] Matched-recall QPS >=1.15x exhaustive and p95 no worse are demonstrated,
      or the issue records an honest negative disposition and a narrower owner.
- [ ] Exact-union parity, failure honesty, reachability, balance, overlap
      storage, and resource gates pass on retained artifacts.
- [ ] The canonical committed 1M high-entropy rerun is completed with explicit
      work/time/resource accounting and is not replaced by calibration data.
- [ ] Parent #4012 and blocked #4019 are updated with the final disposition.
