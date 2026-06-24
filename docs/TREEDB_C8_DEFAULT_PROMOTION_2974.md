# TreeDB c8 default promotion (#2974)

This is the closeout report for the post-#2960 span-native apply default graph:
issues #2975, #2976, #2977, #2978, #2979, and #2980.

## Decision

Promote the default `FlushAdmissionPolicyAuto` span-native apply candidate from
c4-shaped behavior to a machine-aware cap:

```text
if FlushAdmissionPolicyAuto admits the span-native/backlog candidate:
  FlushApplyConcurrency = min(runtime.GOMAXPROCS(0), 8)
  decline only when the normalized worker count is < 2, or durability/policy
  guardrails fail
```

The cap is intentionally **8**, not all logical CPUs. Explicit c4/c8/c16 rows
remain available through `FlushAdmissionPolicyExplicit` plus the existing
`FlushApplyConcurrency`, `FlushApplySpanNative`, and `FlushBacklogCoalescing`
knobs. Immediate rollback is still `FlushAdmissionPolicyOff` or
`-treedb-flush-admission-policy=off`.

## Policy table (#2976)

| Host / setting | Previous auto behavior | New auto behavior | Override / rollback | Proof counter |
| --- | --- | --- | --- | --- |
| `GOMAXPROCS=1` | decline/fail closed | decline/fail closed | explicit policy can still run experimental c1 span-native; off disables | `treedb.flush_admission.reason=low_concurrency` |
| `GOMAXPROCS=4` | c4 adaptive | c4 adaptive (`min(4,8)`) | explicit c4 remains available | `treedb.flush_admission.flush_apply_concurrency=4` |
| 6c/12t host (`GOMAXPROCS=12`) | c4 adaptive | c8 capped adaptive | explicit c4/c8/c16 remain available | `treedb.flush_admission.flush_apply_concurrency=8`, `scheduled_workers_max=8` |
| Large host (`GOMAXPROCS>8`) | c4 adaptive | c8 capped adaptive | use explicit policy for c16+ experiments | `flush_apply_concurrency=8`; c16 remains opt-in |
| `FlushAdmissionPolicyExplicit` | caller knobs preserved | caller knobs preserved | use for c4/c8/c16 rows | `reason=explicit_opt_in` |
| `FlushAdmissionPolicyOff` | force off | force off | rollback path | `reason=policy_off`, concurrency/span/backlog false/0 |

The stats reason is now `auto_admitted_capped_adaptive`; the selected worker
count remains visible through `treedb.flush_admission.flush_apply_concurrency`
and `treedb.flush_apply.span_native.scheduler.scheduled_workers_max`.

## M0 evidence matrix (#2975)

Base commit: `d504cf39b9570052a42fef67e1ee0ea202133d43` (post-#2960 / PR #2973).
Host: 6c/12t Intel i5-11400F.

Artifacts:

- default row: `/tmp/treedb_post2960_default_probe_20260623_195520/default`
- explicit saturation rows: `/tmp/treedb_post2960_saturation_20260623_182712/{c4,c8,c16}`
- perf-stat confirmation: `/tmp/treedb_post2960_perfstat_20260623_183251/{c4,c8,c16}`
- summary: `/tmp/treedb_post2960_saturation_summary.md`

| row | ops/s | effective/default workers | scheduled workers max | span-native fallback | append lanes used | leaf-vlog files | read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| current default | 642,203 | 4 | 4 | 0 | n/a | n/a | current auto is c4-shaped |
| explicit c4 | 627,858 | 4 | 4 | 0 | 5 | 66 | c4 control |
| explicit c8 | 807,958 | 8 | 8 | 0 | 9 | 66 | +25.8% vs current default |
| explicit c16 | 806,142 | 12 (GOMAXPROCS cap) | 12 | 0 | 13 | 74 | flat vs c8; higher lane/file footprint |

Perf-stat no-profile confirmation showed c4 624,439 ops/s, c8 794,593 ops/s,
and c16 812,618 ops/s. c8 was the material improvement over current default;
c16 did not justify becoming the default.

## Implementation (#2977)

Changed the auto admission policy only:

- auto cap is now `8` workers;
- low-concurrency guardrail declines normalized worker counts `<2`;
- admitted auto uses the selected candidate worker count, not a hard c4;
- explicit and off policies return before auto selection and remain unchanged;
- unified-bench help/tests now describe the capped auto default.

No on-disk format, value-log, leaf-log storage, maintenance, rewrite, or recovery
format changed.

## Candidate performance/default gate (#2979)

Candidate artifacts: `/tmp/treedb_c8_default_candidate_20260623_202151`.
Baseline no-profile context rerun: `/tmp/treedb_c8_default_base_now_20260623_202514`.

Profile-dir rows on the candidate PR head:

| row | ops/s | admission | scheduled workers max | fallback | lane tasks used | append lanes used | leaf-log append wait | worker busy ratio | index | leaf-vlog |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| candidate default | 810,414 | auto capped adaptive, c8 | 8 | 0 | 8 | 9 | 0.100s | 0.839 | 88 MiB | 968 MiB / 66 files |
| explicit c4 | 607,707 | explicit c4 | 4 | 0 | 4 | 5 | 0.090s | 0.756 | 97 MiB | 981 MiB / 66 files |
| explicit c8 | 829,271 | explicit c8 | 8 | 0 | 8 | 9 | 0.097s | 0.844 | 88 MiB | 968 MiB / 66 files |
| explicit c16 | 731,966 | explicit c16 (effective c12) | 12 | 0 | 12 | 13 | 0.111s | 0.858 | 105 MiB | 996 MiB / 74 files |

No-profile default context:

| row | ops/s | notes |
| --- | ---: | --- |
| base current default rerun | 623,867 | c4-shaped auto, `auto_admitted_c4_adaptive` |
| candidate default rerun | 859,865 | c8 capped auto, `auto_admitted_capped_adaptive` |

The candidate default is close to explicit c8 behavior in the profile-dir matrix
(810,414 vs 829,271 ops/s) and materially improves the original current default
(+26.2% vs 642,203 ops/s). The no-profile base/candidate default rerun also
shows a material improvement (+37.8%). c16 remains opt-in: it used more lanes and
leaf-vlog files; it was flat with c8 in the profile-dir matrix and only slightly
ahead in the no-profile perf-stat confirmation, so it did not justify becoming
the default cap.

## Maintenance/correctness gate (#2978)

Candidate maintenance artifacts: `/tmp/treedb_c8_default_maintenance_20260623_202948`.

Commands passed:

```sh
GOWORK=off go test ./TreeDB/db \
  -run 'Test(LeafPageLogLanes|LeafGenerationGC|ValueLogGC|ValueLogRewrite|MaintenanceRoots|Maintenance_ModerateChurn|CurrentSetRefresh|OuterLeafCommit|RegisterValueLogSegment|Vacuum.*OuterLeaves|VacuumRewriteCollectionRoot|NormalizeFlushAdmission|FlushAdmissionStatsExposePolicyReasonAndCandidate)' \
  -count=1

GOWORK=off go test ./TreeDB/collections \
  -run 'Test(TextV2RewriteStorageMaintenanceAndValueLogGC2630|TextHybridScaleMaintenanceRewritePostconditions2731|TextV2WritePathPostingBlocksGCCompactReopen2626|TextV2PostingBlocksReachValueLogMaintenance2625|ColumnRetainedPayloadValueLogPlacementGCRewrite|ColumnRetainedPayloadSemanticStreamV1SideRootRewrite|ColumnAssetGC|ColumnAssetRewrite|TypedColumnMultipartPartSetRefsRewriteAndGCSafe1787|TypedColumnReachabilityRefsExposedForMaintenance|TextV2MaintenancePolicy.*|CollectionReadViewMappedPinsProtectRewriteCandidates)' \
  -count=1

GOWORK=off go test -race ./TreeDB/db \
  -run 'Test(LeafPageLogLanes|LeafGenerationGC|ValueLogGC|ValueLogRewrite|MaintenanceRoots|Maintenance_ModerateChurn|CurrentSetRefresh)' \
  -count=1

GOWORK=off go test -race ./TreeDB/collections \
  -run 'Test(TextV2RewriteStorageMaintenanceAndValueLogGC2630|ColumnAssetGC|ColumnAssetRewrite|ColumnRetainedPayloadValueLogPlacementGCRewrite)' \
  -count=1
```

Additional package validation passed:

```sh
GOWORK=off go test ./TreeDB/db ./cmd/unified_bench -count=1
```

## Caveats and non-goals (#2980)

- c16/all-logical-core default is **not** introduced.
- Wide generic leaf-log defaults remain separate; this change only affects the
  existing admitted span-native apply path.
- Maintenance/rewrite/recovery lanes are unchanged.
- The value log and leaf log remain persistent storage; this PR changes only
  in-memory default worker selection for admitted cached-mode apply.
- If workload-specific checkpoint, read latency, allocation, or footprint
  guardrails regress, rollback is immediate with `FlushAdmissionPolicyOff` or
  `-treedb-flush-admission-policy=off`.
