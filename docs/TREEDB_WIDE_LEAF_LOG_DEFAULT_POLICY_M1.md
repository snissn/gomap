# TreeDB wide leaf-log default policy M1 (#2950)

Status: design/policy only. This report does **not** change production
behavior, on-disk format, benchmark wiring, or c8/c16 defaults. It turns the
#2949 append-path inventory into an implementation-ready policy for the #2943
lane-default stream. #2952 remains the go/no-go decision gate.

Inputs:

- #2949 inventory:
  `docs/TREEDB_LEAF_LOG_APPEND_PATHS_LANE_M0_INVENTORY.md`
- #2925/#2931 final gate:
  `docs/TREEDB_LEAF_LOG_APPEND_LANES_M5_REPORT.md`
- Current code contracts in `TreeDB/caching/leaf_page_log.go`,
  `TreeDB/caching/leaf_page_log_lanes.go`, `TreeDB/db/leaf_page_log.go`,
  `TreeDB/zipper/zipper.go`, `TreeDB/zipper/span_native_apply.go`, and
  `cmd/unified_bench` option/report plumbing.

## Summary policy

If #2952 and #2948 later approve implementation, TreeDB should widen only the
normal cached, non-maintenance, multi-page leaf-log append paths by default:

- Keep the already-admitted span-native worker-owned selected-lane behavior.
- Stripe normal cached multi-page `AppendLeafPages` output by default, but only
  on the normal apply path and only behind a direct leaf-log append-lane policy.
- Stripe normal cached prepared-batch and ChildRef prepared-batch output by
  default under the same policy.
- Keep plain single-page `AppendLeafPage` on lane 0. Explicit selected-lane
  handles used by span-native workers may still append one page on their selected
  lane.
- Exclude maintenance, rewrite/pack/vacuum, recovery/replay, standalone direct
  backend logs, and cold-build/rebuild/bulk callers from default widening.
- Add a direct rollback/control knob; `FlushAdmissionPolicyOff` remains the
  coarse full rollback but is not sufficient as the only rollback for generic
  leaf-log default striping.
- Use a separate conservative c4 cap for the default stripe width. Do not derive
  the default width from an explicit c8/c16 `FlushApplyConcurrency` row.

## Policy questions answered

| Question | Decision |
| --- | --- |
| Should lane-capable batch leaf-page appends stripe by default? | **Yes, after #2952/#2948 approval, but only for normal cached non-maintenance multi-page apply output.** Do not implement this as a blanket change to every `cachingLeafPageLogGroup.AppendLeafPages` caller, because cold-build/rebuild/bulk callers remain excluded. |
| Should lane-capable prepared-batch / ChildRef batch appends stripe by default? | **Yes, with the same scope and gates as batch appends.** The policy admits normal cached non-maintenance prepared-batch and ChildRef batch output; it requires positional refs, prepared-payload validation, cache ownership, and abandoned-output accounting proof. |
| Should single-page `AppendLeafPage` remain lane 0 initially? | **Yes.** Plain/default `AppendLeafPage` stays lane 0. Only a caller that already selected a lane, such as the admitted span-native worker path, may append one page through that selected handle. |
| Which maintenance/rewrite/recovery paths are excluded? | Compact/rewrite/leaf-pack/vacuum maintenance, online/offline value-log/leaf-log rewrite and pack writers, command WAL replay/recovery inline appenders, standalone direct-backend leaf logs, bulk/ordered-root cold-build or fallback rebuild callers, and benchmark rows that are backend-direct or lack leaf-log lane stats. |
| Is `FlushAdmissionPolicyOff` sufficient rollback? | **No.** It is a useful coarse rollback because it disables span-native/backlog/concurrency and should force auto generic leaf-log striping to width 1, but it is too broad and does not name generic leaf-log default striping. #2953 should add a direct leaf-log append-lane policy knob. |
| What default width? | **Separate capped default width.** Auto default striping should use at most c4 total generic lanes when the durable cached admission guardrails pass; c8/c16 remain explicit diagnostics. Configured `FlushApplyConcurrency` is an input/upper bound for admission, not a default promotion source. |

## Proposed default policy table

| Append surface | Default policy after implementation approval | Effective default width | Rollback/fallback | Must remain excluded |
| --- | --- | --- | --- | --- |
| Span-native worker output | Keep current worker-owned selected lanes (`workerID+1`) for admitted span-native apply. No new widening from this policy. | Existing admitted c4 under `FlushAdmissionPolicyAuto`; c8/c16 only explicit. | `FlushAdmissionPolicyOff` disables this path; direct generic leaf-log knob must not break explicit selected-lane semantics unless it is documented to do so. | Recovery/replay and maintenance. |
| Normal cached `AppendLeafPages` multi-page output | Stripe by default only when the caller is the normal cached non-maintenance apply path. | Separate cap, default max 4 total generic lanes, e.g. lane indices `0..width-1`; effective width 1 if not admitted. | Direct leaf-log append-lane policy `off` returns to lane 0; unavailable selected lanes or batches of size `<2` fall back to existing lane-0 behavior. | Bulk/cold-build/rebuild and standalone backend direct logs. |
| Normal cached prepared-batch output | Stripe by default under the same path predicate as batch output. | Same capped width as normal batch. | Same direct rollback and lane-0 fallback. | Maintenance, recovery, cold-build/rebuild. |
| Normal cached prepared ChildRef batch output | Stripe by default under the same path predicate as prepared batch. | Same capped width as normal batch. | Same direct rollback and lane-0 fallback. | Maintenance, recovery, cold-build/rebuild. |
| Plain/default single `AppendLeafPage` | Keep lane 0. | 1 | Direct knob is a no-op for this path because it is intentionally not widened. | Any generic single-page striping. |
| Compact/rewrite/leaf-pack/vacuum maintenance | Exclude. | 1 / current maintenance-owned writer semantics | No change. | All default widening. |
| Command WAL replay/recovery inline appends | Exclude. | 1 / current mutex-protected inline writer semantics | No change. | All default widening. |
| Standalone direct-backend leaf logs | Exclude from cached defaults. | Current behavior unless separately audited. | No change. | Generic cached policy must not silently widen standalone tools. |
| Benchmark open paths | Follow #2951 scope decision. | N/A | Rows must label whether lane stats are meaningful. | Backend-direct rows must not be used as proof of cached default widening. |

## Rollback model and fallback semantics

#2953 should introduce a direct leaf-log append-lane policy, conceptually:

```text
LeafLogAppendLanePolicy:
  auto      # default; allow only the admitted conservative default width
  off       # force generic default batch/prepared/ChildRef appends to lane 0
  explicit  # future diagnostic opt-in; width is explicitly configured/capped
LeafLogAppendLaneWidth: 0 means policy default; >0 is explicit/diagnostic only.
```

Naming may differ in code, but the behavior should be this strict:

1. Direct `off` wins for generic default striping and restores previous lane-0
   behavior for batch, prepared-batch, and ChildRef batch appends.
2. `FlushAdmissionPolicyOff` remains the coarse rollback: it disables
   span-native apply/backlog/concurrency and should make auto generic leaf-log
   striping resolve to width 1. It is still appropriate when the whole admitted
   span-native path must be turned off.
3. Existing data needs no migration. Leaf-log pointers are persistent; rollback
   affects only future append placement. Segments written while striping was on
   remain readable and remain protected by reachability/current-set rules.
4. If a selected lane cannot be acquired, if the effective width is `<2`, if the
   DB is not a cached outer-leaf DB, or if the batch has fewer than two pages,
   the implementation falls back to the existing lane-0 path.
5. Errors fail closed. No root is published unless all refs for the batch are
   returned and validated. Any durable output written before an error is treated
   as abandoned/unpublished output and reclaimed only by existing reachability
   maintenance.

## Code-touch map for #2953/#2954

The implementation should be split so #2953 can land the substrate/rollback knob
before #2954 changes behavior.

### #2953 substrate and rollback knob

| Area | Expected touch points | Notes |
| --- | --- | --- |
| Public/backend options | `TreeDB/db/db.go`, `TreeDB/db/flush_admission_policy.go`, `TreeDB/public.go`, cached option plumbing in `TreeDB/caching/db.go` | Add/normalize the direct leaf-log append-lane policy and resolved effective width. Keep zero values backward-compatible. |
| Benchmark flags/reporting | `cmd/unified_bench/adapter_treedb.go`, related README/tests | Add flags and resolved-option reporting for the direct policy/width. Do not change benchmark open paths in #2953. |
| Stats/counters | `TreeDB/caching/flush_apply_stats.go` and tests | Report effective policy, width, and reason. Add path-attribution counters if mixed-run proof will be required for #2954/#2956. |
| Tests | `TreeDB/db/flush_admission_policy_test.go`, `cmd/unified_bench/*treedb*test.go`, focused cached tests | Prove default normalization, direct `off`, interaction with `FlushAdmissionPolicyOff`, and no behavior change while widening is not yet enabled. |
| Docs | `docs/TREEDB_TUNING.md` and this report if names change | Document rollback semantics without claiming a default behavior change before #2954. |

Recommended stats names for mixed-run proof, following #2949's gap analysis:

- `treedb.cache.leaf_log_lanes.path.batch.append_lanes_used`
- `treedb.cache.leaf_log_lanes.path.batch.append_pages_total`
- `treedb.cache.leaf_log_lanes.path.prepared_batch.append_lanes_used`
- `treedb.cache.leaf_log_lanes.path.prepared_batch.append_pages_total`
- `treedb.cache.leaf_log_lanes.path.prepared_childref.append_lanes_used`
- `treedb.cache.leaf_log_lanes.path.prepared_childref.append_pages_total`

### #2954 widen approved batch paths

| Area | Expected touch points | Notes |
| --- | --- | --- |
| Path-aware admission | `TreeDB/zipper/zipper.go` apply/run config and batch persist helpers | Gate striping on normal cached non-maintenance apply output. Avoid a blanket change that would widen cold-build/rebuild/bulk callers. |
| Cached lane helpers | `TreeDB/caching/leaf_page_log_lanes.go`, `TreeDB/caching/leaf_page_log.go` | Provide lane handles/striped append helpers while preserving positional output and existing `AppendLeafPage` lane-0 semantics. Aggregate created/current snapshots already cover active lanes and must remain so. |
| Prepared output | `TreeDB/zipper/zipper.go`, `TreeDB/zipper/span_native_apply.go` | Preserve prepared payload ownership, ChildRef return semantics, and span-native selected-lane behavior. |
| Observability | cached stats + benchprof export/reporting | Prove generic lane use with path counters and raw per-lane counters; do not infer usage from `flush_admission.admitted`. |
| Tests | `TreeDB/caching`, `TreeDB/db`, `TreeDB/zipper` | Add rollback, lane-use, positional, checkpoint/reopen, GC/current-set, and negative-exclusion tests. |

Implementation detail to avoid scope drift: the generic `cachingLeafPageLogGroup`
methods may stay lane-0 by default unless the implementation can prove the
caller is an admitted normal apply path. A path-aware wrapper/helper is safer
than widening every caller of the group interface.

## Correctness gates for admitted paths

### Existing span-native selected-lane path

- `FlushAdmissionPolicyAuto` still admits only the conservative durable c4
  candidate.
- `FlushAdmissionPolicyOff` still disables span-native/backlog/concurrency.
- Selected worker lanes remain worker-owned and dynamically scheduled; no global
  collector/central append queue is introduced.
- Counters prove usage on the intended path:
  - `treedb.flush_apply.span_native.used_ops_total > 0`
  - `treedb.flush_apply.leaf_log_output.lane.tasks_lanes_used > 1`
  - raw `treedb.flush_apply.leaf_log_output.lane.%02d.tasks_total` distribution
- Existing reopen/GC/rewrite/read-surface tests remain green.

### Normal cached multi-page batch appends

Before default striping may merge:

- Returned pointers are positional: `ptrs[i]` references `leafPages[i]` across
  lane boundaries.
- Read-cache population and per-page record-length hints remain correct.
- Checkpoint/reopen/read-only open can resolve every ref written through striped
  lanes.
- Leaf-generation GC retains referenced lane segments and current unreachable
  segments, and deletes fully unreachable segments after they are no longer
  current/protected.
- Publish uses created+current snapshots for every active lane without forced
  directory refresh scans.
- Current writable file count and sealed segment count are bounded by the width
  policy plus normal rotation; no tiny-file fan-out from single-page appends.
- Direct rollback restores lane 0, proven by raw per-lane counters
  (`lane.00` nonzero, lanes `>00` zero for the generic batch path).

### Normal cached prepared-batch and ChildRef batch appends

All batch gates above also apply, plus:

- Prepared payload count and page-size validation fail closed before publish.
- Returned refs are all leaf-log refs and positional.
- ChildRef batching does not allocate an intermediate pointer slice when the
  caller path is intended to avoid one, unless a measured/accepted tradeoff is
  documented.
- In-flight leaf-ref cache owns leaf bytes and cannot observe later scratch reuse.
- Prepared-output accounting remains meaningful:
  - `treedb.flush_apply.prepared_output.leaf_log_pages_prepared_total`
  - `treedb.flush_apply.prepared_output.leaf_log_pages_installed_total`
  - `treedb.flush_apply.prepared_output.leaf_log_pages_abandoned_total`
- Error paths leave unpublished durable output as abandoned output only; they do
  not publish partial roots.

### Negative/exclusion gates

- Plain/default `AppendLeafPage` remains lane 0 under auto and direct off.
- Bulk/cold-build/rebuild callers do not use the new default stripe policy.
- Compact/rewrite/leaf-pack/vacuum maintenance tests show no leftover segment
  debt and no protected-path regression.
- Command WAL recovery/replay tests do not use generic default striping.
- Standalone/direct-backend leaf logs are unchanged unless a separate issue
  audits and admits them.

## Performance gates and required before/after rows

A later implementation PR must provide identical-command before/after evidence
from the base SHA and implementation head SHA. Each row should record command,
SHAs, host notes, artifact directory, ops/sec (or latency), checkpoint stages,
leaf-log lane counters, allocation evidence if relevant, and storage/file-count
impact.

Required rows if #2954 changes defaults:

| Row | Purpose | Required evidence |
| --- | --- | --- |
| `treedb` `random_write` #2916 shape, default auto | Primary random-write target. | Throughput, checkpoint total/stages, `span_native` counters, generic leaf-log path counters, storage/file counts. |
| `treedb` `batch_random` #2916 shape, default auto | Batch write throughput and leaf-output shape. | Same as above, plus generic batch lane distribution. |
| Rollback/off row | Prove direct rollback and coarse rollback. | Direct leaf-log policy off returns generic paths to lane 0; `FlushAdmissionPolicyOff` disables span-native and auto generic striping. |
| c8/c16 explicit diagnostics | Ensure defaults did not silently promote higher widths. | Explicit labels only; lane width/counters must match the explicit policy, not default auto. |
| Read/scan guardrail row | Ensure fresh lane placement does not regress reads/scans. | Random read, parallel read, batch read, full scan, prefix scan, cache counters, no-cache row if affected. |
| Storage/file-count/current-set row | Catch segment fan-out or current-set debt. | `index.db`, `leaf_vlog` bytes, total files, current writable files, sealed files, rotations, GC/rewrite sanity. |
| Collection rows, if #2951 says affected | Avoid benchmark-scope overclaim. | `collection_storage document_only insert_batch` and at least one indexed/template-v1 collection `InsertBatch` row, with open-path identity and lane stats. |

Performance acceptance:

- Material regressions in throughput, checkpoint wall, allocation rate, storage
  footprint, file count, current-set size, GC/rewrite time, read/cache/scan
  throughput, or correctness counters are blocking unless minimized,
  correctness-required, and explicitly accepted.
- A neutral result is not enough for an optimization claim unless #2952/#2948
  explicitly rescope the branch as instrumentation/safety-only.
- If generic lane counters do not prove widened-path usage, the benchmark row
  cannot be used as evidence for the wide-default policy.

## Interaction with #2951 and #2952

- #2950 is not a go/no-go decision. It supplies the policy boundary, rollback
  model, and acceptance gates to #2952.
- #2952 must combine this policy with #2946 checkpoint-wait classification,
  #2947 apply-ceiling classification, and #2951 benchmark-scope classification.
- #2951 decides whether collection/document rows remain backend-direct, switch
  to normal TreeDB option resolution, split labels/modes, or defer for missing
  evidence. This policy must not treat backend-direct rows or rows without lane
  stats as proof of generic cached default striping.
- #2953/#2954/#2956 remain blocked until #2952 and #2948 select the wide-default
  implementation branch. If #2952 chooses checkpoint/apply-first,
  benchmark-wiring-only, keep-opt-in, or missing-evidence, this policy remains a
  documented boundary rather than an implementation mandate.

## Non-goals and exclusions

- No production lane widening in this report.
- No on-disk format change or data migration.
- No default c8/c16 promotion.
- No global collector, central append queue, or collector goroutine.
- No maintenance/rewrite/recovery widening without a separate audit and proof.
- No standalone/direct-backend widening without separate observability and proof.
- No benchmark wiring or relabeling implementation.
- No claim that #2916/#2899 or #2943 are resolved.

## Downstream acceptance checklist

For #2953/#2954/#2956, do not claim mergeability until all applicable items are
checked:

- [ ] Direct leaf-log append-lane policy and resolved width are documented,
      reported by benchmarks, and tested.
- [ ] Direct policy `off` restores lane-0 behavior for generic batch,
      prepared-batch, and ChildRef batch paths.
- [ ] `FlushAdmissionPolicyOff` remains a coarse rollback and does not leave auto
      generic striping enabled.
- [ ] Normal cached non-maintenance batch path stripes by default only when
      admitted; cold-build/rebuild/bulk callers remain excluded.
- [ ] Normal cached prepared-batch and ChildRef batch paths satisfy positional,
      cache ownership, validation, and accounting gates.
- [ ] Plain/default single `AppendLeafPage` remains lane 0.
- [ ] Span-native selected worker output remains unchanged and proven by
      span-native counters.
- [ ] Checkpoint/reopen/read-only/GC/current-set/rewrite guardrails pass.
- [ ] Performance before/after rows include lane counters, checkpoint stages,
      storage/file counts, and rollback rows.
- [ ] #2951 benchmark-scope decision is reflected before collection/document
      benchmark rows are used as evidence.
- [ ] #2952/#2948 explicitly authorize the implementation branch.
