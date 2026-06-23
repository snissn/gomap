# TreeDB leaf-log append paths LANE-M0 inventory (#2949)

Status: planning/classification only. This document makes no production behavior
change and does not approve a default lane-width change by itself. It feeds the
#2943 lane-default stream (#2950/#2952) after the #2925/#2931 selected
span-native lane final gate.

## Scope and contracts inspected

Current `origin/main` (`3b06cc011c2aae3dccc244fdbf61884a0244732e`) has these
relevant contracts:

- Leaf-log/value-log bytes are persistent storage. Leaf-log segments must remain
  reachable until GC/rewrite proves they are unreachable; they are not ephemeral
  WAL records or temporary cache/spill paths.
- `DB.SetLeafPageLog` wraps logs with record-length/cache hints and then lane
  selection. A caller that just calls `AppendLeafPage` or `AppendLeafPages` uses
  the default lane. A caller that explicitly asks `LeafPageLogLane(workerIndex)`
  can append through a selected lane.
- `CreatedLeafPageLogSegmentsSnapshot()` and
  `CurrentLeafPageLogSegmentsSnapshot()` are the publish contract for leaf-log
  segments. Publish combines created+current snapshots, promotes current
  writable file IDs, demotes stale current leaf-log file IDs, and marks created
  segments registered after publication.
- Cached leaf-log groups aggregate created/current snapshots across every active
  cached append lane. Standalone/rewrite lane groups also aggregate snapshots
  across cloned selected lanes.
- The #2925/#2931 final gate approved selected worker-owned lanes only for the
  admitted span-native output path. Plain `AppendLeafPage` remained lane 0;
  `FlushAdmissionPolicyOff` is the current rollback for that admitted path;
  c8/c16 remain explicit diagnostic rows.

## Append-path matrix

| Path | Current lane behavior | Candidate default | Safety classification | Required proof before any change |
| --- | --- | --- | --- | --- |
| span-native worker output | When span-native apply is used with multiple scheduled workers, workers use `workerID+1` selected lanes through `LeafPageLogLane`; otherwise they fall back to the base log/default lane. | Keep existing admitted worker-owned selected-lane behavior; do not widen beyond admission policy here. | **Approved/current path**: already admitted by #2925/#2931 for c4 auto; c8/c16 remain explicit. | Existing final-gate evidence plus latest-head counters: `treedb.flush_apply.span_native.used_ops_total > 0`, `treedb.flush_apply.leaf_log_output.lane.tasks_lanes_used` equals scheduled worker-owned lane count, and cached raw lane stats show appends across selected lanes. Keep reopen/GC/rewrite guardrails green. |
| cached leaf-log batch appends | `cachingLeafPageLogGroup.AppendLeafPages` uses `defaultLog()` (`workerIndex=0`) unless the caller already selected a lane. Multi-page appends preserve positional `ptrs[i] -> leafPages[i]` mapping and cache/record-length hints. The same `AppendLeafPages` surface is also reached by `bulk.BuildWithOptions` cold-build/rebuild callers when they are passed a leaf log (for example ordered-root cold builds). | Candidate for a future conservative wide default, likely capped at admitted c4 width, **only for normal cached non-maintenance apply**. Bulk/cold-build callers are explicitly excluded from that generic candidate until separately classified/proven. | **Unknown / not approved for behavior change**. | Tests must prove positional pointer mapping, read-cache and record-length hints, checkpoint/reopen, GC current-set retention/deletion, no forced current-set refresh scans, bounded current segment/file-count growth, and no regression to publish snapshot semantics. Lane usage proof can use isolated workloads with `treedb.cache.leaf_log_lanes.append_lanes_used`, `treedb.cache.leaf_log_lanes.append_pages_total`, and raw `treedb.cache.leaf_log_lanes.lane.%02d.*` counters. Bulk/cold-build proof would also need root-build/rebuild fail-closed behavior and publish/retirement correctness; without that proof, those callers remain excluded. Mixed-workload attribution is missing (see counters section). |
| cached prepared-batch / ChildRef appends | `AppendPreparedLeafPages` and `AppendPreparedLeafPageChildRefs` also route through `defaultLog()` unless a selected lane was requested. ChildRef batching avoids intermediate pointer slices but still requires positional leaf/ref correspondence and cache population. | Candidate for a future conservative wide default together with cached batch appends, if proof covers prepared payload ownership and ChildRef return semantics. | **Unknown / not approved for behavior change**. | Tests must prove prepared payload length/page-size validation, returned refs are all leaf-log refs and positional, in-flight cache owns leaf bytes, record-length hints survive, output ownership/abandoned-output accounting is unchanged, and checkpoint/reopen/GC/rewrite remain correct. Existing aggregate lane counters can prove usage only in isolated prepared/ChildRef workloads; path-specific counters are missing for mixed runs. |
| standalone backend leaf-log batch appends | `NewStandaloneLeafPageLog` installs a `rewriteWriter` leaf log. `SetLeafPageLog` wraps it in lane selection; plain `AppendLeafPages` uses lane 0, while explicit selected lanes can clone writer state with shared seq/RID allocators. | Possible future candidate only if direct-backend benchmarks/tools intentionally opt in; not a cached default. | **Unknown + missing observability**. | Must prove file-ID lane/sequence uniqueness, shared RID allocator correctness, all cloned lanes appear in created/current snapshots, publish current-set registration/demotion is correct, checkpoint/reopen and recovery see every segment, GC/rewrite keeps referenced segments and deletes unreachable ones, and file-count growth is bounded. Missing standalone lane counters block mixed benchmark proof. |
| single `AppendLeafPage` | Plain calls use lane 0. A selected-lane handle can append one page only when a caller explicitly requested that lane (for example span-native worker output). | Keep lane 0 initially. | **Explicitly excluded** from wide default for now. | If revisited, prove tiny-generation/file-count behavior, current-set churn, read-cache hints, checkpoint/reopen, GC retention of current unreachable segments, and no pathological segment fan-out. Targeted tests should prove lane 0 with raw per-lane counters (`lane.00.append_*` nonzero and lanes `>00` zero); aggregate `append_lanes_used=1` alone is insufficient. |
| compact/rewrite/leaf-pack maintenance | Special `rewriteWriter`/leaf-ref rewrite/pack/compact writers use the reserved leaf-log lane and maintenance-owned write/cleanup semantics; batch K and segment creation are tuned for rewrite/pack. | Exclude unless separately audited. | **Explicitly excluded / high risk**. | A separate audit would need no-leftover-debt proof: created segment cleanup on failure, source/destination generation accounting, protected path handling, rewrite/pack manifest updates, GC/rewrite interlock, offline/online vacuum behavior, and bounded file-count debt. Do not infer safety from span-native counters. |
| command WAL / replay / recovery | `replayInlineAppender` and `replayInlineLeafPageLog` append through a mutex-protected inline rewrite writer during recovery/replay; no selected-lane distribution. | Exclude. | **Explicitly excluded / recovery-sensitive**. | Only change with recovery-specific proof: command WAL replay ordering, RID reservation/hand-off to live appenders, meta/root/AppliedLSN atomicity, checkpoint/reopen, crash/truncated-tail tests, and current leaf segment registration. No lane widening in #2949. |
| collection/document benchmark open path | Mixed. Normal `treedb` in `unified_bench` uses cached `treedb.Open` and the resolved options report. Hidden `treedb_backend` installs a standalone leaf log when outer leaves are enabled. `suite collection_storage` currently opens `backenddb.Open` directly with command WAL and does not use cached leaf-log lane defaults. Package collection benches and several document/gateway harnesses use `OpenBackendWithCachedLeafLog` when their storage policy enables outer leaves. | Benchmark-scope decision, not a storage-default decision. Rows should either use normal cached TreeDB defaults, remain backend-direct and be labeled as such, or split labels/rows. | **Unknown benchmark semantics**. | For each benchmark row, report resolved options/open path, `IndexOuterLeavesInValueLog`, cached-vs-backend-direct, `treedb.flush_admission.*`, generic leaf-log lane stats when present, and whether stats are absent because the row is backend-direct/no leaf-log. Collection/document rows must not be interpreted as proof of a generic wide default unless lane stats and open-path identity prove it. |

## Exit-gate classification

### 1. Paths approved for possible wide default

- **Already approved/current behavior:** span-native worker output on the admitted
  path. Keep existing worker-owned selected lanes; do not widen c8/c16 by
  default from this inventory.
- **Approved only as future candidates requiring proof:** cached normal batch
  `AppendLeafPages` and cached prepared batch/ChildRef appends on non-maintenance
  cached apply paths. They are candidates for #2950 policy design, not approved
  for production behavior change yet.

### 2. Paths explicitly excluded

- Plain single `AppendLeafPage` default widening.
- Bulk/ordered-root cold-build or fallback-rebuild callers of
  `bulk.BuildWithOptions`, unless #2950 separately classifies and proves them.
- Compact/rewrite/leaf-pack/vacuum maintenance writers.
- Command WAL replay/recovery inline appenders.
- c8/c16 as defaults.
- Any collection/document benchmark row whose open path is backend-direct or
  lacks lane stats, unless the benchmark-scope issue deliberately splits or
  relabels it.

### 3. Unknown paths requiring tests or measurement

- Cached batch appends and cached prepared/ChildRef appends require the proof
  listed above before implementation.
- Standalone backend batch appends require both tests and new observability.
- Collection/document benchmark rows require open-path classification and stats
  reporting before they can be used as default-policy evidence.

### 4. Rollback/control knobs

Existing knobs:

- `Options.FlushAdmissionPolicy = FlushAdmissionPolicyOff` and
  `-treedb-flush-admission-policy=off` roll back the current admitted
  span-native selected-lane path by disabling span-native apply, backlog
  coalescing, and flush-apply concurrency.
- `FlushAdmissionPolicyExplicit`, `FlushApplyConcurrency`,
  `FlushApplySpanNative`, and `FlushBacklogCoalescing` keep c4/c8/c16 and other
  experiments explicit.
- `IndexOuterLeavesInValueLog=false` removes leaf-log-backed outer leaves for a
  rebuilt/new DB format, but it is not a lightweight runtime rollback for an
  existing leaf-log DB.
- Benchmark harnesses can choose cached `treedb`, direct `treedb_backend`, or
  collection-specific backend open paths; labels must say which path was used.

Missing for future generic widening:

- A narrow generic leaf-log default-lane control knob/policy is still needed if
  #2950 chooses to widen cached batch paths independently of span-native apply.
  `FlushAdmissionPolicyOff` currently rolls back span-native admission, but it
  does not name a standalone generic batch lane-width policy. A future #2950/#2953
  design should add or explicitly bind such a rollback knob before #2954 changes
  defaults.

### 5. Exact counters/proofs for lane usage

Existing counters/proofs to use:

- Span-native selected output:
  - `treedb.flush_apply.span_native.used_ops_total`
  - `treedb.flush_apply.leaf_log_output.lane.tasks_lanes_used`
  - `treedb.flush_apply.leaf_log_output.lane.%02d.tasks_total`
  - `treedb.flush_apply.leaf_log_output.append_pages_total`
- Cached generic leaf-log lanes:
  - `treedb.cache.leaf_log_lanes.append_lanes_used`
  - `treedb.cache.leaf_log_lanes.append_pages_total`
  - `treedb.cache.leaf_log_lanes.lane.%02d.append_pages_total`
  - `treedb.cache.leaf_log_lanes.lane.%02d.append_calls_total`
  - `treedb.cache.leaf_log_lanes.lane.%02d.segments_active`
  - `treedb.cache.leaf_log_lanes.lane.%02d.segments_closed`
  - Lane-0 proofs must inspect raw per-lane keys (lane `00` nonzero,
    lanes `>00` zero), not only aggregate `append_lanes_used=1`.
- Prepared output ownership/accounting adjuncts:
  - `treedb.flush_apply.prepared_output.leaf_log_pages_prepared_total`
  - `treedb.flush_apply.prepared_output.leaf_log_pages_installed_total`
  - `treedb.flush_apply.prepared_output.leaf_log_pages_abandoned_total`

Known counter gaps (do not implement in #2949):

- Mixed cached workloads cannot separate ordinary batch appends from prepared
  batch/ChildRef appends using only existing aggregate lane counters. Missing
  path-attribution counters, if #2950 requires mixed-run proof:
  - `treedb.cache.leaf_log_lanes.path.batch.append_lanes_used`
  - `treedb.cache.leaf_log_lanes.path.batch.append_pages_total`
  - `treedb.cache.leaf_log_lanes.path.prepared_batch.append_lanes_used`
  - `treedb.cache.leaf_log_lanes.path.prepared_batch.append_pages_total`
  - `treedb.cache.leaf_log_lanes.path.prepared_childref.append_lanes_used`
  - `treedb.cache.leaf_log_lanes.path.prepared_childref.append_pages_total`
- Standalone/direct-backend leaf logs do not expose cache-style lane counters.
  Missing counters if standalone backend lanes become policy candidates:
  - `treedb.backend.leaf_log_lanes.append_lanes_used`
  - `treedb.backend.leaf_log_lanes.append_pages_total`
  - `treedb.backend.leaf_log_lanes.lane.%02d.append_pages_total`
  - `treedb.backend.leaf_log_lanes.lane.%02d.append_calls_total`
- Benchmark reports need open-path identity alongside stats. If existing reports
  are insufficient, add benchmark metadata rather than inferring from admission:
  - `treedb.benchmark.open_path` (`cached`, `backend_direct`,
    `backend_with_cached_leaf_log`, `standalone_leaf_log`, or `none`)
  - `treedb.benchmark.index_outer_leaves_in_vlog`
  - `treedb.benchmark.leaf_log_stats_available`

## No behavior change

This inventory intentionally changes no Go code, no on-disk format, no default
lane policy, no checkpoint coordination, no global collector/queue, and no c8/c16
defaults. It documents the current contracts and names the proof required before
later issues can change behavior.
