## 2026-04-10

### Scope

- first-stack branch head:
  - `eb1325b3` `treedb: use debt ledger for referenced segment reads`
- stacked draft PRs:
  - `#951` selected-source rewrite accounting
  - `#952` persisted debt-ledger scaffold
  - `#953` planner-from-ledger cutover
  - `#954` locator-catalog scaffold
  - `#955` locator-backed selected rewrite scans
  - `#956` debt-ledger-backed referenced segment reads

### Celestia Validation

- artifact root:
  - `artifacts/celestia_profile_signals/authoritative_catalogs_stack_20260410154942`

#### `wal_on_fast`

- `sync_only`
  - `t_sync = 360s`
  - `max_rss_kb = 10540612`
  - `post_run_app_bytes = 3505308754`
  - `post_rewrite_app_bytes = 2070383609`
- `dwell15m`
  - `t_sync = 374s`
  - `t_total = 1274s`
  - `max_rss_kb = 12364776`
  - `max_hwm_kb = 13182896`
  - minute 15:
    - `app_bytes = 3257479284`
    - `wal_bytes = 3200670298`
    - `maintenance_attempts = 44`
    - `maintenance_acquired = 39`
    - `maintenance_with_rewrite = 9`
    - `rewrite_runs = 9`
    - `queued_exec_runs = 8`
    - `debt_visible_source = ledger`
    - `debt_visible_bytes_stale = 3448029`

#### `fast`

- `sync_only`
  - `t_sync = 425s`
  - `max_rss_kb = 10764388`
  - `post_run_app_bytes = 3498758005`
  - `post_rewrite_app_bytes = 2080519205`
- `dwell15m`
  - `t_sync = 312s`
  - `t_total = 1212s`
  - `max_rss_kb = 12353964`
  - `max_hwm_kb = 12373524`
  - minute 15:
    - `app_bytes = 3241285403`
    - `wal_bytes = 3178152696`
    - `maintenance_attempts = 140`
    - `maintenance_acquired = 140`
    - `maintenance_with_rewrite = 11`
    - `rewrite_runs = 11`
    - `queued_exec_runs = 11`
    - no visible runtime debt remained

### Conclusion

- the first stack is worth keeping:
  - both `wal_on_fast` and `fast` now execute rewrite work naturally during the 15-minute dwell
  - both profiles reduce live minute-15 app size materially versus the previous steady-state failure mode
- the first stack is not the end state:
  - `wal_on_fast` minute-15 gap versus offline rewrite remains about `1.19 GB`
  - `fast` minute-15 gap versus offline rewrite remains about `1.16 GB`

### Second-Sprint Root Cause

- current commit-path metadata is still not authoritative for the workload we care about:
  - `buildValueLogRefDelta(...)` returns `nil` when `indexOuterLeavesInValueLog` is enabled
  - rewrite leafref publication also commits with `nil` ref delta
- this means the persisted debt ledger still depends on rebuild/scan truth instead of commit publication truth in the exact `fast` / `wal_on_fast` shape we are optimizing

### Second-Sprint Start

- first bounded slice:
  - make the locator catalog commit-path maintained for normal writes and rewrite swap commits
- follow-on required for true end state:
  - replace count-only ref deltas with physical-record debt deltas that cover grouped values and outer-leaf value-log records

### Second-Sprint Slice 2

- status:
  - complete locally on top of `8b6972ae`
- goal:
  - make the debt ledger commit-path authoritative for the same write shapes that still left live minute-15 size above the offline rewrite floor

#### Landed locally

- added a physical-record debt delta path in `TreeDB/db/vlog_debt_ledger.go`
  - tracks `(file_id, record_start)` refcounts with exact record lengths
  - rebuilds the transient record catalog from the live tree and leafref-backed outer leaves
  - updates segment live/stale bytes from commit deltas instead of forcing planner-time rediscovery
- wired commit publication to apply debt deltas in `TreeDB/db/db.go`
  - normal batch commits now build and publish `vlogDebtDelta`
  - rewrite swap commits now build and publish `vlogDebtDelta`
  - leafref rewrite commits now publish debt deltas instead of invalidating the ledger
- added outer-leaf observation support in `TreeDB/zipper/zipper.go`
  - leaf page persistence now exposes the new value-log pointer
  - merge/copy/rebalance paths report old/new outer-leaf record pointers so commit deltas can stay exact
- kept locator-catalog authority intact for the same leafref rewrite path
  - leafref rewrite commits now send an explicit empty locator delta instead of forcing invalidation
- adjusted planner expectation tests
  - with commit-path-authoritative debt tracking, `ValueLogRewritePlan` no longer needs a fallback live-byte estimate on first plan after reopen

#### Validation

- `GOWORK=off go test ./TreeDB/zipper ./TreeDB/db -run 'Test(ValueLogDebtLedger_(RebuildAndLoad|TracksCommitDeltaAcrossPointerWrites|TracksOuterLeafCommitDeltaAcrossWrites|TracksLeafRefRewriteCommits)|ValueLogOuterLeafChangeCollector_CapturesLeafSplit|ValueLogLocatorCatalog_TracksLeafRefRewriteCommits|ZipperLeafRefCacheAvoidsUnflushedReads|MergeLeaf_SplitKeysDoNotAliasBatchKeys)$' -count=1`
- `GOWORK=off go test ./TreeDB/db -run 'Test(ValueLogLocatorCatalog_RebuildAndLoad|ValueLogRewriteOnline_SourceFileIDs_UsesLocatorCatalogWhenEnabled|ValueLogLocatorCatalog_AppliesCommitDeltaAcrossWrites|ValueLogLocatorCatalog_TracksRewriteSwapCommits|ValueLogRewriteOnline_SourceFileIDs_RestrictsRewriteSet|ValueLogDebtLedger_RebuildAndLoad|ReferencedValueLogSegments_UsesPersistedDebtLedgerAcrossReopen|ValueLogRewritePlan_UsesPersistedDebtLedgerAcrossReopen|ValueLogRewritePlan_ShadowCompareRepairsDebtLedgerMismatch|ValueLogDebtLedger_TracksCommitDeltaAcrossPointerWrites|ValueLogDebtLedger_TracksOuterLeafCommitDeltaAcrossWrites|ValueLogDebtLedger_TracksLeafRefRewriteCommits|ValueLogOuterLeafChangeCollector_CapturesLeafSplit|ValueLogLocatorCatalog_TracksLeafRefRewriteCommits)' -count=1`
- `GOWORK=off go test ./TreeDB/caching -run 'Test(VlogGenerationRewrite_|Checkpoint_KicksVlogGenerationRewriteDespiteRecentForegroundActivity)' -count=1`

#### Remaining gap after this slice

- the debt ledger is now authoritative in-memory once rebuilt and then maintained on the commit path for the covered shapes
- the remaining world-class gap is durability of the richer catalogs themselves:
  - persist enough authoritative record/locator state to avoid rebuild hydration on reopen
  - converge cleanup and GC fully onto the same catalog truth

### Second-Sprint Slice 3

- status:
  - complete locally on top of `ac41fed8`
- goal:
  - keep the debt ledger trackable across close/reopen after commit-path deltas, instead of dropping back to a segment-only sidecar that forces rebuild hydration

#### Landed locally

- extended `vlog_debt_ledger.meta` to persist physical record refs alongside segment summaries
- persisted debt-ledger state after successful commit-path delta application in `finalizeCommitPostWork`
- kept segment-only debt snapshots valid as a compatibility fallback when records are absent, but made the normal rebuild-and-delta path durable

#### Validation

- `GOWORK=off go test ./TreeDB/db -run 'Test(ValueLogDebtLedger_(RebuildAndLoad|TracksCommitDeltaAcrossPointerWrites|PersistsTrackableStateAcrossReopen|TracksOuterLeafCommitDeltaAcrossWrites|TracksLeafRefRewriteCommits)|ReferencedValueLogSegments_UsesPersistedDebtLedgerAcrossReopen|ValueLogRewritePlan_UsesPersistedDebtLedgerAcrossReopen)$' -count=1`
- `GOWORK=off go test ./TreeDB/zipper ./TreeDB/db ./TreeDB/caching -run 'Test(ValueLogDebtLedger_(RebuildAndLoad|TracksCommitDeltaAcrossPointerWrites|PersistsTrackableStateAcrossReopen|TracksOuterLeafCommitDeltaAcrossWrites|TracksLeafRefRewriteCommits)|ReferencedValueLogSegments_UsesPersistedDebtLedgerAcrossReopen|ValueLogRewritePlan_UsesPersistedDebtLedgerAcrossReopen|ValueLogLocatorCatalog_TracksLeafRefRewriteCommits|VlogGenerationRewrite_|Checkpoint_KicksVlogGenerationRewriteDespiteRecentForegroundActivity)' -count=1`

#### Remaining gap after this slice

- commit-path authority now survives reopen for the debt ledger
- the next structural gap is executor/cleanup quality, not ledger survival:
  - reduce remaining rediscovery around locators and cleanup/GC eligibility
  - move more of the common reclaim lifecycle onto the same maintained catalog truth

### Validation Follow-up

- status:
  - complete locally on top of `46b4a6e1`
- trigger:
  - the broad touched-package sweep (`./TreeDB/zipper ./TreeDB/db ./TreeDB/caching`) exposed four caching scheduler failures after the selected-source accounting and durable debt-ledger slices

#### Landed locally

- fixed the scheduler test backend so synthesized `DrainedSourceFileIDs` matches real backend semantics when some requested source IDs remain referenced
- kept chunk-plan penalty filtering behavior aligned with segment-plan penalty filtering in tests by refilling budget tokens before the second pass where the assertion is about penalty behavior, not budget starvation

#### Validation

- `GOWORK=off go test ./TreeDB/caching -run 'Test(VlogGenerationRewriteQueue_KeepsStillReferencedSegmentQueuedWhenBounded|VlogGenerationRewriteQueue_DebtDrainSelectsMultipleSegmentsAndBoundsExecution|VlogGenerationRewritePlan_FiltersPenalizedSegments|VlogGenerationRewritePlan_ReadmitsPenalizedSegmentWhenStaleBytesImprove)$' -count=1`
- `GOWORK=off go test ./TreeDB/zipper ./TreeDB/db ./TreeDB/caching -count=1`

### Second-Sprint Slice 4

- status:
  - complete locally on top of `ad3d58e2`
- goal:
  - expose the remaining common-path execution cost in online rewrite after the locator catalog and debt ledger cutovers

#### Landed locally

- extended `TreeDB/db/vlog_rewrite.go` rewrite stats with leafref traversal counters:
  - `LeafRefTreeNodesVisited`
  - `LeafRefInternalNodesVisited`
  - `LeafRefPagerLeafNodesVisited`
  - `LeafRefRefsVisited`
  - `LeafRefRefsSelected`
  - `LeafRefRefsSkipped`
- threaded a dedicated traversal stats struct through `rewriteLeafRefsOnline`
  - direct pointer-candidate rewrite already reports `CandidateScanMode=locator_catalog` for selected-source execution
  - leafref rewrite still recursively walks the pager tree and now returns the amount of traversal paid to find eligible leafrefs
- tightened existing leafref rewrite tests so this observability is part of the normal regression surface

#### Validation

- `GOWORK=off go test ./TreeDB/db -run 'Test(ValueLogLocatorCatalog_TracksLeafRefRewriteCommits|ValueLogDebtLedger_TracksLeafRefRewriteCommits|ValueLogRewriteOnline_LeafRefsReserveRIDs_DoesNotRefreshManager|ValueLogRewriteOnline_MaxCopiedBytes_DoesNotRunLeafRefRewriteWhenBudgetExhausted)$' -count=1`
- `GOWORK=off go test ./TreeDB/db -count=1`

#### Remaining gap after this slice

- selected-source direct value-pointer rewrite is already locator-driven
- the remaining execution rediscovery gap is now explicit and measurable:
  - leafref rewrite still traverses the tree recursively to rediscover eligible outer-leaf pages
  - a true next structural slice requires a stronger leafref locator form, not just more point optimizations inside the current walk

### Second-Sprint Slice 5

- status:
  - complete locally on top of `40b061cd`
- goal:
  - skip the leafref rewrite walk entirely when the selected source segments have no live outer-leaf pages, using the commit-path-authoritative debt ledger

#### Landed locally

- extended the persisted debt-ledger record catalog to distinguish normal value records from outer-leaf records
  - bumped `vlog_debt_ledger.meta` version to `2`
  - tracked `Kind` on physical record refs
  - added `OuterLeafLiveBytes` to per-segment debt summaries
- updated debt-ledger rebuild and commit-path delta application to maintain outer-leaf live bytes per segment
- added a ledger query path that answers whether a selected source set contains any live outer-leaf bytes
- gated `ValueLogRewriteOnline` so `rewriteLeafRefsOnline` is skipped when the selected source IDs have no tracked outer-leaf bytes
- added a focused rewrite test that proves:
  - the selected value-pointer source is tracked as having no outer-leaf live bytes
  - value-pointer rewrite still runs
  - leafref traversal counters remain zero because the leafref pass is skipped
- tightened outer-leaf debt-ledger tests to assert that rebuild/delta tracking preserves non-zero `OuterLeafLiveBytes` for actual leafref segments

#### Validation

- `GOWORK=off go test ./TreeDB/db -run 'Test(ValueLogRewriteOnline_SkipsLeafRefTraversalWhenSelectedSourcesHaveNoOuterLeaves|ValueLogRewriteOnline_LeafRefsReserveRIDs_DoesNotRefreshManager|ValueLogRewriteOnline_MaxCopiedBytes_DoesNotRunLeafRefRewriteWhenBudgetExhausted|ValueLogDebtLedger_TracksOuterLeafCommitDeltaAcrossWrites)$' -count=1`
- `GOWORK=off go test ./TreeDB/caching -run 'Test(VlogGenerationRewrite_|Checkpoint_KicksVlogGenerationRewriteDespiteRecentForegroundActivity)' -count=1`
- `GOWORK=off go test ./TreeDB/db -count=1`

#### Remaining gap after this slice

- online rewrite no longer pays a leafref tree walk for selected source segments that only contain direct value-pointer debt
- the remaining structural gap is narrower and more explicit:
  - when selected source segments do contain live outer-leaf pages, execution still rediscover those pages by recursively traversing the tree
  - removing that cost still requires a stronger leafref locator form than the current key-only locator catalog

### Post-`#962` Celestia Validation

- artifact root:
  - `artifacts/celestia_profile_signals/skip_leafref_nonleaf_20260410181120`
- fixed sync stop height:
  - `10611331`
- candidate head:
  - `9f0476d9` `treedb: skip leafref rewrite on non-leaf sources`

#### `wal_on_fast`

- `dwell15m`
  - `t_sync = 260s`
  - `t_total = 1160s`
  - `max_rss_kb = 10785428`
  - `max_hwm_kb = 10816860`
  - minute 15:
    - `app_bytes = 3034120332`
    - `wal_bytes = 2968366016`
    - `maintenance_attempts = 36`
    - `maintenance_acquired = 33`
    - `maintenance_with_rewrite = 11`
    - `rewrite_runs = 11`
    - `queued_exec_runs = 10`
    - `plan_runs = 26`
    - `plan_last_result = empty`
    - `debt_visible_bytes_stale = 0`
- post-dwell offline maintenance:
  - `post_dwell_app_bytes = 3030649985`
  - `post_dwell_post_rewrite_app_bytes = 2082847549`
  - remaining offline gap:
    - `947802436 bytes`
    - `31.27%`

#### `fast`

- `dwell15m`
  - `t_sync = 314s`
  - `t_total = 1214s`
  - `max_rss_kb = 12242324`
  - `max_hwm_kb = 12833272`
  - minute 15:
    - `app_bytes = 3238457902`
    - `wal_bytes = 3179812963`
    - `maintenance_attempts = 109`
    - `maintenance_acquired = 109`
    - `maintenance_with_rewrite = 10`
    - `rewrite_runs = 10`
    - `queued_exec_runs = 10`
    - `plan_runs = 64`
    - `plan_last_result = selected`
    - `plan_last_selected_segments = 1`
    - `plan_last_selected_bytes_stale = 2669693`
    - `debt_visible_source = ledger`
    - `debt_visible_bytes_stale = 373434549`
    - `debt_last_deferral_reason = stage_confirm`
    - `rewrite_ledger_bytes_total = 1073746546`
    - `rewrite_ledger_bytes_stale = 373434549`
- post-dwell offline maintenance:
  - `post_dwell_app_bytes = 3246018511`
  - `post_dwell_post_rewrite_app_bytes = 2116498171`
  - remaining offline gap:
    - `1129520340 bytes`
    - `34.80%`

#### Conclusion

- `#962` materially improved `wal_on_fast`:
  - minute-15 size and max RSS both dropped
  - rewrite activity increased from `9` to `11` runs
- `#962` did not close the `fast` planner gap:
  - the scheduler still saw about `373 MB` of stale debt at minute 15
  - the last staged plan only selected about `2.7 MB` of stale bytes in one segment
  - that left a large offline rewrite delta even though runtime rewrite was active

### Next Slice

- target:
  - let a small non-empty stale-ratio plan piggyback the existing low-pressure tail-fill path instead of blocking it
- reason:
  - the current scheduler only tail-fills after the dedicated tail-compaction probe when the generic stale-ratio plan is empty
  - in `fast`, the generic path can select a tiny stale segment, set `haveRewritePlan = true`, and skip the fill opportunity that would pack adjacent live-only tail segments into the same staged debt set

### Second-Sprint Slice 6

- status:
  - complete locally on top of `9f0476d9`
- goal:
  - stop a small non-empty stale-ratio / pre-rewrite plan from collapsing stage-confirm execution back to a single segment when low-pressure tail packing could safely widen the staged debt set

#### Landed locally

- added plan-merging support for both segment and chunk rewrite plans in `TreeDB/caching/db.go`
- extended the scheduler so low-pressure tail-fill can piggyback onto:
  - sparse stale-ratio trigger plans
  - ordinary pre-rewrite plans once rewrite is already justified
- kept stale-ratio stage semantics intact:
  - the widened plan is still staged/confirmed as stale-ratio debt
  - the first stage-confirm execution remains bounded by the normal queued rewrite cap instead of turning into an unbounded tail-packing burst
- added a focused cross-profile scheduler regression test:
  - `TestVlogGenerationRewritePlan_StagesTailFilledFreshStaleRatioDebt_FastModes`
  - proves `fast` and `wal_on_fast` both stage the widened debt set
  - proves the first stage-confirm rewrite executes more than the single stale segment and preserves the remaining widened queue instead of collapsing back to one-segment debt

#### Validation

- `GOWORK=off go test ./TreeDB/caching -run 'Test(VlogGenerationRewritePlan_StagesTailFilledFreshStaleRatioDebt_FastModes|VlogGenerationRewritePlan_StagesFreshStaleRatioDebtBeforeRewrite|VlogGenerationMaintenance_PeriodicTailPlanFillAddsLiveOnlySegments_FastModes|VlogGenerationMaintenance_PeriodicTailPackingRunsAfterEmptyTailPlan_FastModes)$' -count=1`
- `GOWORK=off go test ./TreeDB/caching -run 'Test(VlogGenerationRewrite_|Checkpoint_KicksVlogGenerationRewriteDespiteRecentForegroundActivity)' -count=1`
- `GOWORK=off go test ./TreeDB/caching -count=1`

#### Expected effect

- `fast` should no longer stage a single tiny stale segment and then confirm only that one segment when low-pressure tail fill can safely widen the same debt set
- the bounded executor still limits each confirm pass, but the queue now retains the widened tail-packed debt instead of repeatedly rediscovering only the initial tiny stale segment
