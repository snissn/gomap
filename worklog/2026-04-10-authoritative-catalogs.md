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
