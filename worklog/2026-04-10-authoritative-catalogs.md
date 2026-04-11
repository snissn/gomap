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
