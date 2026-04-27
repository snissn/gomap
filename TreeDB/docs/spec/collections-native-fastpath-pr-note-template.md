# Collections Native Fast-Path PR Note Template

Status: template, non-normative.

Use this template verbatim for rewrite PR notes unless a phase-specific reason
requires an additive field.

## Summary

- Phase:
  - `R?`
- Classification:
  - `scaffolding|performance|cleanup`
- Base branch / PR:
  - `...`
- Compare against:
  - oracle branch + SHA: `...`
  - oracle worktree path: `...`
  - previous phase branch + SHA: `...`
  - raw TreeDB baseline note/date/SHA: `...`
- Targeted hot path:
  - `...`
- Expected win shape:
  - `...`

## Scope

- In scope:
  - `...`
- Explicitly out of scope:
  - `...`
- Forbidden-path assertion for this phase:
  - `...`

## Correctness Gates

- Commands:
  - `...`
  - `...`
- Result:
  - `PASS|FAIL`
- New tests added:
  - `...`
- Regression tests added:
  - `...`

## Benchmark Commands

- Raw TreeDB anchors (`fast`):
  - `...`
- Raw TreeDB anchors (`wal_on_fast`):
  - `...`
- Deferred work (`flushdrain`):
  - `...`
- Collection bundle:
  - `...`
- Phase-local focused benches:
  - `...`

If the phase predates native collection harness bring-up on the execution
branch, mark the native collection bundle as `N/A before R0 harness bring-up`
instead of fabricating a comparison.

## Benchmark Results: Raw TreeDB Anchors

| Test | Baseline ops/s | PR ops/s | Delta % | Noise band | Status |
|---|---:|---:|---:|---:|---|
| `write_seq` |  |  |  | `2-5%` | `PASS|FAIL` |
| `write_rand` |  |  |  | `2-5%` | `PASS|FAIL` |
| `batch_write` |  |  |  | `2-5%` | `PASS|FAIL` |
| `batch_random` |  |  |  | `2-5%` | `PASS|FAIL` |
| `batch_delete` |  |  |  | `2-5%` | `PASS|FAIL` |
| `delete_rand` |  |  |  | `2-5%` | `PASS|FAIL` |
| `random_read` |  |  |  | `2-5%` | `PASS|FAIL` |
| `random_read_parallel_acquire_snapshot` |  |  |  | `2-5%` | `PASS|FAIL` |
| `full_scan` |  |  |  | `2-5%` | `PASS|FAIL` |
| `prefix_scan` |  |  |  | `2-5%` | `PASS|FAIL` |

## Benchmark Results: Collections

| Benchmark | Mode | Batch size | Baseline ns/op | PR ns/op | Baseline docs/s | PR docs/s | Delta % | B/op delta | allocs/op delta | Status |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `BenchmarkCollectionInsertBatchProvidedID` | `mixed` | `256` |  |  |  |  |  |  |  | `PASS|FAIL` |
| `BenchmarkCollectionInsertBatchWithSecondaryIndexes` | `mixed` | `256` |  |  |  |  |  |  |  | `PASS|FAIL` |
| `BenchmarkCollectionInsertBatchCheckpointWithSecondaryIndexes` | `settled/checkpoint` | `256` |  |  |  |  |  |  |  | `PASS|FAIL` |

## Benchmark Results: Deferred Work / Settled State

| Measurement | Baseline | PR | Delta % | Interpretation |
|---|---:|---:|---:|---|
| `flushdrain total time` |  |  |  | `...` |
| `checkpoint-focused batch ingest` |  |  |  | `...` |
| settled read/scan follow-up |  |  |  | `...` |

## Architectural Counters

| Counter | Baseline | PR | Expected direction | Status |
|---|---:|---:|---|---|
| `tiny_batch_fallback_count` |  |  | `down or zero` | `PASS|FAIL|N/A` |
| `per_item_key_probe_fallback_count` |  |  | `down or zero` | `PASS|FAIL|N/A` |
| `per_item_prefix_probe_fallback_count` |  |  | `down or zero` | `PASS|FAIL|N/A` |
| `detached_batch_replay_fallback_count` |  |  | `down or zero` | `PASS|FAIL|N/A` |
| `warm_apply_rebuild_fallback_count` |  |  | `down or zero` | `PASS|FAIL|N/A` |
| `warm_apply_per_key_retention_lookup_count` |  |  | `down or zero` | `PASS|FAIL|N/A` |

## Focused Profile Readout

- Dominant CPU frames before:
  - `...`
- Dominant CPU frames after:
  - `...`
- Dominant alloc frames before:
  - `...`
- Dominant alloc frames after:
  - `...`
- Forbidden-path check:
  - `present|reduced|removed`
- Interpretation:
  - `...`

## Artifacts

- Raw TreeDB `fast` profile dir:
  - `/tmp/...`
- Raw TreeDB `wal_on_fast` profile dir:
  - `/tmp/...`
- `flushdrain` profile dir:
  - `/tmp/...`
- Collection report bundle:
  - `/tmp/...`
- Focused pprof bundle:
  - `/tmp/...`
- `benchprof_results.md`:
  - `/tmp/.../benchprof_results.md`
- `collections_report.md`:
  - `/tmp/.../collections_report.md | N/A before R0 harness bring-up`
- `collections_report.html`:
  - `/tmp/.../collections_report.html | N/A before R0 harness bring-up`

## Go / No-Go

- `GO` only if:
  - correctness gates are green
  - no raw TreeDB anchor regresses beyond noise band without explicit tradeoff
  - this phase’s target benchmark family improved, or the PR is explicitly scaffolding-only
  - `flushdrain` / checkpoint results do not show hidden deferred-work regression
  - architectural counters support the claimed path removal or remain at zero
  - profiles move away from the forbidden path for this phase
- Decision:
  - `GO|NO-GO`
- Reason:
  - `...`

For scaffolding phases, use this wording if appropriate:

- `GO`: prerequisite-only phase; no material regression beyond noise margin on raw TreeDB anchors or collection bundle
- `NO-GO`: prerequisite-only phase caused material regression without clearing a required architectural blocker

## Next Phase

- Unblocked next phase:
  - `...`
- Remaining blocker:
  - `...`
