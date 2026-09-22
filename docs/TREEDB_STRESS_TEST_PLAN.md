# TreeDB Stress + Reliability Test Plan

## Goals
- Catch stalls, deadlocks, and correctness regressions that do not surface via unit tests or coverage.
- Provide fast, deterministic “progress” checks for known high-risk paths (backpressure, flush, checkpoint, snapshot restore).
- Add a structured stress harness that can run locally and in CI with clear failure artifacts (goroutine dumps, logs).

## Non-Goals
- Replace functional unit tests.
- Benchmark absolute performance or latency SLOs (use separate perf tooling).

## Scope
- TreeDB cached mode (caching layer, journal/value log, memtables, flush, checkpoint).
- Backend mode correctness under stress.
- Snapshot-style workloads (large batched writes, background disabled).
- Error-handling and backpressure edge cases (timeouts, deadlocks).

## Guiding Principles
- Tests must assert forward progress within a bounded deadline.
- Failures should dump goroutines and key stats for root-cause triage.
- Keep determinism; avoid purely “random hang” tests without watchdogs.
- Tests must be cheap by default; heavy stress is opt-in.
- Prefer layered coverage: unit -> progress -> stress -> long soak.
- Every stress test should have a clear "progress signal" and a bounded timeout.

## Test Classes (Planned)

### 1) Progress/Timeout Tests (Target: Backpressure, Flush, Checkpoint)
Purpose: Prove that writes do not block indefinitely when backpressure is triggered.
Plan:
- Add “progress” tests with timeouts on `waitForStop` paths and flush triggers.
- Run with `-count=100 -race` locally for flake detection.
- Example: `TestWaitForStopSchedulesFlush` (already added).

Artifacts on timeout:
- Goroutine dump.
- Queue backlog stats.
- Journal/value-log stats if available.

Concrete tests (caching):
- `TestWaitForStopSchedulesFlush` (already added)
  - Location: `TreeDB/caching/backpressure_wait_test.go`
  - Asserts `Set()` returns within 2s when backlog exceeds stop and no flush signal exists.
- `TestBackpressure_StopUnblocksAfterFlush`
  - Build backlog, trigger explicit flush, assert blocked writer resumes.
  - Validates `bpCond.Broadcast` wake path.
- `TestCheckpointProgressNoBG`
  - Disable BG checkpoint, call `Checkpoint()` explicitly under heavy backlog.
  - Assert returns and journal shrinks (if journal enabled).
- `TestBackpressure_NoFlushSignalIteratorRotation`
  - Create backlog via iterator-driven rotation path and ensure writes progress.

Concrete tests (db):
- `TestDB_WriteProgressUnderFlushDebt`
  - Open TreeDB cached, saturate queue; assert `SetSync()` completes.

Parameters to standardize:
- `FlushThreshold`: 1-4 KiB for unit tests (fast).
- `MaxBacklogBytes`: 4-16 KiB (small, triggers backpressure quickly).
- Timeout: 1-3 seconds per progress test.

Pseudo-code sketches:
- `TestBackpressure_StopUnblocksAfterFlush`
  - open cached db with `FlushThreshold=1024`, `MaxBacklogBytes=4096`, `MemtableShards=1`.
  - create backlog by rotating memtables without flush signal.
  - start goroutine `Set("blocked", "v")` and confirm it blocks briefly.
  - call `db.TriggerFlush()` and wait for `Set` to finish within 2s.
- `TestCheckpointProgressNoBG`
  - open cached db with `BackgroundCheckpointInterval=-1`, `MaxWALBytes=-1` (journal cap).
  - write until backlog exceeds stop.
  - call `db.Checkpoint()` with watchdog; ensure returns and `Stats()` shows journal trimmed.
- `TestBackpressure_NoFlushSignalIteratorRotation`
  - create large iterator that forces `rotateMemtableLockedForIterator`.
  - verify subsequent `Set()` completes quickly (no deadlock).

Key progress signals:
- `db.nextSeq` monotonic increase.
- `QueueBacklogBytes` decreases after flush.
- `Checkpoint()` returns within deadline.

### 2) CI Timeout + Goroutine Dump Hook
Purpose: Ensure test hangs fail fast with actionable info.
Plan:
- Central helper that installs a deadline watchdog; on timeout:
  - dumps goroutines (`runtime/pprof` or `runtime.Stack`),
  - logs TreeDB stats (queue backlog, flush throughput).

Concrete helper API (new file `TreeDB/caching/testutil_timeout.go`):
- `WithTimeout(t *testing.T, d time.Duration, fn func(ctx context.Context))`
  - Runs `fn` with `context.WithTimeout`.
  - On timeout: dump goroutines and print `db.Stats()` snapshots if provided via context.
- `DumpGoroutines(t *testing.T, label string)`
  - Uses `runtime/pprof.Lookup("goroutine").WriteTo`.

Integration:
- Wrap all progress tests with `WithTimeout`.
- Add a short `t.Helper()` in wrappers for clear file/line reporting.

Concrete timeout helper behavior:
- Create a `done := make(chan struct{})`.
- Run `fn(ctx)` in goroutine; close `done` on return.
- `select` on `done` vs `ctx.Done()`.
- On timeout:
  - call `DumpGoroutines`.
  - if a `*DB` is registered, call `db.Stats()` and `db.QueueBacklogBytes()`.
  - call `t.Fatalf` with concise reason.

Optional context pattern:
- `ctx = context.WithValue(ctx, ctxKeyDB, db)`
- `WithTimeout` checks for `ctxKeyDB` to dump stats.

### 3) Goroutine Leak Checks
Purpose: Detect stuck background loops after test completion.
Plan:
- Provide a leak-check helper for selected long tests.
- Record baseline goroutine count, allow small delta, fail if growth persists.

Concrete approach:
- Capture baseline `runtime.NumGoroutine()` at test start.
- After test, wait up to 1s for goroutines to settle.
- Allow delta <= 3 (GC, logging, or test helpers).
- If exceeded, dump goroutines and fail.
- Helper: `AssertNoGoroutineLeak(t, baseline, maxDelta)`.

Pseudo-code:
- `baseline := runtime.NumGoroutine()`
- run test body
- `deadline := time.Now().Add(1 * time.Second)`
- loop until deadline:
  - if `runtime.NumGoroutine() <= baseline+maxDelta`, return
  - `time.Sleep(50 * time.Millisecond)`
- dump goroutines and fail.

### 4) Fault Injection (I/O pause + transient errors)
Purpose: Verify error paths don’t deadlock or corrupt.
Plan:
- Wrap journal/value-log writers to inject:
  - transient errors (N% of ops),
  - artificial delays (pause/slow I/O),
  - intermittent read failures for vlog reads.
- Verify:
  - write paths fail quickly,
  - no deadlocks,
  - errors are surfaced and/or recorded as expected.

Concrete implementation:
- Add `faultyLogWriter` (test-only) implementing `logWriter` with hooks:
  - `NextAppendErr`, `NextBatchErr`, `NextFlushErr`, `NextSyncErr`.
  - `DelayAppend`, `DelayBatch` (sleep).
- Add `faultyValueLogReader` for `Read(ptr)` error injection.

Test cases:
- `TestJournalAppendErrorPropagates`
  - Inject error; assert `Set()` returns error and no deadlock.
- `TestFlushValueLogErrorUnblocks`
  - Force `flushValueLog()` error during `flushOneLocked`.
- `TestValueLogReadErrorInFlush`
  - Inject read error during copy-on-flush; assert error surfaced.

Fault injection mechanics:
- `faultyLogWriter` wraps a real writer and delegates by default.
- `Next*Err` is a channel or atomic that, when set, is consumed once.
- `Delay*` uses `time.Sleep` before delegating to real writer.
- For concurrency, use a `sync.Mutex` or `atomic.Pointer[error]`.

Additional test cases:
- `TestJournalSyncErrorDoesNotDeadlock`
  - Inject sync error; verify writer returns error quickly.
- `TestValueLogReadTransientError`
  - Inject one read error, retry with subsequent `Set()` and verify progress.

### 5) Concurrency Fuzz/Stress
Purpose: Shake out race conditions and ordering bugs.
Plan:
- Spawn multiple goroutines doing:
  - writes, deletes, iterators, memtable rotations.
- Randomized pauses and batch sizes.
- Watchdog asserts forward progress (e.g., steady increase in sequence or applied ops).
- Run under `-race`.

Concrete test design:
- `TestConcurrentMixedOpsProgress` (caching)
  - 8-16 workers for 3-5 seconds.
  - Mix: 60% Set, 20% Delete, 10% Iterator, 10% Flush/Checkpoint.
  - Watchdog: every 200ms ensure `db.nextSeq` increases or `QueueBacklogBytes` decreases.
- Add deterministic RNG seed (configurable via env).
- Collect per-worker error channel; fail on first error.

Pseudo-code structure:
- `ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)`
- `for i := 0; i < workers; i++ { go worker(ctx, rngSeed+i) }`
- Worker loop:
  - choose op by RNG.
  - `Set`/`Delete` random key/value.
  - Iterator op: `it := db.Iterator(nil,nil)` then `it.Next()` few steps.
  - Occasionally call `db.TriggerFlush()` or `db.Checkpoint()`.
  - small sleep (0-5ms).
- Watchdog goroutine:
  - record `lastSeq := db.nextSeq.Load()`
  - every 200ms: if `nextSeq == lastSeq` and backlog not dropping for >1s => fail.

Determinism:
- Read `TREEDB_TEST_SEED`; default `1`.
- Log seed on failure.

### 6) Snapshot-Restore Stress (BG Disabled)
Purpose: Reproduce mainnet restore paths in a scaled-down test.
Plan:
- Simulate large `BatchWithFlusher` style writes.
- Disable background maintenance to match `TREEDB_BENCH_DISABLE_BG=1`.
- Ensure final batch completes and data reads back correctly.
- Track backlog and flush throughput under load.

Concrete test design:
- `TestSnapshotStyleRestore_NoBG` (db package)
  - Open TreeDB cached with:
    - `BackgroundIndexVacuumInterval = -1`
    - `BackgroundCheckpointInterval = -1`
    - `BackgroundCheckpointIdleDuration = -1`
    - `MaxWALBytes = -1` (journal cap)
  - Write N batches of size M (e.g., 100 batches x 10k keys).
  - Periodically rotate memtables to emulate snapshot chunk boundaries.
  - Final batch must complete within 5-10 seconds.
  - Validate random reads (sample 1% of keys).

Pseudo-code sketch:
- `for b := 0; b < batches; b++ {`
  - `batch := db.NewBatch()`
  - `for i := 0; i < batchSize; i++ { batch.Set(key, value) }`
  - `batch.Write()`
  - optionally rotate memtable via `db.TriggerFlush()` or internal hook.
`}`
- After writes, sample K keys and `Get()` values.
- Use `WithTimeout` to bound entire test.

Failure signals:
- blocked `Write()` or `Set()` exceeding deadline.
- missing keys on read-back.

## Test Harness Utilities (Planned)
- `WithTimeout(t, d, func(ctx) error)` to enforce deadline and collect diagnostics.
- `DumpGoroutines(prefix)` helper to collect goroutine stacks on failure.
- `CollectTreeDBStats(db)` to include queue backlog, flush bps, journal bytes.
- `FaultyLogWriter` wrapper for journal/value log that can inject delays/errors.
- `ProgressWatchdog` that validates monotonic progress signals.

Concrete file layout:
- `TreeDB/caching/testutil_timeout.go`
- `TreeDB/caching/testutil_faults.go`
- `TreeDB/caching/testutil_watchdog.go`
- `TreeDB/db/testutil_timeout.go` (thin wrapper, reusing caching helpers if possible)

Helper details:
- `CollectTreeDBStats` should emit:
  - `queue_backlog_bytes`
  - `flush_bps_ewma`
  - `auto_checkpoint.count`
  - `walClosedBytes`, `walLiveBytes`
- Store stats in JSON for easy comparison across runs.

## CI Strategy
- Fast tier: run progress + targeted stress with short deadlines.
- Heavy tier (nightly): run concurrency stress and fault injection with `-race`.
- Suggested commands:
  - `go test ./TreeDB/caching -run TestBackpressure -race -count=50 -timeout=5m`
  - `go test ./TreeDB/db -run TestSnapshotStress -race -count=10 -timeout=10m`

Concrete CI tiers:
- Tier 1 (PR): progress tests only, `-race` optional.
- Tier 2 (daily): concurrency stress + fault injection, `-race`.
- Tier 3 (weekly): long soak (30-60s) with watchdogs, `-race`.

Environment toggles:
- `TREEDB_TEST_SEED` for deterministic RNG.
- `TREEDB_TEST_STRESS=1` to enable heavy tests by default only in nightly.
- `TREEDB_TEST_TIER=pr|daily|nightly` to select matrix size and durations.

Concrete CI matrix:
- PR: `go test ./TreeDB/caching -run 'TestWaitForStop|TestBackpressure' -timeout=3m`
- Daily: `go test ./TreeDB/caching -run 'TestConcurrentMixedOpsProgress|TestJournal.*Error' -race -timeout=10m`
- Weekly: `go test ./TreeDB/caching -run 'TestConcurrentMixedOpsProgress' -race -count=20 -timeout=30m`

## Failure Artifacts
Each stress/progress test should log:
- goroutine dump,
- TreeDB stats snapshot,
- recent error logs (if available).

Concrete artifacts on timeout:
- Goroutine dump file name pattern: `treedb_goroutines_<test>_<unixnano>.txt`.
- JSON stats snapshot: `treedb_stats_<test>_<unixnano>.json`.

Storage location:
- default under `t.TempDir()` so artifacts are retained in CI logs.
- print path to artifacts on failure.

## Priorities (Implementation Order)
1) Progress/timeout helpers + watchdog (fast, immediate value).
2) Snapshot-style stress test with BG disabled.
3) Fault injection wrappers.
4) Concurrency fuzz/stress suite.
5) Goroutine leak checks (opt-in for long tests).

Detailed milestones:
- M1: Add timeout + dump helpers and convert existing progress tests.
- M2: Add snapshot-style restore test (BG disabled).
- M3: Add faulty journal/value-log injection and 3 error-path tests.
- M4: Add concurrent mixed-ops stress test with watchdog.
- M5: Add goroutine leak check helper and apply to stress tests.

## Success Criteria
- Reproduces previous stall classes as failing tests.
- No unexplained hangs after N runs with `-race -count=100`.
- CI produces actionable artifacts on hang.

Additional criteria:
- No deadlocks under `-race` for 50+ runs on stress suite.
- Backpressure tests pass under `TREEDB_BENCH_DISABLE_BG=1` settings.
- Error injection tests validate error propagation and no silent stalls.

## Open Questions
- Should stress tests run against both command-WAL public profiles
  (`command_wal_durable` vs `command_wal_relaxed`) plus a smaller
  benchmark-only no-WAL ceiling subset?
- Should we gate "heavy" tests on an env var or a build tag?
- Which command-WAL profiles must be covered in each stress test?

## Appendix: Config Matrix (Recommended)
For each stress suite, consider these variants:
- Public command-WAL profile: durable vs relaxed.
- Benchmark-only no-WAL ceiling: opt-in unsafe subset only.
- Background tasks: enabled vs disabled (`TREEDB_BENCH_DISABLE_BG=1` parity).

Example matrix for `TestConcurrentMixedOpsProgress`:
- command-WAL durable + BG on
- command-WAL durable + BG off
- command-WAL relaxed + BG off
- benchmark-only no-WAL + BG off (unsafe ceiling)

Tier selection rule (recommended):
- `pr`: minimal critical path
  - command-WAL durable + BG on
  - command-WAL durable + value log + BG off
- `daily`: expanded coverage
  - command-WAL durable + value log + BG on
  - command-WAL durable + value log + BG off
  - command-WAL relaxed + value log + BG off
  - benchmark-only no-WAL + value log + BG off
- `nightly`: full matrix
  - command-WAL durable/relaxed plus benchmark-only no-WAL ceiling × value log
    pointer policy × BG on/off
  - Skip invalid legacy compatibility combinations unless a test explicitly opts
    into forensic legacy replay.

Implementation sketch:
- Define `type testVariant struct { profile, valueLogPolicy, bg string }`.
- Build `variants := variantsForTier(os.Getenv("TREEDB_TEST_TIER"))`.
- Each stress test loops over `variants`, runs subtests, and applies per-tier timeouts.

## Tracking
Add this doc to review checkpoints; update as tests land.
