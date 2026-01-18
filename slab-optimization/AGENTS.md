# Optimization Sprint Runbook (slab-optimization/spec.md)
Repo: `/Users/michaelseiler/dev/snissn/gomap`
Commit: `d23d871e1a17baaa23f20f5e023e9650c99116f2`
Generated: `2026-01-17 15:53:23 HST`

## Resume Prompt
You are a Codex-style coding agent working in `/Users/michaelseiler/dev/snissn/gomap`.

Your job: execute the PR plan in this file (PR0..PR7+) in order, keeping strict scope boundaries from `slab-optimization/spec.md`.

Operational rules:
- MUST follow the PR plan in this file; MUST NOT expand scope beyond the spec.
- MUST update the “Local Activity Log” after every meaningful action (file edits, tests/benches, branch/PR creation).
- MUST use `rg -n` to re-confirm symbol line numbers before editing.
- MUST create a branch per PR: `sprint/slabopt-pr<N>-<slug>`.
- MUST write a PR description at `.pr/PR<N>_description.md` and create the PR via `gh` (see “GitHub CLI policy”).
- MUST open a PR via the GitHub CLI for every PR stage (no web UI/manual PR creation).
- MUST include unified_bench output samples in every PR body (include the relevant suite(s) for that stage).
- MUST NOT merge PRs.
- MUST fail-closed: all new parsers MUST cap lengths before allocation; on invalid data MUST return errors (no panic/OOM).

## Spec Traceability
Authoritative spec: `slab-optimization/spec.md`
- End state (spec “## 0) Sprint End State…”): drives PR1..PR6 outcomes.
- Constraints (spec “## 1) Hard Constraints…”): applies to ALL PRs.
- Journal semantics (spec “### 3.1 Journal Semantics…”): PR1.
- DictDB design (spec “### 3.2 Dictionary Storage System…”): PR2 + PR4 + PR6.
- ValueLog encoding / dynamic-K (spec “### 3.3 ValueLog Encoding…”): PR3 + PR4 + PR6.
- CommitLog contents (spec “### 3.4 CommitLog Contents…”): PR3 + PR6.
- Parallel lanes (spec “### 3.5 Parallel Active Write Lanes…”): PR0 + PR5.
- Recovery v1 (spec “## 5) Recovery v1…”): PR3 + PR6.
- Milestones (spec “## 6) PR Plan…”): PR0..PR7+.
- Non-goals (spec “## 8) Explicit Non-Goals…”): applies to ALL PRs.

## PR Plan Index
- PR0: Stable benchmarks + lane probe harness (no behavior change)
- PR1: Journal durability abstraction + crash-injection ACK invariants
- PR2: DictDB (separate TreeDB instance) + key scheme + lagging rule
- PR3: RID-based join between CommitLog and ValueLog + basic two-pass recovery
- PR4: Dict + dynamic-K grouped compression on write path (primary encoding)
- PR5: Parallel active lanes (N CommitLog/ValueLog pairs) + durability tickets
- PR6: Recovery hardening (two-pass default-safe) + fuzz + crash edge cases
- PR7+: Index work behind flags (aligned with new storage contract)

---

## PR0 — Benchmarks and Lane Probe (no behavior change)
### Goal
- MUST add stable benchmark entry points and a simple lane-count probe harness.
- MUST NOT change TreeDB runtime behavior in this PR.

### Non-goals (from spec constraints)
- MUST NOT introduce zone packing / 2MB zones.
- MUST NOT add mmap usage to mutable/truncating files.
- MUST NOT introduce ValueIndex / ValueID indirection.

### Preconditions / Dependencies
- None.

### TODOs (exact)
- MUST create directory `.pr/` (new; PR descriptions live here).
  - Path: `.pr/` (new directory)
  - Location: repo root
- MUST add a “lane probe” suite hook to unified bench runner.
  - File: `cmd/unified_bench/main.go`
  - Symbol/anchor: `switch suite {` (line `224`), add a new `case` before `default` (line `273`)
  - Context anchor:
    - `222		suite := strings.ToLower(strings.TrimSpace(*suiteArg))`
    - `224			switch suite {`
  - Change:
    - MUST add `case "lanes_probe", "lanes-probe":` that calls `runLaneProbeSuite(baseCfg)` and prints output.
    - MUST keep existing suites unchanged.
- MUST implement a placeholder lane probe suite function (no TreeDB behavior change yet).
  - File: `cmd/unified_bench/suite_lanes_probe.go` (new file)
  - Symbol: `func runLaneProbeSuite(cfg BenchConfig) (string, error)`
  - Change:
    - MUST run a short, deterministic workload (fixed seed from `cfg.SeedUsed`) against TreeDB only.
    - MUST accept a `-treedb-journal-lanes` flag (wired in adapter) but MAY ignore it until PR5 exists.
    - MUST print: lanes requested, ops/sec, wall time, and final on-disk sizes (index.db, wal dir).
    - MUST NOT change TreeDB code or defaults.
- MUST add a TreeDB flag placeholder for lane count (no behavior change yet).
  - File: `cmd/unified_bench/adapter_treedb.go`
  - Symbol/anchor: `var (` block (line `16`)
  - Location: add near other cached-mode knobs, e.g. after `treedbFlushThreshold` (line `17`)
  - Change:
    - MUST add `treedbJournalLanes = flag.Int("treedb-journal-lanes", 0, "TreeDB: journal lane count (0=default)")`
    - MUST thread the value into `treedb.Options` in `NewTreeDB` (line `97`) and `NewTreeDBBackend` (line `138`) ONLY IF the option exists by then; otherwise leave a TODO comment in PR0 code (do not create the option in PR0).
- MUST document “current benchmark entry points” in PR0 description.
  - File: `.pr/PR0_description.md` (new file)
  - Content MUST include:
    - `go test -run` and `go test -bench` entry points: `TreeDB/bench_test.go` (`BenchmarkWriteParallelCached` at `TreeDB/bench_test.go:121`) and `TreeDB/db/bench_test.go` (e.g., `BenchmarkBatch` at `TreeDB/db/bench_test.go:167`)
    - unified bench usage: `go run ./cmd/unified_bench -suite lanes_probe -dbs treedb ...`

### Tests / benches to run
- `go test ./cmd/unified_bench -count=1`
- `go test ./... -count=1`

### Success criteria
- `go test ./...` passes.
- `go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 100000 -valsize 128 -batchsize 1000` runs and prints a result.
- No TreeDB package diffs other than adding the new bench suite entry points.

### Git / PR steps (DO NOT RUN IN THIS RUN)
- Branch: `sprint/slabopt-pr0-bench-lanes-probe`
- Create PR description: `.pr/PR0_description.md`
- Create PR:
  - `gh pr create --title "PR0: unified bench lane probe harness" --body-file .pr/PR0_description.md --head sprint/slabopt-pr0-bench-lanes-probe --base main`
- CI wait:
  - `gh pr checks <PR_NUMBER> --watch`

### Artifacts
- Branch: `sprint/slabopt-pr0-bench-lanes-probe`
- PR description: `.pr/PR0_description.md`

---

## PR1 — Journal Abstraction
### Goal
- MUST replace “WAL-centric durability” reasoning with a single DB-level “Journal durable” boundary.
- MUST add crash-injection coverage for ACK invariants (commit intent durable AND payload durable).

### Non-goals (from spec)
- MUST NOT introduce new mmap for CommitLog/ValueLog segments (spec C3).
- MUST NOT introduce ValueIndex / ValueID indirection (spec C4).
- MUST NOT introduce zone packing / 2MB slab zones (spec C2).

### Dependencies / Preconditions
- PR0 merged (bench entry points exist).

### TODOs (exact)
- MUST introduce a named “Journal” durability concept in cached mode (initially still backed by existing WAL/vlog code paths).
  - File: `TreeDB/caching/db.go`
  - Symbols/locations:
    - `type walDurability uint8` (line `1894`)
    - `func (db *DB) appendWAL(...)` (line `1902`)
    - `func (db *DB) appendValueLog(...)` (line `1932`)
    - `func (db *DB) syncBarrierAfterWrite(sync bool) error` (line `2672`)
  - Change:
    - MUST rename/replace `walDurability` with a conceptually-correct type (e.g., `journalDurability`) that explicitly represents “commit intent durable + payload durable”.
    - MUST ensure that when `SplitValueLog` is enabled, payload durability is completed before commit-intent durability (ordering).
    - MUST preserve current behavior for `DisableWAL` / `RelaxedSync` paths.
- MUST add explicit unit tests validating payload-before-commit ordering for sync writes.
  - File: `TreeDB/caching/unified_wal_comprehensive_test.go`
  - Symbol/anchor: `func TestUnifiedWAL_CrashRecovery...` (around line `96` per `rg -n`; re-locate with `rg -n "TestUnifiedWAL_CrashRecovery" TreeDB/caching/unified_wal_comprehensive_test.go`)
  - Change:
    - MUST add a new test that simulates: payload append succeeds, commit record missing (crash), and recovery MUST NOT apply the write.
    - MUST add a second test: commit record exists, payload missing (corrupt/torn payload) and recovery MUST fail fast (hard error).
- MUST extend crash-injection durability tests to assert “Journal durable” boundary semantics.
  - File: `TreeDB/recovery_spec_test.go`
  - Symbol/anchor: `func TestCrashRecovery_DurabilityTiers(t *testing.T)` (line `267`)
  - Change:
    - MUST update test expectations and filesystem paths if/when PR2 changes DB directory layout.
    - MUST add cases that validate “commit intent durable AND payload durable” before acknowledging `WriteSync`/`SetSync`.

### Tests / benches to run
- `go test ./TreeDB/caching -run TestUnifiedWAL -count=1`
- `go test ./TreeDB -run TestCrashRecovery_DurabilityTiers -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`

### Success criteria
- All tests pass (including race).
- Crash-injection tests demonstrate that missing payload or missing commit intent is fail-closed.

### Git / PR steps (DO NOT RUN IN THIS RUN)
- Branch: `sprint/slabopt-pr1-journal-abstraction`
- PR description: `.pr/PR1_description.md`
- PR create:
  - `gh pr create --title "PR1: journal durability abstraction + crash tests" --body-file .pr/PR1_description.md --head sprint/slabopt-pr1-journal-abstraction --base main`
- CI wait:
  - `gh pr checks <PR_NUMBER> --watch`

### Artifacts
- Branch: `sprint/slabopt-pr1-journal-abstraction`
- PR description: `.pr/PR1_description.md`

---

## PR2 — DictDB (Separate TreeDB Instance)
### Goal
- MUST implement dictdb as a separate TreeDB instance with the key scheme from the spec.
- MUST enforce “lagging dictionary” rule: values MUST NOT reference dictID unless dictID is already durable in dictdb.

### Non-goals (from spec)
- MUST NOT store dictionaries inside maindb (no reserved namespace).
- MUST NOT introduce bespoke on-disk dictionary formats (dictdb is TreeDB).
- MUST NOT introduce ValueIndex / ValueID indirection.

### Dependencies / Preconditions
- PR1 merged (Journal semantics named and tested).

### TODOs (exact)
- MUST implement the directory layout:
  - Spec layout: `<db_root>/maindb` and `<db_root>/dictdb` (spec “DictDB Layout”).
  - File: `TreeDB/public.go`
  - Symbols/locations:
    - `func Open(opts Options) (*DB, error)` (line `84`)
    - `type DB struct` (line `62`)
  - Change:
    - MUST treat `opts.Dir` as `<db_root>` and open maindb in `<db_root>/maindb`.
    - MUST open dictdb in `<db_root>/dictdb` BEFORE opening maindb (spec “DictDB and Recovery Ordering”).
    - MUST store dictdb handle on the public `*treedb.DB` struct (add field to `type DB struct` at line `62`) so caching + recovery can access it.
    - MUST ensure `Close()` closes dictdb too (update `func (db *DB) Close() error` at line `391`).
- MUST add a minimal dictdb helper with the normative key scheme.
  - MISSING today: no `dictdb` package exists (`rg -n "dictdb" -S TreeDB` returns empty).
  - File: `TreeDB/internal/dictdb/store.go` (new file; new package `dictdb`)
  - Symbols (new):
    - `type Store struct { ... }`
    - `func Open(path string, opts treedb.Options) (*Store, error)` OR `func New(db *treedb.DB) *Store`
    - `func (s *Store) GetCurrent(ctx context.Context) (dictID uint64, err error)`
    - `func (s *Store) PutDictBytes(ctx context.Context, dictBytes []byte) (dictID uint64, err error)` (dedup by hash)
    - `func (s *Store) SetCurrent(ctx context.Context, dictID uint64) error`
    - `func (s *Store) GetDictBytes(ctx context.Context, dictID uint64) ([]byte, error)`
  - Change (normative):
    - MUST implement keys:
      - `bytes/<u64_be dictID>` → raw dict bytes
      - `hash/<32B sha256>` → `<u64_be dictID>`
      - `current` → `<u64_be dictID>`
    - MUST enforce immutability: re-putting `bytes/<id>` MUST be rejected.
    - MUST be thread-safe (MUST protect multi-step Put+SetCurrent via a `sync.Mutex`).
- MUST wire dictdb into cached-mode write path so batch start freezes dictID.
  - File: `TreeDB/caching/db.go`
  - Symbols/locations:
    - `type DB struct` (line `633`): add field to hold dict store reference.
    - `type Batch struct` (line `5132`): add `dictID uint64` (frozen per batch) and `dictIDValid bool` if needed.
    - `func (b *Batch) write(sync bool) error` (line `5474`): at entry, MUST read+freeze dictID from dictdb/current.
    - `func (db *DB) set(key, value []byte, sync bool) error` (line `2690`): MUST also freeze dictID for single-key Set/SetSync.
  - Change:
    - MUST ensure dictID is fetched once per batch (not per entry).
    - MUST NOT generate a new dict and use it in the same batch (spec “Lagging Dictionary Model”).
- MUST update recovery tests and any wal-dir assumptions to account for `<db_root>/maindb/wal`.
  - File: `TreeDB/recovery_spec_test.go`
  - Symbols/locations:
    - `os.ReadDir(filepath.Join(dir, "wal"))` (line `336`)
    - `walDir := filepath.Join(dir, "wal")` (line `371`)
  - Change:
    - MUST change these to read from `filepath.Join(dir, "maindb", "wal")` once PR2 lands the new layout.

### Tests / benches to run
- `go test ./TreeDB/internal/dictdb -count=1` (new)
- `go test ./TreeDB -run TestCrashRecovery_DurabilityTiers -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`

### Success criteria
- DictDB exists on disk at `<db_root>/dictdb`.
- DictDB key scheme works and is tested (dedup + current pointer).
- maindb open occurs after dictdb open; missing dictID is treated as hard error in later PRs.

### Git / PR steps (DO NOT RUN IN THIS RUN)
- Branch: `sprint/slabopt-pr2-dictdb`
- PR description: `.pr/PR2_description.md`
- PR create:
  - `gh pr create --title "PR2: dictdb separate TreeDB instance (lagging dict rule)" --body-file .pr/PR2_description.md --head sprint/slabopt-pr2-dictdb --base main`
- CI wait:
  - `gh pr checks <PR_NUMBER> --watch`

### Artifacts
- Branch: `sprint/slabopt-pr2-dictdb`
- PR description: `.pr/PR2_description.md`

---

## PR3 — RID-Based Join
### Goal
- MUST introduce RID-based join between CommitLog and ValueLog:
  - ValueLog frames carry RID.
  - CommitLog references RIDs and stores embedded values when required.
- MUST implement basic two-pass recovery v1:
  1) scan ValueLog segments to build RID→location map
  2) replay CommitLog to rebuild state

### Non-goals (from spec)
- MUST NOT use slab pointers as the CommitLog dependency (no pointer-based replay).
- MUST NOT implement large-value chunking redesign.
- MUST NOT implement ValueIndex / ValueID.

### Dependencies / Preconditions
- PR2 merged (dictdb exists and is opened before maindb).

### TODOs (exact)
- MUST introduce a RID type and generator.
  - File: `TreeDB/caching/db.go`
  - Symbol/location: `type DB struct` (line `633`), add `nextRID atomic.Uint64` (or equivalent)
  - Change:
    - MUST allocate RIDs deterministically per batch (monotonic counter is acceptable).
- MUST add new internal packages for CommitLog and ValueLog (v1 format).
  - MISSING today: there is only `TreeDB/internal/wal` and `TreeDB/internal/vlog` (existing formats).
  - Files (new):
    - `TreeDB/internal/commitlog/commitlog.go`
    - `TreeDB/internal/commitlog/reader.go`
    - `TreeDB/internal/commitlog/writer.go`
    - `TreeDB/internal/valuelog/valuelog.go`
    - `TreeDB/internal/valuelog/reader.go`
    - `TreeDB/internal/valuelog/writer.go`
  - Format requirements (normative):
    - MUST cap lengths before allocation (spec C1).
    - MUST include per-record CRC (or segment CRC) and fail closed on corruption.
    - MUST NOT use mmap for these logs (spec C3).
    - MUST include RID in ValueLog frames and in CommitLog references.
    - MUST support embedded values in CommitLog records.
- MUST switch cached-mode write path to write:
  - ValueLog payload frames first (RID assigned), then CommitLog intent record.
  - File: `TreeDB/caching/db.go`
  - Symbols/locations:
    - `func (db *DB) set(key, value []byte, sync bool) error` (line `2690`)
    - `func (b *Batch) writeRegular(sync bool) error` (line `5519`)
  - Change:
    - MUST remove reliance on `logOpSetPointer` (defined in `TreeDB/caching/log_writer.go:12`) for correctness of replay.
    - MUST write commit intent records that reference RIDs (not `page.ValuePtr`).
    - MUST embed values when the write path requires inline value semantics (threshold-based decision is acceptable).
- MUST implement recovery v1 in backend open path.
  - File: `TreeDB/db/db.go`
  - Symbol/location: WAL replay block in `Open`:
    - `includeValueLog := ...` (line `529`)
    - `segments, err := listWALSegments(...)` (line `530`)
    - `replayWALIntoBackend(...)` (line `535`)
  - Change:
    - MUST replace this WAL/vlog replay with CommitLog/ValueLog recovery v1:
      - pass 1: scan ValueLog segments to build RID map
      - pass 2: replay CommitLog segments to apply committed batches
    - MUST stop at first torn tail per segment and treat corruption as hard error unless it is a clean truncation.
    - MUST ignore unreferenced ValueLog bytes.
- MUST update existing recovery tests to target the new recovery implementation.
  - File: `TreeDB/recovery_spec_test.go`
  - Symbols:
    - `TestRecovery_TruncatedWALRecord` (line `368`)
  - Change:
    - MUST replace this test with analogous tests for CommitLog/ValueLog truncation and RID join correctness.

### Tests / benches to run
- `go test ./TreeDB/internal/commitlog -count=1` (new)
- `go test ./TreeDB/internal/valuelog -count=1` (new)
- `go test ./TreeDB -run TestCrashRecovery -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`

### Success criteria
- Basic crash recovery works with RID join (no pointers required).
- Missing RID resolution is a hard error (fail fast).
- All tests pass.

### Git / PR steps (DO NOT RUN IN THIS RUN)
- Branch: `sprint/slabopt-pr3-rid-join`
- PR description: `.pr/PR3_description.md`
- PR create:
  - `gh pr create --title "PR3: RID join for CommitLog/ValueLog + basic recovery v1" --body-file .pr/PR3_description.md --head sprint/slabopt-pr3-rid-join --base main`
- CI wait:
  - `gh pr checks <PR_NUMBER> --watch`

### Artifacts
- Branch: `sprint/slabopt-pr3-rid-join`
- PR description: `.pr/PR3_description.md`

---

## PR4 — Dict + Dynamic-K on Write Path
### Goal
- MUST make dict + dynamic-K grouped compression the primary ValueLog encoding.
- MUST demonstrate stored-bytes reduction on compressible data.

### Non-goals (from spec)
- MUST NOT introduce zone packing / physical boundaries.
- MUST NOT add new mmap for CommitLog/ValueLog segments.
- MUST NOT add ValueIndex / ValueID.

### Dependencies / Preconditions
- PR3 merged (RID join + basic recovery v1 exist).

### TODOs (exact)
- MUST add `TreeDB/internal/compression` by porting from `origin/feature/slab-optimizations`.
  - MISSING today on this branch (`TreeDB/internal/compression` does not exist).
  - Source files to port (from `git ls-tree -r --name-only origin/feature/slab-optimizations`):
    - `TreeDB/internal/compression/types.go`
    - `TreeDB/internal/compression/config.go`
    - `TreeDB/internal/compression/metrics.go`
    - `TreeDB/internal/compression/profile.go`
    - `TreeDB/internal/compression/trainer.go`
    - `TreeDB/internal/compression/trainer_test.go`
  - Change:
    - MUST keep compression logic isolated to `TreeDB/internal/compression/*` (no slab/async writer coupling).
- MUST extend ValueLog v1 format to support grouped frames:
  - File: `TreeDB/internal/valuelog/valuelog.go` (created in PR3)
  - Symbols (expected):
    - `type FrameHeader struct { ... }`
    - `func EncodeFrame(...)`
    - `func DecodeFrame(...)`
  - Change:
    - MUST include `dictID` in frame header.
    - MUST include group size `K` and carry `K` logical values per frame.
    - MUST be deterministic given batch contents for K-selection (spec).
- MUST wire dictdb dictID and dynamic-K selection into write path.
  - File: `TreeDB/caching/db.go`
  - Symbols/locations:
    - `func (db *DB) set(...)` (line `2690`)
    - `func (b *Batch) writeRegular(...)` (line `5519`)
  - Change:
    - MUST choose dictID once per batch (already frozen in PR2) and pass through to ValueLog writes.
    - MUST select K using `TreeDB/internal/compression` logic (deterministic per batch).
    - MUST write grouped frames by default (not optional scaffolding).
- MUST add a stored-bytes reduction test on compressible data.
  - File: `TreeDB/caching/unified_wal_comprehensive_test.go` OR a new test file `TreeDB/caching/dict_k_compression_test.go`
  - Change:
    - MUST write a dataset with high redundancy and assert on-disk ValueLog bytes shrink relative to raw.
    - MUST not assert exact ratios; MUST assert meaningful reduction (e.g., >20%) to avoid flakiness.

### Tests / benches to run
- `go test ./TreeDB/internal/compression -count=1`
- `go test ./TreeDB/internal/valuelog -count=1`
- `go test ./TreeDB/caching -run \"Dict|K|Grouped|UnifiedWAL\" -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`

### Success criteria
- ValueLog uses dictID+grouped frames by default.
- Compressible test data shows stored-bytes reduction.
- All tests pass.

### Git / PR steps (DO NOT RUN IN THIS RUN)
- Branch: `sprint/slabopt-pr4-dict-dynamick`
- PR description: `.pr/PR4_description.md`
- PR create:
  - `gh pr create --title "PR4: dict + dynamic-K grouped ValueLog encoding" --body-file .pr/PR4_description.md --head sprint/slabopt-pr4-dict-dynamick --base main`
- CI wait:
  - `gh pr checks <PR_NUMBER> --watch`

### Artifacts
- Branch: `sprint/slabopt-pr4-dict-dynamick`
- PR description: `.pr/PR4_description.md`

---

## PR5 — Parallel Active Lanes
### Goal
- MUST maintain N active CommitLog/ValueLog lane pairs in parallel.
- MUST overlap fsync with continued writes.
- MUST preserve batch semantics via Journal durability tickets.

### Non-goals (from spec)
- MUST NOT implement complex autotuning.
- MUST NOT introduce slab zone packing or async slab writer machinery.

### Dependencies / Preconditions
- PR4 merged (write path uses dict+K, RID join stable).

### TODOs (exact)
- MUST add lane-count configuration in public options and cached options.
  - File: `TreeDB/db/db.go`
  - Symbol/location: `type Options struct` (line `99`)
  - Change:
    - MUST add `JournalLanes int` (0=default) to `db.Options`.
  - File: `TreeDB/caching/db.go`
  - Symbol/location: `type Options struct` (line `567`)
  - Change:
    - MUST add matching `JournalLanes int` to `caching.Options`.
  - File: `cmd/unified_bench/adapter_treedb.go`
  - Symbol/location: `NewTreeDB` (line `97`) and `NewTreeDBBackend` (line `138`)
  - Change:
    - MUST thread `-treedb-journal-lanes` into `treedb.Options.JournalLanes`.
- MUST refactor cached-mode durability state from single WAL/vlog writers to per-lane writers.
  - File: `TreeDB/caching/db.go`
  - Symbols/locations:
    - `type DB struct` (line `633`): replace `wal`, `walPath`, `walSeq`, `vlog`, `vlogPath`, `vlogSeq` (lines `667-672`) with per-lane arrays/slices.
    - `func (db *DB) rotateWALLocked() error` (line `3722`) and `func (db *DB) rotateValueLogLocked() error` (line `3777`)
  - Change:
    - MUST introduce `type lane struct { commitWriter; valueWriter; paths; seqs; syncing state; ... }` (new, likely in `TreeDB/caching/lane.go`).
    - MUST assign each batch to a lane deterministically (e.g., round-robin on `nextLane` atomic).
    - MUST implement durability tickets per batch:
      - ticket waits until both lane commitlog and valuelog reach durable boundary for that batch.
    - MUST ensure a syncing lane is not used for new writes while its fsync is in flight (other lanes remain writable).
- MUST update segment naming to include lane ID (normative).
  - File: `TreeDB/caching/db.go`
  - Symbol/location: `func (db *DB) logSegmentPrefix() string` (line `267`)
  - Change:
    - MUST include lane in filename, e.g. `commit-l<lane>-<seq>.log` / `value-l<lane>-<seq>.log` (exact naming to be decided, but must be parseable without ambiguity).
    - MUST update `parseLogSeq` (line `5881`) accordingly.
- MUST update recovery segment discovery to scan per-lane segments.
  - File: `TreeDB/db/wal_recovery.go` (or the new recovery files from PR3/PR4)
  - Symbol/location: `listWALSegments` (line `25`) and `parseLogSeq` (line `67`)
  - Change:
    - MUST parse lane ID and sequence from filenames and return ordered segments per lane.

### Tests / benches to run
- `go test ./TreeDB/caching -run \"Race|Rotate|Consistency\" -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`
- Bench (from PR0):
  - `go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 500000 -valsize 128 -batchsize 1000 -treedb-journal-lanes 1`
  - `go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 500000 -valsize 128 -batchsize 1000 -treedb-journal-lanes 2`

### Success criteria
- Lane count >1 works and improves throughput in lane probe (measured).
- Batch ACK waits on per-batch durability ticket (commit+payload durable).
- Tests pass, including race.

### Git / PR steps (DO NOT RUN IN THIS RUN)
- Branch: `sprint/slabopt-pr5-parallel-lanes`
- PR description: `.pr/PR5_description.md`
- PR create:
  - `gh pr create --title "PR5: parallel active journal lanes + durability tickets" --body-file .pr/PR5_description.md --head sprint/slabopt-pr5-parallel-lanes --base main`
- CI wait:
  - `gh pr checks <PR_NUMBER> --watch`

### Artifacts
- Branch: `sprint/slabopt-pr5-parallel-lanes`
- PR description: `.pr/PR5_description.md`

---

## PR6 — Recovery Hardening
### Goal
- MUST make two-pass recovery v1 default-safe and hardened.
- MUST add fuzz coverage for new parsing.
- MUST add crash tests for edge cases.

### Non-goals (from spec)
- MUST NOT prioritize recovery speed over correctness.
- MUST NOT introduce silent corruption.

### Dependencies / Preconditions
- PR5 merged (lanes exist).

### TODOs (exact)
- MUST implement fuzz tests for CommitLog and ValueLog parsers.
  - Reference: existing fuzz tests in `TreeDB/internal/vlog/vlog_fuzz_test.go` (seen in `rg -n "vlog_fuzz" TreeDB/internal/vlog`)
  - Files (new):
    - `TreeDB/internal/commitlog/commitlog_fuzz_test.go`
    - `TreeDB/internal/valuelog/valuelog_fuzz_test.go`
  - Change:
    - MUST cap all decoded lengths before allocation (spec C1).
    - MUST never panic; malformed input MUST return errors.
- MUST expand crash recovery integration tests to cover:
  - truncated tails,
  - missing dictID (hard error),
  - partial batches (commit boundary missing → ignore),
  - multi-lane ordering.
  - File: `TreeDB/recovery_spec_test.go`
  - Anchors:
    - `TestCrashRecovery_WALReplayIsCoherentAcrossModes` (line `164`)
    - `TestCrashRecovery_DurabilityTiers` (line `267`)
    - `TestRecovery_TruncatedWALRecord` (line `368`) (must be replaced by CommitLog/ValueLog analogs)
- MUST ensure recovery deletes only segments that are safely past durability boundary, per Journal semantics.
  - File: recovery implementation from PR3/PR5 (expected new files under `TreeDB/db/`)
  - Change:
    - MUST NOT delete ValueLog segments that contain referenced payloads for committed batches unless and until they are durable elsewhere (if that concept exists).
    - For this sprint, it is acceptable to keep segments conservatively (correctness-first).

### Tests / benches to run
- `go test ./TreeDB/internal/commitlog -run Fuzz -fuzz=Fuzz -fuzztime=10s`
- `go test ./TreeDB/internal/valuelog -run Fuzz -fuzz=Fuzz -fuzztime=10s`
- `go test ./TreeDB -run \"CrashRecovery|Recovery\" -count=1`
- `go test ./... -count=1`
- `go test ./... -race -count=1`

### Success criteria
- Fuzz tests run without panics.
- Recovery is fail-closed on corruption/missing dict.
- Crash tests cover key edge cases and pass.

### Git / PR steps (DO NOT RUN IN THIS RUN)
- Branch: `sprint/slabopt-pr6-recovery-hardening`
- PR description: `.pr/PR6_description.md`
- PR create:
  - `gh pr create --title "PR6: recovery hardening + fuzz + crash edge cases" --body-file .pr/PR6_description.md --head sprint/slabopt-pr6-recovery-hardening --base main`
- CI wait:
  - `gh pr checks <PR_NUMBER> --watch`

### Artifacts
- Branch: `sprint/slabopt-pr6-recovery-hardening`
- PR description: `.pr/PR6_description.md`

---

## PR7+ — Index Work (Aligned; behind flags)
### Goal
- MUST ensure index optimization work is aligned with the new stable storage contract and is gated behind flags.

### Non-goals (from spec)
- MUST NOT couple index encoding to unstable pointer semantics.
- MUST NOT ship default-on index format changes without explicit flags.

### Dependencies / Preconditions
- PR6 merged (storage contract stable; recovery hardened).

### TODOs (exact)
- MUST add explicit feature flags for index layout experiments.
  - File: `TreeDB/db/db.go`
  - Symbol/location: `type Options struct` (line `99`)
  - Change:
    - MUST add opt-in booleans (exact naming TBD) such as:
      - `IndexColumnarLeaves bool`
      - `IndexInternalBaseDelta bool`
    - MUST default to false (no behavior change by default).
- MUST plumb flags into node builder options without changing default encoding.
  - File: `TreeDB/node/builder.go`
  - Symbols/locations:
    - `type BuilderOptions struct` (line `23`)
    - `func NewBuilderWithOptions(...)` (line `32`)
  - Change:
    - MUST extend `BuilderOptions` with new booleans for experimental layouts.
    - MUST keep existing behavior unchanged when flags are false.
- MUST add a minimal “columnar leaf” experimental path behind flags (initially off).
  - Files (new):
    - `TreeDB/node/leaf_columnar.go`
  - Integration points:
    - `TreeDB/node/leaf.go` (anchor: `func (n *Node) ...` leaf encode/decode; re-locate before editing with `rg -n "leafPrefixCompressed" TreeDB/node/leaf.go`)
  - Change:
    - MUST keep on-disk format stable unless flag is explicitly enabled.

### Tests / benches to run
- `go test ./TreeDB/node -count=1`
- `go test ./... -count=1`
- Optional bench:
  - `go test ./TreeDB/db -bench BenchmarkGet -count=1`

### Success criteria
- Default index encoding unchanged (flags off).
- Flags compile and are test-covered (at least smoke tests).

### Git / PR steps (DO NOT RUN IN THIS RUN)
- Branch: `sprint/slabopt-pr7-index-flags`
- PR description: `.pr/PR7_description.md`
- PR create:
  - `gh pr create --title "PR7: index work flags + plumbing (no default change)" --body-file .pr/PR7_description.md --head sprint/slabopt-pr7-index-flags --base main`
- CI wait:
  - `gh pr checks <PR_NUMBER> --watch`

### Artifacts
- Branch: `sprint/slabopt-pr7-index-flags`
- PR description: `.pr/PR7_description.md`

---

## GitHub CLI policy (future runs)
- MUST create PRs with:
  - `gh pr create --title "..." --body-file .pr/PR<N>_description.md --head <branch> --base main`
- MUST include unified_bench output samples in each PR description (store outputs in `.pr/PR<N>_description.md`).
- MUST confirm CI passes before starting the next PR:
  - `gh pr checks <PR_NUMBER> --watch`
  - (alternative) `gh pr view <PR_NUMBER> --json statusCheckRollup`
- MUST record CI results in the “Local Activity Log”.

---

## Local Activity Log (append-only)
`2026-01-17 15:53:23 HST`
- Read `slab-optimization/spec.md` (via `nl -ba slab-optimization/spec.md`).
- Recon: `git rev-parse HEAD` → `d23d871e1a17baaa23f20f5e023e9650c99116f2`; branch `docs/opt-sprint-next`; working tree clean.
- Baseline tests:
  - `go test ./... -count=1` → PASS
  - `go test ./... -race -count=1` → PASS (macOS linker warning building `cmd/unified_bench.test`: malformed `LC_DYSYMTAB`)
- Anchor capture (selected):
  - `TreeDB/public.go:84` (`Open`), `TreeDB/public.go:374` (`OpenCached`), `TreeDB/public.go:380` (`OpenBackend`)
  - `TreeDB/caching/db.go:567` (`type Options`), `TreeDB/caching/db.go:633` (`type DB struct`)
  - `TreeDB/caching/db.go:1894` (`type walDurability`), `TreeDB/caching/db.go:1902` (`appendWAL`), `TreeDB/caching/db.go:1932` (`appendValueLog`), `TreeDB/caching/db.go:2672` (`syncBarrierAfterWrite`)
  - `TreeDB/caching/db.go:3722` (`rotateWALLocked`), `TreeDB/caching/db.go:3777` (`rotateValueLogLocked`)
  - `cmd/unified_bench/main.go:224` (suite `switch`)
  - `cmd/unified_bench/adapter_treedb.go:16` (TreeDB flag var block)
  - `TreeDB/db/db.go:529` (WAL replay block), `TreeDB/db/db.go:611` (`recover`)
  - Existing slab per-value zstd envelope (baseline note): `TreeDB/slab/compression.go:75` (`compressValue`)

`2026-01-17 18:38:04 HST`
- Added lanes_probe suite hook in `cmd/unified_bench/main.go`.
- Added `-treedb-journal-lanes` flag placeholder + TODO plumbing notes in `cmd/unified_bench/adapter_treedb.go`.
- Added `cmd/unified_bench/suite_lanes_probe.go` (TreeDB-only deterministic lane probe + size reporting).
- Created `.pr/` and `.pr/PR0_description.md`.

`2026-01-17 18:40:29 HST`
- Tests: `go test ./cmd/unified_bench -count=1` → PASS
- Tests: `go test ./... -count=1` → PASS

`2026-01-17 18:50:53 HST`
- Updated PR process requirements in `slab-optimization/AGENTS.md` and `slab-optimization/spec.md`.
- Updated `.pr/PR0_description.md` with unified_bench output samples.
- Bench: `go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 100000 -valsize 128 -batchsize 1000`
- Bench: `go run ./cmd/unified_bench -suite lanes_probe -dbs treedb -keys 100000 -valsize 128 -batchsize 1000 -treedb-journal-lanes 2`
- Tests: `go test ./cmd/unified_bench -count=1` → PASS
