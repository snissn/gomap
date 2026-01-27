# TreeDB WAL on/WAL off Merge Gate Runbook (Journal + ValueLog + Autotune + Live Bench)

Status: **Ready-to-execute runbook** (intended to be autonomously executable by a coding agent)  
Updated: 2026-01-22

---

## 0) Outcome (definition of done)

This runbook gates a merge that:

1. Removes the pre-sprint “slab + optional WAL” write path (value-log-off).
2. Makes the new journal + value-log architecture the default and preferred path:
   - **WAL on (durable):** journal ON + value log ON (no “write values twice”).
   - **WAL off (fast ingest / lower durability):** journal OFF + value log ON (unsafe).
3. Consolidates documentation so there is exactly one recommended way to run cached-mode ingest, with deprecated knobs clearly labeled.
4. Adds thorough test coverage plus a benchmark suite with validation marks, including a marquee benchmark that demonstrates the autotuner’s correctness and impact.
5. Ensures index/key optimizations are correctness-gated and performance-characterized before being enabled by default.

Non-goal: user data migration and compatibility guarantees. TreeDB is pre-alpha dev software.

### 0.1 Assumption: live KV throughput bench already exists

This runbook assumes the work described in:
- `slab-optimization/AGENTS_LIVE_BENCH.md`
- `slab-optimization/AGENTS_LIVE_BENCH_PROMPT.md`

…has already been completed and the **live KV throughput benchmark** is available via:
- `go run ./TreeDB/cmd/vlog_dict_realdata ... -bench-kv ...`

Before using this runbook, do a quick existence check:

```bash
go run ./TreeDB/cmd/vlog_dict_realdata -h | rg -n "bench-kv|bench-mode|bench-compression"
```

If those flags do not exist yet, stop and execute `slab-optimization/AGENTS_LIVE_BENCH_PROMPT.md` first.

---

## 1) Terminology (remove confusion)

### 1.1 Canonical terms (use these in code comments + docs)

- **Value log (vlog)**: the append-only large-value store used by cached mode.
- **Journal**: the unified redo log introduced in the sprint (metadata journal + value journal). When journal is enabled, cached-mode writes become durable without writing large values twice.
- **WAL (legacy term)**: historically meant “redo log”. In the new architecture, treat “WAL” in public language as synonymous with journal and avoid introducing additional synonyms.

### 1.2 Historical vs current write path

**Before (deprecated):**

- Cached-mode writes go to WAL (metadata + values) to satisfy durability.
- Later, the backend writes values to the backend slab during flush.
- For large values this can mean writing the payload twice.

**Now (preferred):**

- Cached-mode writes large values to the value log, and metadata to the journal.
- The value log is directly usable by both the durability layer and the read/write engine.
- This reduces data amplification and improves ingest throughput.

### 1.3 Mode mapping (strict)

The repo historically referenced legacy mode names. Treat that numbering as internal only and remove it from user-facing docs.

Use WAL terminology going forward:

- **wal_on**: value log ON, journal ON (preferred default).
- **wal_off**: value log ON, journal OFF (unsafe; opt-in).

---

## 2) Policy decisions (what must be true after this sprint)

### 2.1 Defaults (normative)

1. Cached mode defaults to WAL on (journal ON + value log ON).
2. WAL off is opt-in because it changes durability semantics (select via `Options.Durability = DurabilityWALOffRelaxed`).
3. The value-log compression autotuner is enabled by default when the value log is enabled (WAL on/WAL off), using a conservative preset (e.g., `AutotuneMedium`).
4. All other experimental knobs (index encodings, key encodings) remain off by default until they pass the benchmark+correctness gates in this runbook.

### 2.2 Deprecations (normative)

- All user-facing documentation must clearly label the following as deprecated:
  - any legacy slab/value-log-off guidance
  - any guidance implying “WAL contains values and values are written twice”
- The following must be the only recommended paths in docs:
  - Durable ingest: WAL on
  - Fast ingest (unsafe): WAL off

### 2.3 Public API / developer experience (normative)

- Public docs must not require users to understand internal flags to get a correct setup.
- Provide high-level entry points (examples):
  - `OptionsFor(ProfileDurable, dir)` → WAL on
  - `OptionsFor(ProfileFast, dir)` → WAL off (unsafe)
- Ensure `treedb.Open(...)` is bench-friendly:
  - no unconditional stderr prints that break `benchstat`
  - logs must be behind an explicit debug flag or logger injection

### 2.4 Knob inventory (what to document, what to deprecate)

Document and present these as the only supported, forward-looking knobs (names may differ between library options vs CLI flags):

| Concern | Preferred knobs | Notes | Default after sprint |
|---|---|---|---|
| Mode selection | `Options.Durability` | WAL off changes durability semantics; make it explicit | WAL on |
| Journal compression | `JournalCompression` / `-treedb-journal-compress` | Performance-only; keep default off unless proven | OFF |
| Value-log dict compression | dict compression enable + training/tuning knobs | Gate “on by default” behind benchmarks; require pause/probe guardrails | TBD (see Section 5) |
| Autotuner | `Options.ValueLog.CompressionAutotune` | Default on (Medium) with bounded overhead | ON |
| Index encodings | `IndexColumnarLeaves`, `IndexInternalBaseDelta` | Must pass reopen+scan test; default off until proven | OFF |
| B-tree key optimizations | key encoding flags (if any) | Must pass correctness + scan/point lookup perf | OFF until gated |

---

## 3) Merge gates (hard requirements)

### 3.1 Build + hygiene gate (must pass)

Run from repo root:

```bash
go version
go env GOPATH GOMOD

go fmt ./...
go vet ./...
# If the repo uses staticcheck/golangci-lint, run it here as well.

go test ./... -count=1
```

Additional required runs:

```bash
# Concurrency safety
go test ./... -race -count=1

# Fuzz tests (time-bounded)
# Prefer existing fuzz targets; run at least a short fuzz interval in CI
# and longer locally. Note: Go requires fuzzing a single package/test at a time.
# Example (30s each):
#   go test ./TreeDB/page -run '^$' -fuzz=FuzzDecodeHeader -fuzztime=30s
#   go test ./TreeDB/page -run '^$' -fuzz=FuzzDecodeValuePtr -fuzztime=30s
#   go test ./TreeDB/internal/vlog -run '^$' -fuzz=FuzzVlogReader -fuzztime=30s
#   go test ./TreeDB/internal/merging -run '^$' -fuzz=FuzzMergingIterator -fuzztime=30s
#   go test ./TreeDB/internal/commitlog -run '^$' -fuzz=FuzzCommitLogReader -fuzztime=30s
#   go test ./TreeDB/node -run '^$' -fuzz=FuzzNodeDecode -fuzztime=30s
#   go test ./TreeDB/internal/wal -run '^$' -fuzz=FuzzWALReader -fuzztime=30s
#   go test ./TreeDB/internal/valuelog -run '^$' -fuzz=FuzzDecodeFrame -fuzztime=30s
#   go test ./TreeDB/internal/valuelog -run '^$' -fuzz=FuzzValueLogReader -fuzztime=30s
```

### 3.2 Correctness gate: mode semantics

These are behavioral contracts you must enforce with tests and/or verification tools.

**WAL on (journal ON + value log ON):**
- After `WriteSync`/`Checkpoint`, a crash+reopen preserves all committed keys.
- Reads (`Get`, iterators, scans) return correct values across reopen.
- Large values stored in the value log are readable without requiring a secondary rewrite.

**WAL off (journal OFF + value log ON):**
- Crash+reopen may lose recent writes (by design), but must never corrupt the DB:
  - no panics
  - no invariant violations
  - verify/scan completes

**Legacy value-log-off path:**
- Removed; no additional coverage required beyond ensuring no references remain.

### 3.3 Correctness gate: index/key optimizations

Any new index/key encoding flag must pass:

1. `go test ./...` (unit)
2. `go test ./... -race` (no data races)
3. an integration reopen+scan test (write → close → reopen → full scan → verify)
4. benchmark harness preload (if the bench suite has a preload stage)

If any flag cannot pass all four, it must be:
- default-off
- labeled experimental
- excluded from “recommended defaults”

### 3.4 Benchmark + validation gate

Benchmarks are required for merge readiness, but only some are CI-gatable.

- CI-gatable: deterministic “validation marks” (no wall-clock dependence)
- Local merge gate: real wall-time throughput runs (non-deterministic, but required for PR review)

Minimum required benchmark sets are defined in Section 5.

### 3.5 Documentation gate

Before flipping defaults or removing old docs, ensure:

- one canonical doc explains WAL on/WAL off and uses the terminology in Section 1
- deprecated options are labeled DEPRECATED and moved to a dedicated section
- benchmarks docs specify:
  - how to reproduce
  - what is CI-gated vs exploratory
  - what constitutes a regression

---

## 4) Test coverage plan (what to add / what to confirm)

### 4.1 “Write → close → reopen → verify” integration tests (required)

Add a small set of integration tests that open TreeDB in each relevant configuration, ingest a deterministic dataset, then:

1. Close cleanly
2. Reopen (read-only is fine)
3. Run:
   - `Get` spot checks (random subset)
   - `Iterator(nil,nil)` full scan (counts + checksum-based reads)
   - optional: `FragmentationReport` validation

Required matrix:

| Test name | Mode | Durability action | Expectations |
|---|---|---|---|
| `TestReopenVerify_WAL on_Checkpoint` | WAL on | `Checkpoint()` | all keys preserved; scan passes |
| `TestReopenVerify_WAL on_WriteSync` | WAL on | `SetSync`/`WriteSync` | all keys preserved; scan passes |
| `TestReopenVerify_WAL off_NoJournal` | WAL off | none | scan passes; values correct for keys that survived |

Implementation notes:
- Use a temp dir and clean it per test.
- Use a deterministic dataset generator (seeded RNG) and record the chosen keys so assertions are repeatable.
- Keep payload sizes representative:
  - small values (e.g. 32B)
  - large values (e.g. 16KiB) to force value-log paths

### 4.2 Crash recovery tests (required)

TreeDB already has crash-recovery style tests; extend or add coverage so mode semantics are explicitly gated.

Minimal required crash cases:

- WAL on:
  - write N keys, `Checkpoint()`, simulate crash, reopen, verify all N keys
  - write N keys, `WriteSync()` boundary, crash, reopen, verify all keys up to the boundary

- WAL off:
  - write N keys without checkpoint, crash, reopen, verify:
    - DB opens without repair loops/panics
    - full scan completes
    - (optional) keys may be missing, but no corrupt values

If there is no crash injection facility:
- use “process kill” style tests only if already supported
- otherwise, emulate crash by:
  - closing file descriptors without running normal close hooks
  - forcing partial segment tails and relying on tail repair logic

### 4.3 IndexColumnarLeaves regression test (required)

Because `IndexColumnarLeaves` previously produced preload-time corruption, add an integration test that matches the benchmark’s failure mode:

1. Open DB with the flag enabled
2. Ingest a dataset sufficient to create multiple leaf pages
3. Close
4. Reopen
5. Run a preload-like path if it exists (or force scan that exercises offset decoding)
6. Full scan + spot `Get` checks

If the benchmark harness has a “preload” step, make it callable from tests.

Acceptance criterion: the test fails on the prior broken implementation and passes now.

### 4.4 Value-log dictionary compression tests (confirm + extend)

Confirm and/or add:

- Dictionary round-trip validation: dict bytes written to dict store can be loaded after restart and used to decode values.
- Pause/probe behavior: incompressible payload streams do not waste unbounded CPU attempting dict compression.
- Frame decode invariants: grouped frame offset arrays are bounds-checked and do not panic on corruption.

### 4.5 Autotuner tests (deterministic, CI-friendly)

If the new wall-time autotuner is implemented (or being added), CI gating must rely on deterministic validation marks, not machine wall clock.

Required test harness components:
- deterministic clock (virtual time)
- deterministic IO sink that charges `ns_per_byte`
- deterministic encode-cost model or sampled timing behind the virtual clock

Required scenarios:
1. CPU-bound compressible
2. IO-bound compressible
3. IO-bound incompressible
4. Marquee regime-shift (compressible → incompressible → compressible)

See Section 5.3 for the marquee definition.

---

## 5) Benchmark suite (required) + validation marks

### 5.1 Benchmark tooling

Benchmarks are run with three tools:

1. End-to-end harness (repo root): `cmd/unified_bench`
   - Reproducible dataset generation (seeded).
   - Useful for mode comparisons and scan regressions across DBs.
2. Deterministic autotune suite (TreeDB-local): `TreeDB/cmd/unified_bench`
   - Deterministic “marks” and `-validate` gating for autotuner correctness.
3. Live KV throughput bench (TreeDB-local): `TreeDB/cmd/vlog_dict_realdata -bench-kv`
   - Public TreeDB KV API (`treedb.Open` + `Batch.Set` + `Batch.Write`).
   - Wall-clock throughput numbers (non-deterministic); intended as a **local merge gate** with logs attached to PRs.

Guiding principle:
- Use deterministic validation marks for CI gating (autotune behavior, invariants).
- Use wall-time benchmarks as a local merge gate (throughput/perf regressions), with outputs attached to PRs.

### 5.2 WAL on/WAL off (local merge gate)

You must run the following comparisons on the same host.

#### 5.2.A Live KV throughput (primary, real dataset)

Use the live bench implemented via `AGENTS_LIVE_BENCH_PROMPT.md` (see `AGENTS_LIVE_BENCH.md` for details and option mapping).

Run the full matrix and save logs:

```bash
# Compression OFF baseline set
go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode wal_on -bench-compression off | tee out/live_wal_on_off.log
go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode wal_off -bench-compression off | tee out/live_wal_off_off.log

# Compression ON feature set
go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode wal_on -bench-compression on  | tee out/live_wal_on_on.log
go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode wal_off -bench-compression on  | tee out/live_wal_off_on.log
```

Acceptance criteria (practical, not absolute):
- Each run prints a single “headline” steady-state metric (e.g. `steady_raw_MBps=...`) and the selected write-path keys (`treedb.write_path.*`).
- `compression=off` runs show no dict activity (`dict_id==0` and `kept_frac≈0`).
- For compressible datasets, `compression=on` runs show dict activity (`dict_id!=0`, `kept_frac>0`) and should not regress steady-state throughput versus `off` without an explanation.
- Mode expectations should match:
  - wal_on ⇒ `value_store=value_log`, `redo_log=on`
  - wal_off ⇒ `value_store=value_log`, `redo_log=off`

Notes:
- This bench is explicitly **no-fsync / relaxed durability** (uses `Batch.Write()` only). It is intended to answer “how fast can we ingest” under current development priorities.

#### 5.2.B Synthetic mode comparisons (secondary, regression trending)

**Workload A: large-value ingest (cached mode)**

```bash
# Recommend pinning: seed=1, valsize=16384, keys=20000
go run ./cmd/unified_bench   -dbs treedb   -test random_write,dataset_write_random   -keys 20000 -valsize 16384 -batchsize 1000 -seed 1   -format markdown -progress=false   > out/mode_compare_large_values.md
```

Run it for:
- WAL on (default)
- WAL off (unsafe: disable journal)

Acceptance criteria (normative, starting point):
- WAL on throughput should be within noise of WAL off for ingest-focused workloads, unless a regression is explained.

If any comparison fails:
- identify regression root cause
- either fix or explicitly document exception + keep default-off for the regressing path

**Workload B: scan regressions + index/key flags**

```bash
go run ./cmd/unified_bench   -dbs treedb   -test full_scan,prefix_scan   -keys 100000 -valsize 128 -seed 1   -settle-before-scans   -format markdown -progress=false   > out/scan_compare.md
```

Run baseline and with each index/key encoding flag under test.

#### 5.2.C Head-to-head main vs rc (required before merge)

Before merging `sprint/rc_1` to `main`, run a **head-to-head** comparison on the
same host to highlight deltas between branches.

Minimum required comparisons:

1) **Live KV throughput (one compressible config + one incompressible config)**  
   - Run on **both** branches (`main` and `sprint/rc_1`) with identical flags:
     - `wal_on` + `compression=off`
     - `wal_on` + `compression=on`
   - Use the same dataset and `-bench-raw-mib/-bench-batch/-train/-eval` values.
   - If `main` does **not** have `-bench-kv` flags, record the absence and skip the
     live bench on `main` (do not backport just for this comparison).

2) **Synthetic large-value comparisons**  
   Run the Workload A `cmd/unified_bench` matrix on **both** branches:
   - wal_on (default)
   - wal_off (`DurabilityWALOffRelaxed`)

Artifacts (store separately per branch):
- `out/head_to_head_main/*`
- `out/head_to_head_rc/*`

Summarize deltas:
- Report any **>10% regression** in steady throughput or synthetic writes.
- Note any qualitative differences (e.g., dict activity missing/present).

#### 5.2.C.1 Extended head-to-head (recommended)

The minimum comparisons above are often insufficient to explain tradeoffs across:
- small vs medium vs large values,
- point writes vs dataset-like writes,
- scan regressions,
- and compression on/off costs.

Run a small sweep of `cmd/unified_bench` on **both** branches with identical flags,
capturing markdown outputs and (optionally) keeping the data directories so disk
bytes can be inspected.

Recommended sweep (keep it stable; expand only when investigating a regression):

```bash
# Use a fixed seed for comparability.
seed=1
keys=20000
batch=1000
valsizes="128 1024 16384"
tests="random_write,dataset_write_random,random_read"

# On main: avoid value-log pointer paths if they are known-unsafe on your main
# branch (historic WAL-pointer corruption). Use a huge threshold so values stay
# inline in the index instead of using value-log pointers.
main_flags="-treedb-allow-unsafe -treedb-relaxed-sync -treedb-disable-read-checksum -treedb-value-log-threshold 1073741824"

# On rc: run both "default threshold" (0=default inline threshold) and
# "force pointers" (threshold=1) sweeps when needed.
rc_default_threshold="-treedb-value-log-threshold 0"
rc_force_pointers="-treedb-value-log-threshold 1"

# rc wal_on (journal ON + value log ON) knobs
rc_wal_on="-treedb-allow-unsafe -treedb-relaxed-sync -treedb-disable-read-checksum"

# rc wal_off (journal OFF + value log ON) knobs
rc_wal_off="-treedb-allow-unsafe -treedb-relaxed-sync -treedb-disable-read-checksum -treedb-disable-wal"

# rc compression toggles (dict compression)
rc_comp_off="-treedb-vlog-dict-train-bytes -1"
rc_comp_on="-treedb-vlog-dict-train-bytes 1048576 -treedb-vlog-dict-sample-stride 1"

for v in $valsizes; do
  # main WAL on/off baselines
  go run ./cmd/unified_bench -dbs treedb -test "$tests" -keys $keys -valsize $v -batchsize $batch -seed $seed -format markdown -progress=false -keep $main_flags > out/head_main_wal_on_v${v}.md
  go run ./cmd/unified_bench -dbs treedb -test "$tests" -keys $keys -valsize $v -batchsize $batch -seed $seed -format markdown -progress=false -keep $main_flags -treedb-disable-wal > out/head_main_wal_off_v${v}.md

  # rc wal_on (off/on)
  go run ./cmd/unified_bench -dbs treedb -test "$tests" -keys $keys -valsize $v -batchsize $batch -seed $seed -format markdown -progress=false -keep $rc_wal_on $rc_default_threshold $rc_comp_off > out/head_rc_wal_on_off_v${v}.md
  go run ./cmd/unified_bench -dbs treedb -test "$tests" -keys $keys -valsize $v -batchsize $batch -seed $seed -format markdown -progress=false -keep $rc_wal_on $rc_default_threshold $rc_comp_on  > out/head_rc_wal_on_on_v${v}.md

  # rc wal_off (off/on)
  go run ./cmd/unified_bench -dbs treedb -test "$tests" -keys $keys -valsize $v -batchsize $batch -seed $seed -format markdown -progress=false -keep $rc_wal_off $rc_default_threshold $rc_comp_off > out/head_rc_wal_off_off_v${v}.md
  go run ./cmd/unified_bench -dbs treedb -test "$tests" -keys $keys -valsize $v -batchsize $batch -seed $seed -format markdown -progress=false -keep $rc_wal_off $rc_default_threshold $rc_comp_on  > out/head_rc_wal_off_on_v${v}.md
done

# Scan regressions (settled): run at least once per branch + config.
go run ./cmd/unified_bench -dbs treedb -test full_scan,prefix_scan -keys 100000 -valsize 128 -batchsize $batch -seed $seed -format markdown -progress=false -settle-before-scans -keep $main_flags > out/head_main_scan.md
go run ./cmd/unified_bench -dbs treedb -test full_scan,prefix_scan -keys 100000 -valsize 128 -batchsize $batch -seed $seed -format markdown -progress=false -settle-before-scans -keep $rc_wal_on $rc_default_threshold $rc_comp_off > out/head_rc_wal_on_scan.md
go run ./cmd/unified_bench -dbs treedb -test full_scan,prefix_scan -keys 100000 -valsize 128 -batchsize $batch -seed $seed -format markdown -progress=false -settle-before-scans -keep $rc_wal_off $rc_default_threshold $rc_comp_off > out/head_rc_wal_off_scan.md
```

Interpretation guidance:
- Do not interpret scan results from a “force pointers” run as a general scan regression:
  pointer-chasing scans are expected to be slower than inline leaf scans.
- For compressibility validation, prefer a real dataset (`vlog_dict_realdata -bench-kv`)
  or a synthetic workload that uses non-random values (e.g., repeat/zero patterns).

### 5.3 Autotuner benchmark suite (CI-gated)

If the wall-time autotuner exists, implement a suite similar to:

```bash
# Deterministic: validates behavioral marks and exits non-zero on failure.
go run ./TreeDB/cmd/unified_bench -suite vlog_autotune -validate

# (Optional) JSON output
go run ./TreeDB/cmd/unified_bench -suite vlog_autotune -validate -json > out/vlog_autotune.json
```

Required scenarios:
- `cpu_bound_compressible`
- `io_bound_compressible`
- `io_bound_incompressible`
- `marquee_regime_shift`

#### Marquee benchmark (normative)

Goal: demonstrate both correctness and power.

Structure (same IO regime; three segments):

1. Segment A (compressible): converge to ACTIVE (compression kept).
2. Segment B (incompressible): transition to PAUSED quickly; keep ~0.
3. Segment C (compressible again): recover to ACTIVE via bounded probes.

Validation marks (suggested initial thresholds; tune only if flaky):
- State transitions: `ACTIVE → PAUSED → ACTIVE`
- During Segment B:
  - `kept_frac <= 0.02`
  - `attempted_frac` bounded by probe policy (e.g., <= 0.10 over the segment)
- Net benefit:
  - total throughput across A+B+C is > 10% better than compression-off under the same virtual IO model

### 5.4 Go microbenchmark suite (local merge gate)

Run:

```bash
# Cached mode microbenches
go test ./TreeDB -run '^$' -bench 'Cached' -count 10 -benchtime 1s > out/bench_cached.txt

# Backend engine microbenches
go test ./TreeDB/db -run '^$' -bench '.' -count 10 -benchtime 1s > out/bench_backend.txt

benchstat out/bench_cached.txt
benchstat out/bench_backend.txt
```

Acceptance criteria:
- No unexplained geomean regression > ~10–15% versus main on the same host.
- If regressions are accepted temporarily, they must be explicitly tracked with:
  - a linked issue
  - a benchmark diff artifact
  - a plan and owner

Operational note: ensure `treedb.Open` does not print to stderr by default; otherwise `benchstat` parsing is degraded. Use `TREEDB_WRITE_PATH_LOG=1` only for explicit debugging.

---

## 6) Autonomous implementation plan (PR sequence)

### Global rules for the agent

- Work in small PRs with crisp scope and green tests.
- Every PR must include:
  - `go test ./...` output (or CI link)
  - any new benchmark output artifacts under `out/` or attached to PR
  - updated docs (when behavior changes)
- Keep a running changelog in the PR body that maps old terms to new terms.

Suggested branch naming:
- `sprint/slabopt-pr<NN>-<slug>`

### PR0 — Documentation consolidation (no behavior change)

Goal: remove confusion and clearly mark deprecated options.

Tasks:
1. Create/upgrade a canonical doc (suggested path): `docs/TREEDB_WRITE_PATHS.md` that:
   - defines WAL on/WAL off using the terminology in Section 1
   - documents durability semantics
2. Update other docs to:
   - link to the canonical doc
   - add a `DEPRECATED:` banner at the top of old slab-era docs
3. Prune unused/irrelevant files:
   - search for references to legacy slab-era guidance and either:
     - delete docs that are clearly obsolete and unreferenced, OR
     - move them to a `docs/deprecated/` folder with a banner

Validation: `go test ./... -count=1` (docs-only PR should not change behavior).

### PR1 — Correctness gates for WAL on/WAL off + Index flags

Goal: add tests so the new defaults cannot regress silently.

Tasks:
1. Add integration tests from Section 4.1.
2. Add/extend crash tests from Section 4.2.
3. Add the `IndexColumnarLeaves` integration regression test from Section 4.3.
4. Fix known test hygiene issues: any benchmark/test that selects an unsafe durability/integrity mode should do so explicitly and document why.

Validation:
```bash
go test ./... -count=1
go test ./... -race -count=1
```

### PR2 — Benchmark harness + CI validation marks

Goal: make behavior and regressions visible and reviewable.

Tasks:
1. Add `TreeDB/cmd/unified_bench -suite vlog_autotune -validate` (or equivalent) per Section 5.3.
2. Ensure benchmark output is machine-readable (JSON) and human-readable (markdown summary).
3. Add a “marks” layer (pass/fail assertions) that is deterministic.
4. Add CI wiring so marks run on every PR.

Validation:
```bash
go run ./TreeDB/cmd/unified_bench -suite vlog_autotune -validate
```

### PR3 — Defaults flip + feature flag cleanup

Goal: make WAL on the default and reduce surface area.

Tasks:
1. Ensure default profiles map cleanly:
   - `ProfileDurable` → WAL on
   - `ProfileFast` → WAL off (unsafe)
   - `ProfileWALOnFast` → WAL on + relaxed durability (unsafe)
2. Ensure the value-log autotuner default is “on” for value log enabled, but bounded (medium preset).
3. Hide noisy logs:
   - remove unconditional `fmt.Fprintf(os.Stderr, ...)` in open paths
   - provide an opt-in debug mechanism (env var or injected logger)

Validation:
- run local merge gate benchmarks from Section 5.2 and attach outputs
- run microbench suite from Section 5.4 and attach outputs

### PR4 — Remove legacy paths (optional, only after confidence)

Goal: delete legacy slab-era code and reduce maintenance burden.

Preconditions:
- WAL on and WAL off are stable
- docs are consolidated
- benchmarks show no regressions that require fallback

Tasks:
- delete any remaining slab-era code paths
- remove deprecated flags from CLI adapters
- delete deprecated docs or move to archive

---

## 7) Documentation deliverables (ready-to-merge)

### 7.1 Canonical documentation set

After the sprint, docs should present a single, low-confusion surface:

1. Write paths and durability: `docs/TREEDB_WRITE_PATHS.md`
   - WAL on (default) and WAL off (unsafe)
   - explicit explanation that the old slab/WAL path is deprecated
   - examples using `OptionsFor(Profile...)`

2. Autotuner operation: `docs/TREEDB_VALUELOG_AUTOTUNE.md`
   - what it does
   - how to enable/disable
   - what metrics/telemetry to inspect
   - safety semantics and guardrails

3. Benchmarks: `docs/benchmarks/VLOG_AUTOTUNE.md`
   - scenario definitions
   - deterministic validation marks
   - marquee benchmark

4. Bench harness usage: `cmd/unified_bench/README.md` (if present)
   - stable command lines for mode comparisons
   - guidance for adding new knobs without breaking comparability

### 7.2 Deprecation UX

- Deprecated flags and docs must be obvious:
  - top-of-doc `DEPRECATED:` banner
  - command-line help indicates deprecation
  - recommend the new replacement in the same location

### 7.3 File pruning checklist (autonomous)

1. Enumerate docs and runbooks:

```bash
find docs -type f -maxdepth 3 | sort > out/docs_files.txt
```

2. Find duplicate/outdated content:

```bash
grep -RIn --line-number "legacy slab\|old slab\|slab WAL\|write twice" docs | tee out/docs_mode_mentions.txt
```

3. For each file flagged:
   - if superseded and unreferenced: delete
   - if historical value: move to `docs/deprecated/` and add a banner
   - update backlinks so only the canonical docs are linked from README and other entry points

---

## 8) Final merge checklist (single page)

### 8.1 Must-pass checks

- [ ] `go test ./...` passes
- [ ] `go test ./... -race` passes
- [ ] Crash recovery coverage covers WAL on and WAL off semantics
- [ ] `IndexColumnarLeaves` and other new index/key flags pass integration reopen+scan tests
- [ ] Autotuner validation marks suite (deterministic) passes in CI (if autotuner is present)
- [ ] Local `unified_bench` mode comparisons are attached to PR and meet acceptance criteria
- [ ] Microbench runs are attached (`benchstat` outputs)
- [ ] Docs consolidated with clear deprecation banners and a single recommended path

### 8.2 Reviewer's “red flags”

Reject or require changes if:

- defaults still reference legacy slab-era knobs
- user-facing docs recommend unsafe durability/integrity without making it explicit
- any flag is promoted by default without passing the correctness gate in Section 3.3
- benchmark artifacts are missing for performance-impacting changes

---

## Appendix A: Suggested `unified_bench` command matrix

Keep these commands stable so results remain comparable across branches.

```bash
# Large-value ingest (TreeDB cached)
go run ./cmd/unified_bench -dbs treedb -test random_write,dataset_write_random   -keys 20000 -valsize 16384 -batchsize 1000 -seed 1 -format markdown -progress=false

# Backend scans
go run ./cmd/unified_bench -dbs treedb -test full_scan,prefix_scan   -keys 100000 -valsize 128 -seed 1 -settle-before-scans -format markdown -progress=false
```

## Appendix B: Benchmark artifact conventions

- Write benchmark outputs to `out/` and commit them only if your repo policy allows.
- Otherwise, attach them to PRs and store them in CI artifacts.
- Prefer:
  - markdown tables for humans
  - JSON for machines
