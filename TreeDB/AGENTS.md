# TreeDB Wall-Time ValueLog Compression Autotuner — Spec + Autonomous Implementation Runbook
Status: **Design spec + run sheet** (intended to be fully implementable by an autonomous coding agent)
Updated: `2026-01-22`

> This runbook defines a production-grade online autotuner for TreeDB’s ValueLog write path. It is written to be executed without ambiguity by a coding bot.

---

## 0) Purpose and non-goals

### Purpose
Implement a **production-grade, online, wall-time autotuner** for TreeDB’s **ValueLog write-to-disk** path that continuously chooses:

- Compression **on/off**
- Grouped-frame batch size `k` (records per frame)
- Dictionary training parameters (training history size / dict size)
- Dictionary rotation cadence (when to retrain / publish)

The tuner’s objective is **sustained ingest throughput in actual wall time** *as experienced under the currently active durability policy* (buffered/flush/sync). Throughput is defined as:

```

throughput = raw_bytes / wall_time_ns

```

Where:
- `raw_bytes` = bytes of the input value payloads before compression
- `wall_time_ns` = elapsed monotonic time observed in the write path boundary that includes durability actions actually performed

### Explicit non-goals (hard scope boundary)
- Memtable / skiplist compression.
- Real-device SSD benchmarking in unit tests (tests MUST be deterministic).
- On-disk format changes beyond strictly necessary instrumentation fields and safe flags.
- Read-path optimization (may be a future extension, but not required here).

### Why “wall time” is non-trivial
Buffered IO hides work until flush/sync. A wall-time tuner must:
- measure time in the same boundary the application experiences,
- attribute costs correctly when work is delayed or amortized,
- avoid destabilizing the system by over-training or thrashing configs.

---

## 1) Glossary and invariants (normative)

### ValueLog “frame”
A grouped frame stores `k` records and includes:
- header + RID array + offsets array + payload bytes
- payload may be **raw** or **dict-compressed** (zstd with dict)

### “Attempted” vs “Kept” (MUST)
For tuning and correctness, we MUST distinguish:

- **Attempted**: zstd encoding was executed for this frame (even if we later fall back to raw)
- **Kept**: compressed bytes were stored (i.e., we kept the encoding result)

This distinction MUST be reflected in:
- runtime stats
- logs/metrics
- benchmarks
- tests

### “Durability mode”
The effective write mode is defined by actual behavior:
- whether the write path calls `Flush()`
- whether it calls `Sync()` (fsync), and how often
- whether WAL+ValueLog, ValueLog-only, etc. are used

The autotuner MUST optimize the mode that is actually being used.

### Safety invariants (MUST)
- Missing dict bytes ⇒ **fail closed** (fallback to raw), never corrupt.
- Invalid dict ⇒ **reject** and keep prior dict.
- Any decode/parsing MUST cap lengths before allocation.
- Database must remain readable and consistent after crashes/restarts.

---

## 2) Product requirements (behavioral, normative)

### R1: Throughput objective
Optimize raw ingest throughput:

```

throughput = raw_bytes / wall_time_ns

````

### R2: Online adaptation
The tuner MUST adapt to workload changes:
- compressible → incompressible (disable quickly; avoid wasting CPU)
- incompressible → compressible (recover via probes)
- shifts in value size distribution (affects optimal `k` and dict utility)

### R3: Bounded overhead
The tuner MUST enforce hard budgets:
- max CPU spent training dictionaries
- max RAM for sample reservoir
- max switching rate (anti-thrash)

### R4: Fail-closed behavior
Any uncertainty or missing dependency MUST degrade to safe behavior:
- store raw
- keep last known-good dict+config
- never crash or corrupt on malformed frames

---

## 3) API surface (public options)

### 3.1 Public options (TreeDB/db.Options)
Add a new option group:

```go
// ValueLogCompressionAutotune configures the wall-time value-log compression autotuner.
// Cached mode only (SplitValueLog must be enabled).
ValueLogCompressionAutotune valuelog.AutotuneOptions
````

### 3.2 AutotuneOptions (TreeDB/internal/valuelog)

Normative type:

```go
package valuelog

type AutotuneMode uint8
const (
  AutotuneOff AutotuneMode = iota
  AutotuneMedium
  AutotuneAggressive
)

type AutotuneOptions struct {
  Mode AutotuneMode

  // Candidate search space (defaults if empty):
  CandidateK []int            // default: [1,2,4,8,16,32]
  CandidateHistoryBytes []int // default: [16KiB,32KiB,40KiB]
  CandidateDictBytes []int    // default: [40KiB] (fixed unless explicitly enabled)

  // Switching hysteresis / anti-thrash:
  MinGainToSwitch float64     // default: 0.05 (5%)
  MinDwellFrames uint64       // default: 1<<16

  // Sampling/training budgets:
  SampleStride uint64         // default: 4 (sample 1/4 records)
  MaxSampleBytes uint64       // default: 8MiB (Medium), 32MiB (Aggressive)
  TrainCPUFraction float64    // default: 0.02 (2% of a core)

  // Probing/backoff:
  ProbeBytes uint64           // default: 16MiB
  PauseBytes uint64           // default: 64MiB

  // Value-size gating (optional, default 0 = disabled):
  DisableBelowValueBytes int  // e.g. 64: don’t compress tiny values
}
```

### 3.3 Default behavior (normative)

* Default `Mode = AutotuneMedium` when `SplitValueLog` is enabled (cached mode).
* Default sampling/training MUST be bounded (no surprise CPU spikes).
* User can explicitly set `Mode = AutotuneOff`.

---

## 4) Runtime instrumentation (must-have)

### 4.1 Extend valuelog.FrameStats (normative)

In `TreeDB/internal/valuelog/valuelog.go`:

```go
type FrameStats struct {
  Records            int
  RawPayloadBytes    int
  StoredPayloadBytes int

  // Semantics:
  // Attempted: zstd encoding executed for this frame (even if fallback to raw).
  // Kept: compressed bytes stored.
  Attempted bool
  Kept      bool

  // Timing (monotonic ns). 0 when not measured (sampling).
  EncodeNs int64
}
```

Compatibility:

* Keep `Compressed` only if needed temporarily; long-term rename to `Kept`.
* Update all call sites and benches.

### 4.2 Writer must report Attempted even on fallback (MUST)

In `TreeDB/internal/valuelog/writer.go`, any path that:

* runs zstd encoding,
* then decides “no benefit” and falls back to raw,

MUST return `Attempted=true`, `Kept=false` and include `EncodeNs` if measured.

This is REQUIRED so the autotuner can price wasted CPU correctly.

### 4.3 Encode timing MUST be sampleable (MUST)

Timing every frame is expensive. Implement:

* `EncodeNs` measured only when a sampling gate says to measure this frame.
* Sampling gate MUST be configurable via `AutotuneOptions`.

Normative mechanism:

* Add `valuelog.Clock` and inject it (writer and/or caching DB):

```go
type Clock interface { Now() time.Time }
type realClock struct{}
func (realClock) Now() time.Time { return time.Now() }
```

Measurements MUST use monotonic time (`t2.Sub(t1)`).

---

## 5) Wall-time accounting model (IO + fsync + buffering)

### 5.1 What we measure (minimum viable model)

Maintain EWMAs for:

* `encode_ns_per_raw_byte` (from `EncodeNs` samples)
* `io_ns_per_stored_byte` (from write/flush/sync time, amortized)

### 5.2 Where to measure wall time (normative)

In cached mode, measure at the DB append boundary:

* `TreeDB/caching/db.go` functions:

  * `appendValueLogBatch` (batch)
  * `appendValueLogOne` (single)

For each append operation measure:

* elapsed wall ns including durability call (Flush/Sync) if invoked
* bytes:

  * `rawPayloadBytes`
  * `storedPayloadBytes` (from frame stats; if missing, fall back to total bytes)

### 5.3 Amortization and buffering

* If durability is “none” and no explicit flush happens, wall time may exclude kernel flush. That is correct **by definition** for this tuner.
* If durability calls `Flush()` / `Sync()`, include that time and attribute it.

Implementation guidance:

* Time the call sites in caching DB (not per-frame inside writer).
* Initially treat framing/CRC time as part of IO term (simpler). Split later only if needed.

---

## 6) Decision logic (throughput-first, not ratio-first)

### 6.1 Core inequality (“keep compressed or not?”) — normative

For a frame, decide to keep compressed based on observed costs.

Let:

* `E = encode_ns` (measured or predicted)
* `B = io_ns_per_stored_byte` (EWMA)
* `raw = raw_payload_bytes`
* `enc = encoded_payload_bytes`

Estimated savings in IO wall time:

* `(raw - enc) * B`

Keep compressed if:

```
(raw - enc) * B > E * (1 + safety_margin)
```

Normative defaults:

* `safety_margin = 0.10` for Medium
* `safety_margin = 0.02` for Aggressive

Additional guard (MUST):

* If `enc >= raw` then never keep compressed (avoid size amplification).

### 6.2 Global config selection (k + dict history bytes)

Choose configuration `C = (dict, k)` that maximizes predicted throughput:

```
score(C) = raw_bytes / ( encode_cost(C) + io_cost(C) )
```

Where:

* `encode_cost(C)` predicted via:

  * EWMA `encode_ns_per_raw_byte` scaled for k/dict choice, OR
  * bounded sample evaluation during training
* `io_cost(C) = stored_bytes(C) * io_ns_per_stored_byte`

The tuner MUST:

* compare against “compression off”
* only switch if predicted gain >= `MinGainToSwitch`
* enforce `MinDwellFrames` between switches

---

## 7) Autotuner state machine (normative)

### 7.1 States

* `OFF`: never attempt compression; metrics optional.
* `WARMUP`: attempt compression frequently to learn costs quickly.
* `ACTIVE`: tuned compression enabled; periodic probes/training.
* `PAUSED`: compression disabled due to poor net benefit; periodic probes.

### 7.2 Transitions

* `OFF → WARMUP`: when `Mode != Off`
* `WARMUP → ACTIVE`: once we have:

  * `min_samples` in reservoir, AND
  * `min_encode_samples`, AND
  * initial IO EWMA stabilized
* `ACTIVE → PAUSED`: sustained degradation or inequality fails broadly
* `PAUSED → ACTIVE`: probes succeed for a window (inequality holds and Kept occurs)

Normative defaults:

* `min_samples = 256`
* `min_encode_samples = 64`
* `probe success window = 8 kept frames`

---

## 8) Sampling & training (bounded, async)

### 8.1 Reservoir (bounded)

Maintain bounded reservoir of recent values for dict training:

* total stored ≤ `MaxSampleBytes`
* record cap ≤ existing trainer max-record limit
* sample stride `SampleStride`

### 8.2 Training cadence (bounded)

Train only when:

* no active dict, OR
* ACTIVE and drift detected, OR
* PAUSED and probes suggest compressibility returned

Training MUST respect `TrainCPUFraction` via a simple time budget:

* track `train_cpu_ns_spent` per wall-time window and throttle accordingly.

### 8.3 Candidate evaluation (normative tie-breaks)

For each `historyBytes` in `CandidateHistoryBytes`:

1. build dict from reservoir history
2. for each `k` in `CandidateK`:

   * estimate expected ratio + encode cost using bounded sample frames
3. compute predicted throughput using current IO EWMA
4. select best within `ratio_slack` to avoid spending CPU for tiny ratio gains

Normative ratio slack:

* `ratio_slack = 0.01` (1%)

Tie-break order:

1. best predicted throughput
2. lower encode cost
3. smaller history bytes

---

## 9) Concurrency & publish semantics

### 9.1 No write-path locks (MUST)

Write critical path MUST NOT block on training work.

Write path responsibilities:

* stride-gated sampling
* cheap counters/EWMAs updates
* per-frame keep decision (constant time)

Training happens async.

### 9.2 Publish ordering (MUST)

Publishing a new dict must obey:

1. Dict bytes MUST be durable before any frame references it.
2. Update “current dict ID” atomically for writers.
3. Update `k` atomically and consistently with dict.

Implementation rule:

* Store dict bytes → SetCurrent(dictID) → update cached current dict pointer in memory.
* Never invert ordering.

---

## 10) Observability (must exist)

Expose (log and/or metrics) the active tuning state:

* `autotune_mode` (off/medium/aggressive)
* `state` (warmup/active/paused)
* `dict_id`, `dict_hash`, `history_bytes`, `dict_bytes`
* `k`
* EWMAs:

  * `encode_ns_per_raw_byte`
  * `io_ns_per_stored_byte`
  * `observed_ratio`
  * `throughput_raw_MBps`
* probe stats:

  * `attempted_frac`, `kept_frac`
* switching events including:

  * old/new config
  * predicted gain
  * reason code

Normative: observability MUST be sufficient to answer:

* “Why did the tuner disable compression?”
* “Why did it pick this k?”
* “Is training thrashing or bounded?”

---

## 11) Testing strategy (expanded, normative)

This section is intentionally explicit: it is the contract for “significant thorough test coverage”.

### 11.1 Test pyramid (required)

Implement tests at multiple levels:

1. **Unit tests**: decision inequality, EWMAs, state machine transitions, sampling bounds.
2. **Component tests**: dict publish ordering, missing dict fallback behavior, trainer budget enforcement.
3. **Integration tests**: caching DB + writer + autotuner under deterministic virtual wall time.
4. **Fuzz tests**: decode/parse surfaces; dict lookup + corrupted inputs; must not panic.
5. **Race tests**: core packages must be race-clean under `-race`.

### 11.2 Deterministic time + deterministic IO (required)

Tests MUST be deterministic and must not sleep.

Implement:

* `VirtualClock`:

  * `Now() time.Time`
  * `Advance(ns int64)` (test-only)
* `VirtualSink`:

  * `Write(p []byte) (int, error)` that calls `clock.Advance(int64(len(p))*nsPerByte)`
  * optional `Sync()` that adds `syncPenaltyNs`

Wire the autotuner measurement boundary to use injected Clock in tests.

### 11.3 Deterministic encode-cost modeling for tests (required)

To avoid non-determinism from real compression CPU cost in tests:

* Provide a test hook or injectable codec that can:

  * set `FrameStats.EncodeNs` deterministically (e.g., `encodeNsPerRawByte * rawBytes`)
  * optionally simulate encoded size outcomes for compressible/incompressible workloads

Normative requirement:

* The core autotuner logic MUST be testable without relying on real-time zstd performance.

Implementation options (choose one; MUST be consistent repo-wide):

* A `valuelog.Codec` interface with real zstd implementation + test fake.
* A `writer` hook that accepts a `EncodeCostModel` used only in tests.
* A “scenario runner” that bypasses actual zstd and directly feeds FrameStats + wall times into the tuner.

### 11.4 Required unit tests (table-driven)

Create table-driven unit tests for:

A) **Keep inequality correctness**

* enc >= raw ⇒ keep=false (always)
* when `(raw-enc)*B` slightly below threshold ⇒ keep=false
* when slightly above threshold ⇒ keep=true
* safety margin sensitivity (Medium vs Aggressive)

B) **Attempted vs Kept invariants**

* Attempted implies encoder executed
* Kept implies Attempted (Kept cannot be true if Attempted is false)
* In incompressible scenarios with probing: Attempted_frac > Kept_frac

C) **EWMA update correctness**

* stable value converges
* step function converges within expected half-life
* handles 0 and tiny sizes without NaN/Inf
* clamps outliers (if outlier clamp is implemented)

D) **State machine transitions**

* WARMUP → ACTIVE only after min samples/encode samples
* ACTIVE → PAUSED after sustained failures (bounded attempts)
* PAUSED → ACTIVE after probe success window
* MinDwellFrames enforced (no thrash)

E) **Budget enforcement**

* sample reservoir never exceeds `MaxSampleBytes`
* training CPU fraction budget throttles training (bounded CPU)

### 11.5 Required integration tests (virtual wall time)

Add deterministic integration tests asserting convergence:

1. **CPU-bound regime** (very cheap IO)

* expected: compression rarely kept; state tends to PAUSED or k minimized

2. **IO-bound regime** (expensive IO)

* expected: compression kept; state ACTIVE; dict trained; k increases to stable value

3. **Workload shift regime** (marquee correctness test)

* compressible → incompressible:

  * transitions to PAUSED quickly
  * bounded attempt rate
* incompressible → compressible:

  * returns to ACTIVE via probes

All integration tests MUST validate:

* MinDwellFrames prevents thrash
* budgets are enforced
* dict publish ordering is never violated
* no sleeps; purely virtual time

### 11.6 Required fuzz tests (Go fuzzing)

Add fuzz tests for:

* frame decode (random bytes) must not panic; must reject safely
* dict lookup with missing/corrupt dict bytes must fail closed, not crash

Normative: fuzz targets must be seeded with:

* a valid encoded frame
* a valid raw frame
* a truncated frame
* a frame with extreme length prefixes (should be capped and rejected safely)

### 11.7 Race + coverage gates

Minimum gating requirements for CI (normative defaults):

* `go test ./... -count=1` must pass
* `go test ./... -race` must pass at least for packages:

  * `TreeDB/caching`
  * `TreeDB/internal/valuelog`
  * `TreeDB/internal/compression`
* Coverage:

  * New autotuner packages/files MUST be ≥ 85% statement coverage
  * The marquee scenario test MUST cover the main state-machine paths

---

## 12) Benchmarks and benchmark validation (expanded)

This section defines the **benchmark suite**, the **validation marks**, and the **marquee benchmark**.

### 12.1 Benchmark categories (required)

Maintain three benchmark tiers:

1. **Microbenchmarks (Go `testing.B`)**

* Purpose: regressions in allocations and CPU for encode/decode and decision logic.
* Stable, low variance; runs fast.

2. **Deterministic simulated wall-time benchmark suite (CI-viable)**

* Purpose: validate *behavioral performance* in CPU-bound vs IO-bound regimes without relying on real disks or CPU timing.
* Uses the same VirtualClock/VirtualSink and deterministic encode-cost modeling.

3. **Real-data / end-to-end (human validation)**

* Purpose: demonstrate real-world effect using actual zstd + dict training.
* Not strictly CI-gated due to environment variance.
* Existing tool `cmd/vlog_dict_realdata` is a baseline; extend or complement if needed.

### 12.2 Standard workloads (required set)

Bench suite MUST include at least these workloads (synthetic, deterministic):

* `highly_compressible_tail64` (values share long common prefix with small random tail)
* `medium_compressible_sparse` (structured base with sparse noise)
* `incompressible` (random)

Each workload MUST be parameterized by:

* value size: at least `1KiB` and `16KiB`
* dataset size: enough frames to stabilize EWMAs and observe state transitions

### 12.3 Modes to compare (required)

Every macro benchmark scenario MUST compare the following modes:

* `off`: never attempt compression
* `no_dict_fixed`: zstd without dict, fixed `k` (e.g., 4)
* `dict_fixed`: zstd with dict, fixed `k` (e.g., 4), dict trained once
* `autotune`: full autotuner (dict + k + keep decision)

### 12.4 Metrics to report (required)

Every benchmark MUST report, per scenario + mode:

* `raw_bytes`
* `stored_bytes`
* `wall_time_ns` (simulated wall time for deterministic tier)
* `throughput_raw_MBps`
* `attempted_frac`
* `kept_frac`
* `observed_ratio = stored_bytes/raw_bytes`
* chosen config fields:

  * `k`, `dict_id`, `history_bytes`

### 12.5 Benchmark validation “marks” (required)

In addition to printing metrics, the deterministic suite MUST compute pass/fail “marks”
that validate the benchmark is meaningful and the tuner behaves correctly.

**Sanity marks (always required):**

* `0 <= kept_frac <= attempted_frac <= 1`
* `stored_bytes <= raw_bytes` whenever Kept is true (size guard respected)
* Reservoir stays ≤ MaxSampleBytes
* No config thrash: switch count ≤ expected bound under MinDwellFrames
* No publish ordering violation (dict durable before referenced)

**Behavioral marks (required thresholds; tuneable but default values are normative):**

A) CPU-bound compressible:

* `autotune.throughput >= off.throughput * 0.95`  (no big regression)
* `autotune.kept_frac <= 0.10` (should mostly disable)

B) IO-bound compressible:

* `autotune.throughput >= off.throughput * 1.15` (meaningful improvement)
* `autotune.kept_frac >= 0.50` (should keep often)
* `autotune.state == ACTIVE` at end

C) IO-bound incompressible:

* `autotune.kept_frac <= 0.02` (should basically never keep)
* `autotune.attempted_frac <= 0.10` after warmup (bounded probe rate)

### 12.6 The marquee benchmark (required)

This is the “showpiece” that demonstrates the correctness and power of the autotuner.

#### Name

`vlog_autotune/marquee_regime_shift`

#### Goal

Demonstrate the autotuner can:

* enable compression when IO-bound and data is compressible,
* disable compression when data becomes incompressible or CPU-bound,
* recover when compressibility returns,
* do so without thrashing and within CPU/memory budgets.

#### Structure (normative)

Run **three segments** back-to-back in one scenario, using deterministic virtual wall-time:

1. Segment A (IO-bound + compressible):

* workload: `highly_compressible_tail64`
* IO cost: high `io_ns_per_stored_byte`
* expected: ACTIVE, high kept_frac, dict trained, k increases

2. Segment B (IO-bound + incompressible):

* workload: `incompressible`
* IO cost: still high
* expected: transitions to PAUSED quickly, attempted_frac low after transition

3. Segment C (IO-bound + compressible returns):

* workload: `highly_compressible_tail64` again
* expected: returns to ACTIVE via probes, kept_frac rises again

#### Required marquee marks (normative)

* By end of Segment A:

  * state == ACTIVE
  * kept_frac >= 0.50
  * dict_id != 0 (or equivalent “dict present”)
* By end of Segment B:

  * state == PAUSED
  * kept_frac <= 0.02
  * attempted_frac bounded (<= 0.10 after warmup window)
* By end of Segment C:

  * state == ACTIVE
  * kept_frac >= 0.30

Performance mark:

* `autotune.total_throughput >= off.total_throughput * 1.10`
  (overall run shows net benefit across the whole scenario)

#### Output requirements

The marquee benchmark MUST print a compact summary, suitable for PR descriptions:

* chosen configs at end of each segment
* state transitions with reasons
* per-segment and overall throughput + attempted/kept fractions
* PASS/FAIL for each mark

### 12.7 How to run benchmarks (normative)

Local developer commands (examples; adapt to repo harness):

* Microbench:

  * `go test ./TreeDB/internal/valuelog -bench . -benchmem -run ^$`
* Deterministic suite:

  * `go run ./cmd/unified_bench -suite vlog_autotune -case marquee_regime_shift -validate`
* Real data (human validation):

  * `go run ./cmd/vlog_dict_realdata -input <dataset.jsonl> -train 200000 -eval 50000 -dict-bytes 16384,32768,40960 -k 1,2,4,8,16,32`

Normative: deterministic suite MUST support:

* JSON output (for CI parsing)
* a `-validate` flag that exits non-zero on mark failure

---

## 13) Autonomous implementation PR plan (strict, sequential, expanded)

This section is a “run sheet” for an autonomous agent.

### Global rules for the agent (normative)

* Create one branch per PR: `sprint/slabopt-pr<N>-<slug>`
* Use `rg -n` before edits.
* For each PR:

  * run listed tests/benches
  * write PR body to `.pr/PR<N>_description.md`
  * include before/after benchmark output in PR body
  * open PR via `gh pr create`

> If the repo uses different PR numbering, adapt numbering but keep stage ordering.

---

## PR-AT0 — Spec + bench hygiene (no behavior change)

Goal:

* Land this runbook.
* Audit existing benches to ensure attempted/kept semantics are not misleading.

Tests:

* `go test ./... -count=1`

---

## PR-AT1 — Fix FrameStats semantics + encode timing plumbing

Goal:

* Attempted means “zstd executed” even on fallback.
* Kept reflects “stored compressed”.
* Add sampled EncodeNs.

Files (normative):

* `TreeDB/internal/valuelog/valuelog.go`
* `TreeDB/internal/valuelog/writer.go`
* Update call sites:

  * `TreeDB/caching/db.go`
  * all ValueLog benches

Acceptance criteria:

* On incompressible data, benchmark output shows `attempted_frac != kept_frac`.
* `go test ./...` passes.

---

## PR-AT2 — Wall-time accounting + EWMAs (no switching yet)

Goal:

* Measure wall time in append boundary.
* Maintain EWMAs:

  * encode_ns_per_raw_byte
  * io_ns_per_stored_byte
  * throughput
* No behavior changes (pure measurement).

Files (normative):

* `TreeDB/caching/db.go`
* New: `TreeDB/caching/vlog_autotune_metrics.go`

Acceptance criteria:

* Metrics can be logged under env var gate.
* Minimal overhead (sampling gated).
* `go test ./...` passes.

---

## PR-AT3 — Throughput-aware per-frame keep decision + unit tests

Goal:

* Replace “keep if encoded smaller” with throughput inequality.
* Add full unit-test coverage for the inequality and invariants.

Files (normative):

* `TreeDB/internal/valuelog/writer.go` (or keep decision helper)
* New: `TreeDB/internal/valuelog/autotune_keep_test.go`

Tests (normative):

* `go test ./TreeDB/internal/valuelog -count=1`
* `go test ./... -count=1`

Acceptance criteria:

* Unit tests cover edge conditions and safety margin variants.
* CPU-bound simulated inputs lead to keep=false often; IO-bound simulated inputs keep=true when beneficial.

---

## PR-AT4 — Candidate tuning: choose K + history bytes by predicted throughput

Goal:

* Implement full tuning loop:

  * dict candidates
  * choose k
  * choose history bytes
  * publish dict+k with hysteresis + dwell

Files (normative):

* `TreeDB/caching/vlog_dict.go` (or new `vlog_autotune.go`)
* `TreeDB/internal/compression/trainer.go`
* `TreeDB/internal/compression/profile.go`

Acceptance criteria:

* Stable compressible workload: converges; rare switching.
* Incompressible: transitions to PAUSED; training throttled.

---

## PR-AT5 — Deterministic virtual wall-time integration tests + fuzz + race gates

Goal:

* Add deterministic integration tests for CPU-bound/IO-bound/shift regimes.
* Add fuzz targets for decode/parse.
* Ensure key packages are race-clean.

Files (normative):

* New: `TreeDB/internal/valuelog/autotune_virtual_time_test.go`
* New: `TreeDB/caching/vlog_autotune_integration_test.go`
* New: `TreeDB/internal/valuelog/fuzz_decode_test.go` (or equivalent)

Tests (normative):

* `go test ./... -count=1`
* `go test ./... -race` (or scoped packages)
* `go test ./... -run Fuzz -fuzztime 5s` (optional in CI; required locally)

Acceptance criteria:

* No sleeps; purely virtual time.
* Tests are fast (<5s each).
* No races in core packages.

---

## PR-AT6 — Benchmark suites + validation marks + marquee benchmark (CI-viable)

Goal:

* Add benchmark suite with deterministic validation marks.
* Add the marquee benchmark `marquee_regime_shift`.
* Ensure suite can be run in CI and fails on mark failure.

Files (normative):

* `cmd/unified_bench/` add suite: `vlog_autotune`
* `TreeDB/internal/valuelog/*bench*` microbench additions
* New: `TreeDB/internal/valuelog/autotune_scenarios.go` (shared runner)

Acceptance criteria:

* Suite prints:

  * chosen mode/k/history
  * throughput
  * attempted vs kept fractions
  * wall time
  * PASS/FAIL marks
* `-validate` exits non-zero on any mark failure.
* Marquee benchmark demonstrates:

  * ACTIVE → PAUSED → ACTIVE across segments
  * net throughput benefit > 10% vs off in marquee scenario

---

## 14) Definition of done + rollout plan

Done means:

1. Safe-by-default: enable without surprise CPU spikes (budgets enforced).
2. Bounded overhead on incompressible streams (attempts bounded; PAUSED reachable quickly).
3. Stable convergence on compressible streams (k/history settle; dwell prevents thrash).
4. Deterministic tests cover CPU-bound, IO-bound, and regime shift.
5. Deterministic benchmark suite includes validation marks and passes in CI.
6. Marquee benchmark exists and demonstrates correctness + power clearly.

Rollout (recommended):

* Start behind option flag (Mode=Off by default), then enable Medium in canary.
* Monitor:

  * attempted/kept fractions
  * throughput
  * CPU utilization
  * dict rotation rate
* Provide a single “kill switch” (env var or option) that forces Off.

---
