# Value Log Autotune Benchmarks and Validation Marks

This document defines the benchmark suite for TreeDB’s **value log compression autotuner**, including:
- benchmark scenarios (“benchmarks”)
- correctness and behavior assertions (“validation marks”)
- a **marquee benchmark** that demonstrates correctness *and* the autotuner’s power
- reproducible commands for local runs and CI

The benchmark suite is designed to validate *behavior* (e.g., converging to the right state under changing regimes), not just raw throughput.

---

## Goals

The benchmark suite must:

1. **Be deterministic and CI-friendly**
   - No sleeps.
   - No reliance on machine-specific wall clock measurements for pass/fail.
   - Prefer simulated (“virtual”) wall-time with fixed parameters.

2. **Prove correctness and safety**
   - The system never keeps compression when it expands bytes.
   - Attempt/keep fractions remain bounded in the PAUSED regime.
   - State transitions follow the expected rules.

3. **Demonstrate performance wins**
   - In IO-bound regimes with compressible data, the autotuner should outperform compression-off by a clear margin.

---

## Running the suite

### Deterministic CI-grade run

The primary suite entry point is:

- `cmd/unified_bench` suite: `vlog_autotune`

Example:

```bash
go run ./cmd/unified_bench \
  -suite vlog_autotune \
  -validate \
  -out out/vlog_autotune.json
```

Expected behavior:
- The command prints scenario results plus PASS/FAIL marks.
- `-validate` exits **non-zero** if any mark fails.

### Local development (fast loop)

```bash
go run ./cmd/unified_bench -suite vlog_autotune -validate
```

### Optional: real-wall-time exploratory run (not CI-gated)

You may additionally run non-deterministic real-wall-time benchmarks (for exploratory tuning only). These should **never** be used as hard CI gates.

For Go microbenchmarks (example):

```bash
go test ./TreeDB/internal/valuelog -run '^$' -bench 'Autotune' -count 10 -benchtime 2s > /tmp/vlog_autotune.txt
benchstat /tmp/vlog_autotune.txt
```

---

## Benchmark design

### Deterministic “virtual wall time”

To avoid hardware variance, scenarios should use:
- a deterministic clock (virtual time)
- a deterministic sink that charges a fixed “IO ns per byte” cost
- a deterministic “encode ns per raw byte” cost model (or measured, but normalized)

This allows a scenario to assert:
- “CPU-bound” vs “IO-bound”
- expected state transitions
- expected attempt/keep fractions
- expected *relative* throughput ordering

### Common reporting fields

Each scenario must report at least:

- `raw_bytes`
- `stored_bytes`
- `frames_total`
- `attempted_frac`
- `kept_frac`
- `wall_time_ns` (virtual wall time)
- derived:
  - `throughput_raw_MBps = raw_bytes / wall_time`
  - `ratio = stored_bytes / raw_bytes`
- chosen config snapshot:
  - `state`
  - `k`
  - `history_bytes`
  - `dict_bytes`
  - `dict_id` (if applicable)

---

## Scenarios

The suite must include at least the following scenarios.

### 1) CPU-bound, compressible

**Name:** `cpu_bound_compressible`

- Data: highly compressible payloads (e.g., repeated patterns).
- Regime: low IO cost per byte, high encode cost per byte.
- Expected:
  - Compression is rarely kept (even if ratio improves).
  - Throughput is close to `off`.

### 2) IO-bound, compressible

**Name:** `io_bound_compressible`

- Data: highly compressible payloads.
- Regime: high IO cost per byte, moderate encode cost.
- Expected:
  - Compression is kept frequently.
  - Throughput improves versus `off`.

### 3) IO-bound, incompressible

**Name:** `io_bound_incompressible`

- Data: high-entropy / incompressible payloads.
- Regime: high IO cost per byte (still), but compression provides little/no savings.
- Expected:
  - The tuner transitions to PAUSED quickly.
  - Attempt rate is bounded (probe/backoff).
  - Keep rate ~ 0.

### 4) Marquee benchmark: regime shift recovery

**Name:** `marquee_regime_shift`

This benchmark must clearly demonstrate both:
- **correctness** (safe transitions, bounded wasted work)
- **power** (net throughput win and recovery after regime change)

#### Structure (normative)

Three segments, same IO regime:

1. **Segment A (compressible)**: warm up and converge to ACTIVE.
2. **Segment B (incompressible)**: tuner must observe no benefit and transition to PAUSED.
3. **Segment C (compressible again)**: tuner must detect via probes and return to ACTIVE.

#### Expected high-level outcome

- State transitions:
  - `ACTIVE → PAUSED → ACTIVE`
- Net throughput benefit:
  - total throughput across A+B+C exceeds `off` by **> 10%** under the scenario’s IO cost model
- Safety:
  - During segment B, `attempted_frac` is bounded (not a runaway CPU burn)
  - `kept_frac` in segment B remains near zero

---

## Validation marks (pass/fail)

Marks are deterministic assertions checked by `-validate`.

### Global invariants (all scenarios)

These must always hold:

1. **Bounds**
   - `0 ≤ kept_frac ≤ attempted_frac ≤ 1`

2. **No expansion kept**
   - For all frames: if `encoded_bytes >= raw_bytes` then `kept=false`

3. **Monotonic accounting**
   - `raw_bytes ≥ stored_bytes` when `kept_frac > 0` (aggregated; tolerances allowed for headers/metadata)

4. **No thrash**
   - Config changes are limited by `MinDwellFrames`
   - In steady regimes, switching count stays below a small threshold (scenario-defined)

5. **Probe/backoff correctness**
   - In PAUSED, attempts occur at most once per `ProbeBytes`
   - After a failed probe, pause lasts `PauseBytes`

### Scenario-specific marks (normative thresholds)

The thresholds below are intended to be robust across small refactors, while still catching behavior regressions.

#### A) CPU-bound compressible

- **Throughput**: `autotune_throughput ≥ 0.95 * off_throughput`
- **Keep rate**: `kept_frac ≤ 0.10` (after warmup window)

#### B) IO-bound compressible

- **Throughput**: `autotune_throughput ≥ 1.15 * off_throughput`
- **Keep rate**: `kept_frac ≥ 0.50` (after warmup)
- **State**: end state must be `ACTIVE`

#### C) IO-bound incompressible

- **Keep rate**: `kept_frac ≤ 0.02`
- **Attempt rate**: `attempted_frac ≤ 0.10` after warmup (bounded probes)
- **State**: end state must be `PAUSED`

#### D) Marquee regime shift

- **State transitions**: must observe `ACTIVE → PAUSED → ACTIVE` at segment boundaries (allowing a small stabilization window).
- **Net throughput**: total throughput across segments exceeds `off` by **> 10%**
- **Segment B bounded attempts**: attempted fraction in segment B is bounded (e.g., `≤ 0.10` after initial detection window)
- **Segment C recovery**: kept fraction increases materially in segment C (e.g., `≥ 0.50` after recovery window)

---

## Microbenchmarks (Go `testing.B`)

In addition to the unified suite, keep a small set of Go microbenchmarks under `TreeDB/internal/valuelog` to measure hot-path costs:

- candidate evaluation (no IO)
- dictionary codec cache behavior
- frame encode/decode throughput

These are not CI-gated for pass/fail, but they are useful for regression tracking via `benchstat`.

---

## Real-data harness

For non-CI, exploratory evaluation on real payload distributions, TreeDB includes a harness:

- `cmd/vlog_dict_realdata`

This tool reads newline-delimited records, trains dictionaries, and prints:
- chosen dict properties (`dict_id`, `history_bytes`, `k`, etc.)
- achieved ratio
- predicted throughput ordering

Example:

```bash
go run ./cmd/vlog_dict_realdata \
  -input /path/to/values.bin \
  -cap_bytes $((32<<20)) \
  -level fast \
  -k 16 \
  -train_records 50000 \
  -eval_records 50000
```

Notes:
- This is intentionally *not* deterministic across datasets unless the input is fixed.
- Use it to sanity-check candidate sets and to size `CandidateHistoryBytes`/`CandidateDictBytes`.

---

## CI integration guidance

Recommended CI gate:

```bash
go run ./cmd/unified_bench -suite vlog_autotune -validate
```

Keep the suite:
- fast (target < 30s)
- deterministic (no flaky failures)
- noisy only on failure (print full state/config on mark failure)

---

## Adding a new scenario

When adding scenarios:

1. Reuse the shared runner (`autotune_scenarios.go` or equivalent) so metrics are consistent.
2. Use virtual-time IO cost models so marks are deterministic.
3. Define clear marks:
   - include at least one “safety” assertion
   - include at least one “behavioral” assertion
4. Keep the scenario name stable; scenarios become part of CI interface.
