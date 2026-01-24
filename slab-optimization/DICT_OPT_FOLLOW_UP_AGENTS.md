# Dict Compression Follow‑Ups (Re‑implement Best Practices)

This file is a focused follow‑up runbook for **dictionary compression** work only.

Goal:
- Review the dictionary/compression work present in `feature/slab-optimizations` (relative to `main-pre-safe-candidate-20260116-154607`),
- Compare it to our current `main` → `sprint/slabopt-pr9-perf-followups` implementation,
- Identify the best ideas we should **re‑implement** (not cherry‑pick) in the PR9 follow‑up stream.

## References (exact refs reviewed)

- Tag (baseline): `main-pre-safe-candidate-20260116-154607` = `43215d419c205929456fe210211990d01cf79389`
- Feature branch: `feature/slab-optimizations` = `666df3711a4d66e80bcafbab2ca4b911602fed10`
- Current `main`: `8bc7bb302d58ba67bc59a77dca4e3ed2be73f8b7`
- Current PR9 head: `sprint/slabopt-pr9-perf-followups` = `6346e9249313cb066427b2c11c1525cba10cdc49`

## What `feature/slab-optimizations` adds (dictionary-specific)

### 1) A real “dictionary system” (generic, reusable)

`TreeDB/internal/compression/` introduces:
- `Trainer`: async training pipeline
  - bounded queue (`DefaultTrainQueue`) with drop stats (backpressure)
  - sampling controls: `TrainBytes`, `MinRecords`, `MaxRecordBytes`, `SampleStride`
  - de‑dup: `DedupWindow` plus “global/ref/cache” modes to avoid re-training or reusing equivalent dicts
  - anti‑thrash gating for accepting dict+K refresh:
    - minimum bytes/records/time (`MinProfileBytes`, `MinProfileRecords`, `MinProfileInterval`)
    - only accept if improvement is meaningful (`ProfileImproveThresh`)
    - drift detection vs rolling baseline (`ProfileDriftThreshold`)
  - durability/perf safety: dict validation and fixed size padding for compatibility (40960 bytes)
  - extensive trainer telemetry (`TrainerStats`): queue depth, dropped samples, accept/reject counts, last reject reason, rolling ratios, etc.

- `profile.go`: K-selection logic
  - evaluates candidate `K` values using an estimated decode cost + estimated bytes saved
  - attempts to choose `K` that maximizes “bytes saved per decode cost”

- `metrics.go`: adaptive pause mechanism (ratio-based)
  - windowed ratio tracking and optional “pause compression/training” when ratio degrades

Key concept: the trainer is designed to run continuously but safely:
- avoid CPU burn on incompressible payloads via adaptive pausing,
- avoid thrashing dictionaries via gating thresholds,
- avoid unnecessary training via dedup/caching.

### 2) Benchmarking that explicitly models compressibility

Feature adds a concrete, reproducible compressibility benchmark:
- `TreeDB/slab/compression_adaptive_bench_test.go`:
  - generates **highly compressible**, **medium compressible**, and **incompressible** workloads
  - reports throughput (MB/s) and an observed ratio metric (`observed_ratio`)
  - sweeps a small parameter grid (`ratio`, `window`, `pause`)

It also adds CI enforcement of baseline expectations:
- `.github/perf_baselines/slab_adaptive_defaults.json`
- `.github/scripts/check_slab_adaptive_bench.go`
  - parses benchmark output
  - uses median-based thresholds for throughput and ratio bounds
  - emits GitHub workflow warnings when defaults regress

This is important: it makes compression performance “testable”, not anecdotal.

## Where PR9 is today (dictionary-specific)

Our PR0–PR9 line implements dictionary compression in the **ValueLog** path (not the slab writer path used in `feature/slab-optimizations`):
- Dict storage: `TreeDB/internal/dictdb` (hash → dictID, dictID → bytes, current pointer, per-dict `K`)
- Dict training pipeline: `TreeDB/internal/compression` (Trainer/Metrics/Profile) exists and is used by cached-mode ValueLog dict training.
- ValueLog grouped frame encoding: `TreeDB/internal/valuelog` supports dictID + grouped frames + dynamic K.
- Codec pooling: `TreeDB/internal/valuelog/dict_codec_cache.go` pools zstd enc/dec by dictID.
- Adaptive pause: `TreeDB/caching/vlog_dict.go` uses windowed ratio metrics and pauses dict usage on degraded streams.
- Sampling guard: `likelyCompressibleSample()` reduces feeding incompressible payloads to the trainer.

Recent perf work (PR9) focused on removing regressions in the **pointer + split ValueLog** hot paths, not on dict quality/ROI yet.

## Compare / Contrast (Feature vs PR9)

### Shared concepts already present in PR9
- Async trainer + bounded queue + sampling knobs.
- Dict validation and fixed-size padding (we also improved the “reduced dict” retry to preserve fixed-size invariants).
- Adaptive pause based on observed ratio.
- K profiling and dynamic K selection.
- Avoid no-op dict publication (PR9 also avoids publishing dicts that don’t save bytes).

### Key differences

1) **Benchmarking maturity**
- Feature branch: has a first-class compressibility benchmark + baseline/CI checks.
- PR9: does not yet have a comparable “dict compression ROI” benchmark that:
  - (a) models compressible vs incompressible data in a controlled way,
  - (b) reports compression ratio as a benchmark metric,
  - (c) is stable enough to baseline in CI.

2) **Observability / tuning loop**
- Feature branch: trainer exposes rich `TrainerStats` and CI uses explicit thresholds.
- PR9: trainer stats are not currently surfaced in a user-facing way (Stats keys / debug output), so tuning relies on manual profiling.

3) **Scope**
- Feature branch focuses on slab writer compression; PR9 focuses on value-log record compression + dictionary lifecycle (dictdb/current/K/lagging rule).
  - We should borrow the **benchmarking + baseline** approach from the feature branch, but implement it for **ValueLog dict compression** (our architecture).

## What we should re‑implement (not cherry‑pick)

### R1 — Add a ValueLog dict “compressibility benchmark” (like the slab one)

Implement a benchmark that:
- generates 3 payload types:
  - **highly compressible** (repeated JSON-ish + small noisy tail)
  - **medium compressible** (half structured, half random)
  - **incompressible** (random)
- measures both:
  - throughput (MB/s or ops/sec)
  - **observed payload ratio** (stored/raw) for the value-log payload stream

Design choice:
- Prefer a package-local bench close to the implementation (`TreeDB/internal/valuelog`), so it runs fast and isolates encoding.
- Add a second bench at the cached DB level (`TreeDB/caching`) only after the lower-level bench is stable.

Acceptance:
- Must be runnable via `go test -bench` with predictable outputs and minimal IO noise.

### R2 — Add “baseline thresholds” and a CI check (median-based)

Reimplement the feature branch idea, but for ValueLog dict benchmarks:
- `.github/perf_baselines/vlog_dict_defaults.json` (new)
- `.github/scripts/check_vlog_dict_bench.go` (new)
- Update workflow to run a small `go test -bench` set and validate:
  - min MB/s for each workload category
  - ratio bounds (e.g., compressible must be < X, incompressible must be ~1.0)

Note:
- CI perf checks should be “warning-only” initially to avoid noisy failures.

### R3 — Expose dict trainer/metrics stats in TreeDB `Stats()`

Add Stats keys for cached-mode dict compression:
- trainer queue: `queue_len`, `dropped`, `enqueued`, `train_count`
- last accepted profile: `dict_hash`, `dict_bytes`, `k`, `payload_ratio`, `total_ratio`, `timestamp`
- adaptive pause state: `pause_remaining_bytes`, rolling ratio baseline/current, last reject reason

Goal:
- allow tuning via real workload telemetry, not guesswork.

### R4 — Tighten “publish cadence” / avoid waste

We should confirm our value-log dict apply loop does not:
- re-check too frequently (timer polling),
- write dictdb “current” too often,
- or re-publish effectively identical dicts.

Reimplement (if needed):
- “publish only on accept edge” (event-based) rather than periodic polling
- explicit “min bytes since last publish” and “min time since last publish” in the integration layer
  - (the trainer has gating; we also need integration-layer safeguards to avoid excessive dictdb writes)

### R5 — Tune defaults using the new benchmark(s)

Once R1/R2 exist, use the bench results to set recommended defaults:
- `TrainBytes` (how often we retrain)
- `SampleStride`
- `DedupWindow`
- `AdaptiveRatio` and metrics window bytes
- `MinSavingsRatio` (avoid “no-op dict”)

Deliverable:
- a documented “recommended baseline config” for:
  - compressible workloads (expect wins)
  - incompressible workloads (expect near-baseline, minimal CPU overhead)

## Milestones (PR9 follow-ups)

### D0 — Bench harness
- [ ] Add `BenchmarkValueLogDictAdaptiveSweep` (3 workloads + observed ratio metric)
- [ ] Ensure bench runs < ~10s locally

### D1 — CI + baselines
- [ ] Add baseline JSON + check script (median-based)
- [ ] Integrate into workflow as warning-only initially

### D2 — Stats/telemetry
- [ ] Surface trainer + pause + ratio metrics in `Stats()`
- [ ] Add a short doc snippet in PR9 describing how to interpret these stats

### D3 — Default tuning
- [ ] Use bench data to propose defaults and safe ranges
- [ ] Validate against “random” (incompressible) and “repeat/structured” (compressible)

## PR9 Process Requirements (Mandatory)

Follow the PR9 policy in `slab-optimization/FOLLOW_UP_AGENTS.md`:
- commit often, push often
- log all benchmark/profile results in both:
  - `slab-optimization/FOLLOW_UP_AGENTS.md`
  - PR9 comments (with `artifacts/bench/...` paths)

