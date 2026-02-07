# TreeDB Issue #384 Perf Gates

This document defines the deterministic throughput/perf gate contract for
Issue #384.

## Baseline

- Fixed baseline commit: `a2d8cbb802e0c611a82011a9ea18424817fcead8`
- Candidate: current branch HEAD

## Determinism Contract

- Fixed seed: `-seed 1`
- Warmup runs: 1 (discarded)
- Measured runs: 5
- Pass/fail metric: median-of-5
- Recommended environment:
  - self-hosted Linux perf runner
  - fixed CPU governor (`performance`)
  - fixed CPU affinity (`taskset` / `CPUSET`)
  - fixed `GOMAXPROCS`

## Scored Workloads (Compression Off)

Scored runs use `-treedb-vlog-compression off` to isolate non-compression
changes for #384.

Per-PR (default 4M keys):

1. `batch_write`, `valsize=256`, `val-pattern=medium_compressible_sparse` (>= +15%)
2. `batch_write`, `valsize=2048`, `val-pattern=medium_compressible_sparse` (>= +15%)
3. `batch_write,random_read`, `valsize=256`, `val-pattern=medium_compressible_sparse` (>= +10%)
4. `batch_write,random_read`, `valsize=2048`, `val-pattern=medium_compressible_sparse` (>= +10%)

Nightly (12M keys):

- Same four workloads, plus `val-pattern=random` variants.

## Non-Scoring Auto Sanity

Each gate run also executes one auto sanity case:

- `batch_write,random_read`, `valsize=256`, `val-pattern=medium_compressible_sparse`,
  `-treedb-vlog-compression auto`

The sanity check ensures candidate throughput remains within a floor relative to
baseline (`AUTO_SANITY_MIN_FRAC`, default `0.95`).

## Scripts

From repo root:

```bash
# Per-PR gate (default keys=4_000_000)
scripts/issue384_perf_gate.sh

# Nightly gate (default keys=12_000_000 + random-pattern matrix)
scripts/issue384_nightly_gate.sh

# Before/after profile pair (CPU + block + mutex + gctrace)
scripts/issue384_profile_pair.sh
```

### Useful env knobs

- `BASELINE_HASH` baseline commit override
- `KEYS` key count override
- `RUNS` measured run count
- `WARMUP_RUNS` warmup run count
- `CPUSET` CPU affinity (uses `taskset`)
- `RUN_PREFIX` command prefix for runner pinning/wrappers
- `GOMAXPROCS` worker parallelism
- `STRICT_GATE=0` do not fail process on threshold miss (debug only)

## Artifacts

Gate script writes to:

- `artifacts/perf/issue384_gate_<timestamp>/summary.md`
- `artifacts/perf/issue384_gate_<timestamp>/summary.json`
- `artifacts/perf/issue384_gate_<timestamp>/runs/...`

Profile script writes to:

- `artifacts/perf/issue384_profiles_<timestamp>/summary.md`
- `artifacts/perf/issue384_profiles_<timestamp>/profiles_baseline/...`
- `artifacts/perf/issue384_profiles_<timestamp>/profiles_candidate/...`

