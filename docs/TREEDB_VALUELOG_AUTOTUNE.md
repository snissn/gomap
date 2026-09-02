# TreeDB Value Log Compression Autotune

This document describes TreeDB’s *value log* (“vlog”) compression autotuner: what it does, how to enable it, how to operate it safely in production, and how to validate its correctness and performance.

For benchmark methodology, reproducibility guidance, and CI-grade validation “marks”, see:
- `docs/benchmarks/VLOG_AUTOTUNE.md`

For implementation details (agent-oriented, non-normative), see:
- `docs/agents/TREEDB_VALUELOG_AUTOTUNE_RUNBOOK.md`

---

## Overview

In TreeDB **cached mode**, large values can be stored in a separate append-only **value log**. Values are written in **frames** (groups of up to `k` records). Each frame can be stored:

- **raw** (uncompressed), or
- **compressed** with:
  - block codecs (`snappy`, `lz4`) without dictionaries, or
  - dictionary compression (zstd + trained dict).

Compression behavior is selected with `Options.ValueLog.Compression`:

- `off`: raw grouped frames only.
- `block`: block compression only (`Options.ValueLog.BlockCodec`).
- `dict`: dictionary path only (when a dict is available).
- `auto`: adaptive lane that chooses `off|block|dict` per stream.

When unset (`0`), `Options.ValueLog.Compression` defaults to `auto`.

Compression reduces bytes written to disk (and potentially read back), but consumes CPU. Whether compression is a net win depends on *wall-time* characteristics of the deployment: storage bandwidth/latency, `fsync` behavior, CPU availability, and the workload’s compressibility.

The **autotuner** continuously adapts compression decisions to maximize sustained ingest throughput in *real wall time*.

At a high level it can adapt:

- **Per-frame decisions**
  - whether to **attempt** compression
  - whether to **keep** compressed bytes or fall back to raw
- **Frame grouping**
  - choose the “best” **`k`** (records per frame) for the current workload
- **Dictionary policy**
  - choose dictionary **history bytes** used during training
  - choose dictionary **size**
  - rotate dictionaries when it improves throughput

---

## Key concepts and terminology

### Value log frames, `k`, and pointers

- **Value log**: append-only log containing large values stored out-of-line.
- **Frame**: a batch of `k` value-log records written together as a unit.
- **`k`**: number of records per frame. Larger `k` can improve ratio (more cross-record redundancy) but may cost more CPU and increase per-frame work.
- **Pointer threshold**: values larger than `Options.ValueLog.PointerThreshold` are written to the value log and referenced by pointers elsewhere.

### Attempted vs kept

For each frame the writer reports:

- **attempted**: compression was executed (CPU work performed)
- **kept**: the compressed bytes were actually stored (i.e., TreeDB chose to store compressed payload rather than raw)

It is valid—and expected—for `attempted != kept`:
- On **incompressible** data, the tuner may *probe* occasionally (attempt), observe little gain, and then choose *not to keep* compression.
- On **compressible** data in an **IO-bound** regime, both attempted and kept fractions should trend upward.

### Objective function: wall-time throughput, not ratio

The tuner optimizes a throughput proxy of the form:

```
predicted_time ≈ encode_time + stored_bytes * io_time_per_byte
predicted_throughput ≈ raw_bytes / predicted_time
```

Compression is kept only when it improves predicted wall time with a safety margin (see “How it works”).

---

## Requirements and applicability

### Supported modes

Value log compression autotune is designed for:

- **Cached mode** (TreeDB caching layer enabled via `treedb.Open`).
- **Command-WAL durable/relaxed** public profiles are the current recommended
  surface. Legacy WAL-off / `DurabilityWALOffRelaxed` use is compatibility or
  checkpoint-only benchmark territory, not normal production guidance.

### Configuration prerequisites

To benefit from vlog autotune:

1. **Large values routed to the value log**
   - Set `Options.ValueLog.PointerThreshold` such that a meaningful fraction of payload bytes go to the value log.
   - If your workload has only tiny values, vlog autotune will have little/no effect.

### Dictionary storage

TreeDB persists trained dictionaries in a dedicated **dictionary store** (“dictdb”) so they can be reused across process restarts.

When using the top-level `treedb.Open(...)`, TreeDB automatically creates and manages:

- `.../maindb` – main storage
- `.../dictdb` – dictionary storage (dict bytes, current dict marker, and per-dict metadata such as preferred `k`)

If you embed the caching layer directly (advanced usage), you must ensure a dictionary store is wired in (see “Advanced integration”).

---

## Quick start: enable autotune

Typical production usage is to start from a profile and explicitly set the autotune mode.

```go
package main

import (
	"log"

	treedb "github.com/snissn/gomap/TreeDB"
)

func main() {
	dir := "/var/lib/treedb"

	opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, dir)

	// Ensure a meaningful fraction of values are externalized.
	opts.ValueLog.PointerThreshold = 4 << 10 // 4 KiB

	// Enable wall-time autotuning (recommended default for cached mode).
	opts.ValueLog.CompressionAutotune.Mode = treedb.AutotuneMedium

	db, err := treedb.Open(opts)
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer db.Close()

	// ...
}
```

### Recommended rollout strategy

For production rollouts (especially on latency-sensitive clusters):

1. Deploy with `Mode = AutotuneOff` (no behavior change).
2. Enable `AutotuneMedium` for a small canary population.
3. Monitor:
   - attempted/kept fractions
   - stored bytes vs raw bytes
   - ingest throughput and p99 write latency (if relevant)
4. Scale out.

---

## Public configuration

### `Options.ValueLog.CompressionAutotune`

TreeDB exposes a small, production-safe configuration surface via:

- `treedb.Options.ValueLog.CompressionAutotune`

The recommended approach is:
- Pick a **Mode** (`Off`, `Medium`, `Aggressive`)
- Leave advanced fields at zero unless you have a concrete reason

#### Modes

- `AutotuneOff`
  - Always store raw frames (no compression).
- `AutotuneMedium`
  - Balanced setting intended for general production usage.
  - Conservative switching, bounded probes, bounded training CPU.
- `AutotuneAggressive`
  - More eager to compress / train / probe.
  - Better for IO-bound deployments with available CPU headroom.

#### Advanced options (expert-only)

The exact struct may evolve, but the intended knobs are:

```go
type AutotuneMode uint8

const (
	AutotuneOff AutotuneMode = iota
	AutotuneMedium
	AutotuneAggressive
)

type AutotuneOptions struct {
	Mode AutotuneMode

	// Candidate sets (defaults are used when empty).
	CandidateK            []int   // e.g. []int{1,2,4,8,16,32}
	CandidateHistoryBytes []int64 // e.g. []int64{256<<10, 1<<20, 4<<20}
	CandidateDictBytes    []int   // e.g. []int{40<<10}

	// Switching hysteresis / stability.
	MinGainToSwitch float64 // e.g. 0.05 => require 5% predicted gain
	MinDwellFrames  int     // e.g. 128 => hold config for at least N frames

	// Sample / training budgets.
	SampleStride   int   // sample every Nth eligible record
	MaxSampleBytes int64 // reservoir cap
	TrainCPUFrac   float64

	// Probe/backoff after “no benefit” observations.
	ProbeBytes int64 // attempt compression once per N raw bytes while PAUSED
	PauseBytes int64 // stay PAUSED for N raw bytes after a failed probe

	// Guardrails.
	DisableBelowValueBytes int // never attempt compression below this payload size
}
```

Guidance:

- If you’re unsure, **do not modify advanced options**.
- If you do modify them, prefer small changes and validate via the deterministic suite in `docs/benchmarks/VLOG_AUTOTUNE.md`.

---

## How it works

This section is intended to make the tuner’s behavior predictable for operators.

### 1) Per-frame keep decision uses wall-time

A frame is never kept compressed when it expands data (`encoded_bytes >= raw_bytes`).

When compression reduces bytes, the keep decision is based on whether the saved IO time outweighs encoding CPU time, with a safety margin:

- IO is modeled as **ns per stored byte** (tracked via EWMA of measured append wall time).
- Encoding cost is measured (when sampled) or estimated (when not sampled).

**Intuition**:
- If the deployment is IO-bound (high ns/byte), compression is more likely kept.
- If the deployment is CPU-bound (low ns/byte), compression is rarely kept.

### 2) Attempted vs kept is bounded

The tuner maintains a PAUSED state to prevent wasting CPU on incompressible data:

- When it detects repeated “no benefit” outcomes, it enters **PAUSED**.
- While PAUSED, it only probes occasionally (`ProbeBytes`) to detect regime changes.
- After an unhelpful probe it backs off again (`PauseBytes`).

This ensures:
- sustained CPU burn from compression attempts is bounded,
- the system can still recover when compressibility changes.

### 3) Candidate selection (dict + `k` + history) is throughput-driven

Periodically, the tuner evaluates a discrete candidate set:

- `k ∈ CandidateK`
- `history_bytes ∈ CandidateHistoryBytes`
- `dict_bytes ∈ CandidateDictBytes`

Each candidate’s predicted throughput is compared; switching is subject to:

- `MinGainToSwitch` (hysteresis)
- `MinDwellFrames` (avoid thrash)

### 4) Dictionary training is bounded

Dictionary training uses a bounded reservoir of sampled payloads:

- Reservoir capped by `MaxSampleBytes`
- Sampling stride controlled by `SampleStride`
- Training frequency and CPU are throttled (e.g., `TrainCPUFrac`)

If the workload is not suitable for dictionary compression (e.g., high-entropy data), training and dict publishing should naturally become rare.

---

## Observability

### High-signal metrics to watch

TreeDB exposes the following high-signal keys via `(*treedb.DB).Stats()` in cached mode (prefixed `treedb.cache.*`):

- Autotune mode:
  - `treedb.cache.vlog_compression_autotune.mode`: `off|medium|aggressive`
- Cost model snapshot:
  - `treedb.cache.vlog_autotune.encode_ns_per_raw_byte`
  - `treedb.cache.vlog_autotune.io_ns_per_stored_byte`
  - `treedb.cache.vlog_autotune.throughput_raw_MBps`
  - `treedb.cache.vlog_autotune.observed_ratio`
- Dict frame outcomes:
  - `treedb.cache.vlog_dict.frames_total`
  - `treedb.cache.vlog_dict.frames_attempted`, `treedb.cache.vlog_dict.attempted_frac`
  - `treedb.cache.vlog_dict.frames_kept`, `treedb.cache.vlog_dict.kept_frac`
- Current dict / config:
  - `treedb.cache.vlog_dict.current_k`
  - `treedb.cache.vlog_dict.last_applied_dict_id`
  - `treedb.cache.vlog_dict.last_applied_dict_hash` (hex)
  - `treedb.cache.vlog_dict.cached_dict_bytes` (dict byte length currently cached in-process)
- Pause/probe:
  - `treedb.cache.vlog_dict.pause_remaining_bytes`
- Trainer outcome counters:
  - `treedb.cache.vlog_dict.trainer.profile_attempts`
  - `treedb.cache.vlog_dict.trainer.profile_accepts`
  - `treedb.cache.vlog_dict.trainer.profile_rejects`
  - `treedb.cache.vlog_dict.trainer.profile_reject_reason`
- Timestamps (Unix nanos):
  - `treedb.cache.vlog_dict.trainer.last_accept_unix_nano` (last accepted profile)
  - `treedb.cache.vlog_dict.last_publish_unix_nano` (last dict publish to dictdb)
  - `treedb.cache.vlog_dict.last_k_update_unix_nano` (last K update for current dict)

### Debug timing logs

TreeDB includes an env-var gated value-log timing logger (useful for diagnosing IO vs CPU regimes):

- `TREEDB_DEBUG_VLOG_TIMINGS=1` enables periodic logs for value log append timing.

---

## Operational guidance

### Expected behavior by regime

**CPU-bound + compressible data**
- attempted fraction may be low (or moderate due to probes)
- kept fraction should be near zero
- throughput should match raw/off within noise

**IO-bound + compressible data**
- attempted and kept fractions should increase
- stored bytes decrease materially
- throughput improves versus off

**Incompressible data (any regime)**
- quickly enters PAUSED
- attempted fraction becomes small and bounded by `ProbeBytes`
- kept fraction near zero

### When to choose `Aggressive`

Use `AutotuneAggressive` when:
- you are IO-bound (or often fsync),
- CPU headroom exists,
- workloads contain large values with stable redundancy.

Avoid `Aggressive` if:
- CPU is scarce, or
- you are primarily latency-sensitive and cannot afford more frequent probes/training.

### Safe disable / kill switch

Set:

- `ValueLog.CompressionAutotune.Mode = AutotuneOff`

This forces raw frame storage immediately for new frames. Existing on-disk data remains readable regardless of mode.

---

## Validation

### Correctness

The autotuner must not change logical correctness:
- Values read must match values written.
- Dictionary rotation must not break reads (frame headers encode which dict was used).

TreeDB’s unit/integration tests should include:
- decode/encode round-trips for compressed and raw frames,
- dictionary rotation safety,
- PAUSED/probe behavior on incompressible inputs,
- deterministic “virtual wall-time” tests ensuring stable convergence.

### Performance / behavior validation (recommended)

Use the benchmark suite and validation marks described in:

- `docs/benchmarks/VLOG_AUTOTUNE.md`

This suite is designed to be:
- deterministic (CI-friendly),
- sensitive to regressions in tuning behavior,
- illustrative (includes a marquee benchmark demonstrating regime-shift recovery).

---

## Advanced integration (embedding caching)

If you use the caching layer directly (instead of `treedb.Open`), you are responsible for wiring dictionary storage and lookup:

- Create a dict store (e.g., `internal/dictdb` store) and attach it to the caching DB:
  - `cached.SetDictStore(store)`
- Provide a `DictLookup` function to the value log reader/writer so compressed frames can resolve dict IDs to bytes.
- Ensure a meaningful fraction of values are routed to the value log; dictionary
  compression only applies to value-log frames.

This path is intentionally “expert-only”; prefer `treedb.Open` unless you have a strong reason.
