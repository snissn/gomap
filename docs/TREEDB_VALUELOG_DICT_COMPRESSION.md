# TreeDB value-log dictionary compression

TreeDB can optionally train and apply Zstandard **dictionaries** to value-log **frames** (groups of up to `k` values) to reduce disk bytes for repetitive / structured value streams.

This guide explains:
- how to enable/disable dict compression,
- how to validate it’s active,
- what the key tuning knobs mean (`TrainBytes`, `DictBytes`, `k`, autotune modes),
- how retraining behaves when the payload stream changes,
- how to reproduce unified-bench “compression matrix” runs and interpret disk usage output.

## When it applies (and when it won’t)

Dict compression only affects values stored in the **value log**. It will not help when:
- most values are stored **inline** (small values below `Options.ValueLog.PointerThreshold`), or
- values are already high-entropy / compressed, or
- dict training is disabled and there is no current dictionary to use.

If you want to evaluate dict compression, force values into the value log:
- Code: set `Options.ValueLog.ForcePointers = true` (or lower `PointerThreshold`).
- `cmd/unified_bench`: add `-treedb-force-value-pointers`.

## Quick start (code)

Enable training (recommended convenience helper):

```go
import treedb "github.com/snissn/gomap/TreeDB"

opts := treedb.Options{Dir: "./my-db"}
treedb.EnableValueLogDictCompression(&opts) // enables training + ensures dictdb is enabled

// (Optional but recommended for quick evaluation)
opts.ValueLog.ForcePointers = true

db, err := treedb.Open(opts)
```

Disable training:

```go
treedb.DisableValueLogDictCompression(&opts)
```

Notes:
- `EnableValueLogDictCompression` sets `opts.DisableSideStores=false` and uses a safe default `TrainBytes` when unset.
- `DisableValueLogDictCompression` disables **training/publishing new dictionaries**; it does not delete `dictdb/` on disk.

## Quick start (unified_bench)

Run a simple on/off matrix for TreeDB dict compression:

```bash
./bin/unified-bench -dbs treedb -test batch_write -profile fast -keys 1000000 -format markdown \
  -treedb-force-value-pointers \
  -treedb-vlog-dict both \
  -val-pattern ultra_compressible_repeat
```

If you also want to compare LevelDB block compression:

```bash
./bin/unified-bench -dbs treedb,leveldb -test batch_write -profile fast -keys 1000000 -format markdown \
  -treedb-force-value-pointers \
  -treedb-vlog-dict both \
  -leveldb-block-compression both \
  -val-pattern ultra_compressible_repeat
```

For more unified_bench examples, see `cmd/unified_bench/README.md`.

## How to validate dict compression is active

### 1) Check TreeDB stats (most reliable)

From code, call `db.Stats()` and inspect:

- `treedb.cache.vlog_dict.frames_total`
- `treedb.cache.vlog_dict.frames_attempted`
- `treedb.cache.vlog_dict.frames_kept`
- `treedb.cache.vlog_dict.kept_frac`
- `treedb.cache.vlog_dict.last_applied_dict_id`
- `treedb.cache.vlog_dict.current_k`
- `treedb.cache.vlog_dict.pause_remaining_bytes` (should usually be `0` on compressible streams)

A typical “it’s active” signal:
- `frames_total > 0`
- `frames_kept > 0`
- `kept_frac` noticeably above `0.0`
- `last_applied_dict_id != 0`

If `frames_attempted > 0` but `frames_kept == 0`, compression is being *tried* but not kept (usually incompressible data, tiny values, or a paused state).

### 2) Compare disk usage output (unified_bench)

unified_bench prints TreeDB disk usage like:

```
maindb/wal: total=… files=… commit=… wal=… value=… vlog=… other=…
dictdb/wal: total=… files=… wal=…
```

For dict compression, the primary signal is **smaller value-log bytes**:
- `value=` corresponds to `value-*.log` segments (TreeDB cached lanes).
- `vlog=` corresponds to `vlog-*.log` segments (legacy/other code paths).

You should compare the `value=`/`vlog=` portions between dict `on` and `off` runs.

Keep in mind:
- `dictdb/` will grow because dictionaries are stored persistently.
- The “best” outcome is workload-dependent (ratio vs CPU vs IO). Use throughput + bytes together.

## Core concepts

### Frames and `k`

TreeDB writes value-log records in **frames**: a group of up to `k` values stored in one value-log record.

- Larger `k` can improve ratio (more cross-record matches) and reduce per-record framing overhead.
- Larger `k` can increase encode/decode work per frame and increase tail latency.
- TreeDB currently supports `k` up to 128 (bounded by the on-disk grouped pointer format).

User-facing knobs:
- `Options.ValueLog.DictMaxK` clamps the maximum `k` used (default clamp is 32).
- `Options.ValueLog.CompressionAutotune` can choose better `k` candidates over time.

### Attempted vs kept

For each frame:
- **attempted** means zstd encoding ran (CPU was spent)
- **kept** means TreeDB stored compressed bytes (because it helped enough)

It is normal for attempted ≠ kept on high-entropy streams (TreeDB probes, then backs off).

## Training knobs: `TrainBytes` and `DictBytes`

### `TrainBytes`

`Options.ValueLog.DictTrain.TrainBytes` is the target number of raw sample bytes collected before training a dictionary profile.

Intuition:
- Smaller `TrainBytes` can converge quickly and may “overfit” to a narrower value distribution (sometimes producing a *smaller* stored value log on very repetitive streams).
- Larger `TrainBytes` tends to produce a more general dictionary that tolerates more variety (but may sacrifice peak ratio on a narrow stream).

### `DictBytes`

`Options.ValueLog.DictTrain.DictBytes` is the target dictionary size produced by training.

Intuition:
- Larger dictionaries can improve ratio on complex streams (more match budget).
- Larger dictionaries cost more CPU to train and can add persistent `dictdb/` overhead.

### Early activation (bootstrap)

To avoid a long “nothing compresses until we collect a huge training window” ramp-up, TreeDB uses a **bootstrap** phase:

- The first published dictionary is trained with smaller caps (bootstrap bytes / dict bytes / min records).
- After the first dict is accepted, TreeDB automatically “upgrades” to the configured steady-state `TrainBytes`/`DictBytes` once enough samples are collected.

This makes dict compression less sensitive to exact `TrainBytes` tuning for initial activation.

## Autotune (`Options.ValueLog.CompressionAutotune`)

TreeDB’s value-log compression autotuner can adapt:
- per-frame keep decisions (raw vs compressed),
- the chosen `k`,
- and dictionary history/size candidates.

Modes:
- `AutotuneOff`: disables autotune logic (no adaptive switching/probing); primarily useful for controlled experiments.
- `AutotuneMedium`: balanced production default for cached mode.
- `AutotuneAggressive`: more eager to probe/train/switch (better for IO-bound systems with CPU headroom).

Details (modes, switching, guardrails): `docs/TREEDB_VALUELOG_AUTOTUNE.md`.

## Stream changes, retraining, and pause/probe behavior

TreeDB has two related mechanisms:

1) **Trainer retraining** (dictionary refresh):
   - Samples are continuously collected (subject to `SampleStride` and other train config caps).
   - New profiles are trained after enough bytes/records are seen and retrain gating permits.
   - When the value distribution drifts enough, a new dictionary can be trained and published (stored in `dictdb/`).

2) **Adaptive pause + probe** (runtime CPU protection):
   - If observed savings degrade (payload ratio gets too close to 1.0), TreeDB can pause dict compression for some number of bytes.
   - While paused, TreeDB periodically probes compression; a successful probe clears the pause quickly.

Related knobs:
- `Options.ValueLog.DictAdaptiveRatio` (enable pause when savings degrade; `0` disables)
- `Options.ValueLog.DictMetricsWindowBytes`, `DictMetricsMinRecords`, `DictMetricsPauseBytes`
- `Options.ValueLog.CompressionAutotune.ProbeBytes` / `PauseBytes` (when autotune is enabled)

Operational validation:
- Watch `treedb.cache.vlog_dict.pause_remaining_bytes`.
- Watch trainer stats keys (attempts/accepts/rejects and last accept timestamps) in `db.Stats()`.

## Throughput tuning: encoder knobs and parallelism

### Dict frame encoder knobs

User-facing knobs:
- `Options.ValueLog.DictFrameEncodeLevel` (default `SpeedFastest`)
- `Options.ValueLog.DictFrameEnableEntropy` (default false; ratio↑, throughput↓)

unified_bench exploration:
- `-treedb-vlog-dict-frame-encode-level`
- `-treedb-vlog-dict-frame-entropy`

### Parallel compression pipeline (opt-in)

For batch ingest workloads, TreeDB can parallelize dict frame compression across frames:

- `Options.ValueLog.DictFramePipelineWorkers` (default 0/off)
- `Options.ValueLog.DictFramePipelineMaxInFlightBytes` (bounds queued raw bytes; `0` uses an internal default)

This keeps sequential append semantics (value log is still appended in order) while utilizing multiple CPU cores for compression work.

## Common pitfalls / gotchas

- **No effect because values are inline**: enable `ForcePointers` or lower `PointerThreshold` while evaluating.
- **No effect because training hasn’t published yet**: you may need to write enough data to hit bootstrap thresholds; check `last_applied_dict_id` and `frames_kept`.
- **Paused state**: if `pause_remaining_bytes` is non-zero, TreeDB is intentionally not attempting dict compression (except probes).
- **Disk usage looks “worse”**: remember to include `dictdb/` overhead in the comparison, and evaluate throughput vs bytes together.
- **Changing knobs changes the workload**: `TrainBytes`, `DictBytes`, `k`, and encoder level interact. Prefer controlled, apples-to-apples runs (same seed, same profiles, same key/value patterns).

