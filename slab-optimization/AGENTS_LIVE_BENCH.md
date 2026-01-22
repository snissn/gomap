# Live Bench Runbook: TreeDB ValueLog Compression (mode1/mode3/mode4)

This runbook defines an **autonomous, repeatable** workflow for validating TreeDB’s *real* wall-clock **steady-state write throughput** using the **public TreeDB KV API** (not internal `valuelog.Writer`).

Primary intent:
- Produce a “headline” metric: **steady-state raw ingest MB/s** after training.
- Compare **compression ON vs OFF** for the value-log write paths we care about.
- Compare **mode3 vs mode4** (relaxed/no-fsync focus), and also include a baseline **mode1 compression OFF**.

Scope note:
- We are **not prioritizing fsync / WriteSync** right now. This bench should use `Batch.Write()` only.

---

## Definitions (map “modes” to TreeDB options)

TreeDB exposes write-path strings via `db.Stats()` keys:
- `treedb.write_path.value_store`
- `treedb.write_path.redo_log`

These correspond to `TreeDB/public.go:writePathFromOptions`:

### Mode1 (baseline, compression off)
Goal: compare against the older cached path with no value log.
- Expected stats:
  - `value_store=backend_flush`
  - `redo_log=on`
- Options:
  - `DisableValueLog=true`
  - `DisableJournal=false`
  - `DisableWAL=false`
  - `AllowUnsafe=true` (optional; safe to set uniformly for all modes)

### Mode3 (value log + journal, redo on)
Goal: “value log now” + journal (no fsync when using `Write()`).
- Expected stats:
  - `value_store=value_log`
  - `redo_log=on`
- Options:
  - `DisableValueLog=false`
  - `DisableJournal=false`
  - `DisableWAL=false`
  - `SplitValueLog=true` (or omit; cached mode enables split vlog when value log is enabled)

### Mode4 (value log deferred, redo off)
Goal: “value log eventually” (flush boundary), no journal.
- Expected stats:
  - `value_store=value_log_deferred`
  - `redo_log=off`
- Options:
  - `DisableValueLog=false`
  - `DisableJournal=true`
  - `DisableWAL=false`
  - `MemtableValueLogPointers=false` (default; MUST remain false for deferred behavior)
  - `AllowUnsafe=true` (REQUIRED because `DisableJournal` is treated as unsafe)

Important nuance (mode4 streaming bypass):
- Mode4 has an optimization that can append to the value log during `Write()` for large strictly-increasing batches.
- For this runbook’s “deferred value log” measurement, the benchmark SHOULD use **non-monotonic keys** (e.g., pseudo-random) to avoid the streaming-bypass path unless explicitly enabled as an extra experiment.

---

## Benchmark matrix (what we run)

### Compression OFF (baseline set)
Run these three:
- `mode1` / `compression=off`
- `mode3` / `compression=off`
- `mode4` / `compression=off`

Rationale:
- mode3/off vs mode4/off isolates write-path overhead differences without compression.
- mode1/off provides a “no value log” baseline.

### Compression ON (feature set)
Run these two:
- `mode3` / `compression=on`
- `mode4` / `compression=on`

Rationale:
- mode1 doesn’t use the value log and therefore doesn’t participate in value-log dict compression.

---

## Dataset considerations (must not accidentally benchmark “inline values”)

Your real dataset example (`celestia-db.head.jsonl`) has `avg≈164B`, and TreeDB’s default inline threshold is 256B. Without forcing pointers, most values will **not** go to the value log and you won’t be benchmarking value-log compression.

For this runbook, the benchmark must force values through the value-log path:
- set `ValueLogPointerThreshold=1` (or another value < typical payload size), OR
- pad values up to >256B (less preferred), OR
- disable inlining in some other explicit way.

Also: keep `MaxValueLogRetainedBytesHard=0` (disabled) during the bench to avoid pointer disablement due to retention thresholds.

---

## Implementation task: extend `cmd/vlog_dict_realdata` with KV throughput bench

Target: `TreeDB/cmd/vlog_dict_realdata/main.go`

Add a new benchmark mode that uses the **public TreeDB package**:
- Import: `github.com/snissn/gomap/TreeDB` (package `treedb`)
- Use only:
  - `treedb.Open(treedb.Options{...})`
  - `db.NewBatch()`, `batch.Set(...)`, `batch.Write()`
  - `db.Stats()` (for dict/attempted/kept/k + write_path)

### New flags (suggested)
- `-bench-kv` (bool): enable KV throughput bench.
- `-bench-mode` (string): `mode1|mode3|mode4`.
- `-bench-compression` (string): `on|off`.
- `-bench-raw-mib` (int): raw MiB to write in the steady-state phase (default: 512).
- `-bench-batch` (int): number of ops per batch write (default: 1024).
- `-bench-key-mode` (string): `random|sequential|dataset` (default: `random`).
  - Default MUST be `random` to avoid mode4 streaming bypass unless explicitly requested.
- `-bench-pointer-threshold` (int): value-log pointer threshold (default: 1).
- `-bench-flush-threshold-mib` (int): flush threshold for cached mode (optional; default leave as TreeDB default).
- `-bench-out-json` (string): optional JSON output path (for machine parsing).

### Bench phases (measure, but headline steady-state)
1) **Load/fill** (existing tool behavior): time parsing until train/eval buffers are populated.
2) **Warmup/train** (time it): write `trainN` records into a fresh DB.
   - Optionally: wait until dict becomes active (compression=on only) by polling `db.Stats()`:
     - `treedb.cache.vlog_dict.last_applied_dict_id` becomes non-zero, and/or
     - `treedb.cache.vlog_dict.frames_kept` increases.
3) **Steady state (headline)**: write enough data to reach `bench-raw-mib`.
   - Repeat the eval dataset as needed.
   - Time only this window and compute:
     - `steady_raw_MBps = raw_bytes / elapsed_seconds / 1e6`
   - Also capture:
     - `treedb.cache.vlog_dict.attempted_frac`, `kept_frac`
     - `treedb.cache.vlog_dict.current_k`, `last_applied_dict_id`
     - `treedb.write_path.value_store`, `treedb.write_path.redo_log`

### Mode selection (options mapping)
In the bench, map `bench-mode` to `treedb.Options`:

- Common (all modes):
  - `AllowUnsafe=true`
  - `SplitValueLog=true` (where applicable)
  - `ValueLogPointerThreshold = bench-pointer-threshold` (for mode3/mode4)
  - `MaxValueLogRetainedBytesHard=0`

- Mode1:
  - `DisableValueLog=true`
  - `DisableJournal=false`

- Mode3:
  - `DisableValueLog=false`
  - `DisableJournal=false`

- Mode4:
  - `DisableValueLog=false`
  - `DisableJournal=true`
  - `MemtableValueLogPointers=false`

### Compression ON vs OFF mapping
- `compression=on`:
  - Enable value-log dict training (explicitly; do NOT rely on AllowUnsafe defaults):
    - `ValueLogDictTrain.TrainBytes` > 0 (bounded; e.g. 8MiB or 32MiB depending on needs)
  - Keep the autotuner enabled if you want “real” behavior:
    - `ValueLogCompressionAutotune.Mode = AutotuneMedium` (default when split vlog enabled), OR set explicitly.
- `compression=off`:
  - Ensure training is truly off even when `AllowUnsafe=true`:
    - set `ValueLogDictTrain.TrainBytes = -1`
    - set `ValueLogCompressionAutotune.Mode = AutotuneOff`

Sanity check: print `db.Stats()` write-path keys at startup and fail if the mode doesn’t match expectations.

---

## How to run (examples)

Use a big dataset (or lower `-train/-eval`) so we have enough samples.

Run from repo root (`/Users/michaelseiler/dev/snissn/gomap`). If you’re already in `TreeDB/`, drop the `TreeDB/` path prefix.

### Compression OFF matrix
Run each and save logs:
- `go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode mode1 -bench-compression off | tee ~/bench_mode1_off.log`
- `go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode mode3 -bench-compression off | tee ~/bench_mode3_off.log`
- `go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode mode4 -bench-compression off | tee ~/bench_mode4_off.log`

### Compression ON matrix
- `go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode mode3 -bench-compression on | tee ~/bench_mode3_on.log`
- `go run ./TreeDB/cmd/vlog_dict_realdata -input <dataset.jsonl> -bench-kv -bench-mode mode4 -bench-compression on | tee ~/bench_mode4_on.log`

---

## Acceptance criteria (what “good” looks like)

Functional:
- Mode detection matches:
  - mode1 ⇒ `value_store=backend_flush`, `redo_log=on`
  - mode3 ⇒ `value_store=value_log`, `redo_log=on`
  - mode4 ⇒ `value_store=value_log_deferred`, `redo_log=off`
- compression=off runs show `kept_frac≈0` and `last_applied_dict_id=0`.
- compression=on runs show `last_applied_dict_id!=0` and `kept_frac>0` in steady-state (for compressible datasets).

Performance reporting:
- Output includes a single “headline” line per run with:
  - `steady_raw_MBps` (primary)
  - `speedup_vs_off` (for mode3+mode4 on-runs)
  - `attempted_frac`, `kept_frac`, `current_k`, `dict_id`

Stability:
- Recommend 3 runs per config; report median. (Do not gate CI on this; it’s for human validation.)

---

## Notes / pitfalls

- If you forget to force pointers (threshold too high), mode3/mode4 can silently benchmark the “inline values” path.
- Mode4 can look “too fast” if you accidentally benchmark a key pattern that triggers the mode4 streaming bypass; default to random keys.
- On macOS, results can be noisy. Prefer the Linux dev server for final numbers.
