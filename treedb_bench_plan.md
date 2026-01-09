# TreeDB Celestia Bench Plan

## Scope
Build a synthetic data generator and runner that approximate the Celestia mainnet state sync workload against TreeDB, with toggles for background tasks and WAL/vlog settings. Start with planning and measurements; implement after alignment.

## Latest Server Data Review (2026-01-08)
Most recent run directory (server):
- `/home/mikers/.celestia-app-mainnet-treedb-20260108115023`

High-level sizes:
- total home: ~7.9G
- `data/application.db`: ~6.5G
- `data/blockstore.db`: ~161M
- `data/state.db`: ~74M
- `data/snapshots`: ~977M

TreeDB layout (application.db):
- `data-0000.slab`: ~4.0G
- `index.db`: ~2.6G
- `wal/`: empty (checkpointed or no WAL remaining)

State sync characteristics:
- `sync-time.log`: duration ~676s, final height 9303040
- `node.log`: snapshot total chunks 68; chunk 67 applied successfully

Implication: the workload is a large, sequential snapshot restore with large batch writes and substantial index growth, followed by a catch-up phase.

## Goals
- Reproduce the workload shape: large batch writes, heavy index mutation, value log behavior, checkpoint behavior.
- Detect stalls/deadlocks and data integrity errors in a controlled, deterministic test harness.
- Provide synthetic knobs to emulate Celestia:
  - batch size distribution
  - key cardinality and locality
  - value size distribution
  - write/delete mix
  - snapshot chunk boundaries

## Plan Overview
1) **Capture workload characteristics**
2) **Define synthetic workload model**
3) **Build data generator**
4) **Build runner**
5) **Validate vs. real run**

## 1) Capture Workload Characteristics
Goal: extract actionable distributions without re-running Celestia.

Server review checklist:
- Confirm TreeDB settings used by harness:
  - `run_celestia.sh` envs (WAL/vlog, BG disabled, value pointer threshold).
- Extract summary stats from existing run:
  - slab size, index size, WAL size
  - snapshot chunk count
  - total duration, max RSS

Planned instrumentation (future, opt-in):
- Add a lightweight tracing mode in `cosmos-db/treedb` adapter:
  - log batch sizes, op counts, total bytes per batch
  - key length distribution
  - value size distribution
  - time per `Batch.Write()`
- Add read/iterator telemetry:
  - `Get/Has` counts and key size distribution
  - iterator creation counts, lifespan (time open), and number of `Next()` calls
  - iterator range spans (start/end prefix length)
- Use rolling file logs with sampling to avoid overhead.
- Enable via env flags, e.g. `TREEDB_TRACE_BATCH=1`.

### Phase A vs Phase B Read/Iterator Capture (New)
Concern: synthetic workload drift due to missing iterator activity. We need to know
if snapshot restore (phase A) and catch-up (phase B) include iterator-heavy paths.

Plan:
1) Add temporary logging in the adapter (`cosmos-db/treedb`) to tag ops with phase:
   - Phase A: between `ApplySnapshotChunk` start/end.
   - Phase B: after state sync completion until catch-up reaches head.
2) Log read/iterator events per phase:
   - `Get` / `Has` counts
   - `Iterator` / `ReverseIterator` creation count
   - average iterator lifespan and `Next()` count
3) Emit summary at shutdown:
   - counts by phase
   - top N iterator range patterns (prefix lengths)

Where to hook:
- `kvstore/adapters/treedb` wrapper:
  - wrap `Get`, `Has`, `Iterator`, `ReverseIterator`, `NewBatch().Write`
- `cosmos-db` layer:
  - add a phase flag that can be toggled by the app (state sync callbacks).

Outputs:
- `treedb_op_trace.jsonl` with structured events
- `treedb_phase_summary.json` with aggregate metrics

Initial implementation (in gomap):
- `TREEDB_TRACE_PATH=/path/to/treedb_op_trace.jsonl` enables tracing.
- `TREEDB_TRACE_EVERY_N=100` samples every N ops (default 1).
- `TREEDB_TRACE_SUMMARY_PATH=/path/to/treedb_phase_summary.json` sets summary output.
- `treedbtrace.SetTracePhase("restore"|"catchup")` can be called by the app to tag phases.

Synthetic mapping:
- If iterator creation is significant in Phase A or B, add iterator ops into
  the generator/runner to match:
  - iterator start/end spans
  - iterator frequency relative to writes

## 2) Synthetic Workload Model
Design a parameterized model that mirrors Celestia restore characteristics.

Key model elements:
- **Batch sizes**: large bulk batches (e.g., 1k–20k ops) with long-tailed distribution.
- **Value sizes**: heavy tail (small metadata + large payloads).
- **Key locality**: mostly prefix-grouped (store module prefixes) with moderate skew.
- **Write pattern**:
  - Phase A: snapshot restore (mostly Set, no deletes).
  - Phase B: catch-up (mixed Set/Delete, smaller batches).
- **Chunk boundaries**: flush or rotate memtable every chunk to mimic state sync.

Baseline parameters (initial guess):
- Total keys: 10–50M (scaled down for tests).
- Batch size mean: 5k ops; max: 20k.
- Value sizes: 32B–16KB with 90th percentile ~1–4KB; 99th percentile 8–32KB.
- Key length: 16–64 bytes; module prefix length: 4–8 bytes.
- Delete ratio: 0–5% in restore, 5–15% in catch-up.

## 3) Data Generator
Outputs key/value pairs and delete ops consistent with the model.

Proposed API:
- `Generator.NextBatch() BatchSpec`
  - `[]Op {Type, Key, Value}`
  - `BatchMeta {ChunkID, Phase, Bytes, Ops}`

Controls:
- `--seed`: deterministic RNG
- `--phase`: restore/catchup
- `--batch-ops-min/max`
- `--value-size-dist`
- `--key-prefix-count`
- `--delete-ratio`
- `--chunk-ops-target` (rotation target)

## 4) Runner
Applies generated batches to TreeDB with knobs matching Celestia envs.

Runner responsibilities:
- Open TreeDB with configurable options (WAL, vlog, BG, compression).
- Apply batches in two phases:
  - restore phase (large batches)
  - catch-up phase (smaller mixed ops)
- Force chunk boundaries:
  - trigger memtable rotation or flush after chunk target
- Record metrics:
  - ops/sec, bytes/sec
  - backlog bytes
  - flush/compaction timings

Metrics/Artifacts:
- JSON run summary
- per-phase timings
- TreeDB stats snapshots (`db.Stats()`)

## 5) Validation vs. Real Run
Compare synthetic run output to real run:
- index size / slab size ratio
- total bytes written
- snapshot duration curve
- backlog behavior (queue bytes)

Acceptance criteria:
- Synthetic run completes without stalls under `TREEDB_BENCH_DISABLE_BG=1`.
- Data layout roughly matches real ratios (within 20–30%).

## Implementation Notes
Recommended location:
- `TreeDB/cmd/bench` or `TreeDB/cmd/stress` extension

Config mapping:
- Map env flags from `run_celestia.sh` to runner options.
- Use a `--preset=celestia` profile for parity.

## Next Steps (Execution)
1) Confirm settings from `run_celestia.sh` and record in this doc.
2) Extract key/value distributions via optional tracing (if approved).
3) Implement generator + runner skeleton.
4) Validate against latest run and iterate parameters.
