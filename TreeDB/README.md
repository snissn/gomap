# TreeDB

TreeDB is a high-performance, persistent key-value store optimized for the Cosmos SDK workload. It features a B+Tree index backed by a memory-mapped file plus a persistent value log for large values, with a separate journal/redo log for crash recovery.

## Canonical Spec

The canonical TreeDB specification now lives under:

- `TreeDB/docs/spec/README.md`

Use that folder for architecture, format, durability, recovery, lifecycle, and verification requirements. Practical typed-storage quickstarts and benchmark-adjacent guides live under `TreeDB/docs/guides/`. The root `docs/TREEDB_*.md` files remain supporting material.

## Features

-   **Crash-consistent commits:** Atomic batch commits and recovery behavior according to the selected durability mode.
-   **Snapshot Isolation:** Lock-free concurrent readers using Multi-Version Concurrency Control (MVCC) and Reference Counting.
-   **Hybrid Storage:**
    -   **Index:** Memory-mapped B+Tree for keys and small values.
    -   **Value log:** Append-only log for large values (contract code, blobs) to reduce write amplification and memory pressure.
    -   **Journal/redo log:** Commit metadata for crash recovery and checkpointing.
-   **Crash Recovery:** Automatic recovery from torn writes using strict write-ordering and checksum verification.
-   **Lifecycle Management:** Safe page reclamation using a Graveyard and Reader Registry to protect active snapshots.

## Architecture

### Storage Layout
-   **Root dir:** `Options.Dir` is a root directory with `maindb/` (main data) plus optional side stores such as `dictdb/` and `templatedb/`.
-   **Pages:** 4KB fixed-size blocks in `maindb/index.db`.
-   **Nodes:** Slotted pages supporting variable-length keys.
-   **Value log:** Persistent append-only segments under `maindb/value_vlog/` storing large values with CRC checksums.
-   **Leaf log:** Optional persistent outer-leaf generations under `maindb/leaf_vlog/` when `IndexOuterLeavesInValueLog` is enabled.
-   **Journal/redo log:** Commit metadata for crash recovery under `maindb/wal/`.

### Write Path ("The Zipper")
Writes are batched and applied using a recursive "Zipper" merge algorithm. This creates a new version of the tree path (COW) without modifying existing on-disk pages, ensuring crash safety and snapshot isolation.

### Read Path
Readers acquire a `Snapshot` which pins the version of the tree and the relevant value-log segments. This guarantees a consistent view of the database even while writers are committing new versions.

## Usage

```go
package main

import (
	"errors"
	"fmt"
	"log"

	treedb "github.com/snissn/gomap/TreeDB"
)

func main() {
	// Open the database (recommended: cached wrapper)
	opts := treedb.Options{Dir: "./my-db-data"}
	database, err := treedb.Open(opts)
	if err != nil {
		if errors.Is(err, treedb.ErrLocked) {
			log.Fatal("database is already open in another process")
		}
		log.Fatal(err)
	}
	defer database.Close()

	// Set a key-value pair
	if err := database.Set([]byte("key1"), []byte("value1")); err != nil {
		log.Fatal(err)
	}

	// Get a value
	val, err := database.Get([]byte("key1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Got: %s\n", val)

	// Iterate
	it, _ := database.Iterator(nil, nil)
	defer it.Close()
	for ; it.Valid(); it.Next() {
		fmt.Printf("%s = %s\n", it.Key(), it.Value())
	}
	
	// Atomic Batch
	batch := database.NewBatch()
	batch.Set([]byte("k2"), []byte("v2"))
	batch.Delete([]byte("key1"))
	batch.Write() // Atomic commit
}
```

## Profiles (Command WAL / Bench)

If you want a simple, documented “bundle” of options, start with a profile and
then override a few workload-specific knobs:

```go
opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "./my-db-data")
opts.FlushThreshold = 128 << 20 // optional tuning
db, err := treedb.Open(opts)
```

The public profile surface is intentionally narrow:

- `ProfileCommandWALDurable`: command-WAL collections and raw writes with durable sync/checksum settings. Use this as the default server profile.
- `ProfileCommandWALRelaxed`: command-WAL collections and raw writes with relaxed sync/read-integrity settings for high-throughput ingest and benchmark comparisons.
- `ProfileBench`: benchmark-only no-WAL ceiling for measuring raw in-process storage overhead. Do not use it as a production profile.

Legacy/raw bundles such as `ProfileDurable`, `ProfileFast`, and
`ProfileWALOnFast` remain temporarily available for compatibility and focused
low-level tests, but they are not the current public guidance.

Note: WAL-off is selected by `ProfileBench` or by explicitly setting
`opts.Durability = treedb.DurabilityWALOffRelaxed` in low-level experiments.
The value log remains enabled in cached mode, so large values can still go
through `value_vlog/` even when the redo journal is off.

Details: `docs/TREEDB_WRITE_PATHS.md`.

If you are integrating TreeDB behind a Cosmos/Comet-style wrapper, use
`TreeDB/integration/kvstoreadapter` to standardize profile/env/default handling
instead of re-copying the open-path glue in each downstream repo.

## Leaf Pages in the Value Log

`Options.IndexOuterLeavesInValueLog` stores the B+Tree **leaf pages** (the 4096B
pages that contain keys and values/pointers) in the persistent value log instead
of in `index.db`. Internal pages remain in `index.db` and still provide exact
lookup routing to a single leaf page (no leaf scanning).

Leaf pages may still contain inline values or `ValuePtr` entries using the
existing placement rules.

## Collection Vector Search

Collections can store caller-generated float32 embeddings in JSON document
fields and search them without making ANN storage the source of truth. Canonical
documents and embeddings remain in TreeDB primary storage; vector indexes keep
stable collection document IDs and exact-rerank candidates from canonical rows.

Current entry points:

| Goal | Entry point | Boundary |
| --- | --- | --- |
| High-QPS no-document collection serving | `Collection.SearchVectorIndexWithBuffer` | Caller-owned `VectorIndexSearchBuffer`; exact `IncludeDocuments=false`; warm the `hnsw_search_pack_v1` prepared route before serving. |
| Convenience no-document calls | `Collection.SearchVectorIndex` with `IncludeDocuments=false` | Response-owned results/IDs; healthy exact calls use the cached no-document route but intentionally allocate result storage. |
| Reusable low-level workers | `Collection.OpenVectorIndexSearcher` + `VectorIndexSearcher.SearchWithBuffer` | One opened searcher and one reusable buffer per worker; caller owns searcher/snapshot lifetime. |
| Search plus materialization | `Collection.SearchVectorIndex` with `IncludeDocuments=true` | Explicit with-document path; report document-fetch/materialization counters separately from no-document rows. |
| Quantized score planes | Explicit `quantized_only` / `quantized_rerank` modes | Separate codec-generic follow-up path; not the exact high-QPS demo route or no-document `hnsw_search_pack_v1` claim. |

Exact-only runnable demo from the repository root:

```sh
GOWORK=off go run ./cmd/treedb_vector_highqps_demo \
  -docs 1000 \
  -dims 64 \
  -queries 1000 \
  -warmup-queries 16 \
  -top-k 10 \
  -m 16 \
  -ef-construction 128 \
  -ef-search 128
```

Use
[`TreeDB/docs/guides/vector-search-high-qps-collection-api.md`](docs/guides/vector-search-high-qps-collection-api.md)
for the API chooser and buffer lifetime caveats,
[`TreeDB/docs/guides/vector-search-benchmark-workflow.md`](docs/guides/vector-search-benchmark-workflow.md)
for the canonical Tier S/Tier F workflow and guardrail counters, and
[`cmd/treedb_vector_highqps_demo/README.md`](../cmd/treedb_vector_highqps_demo/README.md)
for demo output interpretation. The exact demo intentionally excludes document
materialization and quantized modes. Healthy exact no-document rows prove
`search_route_hnsw_search_pack/search=1`,
`hnsw_search_pack_active/search=1`, `docs_fetched/search=0`, zero fallback and
scratch counters, and no per-query collection open/setup in timed loops.

Additional collection vector APIs and diagnostics live in `TreeDB/collections`:

- `Collection.SearchVectorsExact` scans live rows and returns exact top-k
  results for cosine, squared L2, or inner-product distance.
- `Collection.BuildVectorIndex` builds an in-memory HNSW-style secondary index
  that returns candidates and exact-reranks final results.
- `BenchmarkCollectionVectorIndexGraphOnlySearch` is a benchmark-only engine
  comparison path that times TreeDB graph search without collection row fetches,
  JSON vector extraction, metadata filtering, or exact rerank.
- `BenchmarkCollectionVectorIndexBuild` and
  `BenchmarkCollectionVectorUSearchBuild` isolate index build cost from query
  latency for TreeDB and the optional USearch comparator.
- Declared quantized score planes use explicit `quantized_only` or
  `quantized_rerank` query modes with codec-generic `quantized_*` route,
  validation, and fail-closed counters. Scalar `scalar_u8` evidence is a
  behavior/storage baseline, not a current speedup claim; keep it separate from
  the exact no-document high-QPS path and demo. See
  [`TreeDB/docs/spec/quantized-vector-index.md`](docs/spec/quantized-vector-index.md).
- `VectorIndex.SaveSnapshot` and `Collection.LoadVectorIndexSnapshot` persist
  immutable index epochs under `vector_indexes/<collection>/<index>/` with a
  manifest plus checksum-verified node, edge, tombstone, and docmap files.
- `VectorIndexRangeFilter` restricts exact or ANN searches with an existing
  scalar secondary-index range. Selective filters use an `exact_filtered`
  strategy; broader filters use `ann_postfilter` and can fall back to exact
  filtered search if ANN underfills.
- `VectorIndex.Stats`, `VectorIndex.Search` traces, and `VectorIndex.CheckRecall`
  expose live/deleted counts, memory and disk bytes, persisted epoch state,
  snapshot dirtiness, candidate counts, selected strategy, exact fallback
  reason, rebuild duration, and recall-at-k.

Native `column_graph` benchmark tiers are documented in
`docs/guides/vector-search-typed-column.md#vector-search-benchmark-tiers`.
Use that matrix to distinguish core graph search, existing public response-owned
`Search`, collection-level caller-buffered `SearchVectorIndexWithBuffer`,
reusable-buffer no-document `SearchWithBuffer`, parallel callers, and explicit
document-fetch paths. Report `ops/sec` (`1e9 / ns/op`) with `ns/op`, `B/op`,
`allocs/op`, and vector/adjacency direct-vs-scratch counters.

Smoke benchmark:

```sh
TREEDB_VECTOR_BENCH_DOCS=1000 TREEDB_VECTOR_BENCH_DIMS=32 \
  go test ./TreeDB/collections -run '^$' \
  -bench 'BenchmarkCollectionVector(SearchExact|Index(Search|SearchInt8|FilteredSearch))$' \
  -benchtime=1x -count=1
```

Optional NumKong dot-product kernel: build with `-tags numkong` on hosts where
cgo and NumKong are available. Default builds, including cgo-enabled builds,
use the pure-Go kernel.

External comparison benchmark with local USearch bootstrap:

```sh
scripts/bench_vector_search_compare.sh
```

The script downloads and extracts a USearch Linux or macOS release package into
`/tmp` when `USEARCH_ROOT` is not set, configures cgo include/library paths, and
writes results under `/tmp/gomap_vector_search_compare_*`. The canonical current
production rows include
`BenchmarkCollectionVectorUSearchProductionCompare/TreeDB_CollectionSearchVectorIndexNoDocsOneShot`
as the response-owned collection convenience API on the cached no-document
`hnsw_search_pack_v1` route,
`.../TreeDB_CollectionSearchVectorIndexWithBuffer` as the collection-level
caller-owned result-buffer seam on the same warmed collection-owned prepared
cache (`open_searcher_calls/op=0`, `open_setup_in_timed_loop=0` in the timed
loop), plus `.../TreeDB_SearchWithBuffer*` versus `.../USearch_Search*`:
TreeDB's high-QPS comparison uses persisted `column_graph` through
`Collection.OpenVectorIndexSearcher` plus `VectorIndexSearcher.SearchWithBuffer`
with one searcher and one reusable buffer per worker, while USearch is a pure
in-memory cosine/f32 HNSW baseline.
Use `CPU_LIST=1,8` for c=1/c=8 evidence. Older `BenchmarkCollectionVectorIndex*`
rows are historical controls, not the current high-QPS production fast path;
with-documents `Collection.SearchVectorIndex` rows still include setup/open and
materialization cost and should not be presented as the no-document fast path.
See `TreeDB/docs/guides/vector-search-high-qps-collection-api.md` for the final
collection API boundary, and
`TreeDB/docs/guides/vector-search-benchmark-workflow.md` for the dated Tier S
snapshot from #2366/#2379, required fast-path counters, canonical Tier S/Tier F
commands, USearch platform path setup, artifact/profile capture notes, and the
zero-allocation vs response-owned vs materialization row boundary. Override
`USEARCH_VERSION`, `USEARCH_ROOT`, `TREEDB_VECTOR_BENCH_DOCS`,
`TREEDB_VECTOR_BENCH_DIMS`, `TREEDB_VECTOR_BENCH_M`,
`TREEDB_VECTOR_BENCH_EF_CONSTRUCTION`, `TREEDB_VECTOR_BENCH_EF_SEARCH`,
`TREEDB_VECTOR_BENCH_TOPK`, `TREEDB_VECTOR_BENCH_QUERIES`, `BENCHTIME`, `COUNT`,
and `CPU_LIST` to change the comparison shape. The USearch filtered Go binding
currently trips Go's cgo pointer checks on this toolchain; set
`RUN_UNSAFE_USEARCH_FILTERED=true` only when you explicitly want that benchmark
with `GODEBUG=cgocheck=0`.

The tiny BERT embedding demo in `../examples/vector_search/tiny_bert/` is a
caller-side fixture/demo; TreeDB core does not generate embeddings. Export it
with `--output-jsonl` and set `TREEDB_VECTOR_BENCH_JSONL` to run
`BenchmarkCollectionVectorTinyBERTFixture`.

Optional external engine baseline: build with `-tags usearch_bench` after
installing the USearch C library and headers for the host. The current comparison
benchmark runs the same deterministic synthetic vector/query generator through
TreeDB's persisted `column_graph` reusable-buffer path and through USearch's Go
bindings with cosine/f32 HNSW and matching `M`, `efConstruction`, `efSearch`,
`topK`, docs, dims, and `-cpu`/concurrency knobs:

The Go binding is intentionally kept as an indirect module dependency because it
is only imported by the optional `usearch_bench` build tag.

```sh
for cpu in 1 8; do
  TREEDB_VECTOR_BENCH_DOCS=10000 TREEDB_VECTOR_BENCH_DIMS=64 \
    TREEDB_VECTOR_BENCH_EF_SEARCH=128 \
    go test -tags usearch_bench ./TreeDB/collections -run '^$' \
    -bench '^BenchmarkCollectionVectorUSearchProductionCompare$' \
    -benchmem -benchtime=1x -count=1 -cpu="$cpu"
done
```

## Durability & Safety Notes

- Safe defaults keep WAL, fsync, and read checksums enabled; relax safety knobs via `Options.Durability` and `Options.ValueLog.ReadIntegrity`.
- In relaxed durability modes (`DurabilityWALOnRelaxed` / `DurabilityWALOffRelaxed`), `SetSync`/`WriteSync` are crash-consistent only (no fsync) and may not survive power loss.
- Page checksums are verified once and cached until the page is rewritten; use `VerifyOnRead` for paranoid always-verify behavior. `Options.ValueLog.ReadIntegrity = IntegritySkipChecksums` disables value-log CRC checks entirely.
- CRC checksums detect accidental corruption, not malicious tampering; use filesystem encryption/HMAC if your threat model includes adversarial disk access.
- `GetUnsafe` on a `Snapshot` and iterator `Key()`/`Value()` return short-lived views; use `Get`, `KeyCopy`, or `ValueCopy` for stable bytes.
- TreeDB does not provide encryption-at-rest or secure deletion; deleted data may remain on disk until compacted. Use OS/disk encryption for confidentiality.
- For production final disk footprint, use `db.CompactStorage(ctx, treedb.CompactStorageOptions{Mode: treedb.CompactStorageFull})` or `treemap compact <db-dir> -rw`. This is the best-practice policy path across `index.db`, `value_vlog`, `leaf_vlog`, and empty segment cleanup; platforms without online index vacuum report that phase as skipped while still compacting the value-log domains. For byte-minimized benchmark/VACUUM-equivalent claims, use `CompactStorageExhaustive` or `treemap compact <db-dir> -rw -mode exhaustive`, which also seals the current leaf generation and bypasses production leaf-pack yield thresholds.
- Low-level APIs such as `ValueLogGC`, `ValueLogRewriteOffline`, `LeafGenerationPack*`, and `VacuumIndex*` are maintenance building blocks. Do not manually chain them for benchmark storage numbers unless you are working on TreeDB internals.
- Optional guardrail: `Options.ValueLog.MaxRetainedBytes` emits a warning when retained value-log bytes exceed the threshold.
- Optional hard cap: `Options.ValueLog.MaxRetainedBytesHard` disables value-log pointers for new large values once retained bytes exceed the threshold.
- TreeDB is pre-alpha; public APIs and on-disk format may change without backward-compatibility guarantees.

### Durability Overview (Cached Mode)

The canonical durability-mode matrix is
`TreeDB/docs/spec/write-path-and-durability.md#1-durability-modes`.

In short: durable mode gives fsync durability at sync/checkpoint boundaries;
WAL-on relaxed mode is process-crash-oriented and is not a power-loss fsync
guarantee; WAL-off relaxed mode has no per-write journal replay and relies on
flush/checkpoint/close boundaries. Collection writes currently remain governed
by `TreeDB/docs/spec/collections-write-domain.md`; the target durable-at-ack
collection overlay is gated by
`TreeDB/docs/spec/collection-wal-durability-plan.md`; it is target behavior
after the collection WAL gate, not current behavior.

## Tuning (Cached Mode)

`treedb.Open` defaults to cached mode (memtable + journal + value log + background flush). The most important knobs:

- `Options.FlushThreshold` + `Options.MaxQueuedMemtables` (throughput vs. backlog/memory)
- Adaptive backpressure: `SlowdownBacklogSeconds`, `StopBacklogSeconds`, `MaxBacklogBytes`
- Cached-mode auto checkpointing: `BackgroundCheckpointInterval`, `BackgroundCheckpointIdleDuration`
- Background pruning: `PruneInterval`, `PruneMaxPages`, `PruneMaxDuration`
- Optional flush build parallelism: `FlushBuildConcurrency`
- Optional piggyback compaction toggle: `DisablePiggybackCompaction`
- Value-log retention guardrails: `ValueLog.MaxRetainedBytes`, `ValueLog.MaxRetainedBytesHard`
- Value-log compression mode: `ValueLog.Compression` (`off|block|dict|auto`) and `ValueLog.BlockCodec` (`snappy|lz4|zstd`)
- Full storage compaction: `db.CompactStorage(ctx, treedb.CompactStorageOptions{Mode: treedb.CompactStorageFull})` or `treemap compact <db-dir> -rw`; benchmark byte-minimized compaction: `CompactStorageExhaustive` or `treemap compact <db-dir> -rw -mode exhaustive`
- Index-only rebuild: `treedb.CompactIndex()` or `treedb.VacuumIndexOffline(opts)` when you explicitly want only `index.db` maintenance

`ValueLog.Compression` defaults to `auto` when unset.

Details: `docs/TREEDB_TUNING.md`.

### Exclusive Open (Process Lock)

TreeDB acquires an **exclusive** lock on `Options.Dir`. If another process has the database open,
`treedb.Open` returns `treedb.ErrLocked`.

## Testing

TreeDB includes a comprehensive test suite covering unit functionality, integration, fuzzing, and crash recovery.

### Unit & Integration Tests
```bash
go test -v ./...
```

### Fuzz Testing
Model-based fuzzing verifies consistency against a simple in-memory map map model.
```bash
go test -v ./db/fuzz_test.go
```

### Crash Simulation
The `verify_crash.sh` script compiles a stress tool, runs it, kills it (`kill -9`), and verifies database integrity upon restart.
```bash
./verify_crash.sh
```

## License
MIT
