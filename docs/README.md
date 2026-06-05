# Documentation Index

Welcome to the `gomap` documentation.

## 🚀 Start Here

- **[Getting Started](GETTING_STARTED.md)**: Prerequisites, local workflow, and minimal code examples.
- **[Repo Map & Architecture](REPO_MAP.md)**: Diagrams and directory layout explaining how the engines work.
- **[API Stability](API_STABILITY.md)**: What is safe to depend on (Stable Surface) vs what might change.

## 🌳 TreeDB

TreeDB is a persistent B+Tree with a high-throughput cached write-back layer.

- **[Canonical Spec](../TreeDB/docs/spec/README.md)**: Normative TreeDB spec set (architecture, storage format, write/recovery lifecycle, verification matrix).
- **[Concepts](TREEDB_CONCEPTS.md)**: High-level design (Pages, Value Log, COW Merge).
- **[Storage Format](TREEDB_STORAGE_FORMAT.md)**: On-disk layout, `ValuePtr`, and value-log lifecycle (GC/rewrite).
- **[Write Paths](TREEDB_WRITE_PATHS.md)**: WAL on/off semantics.
- **[Cached vs Backend](TREEDB_CACHED_VS_BACKEND.md)**: Legacy note (backend-only removed).
- **[Recovery](TREEDB_RECOVERY.md)**: Crash consistency and journal replay details.
- **[Profiles](TREEDB_PROFILES.md)**: High-level command-WAL, legacy-WAL, no-WAL, and benchmark option presets.
- **[Tuning](TREEDB_TUNING.md)**: Configuration knobs for performance.
- **[Typed-Storage Guides](../TreeDB/docs/guides/README.md)**: Collection layout quickstarts, typed-storage benchmark/profile guide, and vector typed-column guidance.

## ⚡ HashDB

HashDB is a high-performance, memory-mapped hash index optimized for random I/O.

- **[HashDB README](../HashDB/README.md)**: Focused HashDB overview, entry points, durability model, and benchmark snapshot target.
- **[Concepts](HASHDB_CONCEPTS.md)**: Design overview (Swiss Tables, Slab Log).
- **[Tuning](HASHDB_TUNING.md)**: Memory policies and performance configuration.
- **[Snapshots](HASHDB_SNAPSHOT.md)**: Export/Restore and consistent iteration.

## 📜 Contracts & Specifications

Behavioral guarantees for downstream systems (e.g., replication layers).

- **[Contracts Index](contracts/README.md)**
  - **[Durability](contracts/DURABILITY.md)**: `Set` vs `SetSync` guarantees.
  - **[Concurrency](contracts/CONCURRENCY.md)**: Writer/Reader models.
  - **[Locking](contracts/LOCKING.md)**: Cross-process exclusive locking.
  - **[Iteration](contracts/ITERATION.md)**: Iterator bounds and validity.
- **[Downstream Primitives](downstream/README.md)**: Building reliable systems on top of these engines.

## 📊 Benchmarking

- **[Benchmark Spec](BENCHMARK_SPEC.md)**: Methodology and test definitions.
- **[TreeDB Canonical Benchmark Runbook](benchmarks/treedb_canonical_benchmark_runbook.md)**: Standard TreeDB engine, collections, Mongo gateway, profiling, and reporting workflows.
- **[YCSB MongoDB / TreeDB Status](benchmarks/ycsb_mongodb_treedb_current.md)**: Current report index and rerun plan for MongoDB, `treedb-native`, and TreeDB Mongo gateway.
- **[TreeDB Collections Canonical Benchmark](benchmarks/collections_canonical_benchmark.md)**: Canonical TreeDB-vs-SQLite collection benchmark and maintenance-phase semantics.
- **[Two-Index Collection Insert Rerun](benchmarks/collections_insert_two_index_exhaustive_main_2026-06-04.md)**: Current TreeDB-vs-SQLite two-index insert throughput and exhaustive/VACUUM-equivalent compacted storage rows.
- **[TreeDB-vs-SQLite Collection Concurrency](benchmarks/collections_concurrency_main_2026-06-04.md)**: Concurrent collection read/lookup and mixed read/write rows with explicit `GOMAXPROCS` and SQLite connection-pool metadata.
- **[TreeDB Fast Mongo/Native Client-Shape Matrix](benchmarks/mongo_gateway_fast_client_matrix_2026-06-04.md)**: Standard fast-client Mongo gateway/native/direct benchmark contract and baseline interpretation.
- **[Unified Bench](../cmd/unified_bench/README.md)**: Usage guide for benchmark execution and artifact capture.
- **[BenchProf](../cmd/benchprof/README.md)**: Analysis tool for CPU/alloc/block/mutex profiles and ops/sec outputs.
- **[Redis Protocol Benchmarking](REDIS_PROTOCOL_BENCHMARKING.md)**: Preferred way to measure Redis wrapper throughput.
- **[Dev Notes](DEV_NOTES.md)**: Performance investigations and future optimization ideas.

## 🏚️ Legacy & Planning

- **[Improvement Plan](IMPROVEMENT_PLAN.md)**: Roadmap pointers.
- **[Agent Runbooks](agents/README.md)**: Implementation-focused notes (not normative contracts).
