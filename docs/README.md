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
- **[Write Paths](TREEDB_WRITE_PATHS.md)**: command-WAL write semantics and legacy compatibility notes.
- **[Cached vs Backend](TREEDB_CACHED_VS_BACKEND.md)**: Legacy note (backend-only removed).
- **[Recovery](TREEDB_RECOVERY.md)**: Crash consistency, command-WAL replay, and legacy compatibility boundaries.
- **[Profiles](TREEDB_PROFILES.md)**: Public command-WAL profiles plus the explicit benchmark-only ceiling.
- **[Downstream Validation](TREEDB_DOWNSTREAM_VALIDATION.md)**: Adapter benchmark checklist for command-WAL counters, checkpoint separation, and load-window evidence.
- **[Ironbird Attribution](TREEDB_IRONBIRD_ATTRIBUTION.md)**: TreeDB lifecycle counters, Ironbird phase boundaries, and non-ABCI accounting contract for low-fanout benchmark rows.
- **[Tuning](TREEDB_TUNING.md)**: Configuration knobs for performance.
- **[Typed-Storage Guides](../TreeDB/docs/guides/README.md)**: Collection layout quickstarts, typed-storage benchmark/profile guide, and vector typed-column guidance.
- **[Document Service API](TREEDB_DOCUMENT_SERVICE_API.md)**: Pre-alpha Haystack-style HTTP/JSON contract for documents, declaration-time metadata filters, filtered keyword/hybrid retrieval, and `ann`/`exact` dense-vector routes.
- **[Python Document Service Client](../clients/python/treedb_client/README.md)**: Haystack-free sync Python client for the document service.
- **[TreeDB Haystack Integration](../clients/python/treedb_haystack/README.md)**: DocumentStore/retriever package and runnable examples for Haystack pipelines.

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
- **[TreeDB VectorDBBench Cohere 10M Lifecycle](benchmarks/treedb_vectordbbench_cohere10m_lifecycle_2026-09-01.md)**: Production-shaped durable load, deferred graph build, cold reopen, search, profiling, and teardown qualification.
- **[YCSB MongoDB / TreeDB Status](benchmarks/ycsb_mongodb_treedb_current.md)**: Current report index and rerun plan for MongoDB, `treedb-native`, and TreeDB Mongo gateway.
- **[TreeDB Collections Canonical Benchmark](benchmarks/collections_canonical_benchmark.md)**: Canonical TreeDB-vs-SQLite collection benchmark and maintenance-phase semantics.
- **[Two-Index Collection Insert Rerun](benchmarks/collections_insert_two_index_exhaustive_main_2026-06-04.md)**: Current TreeDB-vs-SQLite two-index insert throughput and exhaustive/VACUUM-equivalent compacted storage rows.
- **[TreeDB-vs-SQLite Collection Concurrency](benchmarks/collections_concurrency_main_2026-06-04.md)**: Concurrent collection read/lookup and mixed read/write rows with explicit `GOMAXPROCS` and SQLite connection-pool metadata.
- **[TreeDB Fast Mongo/Native Client-Shape Matrix](benchmarks/mongo_gateway_fast_client_matrix_2026-06-04.md)**: Standard fast-client Mongo gateway/native/direct benchmark contract and baseline interpretation.
- **[TreeDB Quantized Buffered Per-Row Profiling](benchmarks/treedb_quantized_buffered_profile_runbook.md)**: Isolated quantized search profile rows for downstream optimization gates.
- **[TreeDB `rabitq_1bit` Profile Gate](benchmarks/treedb_rabitq_1bit_profile_gate.md)**: Same-host RaBitQ baseline/candidate workflow, guardrails, profile artifacts, and no-promote rules.
- **[TreeDB Vector Crossover Benchmark Runbook](benchmarks/treedb_vector_crossover_runbook.md)**: Scale-matrix command contract for exact FP32, scalar_u8, RaBitQ, USearch, and pgvector crossover evidence.
- **[TreeDB Hybrid Search Benchmark Runbook](benchmarks/treedb_hybrid_search_runbook.md)**: Same-fixture text-only, vector-only, and hybrid executor benchmark commands, counters, profile capture, and #2506 evidence requirements.
- **[TreeDB Indexed Insertion/Search Benchmark](benchmarks/treedb_index_insert_search_benchmarks.md)**: Collection scalar/text/vector indexed insert, vector rebuild, and search benchmark commands, counters, #2564 original context, and #2589 optimized allocation closeout evidence.
- **[TreeDB Text v2 Contract Benchmarks](benchmarks/treedb_text_v2_contract_benchmarks.md)**: #2623/#2630 text-v2 matrix for isolated writes, search scale, concurrent serving, rewrite/merge lifecycle, counters, profiles, and required evidence.
- **[ClickHouse JSONBench 1M Part Audit](benchmarks/clickhouse_jsonbench_part_audit_2026-06-12.md)**: Physical byte map for the 1M ClickHouse JSONBench reference part used by TreeDB storage-parity work.
- **[TreeDB RaBitQ Performance Lane Closeout](../TreeDB/docs/spec/rabitq-performance-lane-closeout-2482.md)**: Final Sublane A promoted/no-promote evidence and later Sublane B outcome for the RaBitQ performance lane.
- **[TreeDB Quantized Prepared HNSW Closeout](../TreeDB/docs/spec/quantized-prepared-hnsw-closeout-2588.md)**: #2584/#2588 prepared fast-path stack evidence for scalar_u8 and RaBitQ quantized routes, exact FP32 guardrails, and #2591 10k x 768 rows.
- **[TreeDB Vector Search Closeout](../TreeDB/docs/spec/vector-search-closeout-2483.md)**: Exact FP32, scalar_u8, RaBitQ, and BRQ route-boundary/evidence index with #2487 snapshot caveats and #2494 crossover-pending status.
- **[Unified Bench](../cmd/unified_bench/README.md)**: Usage guide for benchmark execution and artifact capture.
- **[BenchProf](../cmd/benchprof/README.md)**: Analysis tool for CPU/alloc/block/mutex profiles and ops/sec outputs.
- **[TreeDB Checkpoint Wait Classification M1](TREEDB_CHECKPOINT_WAIT_CLASSIFICATION_M1_REPORT.md)**: #2946 immediate/settled checkpoint wait classification and artifact table.
- **[Redis Protocol Benchmarking](REDIS_PROTOCOL_BENCHMARKING.md)**: Preferred way to measure Redis wrapper throughput.
- **[Dev Notes](DEV_NOTES.md)**: Performance investigations and future optimization ideas.

## 🏚️ Legacy & Planning

- **[Improvement Plan](IMPROVEMENT_PLAN.md)**: Roadmap pointers.
- **[Agent Runbooks](agents/README.md)**: Implementation-focused notes (not normative contracts).
