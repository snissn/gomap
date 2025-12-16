# Documentation Index

Welcome to the `gomap` documentation.

## 🚀 Start Here

- **[Getting Started](GETTING_STARTED.md)**: Prerequisites, local workflow, and minimal code examples.
- **[Repo Map & Architecture](REPO_MAP.md)**: Diagrams and directory layout explaining how the engines work.
- **[API Stability](API_STABILITY.md)**: What is safe to depend on (Stable Surface) vs what might change.

## 🌳 TreeDB

TreeDB is a persistent B+Tree with an optional high-throughput cached layer.

- **[Concepts](TREEDB_CONCEPTS.md)**: High-level design (Pages, Slabs, COW Merge).
- **[Cached vs Backend](TREEDB_CACHED_VS_BACKEND.md)**: How to choose the right mode for your workload.
- **[Recovery](TREEDB_RECOVERY.md)**: Crash consistency and WAL replay details.
- **[Tuning](TREEDB_TUNING.md)**: Configuration knobs for performance.

## ⚡ HashDB

HashDB is a high-performance, memory-mapped hash index optimized for random I/O.

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
- **[Unified Bench](cmd/unified_bench/README.md)**: Usage guide for the primary benchmark suite.
- **[Dev Notes](DEV_NOTES.md)**: Performance investigations and future optimization ideas.

## 🏚️ Legacy & Planning

- **[Legacy Docs](legacy/README.md)**: Older design notes and historical context.
- **[Improvement Plan](IMPROVEMENT_PLAN.md)**: Roadmap pointers.