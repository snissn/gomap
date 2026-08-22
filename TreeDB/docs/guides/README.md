# TreeDB Typed-Storage Guides

These guides are user-facing, benchmark-adjacent companions to the canonical
TreeDB specs. They are intentionally practical: start from recommended layouts,
run small smoke commands, collect profiles, and interpret counters without
claiming production-stable APIs.

TreeDB is **pre-alpha**. Public collection APIs, typed-storage metadata, and
on-disk formats may change without migration guarantees. Rebuild benchmark DB
directories when moving between branches.

## Start here

- [Document chunking](document-chunking.md) — deterministic fixed-window and
  recursive chunkers, the `<parentID>#<ordinal>` child linkage convention, and
  re-chunk lifecycle semantics through `IngestChunkedDocument`.
- [Collections quickstart](collections-quickstart.md) — choose document-only,
  typed-row, typed-column, or hybrid layouts; insert fixture data; point-get;
  run a typed-column int64 aggregate; checkpoint/reopen when durability matters.
- [Typed-storage performance guide](typed-storage-performance.md) — workload-fit
  table, benchmark/profile commands, counter interpretation, troubleshooting,
  and performance-engineering playbooks.
- [JSONBench column-store experiment](jsonbench-columnstore-clickhouse-experiment.md)
  — reproduce the preferred 10M TreeDB column-store vs ClickHouse comparison
  and interpret the current results.
- [Vector typed-column guide](vector-search-typed-column.md) — place vector
  payloads in dense typed-column sections, keep metadata ownership explicit, and
  separate search/scoring from final document fetch.
- [High-QPS collection vector-search guide](vector-search-high-qps-collection-api.md)
  — choose between exact buffered no-document serving, quantized route states,
  response-owned convenience calls, explicit materialization, and reusable searchers
  without overclaiming beyond measured routes.
- [TreeDB vs USearch vector benchmark workflow](vector-search-benchmark-workflow.md)
  — reproduce the dated Tier S/Tier F comparison workflow, required fast-path
  counters, USearch bootstrap, artifact directories, profile capture, and the
  #2483 closeout link without blurring exact no-document rows with
  materialization or quantized evidence.
- [Hybrid search guide](hybrid-search.md) — create text, vector, and scalar
  indexes for `SearchHybrid`, issue bounded hybrid queries, interpret counters,
  and keep score-fusion/analyzer/vector-route/reranking caveats explicit.

## Deeper specs

- [Typed-storage naming](../spec/typed-storage-naming.md)
- [Typed-column adapter and durable publication](../spec/typed-column-adapter.md)
- [Typed-storage closeout evidence](../spec/typed-storage-closeout-1758.md)
- [Typed asset maintenance contract](../spec/typed-asset-maintenance-1788.md)
- [Storage format](../spec/storage-format.md)
- [Column graph native vector search](../spec/column-graph-native-vector-search.md)
