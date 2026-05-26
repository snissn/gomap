# TreeDB Typed-Storage Guides

These guides are user-facing, benchmark-adjacent companions to the canonical
TreeDB specs. They are intentionally practical: start from recommended layouts,
run small smoke commands, collect profiles, and interpret counters without
claiming production-stable APIs.

TreeDB is **pre-alpha**. Public collection APIs, typed-storage metadata, and
on-disk formats may change without migration guarantees. Rebuild benchmark DB
directories when moving between branches.

## Start here

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

## Deeper specs

- [Typed-storage naming](../spec/typed-storage-naming.md)
- [Typed-column adapter and durable publication](../spec/typed-column-adapter.md)
- [Typed-storage closeout evidence](../spec/typed-storage-closeout-1758.md)
- [Typed asset maintenance contract](../spec/typed-asset-maintenance-1788.md)
- [Storage format](../spec/storage-format.md)
- [Column graph native vector search](../spec/column-graph-native-vector-search.md)
