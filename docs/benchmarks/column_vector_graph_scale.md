# Column Vector Graph Scale Benchmarks

This track isolates column-backed `ColumnVectorGraph.SearchCosine` traversal
and scoring at larger row counts. The Go benchmark builds the graph before the
timed loop, treats the graph as immutable, warms worker-local scratch, and does
not fetch full documents.

Primary TreeDB in-process benchmark:

```sh
GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkColumnVectorGraphSearchCosineScale' \
  -benchmem \
  -benchtime 500ms \
  -count 5
```

The benchmark has these shapes:

- `rows_100k_dims_128_degree_16/serial`
- `rows_100k_dims_128_degree_16/parallel`
- `rows_1m_dims_128_degree_16/serial`
- `rows_1m_dims_128_degree_16/parallel`

For a lighter default run that only executes the 100k shape:

```sh
scripts/bench_column_vector_graph_scale.sh
```

Set `RUN_1M=true` to include the 1M shape:

```sh
RUN_1M=true scripts/bench_column_vector_graph_scale.sh
```

The reported `B/op` and `allocs/op` are for warmed hot-loop search. Graph
construction, column loading, index build, and document materialization are
outside the timed loop. `graph_payload_bytes` is an approximate in-memory
payload footprint for vectors, inverse norms, CSR adjacency, and compact
document IDs.

Optional comparators:

- `RUN_USEARCH=true scripts/bench_column_vector_graph_scale.sh` runs the
  existing in-process usearch search benchmark with setup outside the timed
  loop. Override `USEARCH_DOCS=1000000` for the 1M shape.
- `RUN_VECTOR_DB_COMPARE=true scripts/bench_column_vector_graph_scale.sh` runs
  the existing persistent vector-db comparison path. It defaults to
  `treedb,vectorlite`; set `COMPARE_BACKENDS=pgvector` or include `pgvector`
  only when a PostgreSQL+pgvector service or Docker is intentionally available.

Interpret comparator output carefully: `ColumnVectorGraph` measures in-process
graph traversal and vector scoring only, while PostgreSQL+pgvector includes
client/server query latency. The vector-db comparison reports insert, build,
reopen/load, validation, and search phases separately so build/load time is not
confused with query throughput.
