# Vector-partition M4 evidence ledger

This ledger binds the M4 local router measurement to exact merge candidate
`e1c3a83fc897cf4b26659d28feeb39b1f230dafd` on base
`2292b334d5114f92859f4e0821f070837d719979`. It exercises the real persisted
M4 build, mapped open, exact representative oracle, and native HNSW search-pack
path. The successor that checks in this ledger changes evidence files only; no
production or benchmark code differs from the measured commit. The result is
explicitly `production_evidence=false`: it does not measure RPC, the
coordinator, Raft, shard search, or M8 end-to-end acceptance.

The reproducible fixture is `vector_partition_10k`: 10,000 vectors, 128
queries, 64 dimensions, cosine distance, seed 1, and checksum
`2413ef7c2f65a4b5ce8ecc3846f473fd85d337a87511538f962af7cdf6aec291`.
The run uses 16 balanced partitions, 16 representatives per partition, and 256
representatives in total.

## Build and storage

| metric | measured value |
| --- | ---: |
| build wall time | 264,848,565 ns |
| build CPU time | 3,481,298,000 ns |
| process peak RSS | 121,511,936 bytes |
| router asset | 174,752 bytes |
| mapped bytes after open | 174,752 bytes |
| heap-copy bytes after open | 0 bytes |
| router bytes/vector | 17.4752 |
| total Lloyd iterations | 188 |
| deterministic empty-cluster repairs | 0 |

The fixture partitions are exactly balanced at 625 source vectors per
partition. Build CPU and process RSS were available from the platform
accounting helpers. Peak RSS covers the whole command process rather than only
the build routine.

## Exact and approximate routing

Each search mode received its own candidate budget of 256, so both scored all
256 representatives for this evidence run. Consequently native HNSW introduced
zero recall loss relative to the exact representative oracle. The remaining
loss at fewer than 16 probes is coarsening loss from selecting fewer
partitions, not HNSW loss.

| probes | exact recall@10 | HNSW recall@10 | HNSW recall loss | exact p50/p95/p99 ns | HNSW p50/p95/p99 ns |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.08125 | 0.08125 | 0 | 28,373 / 34,223 / 42,956 | 84,424 / 95,598 / 122,952 |
| 2 | 0.153125 | 0.153125 | 0 | 23,382 / 29,003 / 44,681 | 78,623 / 88,776 / 105,093 |
| 4 | 0.2984375 | 0.2984375 | 0 | 23,809 / 33,709 / 40,905 | 77,471 / 89,436 / 115,945 |
| 8 | 0.56171875 | 0.56171875 | 0 | 23,153 / 33,923 / 52,037 | 78,320 / 102,613 / 115,441 |
| 16 | 1.0 | 1.0 | 0 | 24,195 / 34,013 / 39,468 | 78,892 / 96,459 / 122,746 |

Exact routing allocated 7,424--7,536 B/op and 13 allocs/op. Native HNSW
allocated 11,512--12,174 B/op and 21--21.125 allocs/op. Across 128
queries, each mode scored 32,768 candidates; the HNSW path visited 902,688
edges.

The full-candidate HNSW configuration is a correctness comparison, not a claim
that approximate routing is faster. Candidate-limited speed/recall curves and
100k/1M scale measurements remain future evidence. This local run also cannot
support production Raft or end-to-end latency, throughput, cancellation, or
failure claims.

## Reproduction and raw record

Run from the repository root:

```sh
OUT=$(mktemp -d /tmp/treedb_vector_partition_m4_XXXXXX)
GOWORK=off go run ./cmd/treedb_vector_partition_bench \
  -dataset testdata/vector_partition_10k \
  -stage router \
  -partitions 16 \
  -probes 1,2,4,8,16 \
  -top-k 10 \
  -seed 1 \
  -format json \
  -out "$OUT"
```

The canonical machine-readable record is
`vector-partition-m4-e1c3a83fc.raw.jsonl`, SHA-256
`2c020dbc8a1a38122d4fda006cfbcfdfcf999a8f10f0150bf128fa1bee80b1f5`.
It contains the five exact emitted JSON objects, one for each probe count,
including the command, fixture identity, candidate/base pair, timed boundary,
build accounting, allocations, and latency percentiles.
