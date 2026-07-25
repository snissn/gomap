# Vector partition M5 service-boundary evidence

Date: 2026-07-24  
Host: Linux/amd64, Intel Core i5-11400F, 12 logical CPUs  
Base: `294e61b5009b92009720ec166b4c3445828563bd`

## Small deterministic CI benchmark scope

This evidence validates the M5 timing/accounting harness and a warm in-process
service path over one 8-row, 2-dimensional exact-fallback M3 asset. It is
deliberately small so normal CI can run it. It is **not** the issue's required
1M-vector production HNSW acceptance shape, does not model network
serialization, and does not justify a distributed-readiness or 10% overhead
claim.

This small CI benchmark uses the real M5 request validation, M1 placement
resolution, generic routed-read coordinator interface, generation/partition
pin lifecycle, M3 no-document searcher, whole-response validation, counters,
and response proof. Its deterministic coordinator supplies production-shaped
proof fields; it does not perform a real Raft quorum exchange.

## Reproduction

```sh
GOWORK=off go test ./TreeDB/nativewire \
  -run '^$' \
  -bench '^BenchmarkVectorPartitionShardSearchServiceV1$' \
  -benchtime=500x \
  -count=5 \
  -benchmem
```

## Result

Five 500-request samples:

| ns/op | p50 ns | p95 ns | p99 ns | service QPS | B/op | allocs/op |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4,220 | 3,633 | 7,801 | 12,527 | 245,744 | 1,308 | 17 |
| 4,161 | 3,608 | 7,435 | 11,667 | 249,056 | 1,306 | 17 |
| 3,947 | 3,694 | 6,500 | 10,179 | 262,479 | 1,306 | 17 |
| 3,742 | 3,568 | 5,679 | 7,932 | 277,936 | 1,308 | 17 |
| 4,062 | 3,628 | 7,107 | 11,506 | 255,667 | 1,308 | 17 |

Stage averages reported by the same five runs:

| Stage | Observed range |
| --- | ---: |
| route/owner | 61.81–71.54 ns/op |
| read-index/apply interface | 64.42–100.6 ns/op |
| warm generation/partition cache | 405.4–478.1 ns/op |
| M3 exact-fallback search | 2,301–2,550 ns/op |
| response validation/copy | 52.04–62.77 ns/op |

Every request hit both the generation and partition caches and reported 8
candidates, 418 conservatively accounted response bytes, and 12 input bytes
for Go benchmark throughput accounting. Read-index/apply timing is separate
from M3 search.

## Acceptance boundary

The implementation provides the production-shaped bounded service contract,
stage attribution, p50/p95/p99/QPS harness, request/response byte accounting,
candidate/edge counters, mapped/heap/open accounting, and normal/race tests.

The required large-scale acceptance run was completed at clean final production
head `bd04ed74cdb788c7bd364e74a014cb94503137ba`. It used the declared
1,000,000-vector, 16-dimensional, 16-partition persistent native HNSW
generation, `top_k=10`, `ef_search=64`, and a three-node in-process
HashiCorp-Raft group with a live leader, committed command, and production
read-index evidence.

| Measurement | Accepted result |
| --- | ---: |
| cold service mean / p50 / p95 / p99 | 4.4316 s / 4.4045 s / 4.5047 s / 4.5047 s |
| warm service mean / p50 / p95 / p99 | 49.299 us / 39.388 us / 88.163 us / 188.688 us |
| warm service QPS | 20,284.49 |
| mean read-index/apply | 17.848 us |
| direct partition-local HNSW mean | 32.944 us |
| warm service-only overhead | 2.812 us |
| overhead ratio / gate | 8.5366% / **PASS** (limit 10%) |

The retained JSON report is
`/mnt/fast4tb/tmp/m5_artifacts_retry7_Pjdql5/vector_partition_m5_3200dafc3b75_294e61b5009b_bd04ed74c.json`
with SHA-256
`795b52288a5f9efbd112fde640f2b71bb10d73f132aa172cab69bd52a3580f21`.
This successor retains the corrected stage attribution and makes large
retained-manifest checkpoint re-encoding cancellable during cloning, sorting,
digest construction, validation, and binary emission. The stable JSON digest
and binary manifest formats remain unchanged. It also guards the hand-written
stable JSON digest against manifest-struct drift, fails closed if
caller-owned membership input changes between count and materialization, and
checks cancellation before allocating/copying response neighbors. Earlier
`1fead9fed...`, `3e0ae91c9...`, and `07ef127eb...` artifacts are historical
and are not used for acceptance.
Only the first cold sample is OS-page-cache cold; every cold sample reopens the
generation source and mapped search pack. The input M3 construction run
completed successfully, but its first 97 seconds were externally contaminated
and are not used as timing evidence. The acceptance claim is limited to the
reported local, in-process service shape and does not claim network
serialization performance.

## Persistent 1M input path

The M3 benchmark has the smallest bounded path needed to construct and retain
the declared persistent input database. This command prepares the input; the
separate real-Raft M5 runner below produces the acceptance result:

```sh
FIXTURE=$(mktemp -d /mnt/fast4tb/tmp/m5_fixture_XXXXXX)
OUT=$(mktemp -d /mnt/fast4tb/tmp/m5_artifacts_XXXXXX)
DB=$(mktemp -d /mnt/fast4tb/tmp/m5_db_XXXXXX)

GOWORK=off go run ./cmd/treedb_vector_partition_bench generate-fixture \
  -out "$FIXTURE" \
  -vectors 1000000 \
  -queries 1 \
  -dimensions 16 \
  -seed 1

GOWORK=off go run ./cmd/treedb_vector_partition_bench \
  -dataset "$FIXTURE" \
  -out "$OUT" \
  -partitions 16 \
  -probes 4 \
  -overlap 0 \
  -top-k 10 \
  -stage overlap,partition_index \
  -partition-repetitions 1 \
  -partition-pivots 8 \
  -partition-max-leaf-bucket 32 \
  -partition-degree 4 \
  -m3-persist-db "$DB"
```

`generate-fixture` writes an exclusive deterministic manifest, validates the
declared bounds before allocation, and binds the generated vector/query/truth
stream with its checksum. `-m3-persist-db` accepts only a missing or empty
explicit directory and only one overlap row, then records the absolute
directory in the M3 report. The default path remains temporary and is removed
after the benchmark.

## Real-Raft acceptance runner

`TreeDB/cmd/treedb_vector_partition_m5_bench` composes the retained M3 database
with a real local `HashicorpRaftProvider` leader and
`VectorPartitionShardSearchServiceV1`. It:

1. open collection `m3_partition_source`, index `embedding_graph`, generation
   1 from the M3 report;
2. start a real local Raft leader, commit and apply at least one command, and
   use `NewGroupRoutedReadIndexCoordinator` rather than a proof fixture;
3. issue cold and warm service requests against the native
   `hnsw_search_pack_v1` route;
4. emit p50/p95/p99, QPS, B/op, allocs/op, request/response bytes,
   candidates/edges, mapped/heap bytes, cache hits/misses, and independently
   attributed read-index/apply, open, search, and response-copy times;
5. compare service overhead excluding read-index/apply and HNSW search against
   the same generation's partition-local HNSW time and evaluate the 10% gate.

Run it against the retained M3 report/database:

```sh
GOWORK=off go run ./TreeDB/cmd/treedb_vector_partition_m5_bench \
  -m3-report "$M3_REPORT" \
  -db "$DB" \
  -out "$M5_REPORT"
```

The runner fails by default when the measured service-only overhead exceeds
10% or when the required measurements are unavailable. It writes a
machine-readable PASS or FAIL artifact before returning the gate error.
