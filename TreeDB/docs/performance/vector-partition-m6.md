# Vector partition M6 coordinator evidence ledger

Date: 2026-07-24

Measured code head: `09b2f0cfc5df95a9b918738c4237172cafff237b`

Base M5 merge: `93f48763467aefdf9b45ba0f7d22847f7f0c66ed`

Status: experimental local-service evidence, not production evidence

## Claim boundary

These results compose the real persisted M4 router and production M6
coordinator with an in-process dispatcher that implements the M5 request and
response contract. They validate bounded planning/fanout, strict response
validation, stable-ID dedupe/merge, stage attribution, the requested
lower-probe recall/latency curve, and exact all-partition FP32 cosine
score/ID parity at the 1M-vector shape.

The measured head fixes an invalid earlier evidence scorer. The old dispatcher
and its global oracle both used raw dot product after converting fixture
vectors from float64 to float32. That could hide rank changes caused by the
converted vectors no longer having exact unit norm. The measured implementation
precomputes the same FP32 vector norms as the production exact searcher and
divides both local and oracle scores by query and vector norms. A regression
uses collinear vectors with different magnitudes, for which raw dot product
would reverse the required stable-ID tie order.

These rows do **not** measure a network transport, remote service,
serialization, production Raft read-index/apply, or a production M5 service.
Every row records:

- `result_kind=coordinator_local_service_simulation`;
- `production_evidence=false`;
- `transport=in_process_transport_neutral_m5_contract_simulation`;
- `network=in_process_no_production_network`;
- `read_proof=synthetic_local_proof_not_measured`;
- `experimental=true`.

The synthetic proof has production-shaped fields so M6's strict proof
validator executes, but `read_proof_timing` is zero and supports no Raft
performance or consistency claim. Real transport and multi-group acceptance
remain M8 work.

## Shape and provenance

| Field | Value |
| --- | --- |
| fixture | `deterministic_1000000` |
| checksum | `71239d1335ddd724835d415f57acae7f8bb36a6af52642d1e710392a883b2d6f` |
| generator / arithmetic | `treedb_vector_partition_fixture_v2` / `ieee754_binary64_explicit_fma_v1` |
| corpus / queries / dimensions | 1,000,000 / 32 / 16 |
| metric / seed | cosine / 1 |
| partitions / probes / overlap | 16 / 1, 2, 4, 8, 16 / 0 |
| `top_k` | 10 |
| source `column_graph` HNSW degree | 4 |
| router representatives | 4 per partition, 64 total |
| `GOMAXPROCS` | 4 |
| Go memory limit | 17,179,869,184 bytes (16 GiB) |
| Go / platform | go1.26.0 / amd64/linux |
| modeled benchmark-owned peak / cap | 360,006,080 / 4,294,967,296 bytes |

The source degree is an explicit bounded benchmark-builder control. The normal
default remains 16; this host-safe evidence uses 4. The modeled peak excludes
TreeDB engine/index internals and Go runtime/GC metadata, so the Go memory
limit and externally measured process RSS remain separate evidence.

The benchmark's hard work planner rejects all five 1M rows in one process:
the combined modeled corpus visits exceed its 200,000,000-unit cap. Each probe
point therefore ran as an isolated process with identical fixture, source,
seed, topology, code, and runtime controls. Every process rebuilt the same
persisted source/router; each result's query timings exclude that reported
build stage. Wall times were 2:09.06-2:15.79, maximum RSS was
2,468,340-2,732,184 KiB, and every process recorded zero swaps.

## Recall and latency curve

All rows completed 32 queries with zero retries, redirects, cancellations, or
failures.

| Probes | Recall@1 | Recall@10 | End-to-end p50 / p95 / p99 | QPS | Peak RSS | Placement + dedupe + merge / total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.093750 | 0.081250 | 1.853718 / 1.884922 / 1.889115 s | 0.538560 | 2,797,756,416 B | 0.000479% |
| 2 | 0.125000 | 0.156250 | 1.859095 / 1.890864 / 1.894958 s | 0.536998 | 2,797,412,352 B | 0.000732% |
| 4 | 0.281250 | 0.271875 | 1.866398 / 1.901220 / 1.908622 s | 0.535203 | 2,527,580,160 B | 0.000940% |
| 8 | 0.531250 | 0.503125 | 1.877921 / 1.915082 / 1.951255 s | 0.531685 | 2,673,303,552 B | 0.001480% |
| 16 | 1.000000 | 1.000000 | 1.919648 / 1.993153 / 1.999855 s | 0.519251 | 2,752,823,296 B | 0.002332% |

The all-partition row passed both exact global gates:

| Gate | Result |
| --- | ---: |
| exact all-partition parity checked | true |
| exact all-partition parity passed | true |
| exact cosine recall@1 / recall@10 | 1.0 / 1.0 |
| scoped coordinator-overhead ceiling | <15%: **PASS** |

The overhead ratio is exactly
`(placement + dedupe + merge) / coordinator total` over the 32 query samples.
It is a narrow M6 coordinator-bookkeeping gate. It is not a production
network-overhead ratio, and the large router-open component in this local
shape is part of its denominator.

Each query reopens and verifies the pinned persisted M4 router. Router open
therefore dominates end-to-end latency at every probe count. This is visible
rather than hidden in shard-search or coordinator bookkeeping.

## All-partition stage timing

Router, placement, queue, dispatcher, response, dedupe, merge, and end-to-end
percentiles use 32 per-query samples. Queue and dispatcher RPC values are sums
over each query's 16 fanout tasks, so they can exceed elapsed end-to-end time
under concurrency. Shard search uses 512 individual task samples.
`network_timing` is only the in-process dispatcher residual
`rpc_elapsed - reported_service_total`; it is not network measurement.

| Stage | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| persisted M4 router open | 1,856,425,246 ns | 1,919,938,975 ns | 1,929,018,508 ns |
| M4 router search | 8,329 ns | 10,914 ns | 12,658 ns |
| M1 placement/group planning | 13,925 ns | 23,697 ns | 24,605 ns |
| summed task queue | 299,365,690 ns | 386,719,112 ns | 389,862,428 ns |
| summed in-process dispatcher RPC | 342,290,431 ns | 461,302,567 ns | 493,218,342 ns |
| summed in-process residual | 12,235 ns | 22,624 ns | 30,134 ns |
| synthetic read proof | 0 ns | 0 ns | 0 ns |
| partition-local exact scan (512 samples) | 16,512,993 ns | 43,133,950 ns | 59,553,095 ns |
| response validation/materialization | 4,215 ns | 5,321 ns | 13,094 ns |
| stable-ID dedupe | 20,192 ns | 31,372 ns | 34,293 ns |
| top-k merge | 7,454 ns | 12,624 ns | 19,598 ns |
| end to end | 1,919,648,266 ns | 1,993,153,134 ns | 1,999,854,869 ns |

The all-partition harness reports 1,181,549,971.25 B/op and
2,067,595.65625 allocs/op for the complete local operation. Those figures
include the deliberately exhaustive in-process partition scans and their
response construction. They are evidence for this simulator shape, not an
allocation target for a production remote M5 implementation.

## All-partition counters

The 32 queries selected all 16 partitions, with one local owner group per
partition:

| Counter | Aggregate | Per query |
| --- | ---: | ---: |
| selected partitions | 512 | 16 |
| selected groups | 512 | 16 |
| M5 requests / RPCs | 512 / 512 | 16 / 16 |
| retries / redirects | 0 / 0 | 0 / 0 |
| cancellations / failures | 0 / 0 | 0 / 0 |
| query bytes | 2,048 | 64 |
| request bytes | 281,088 | 8,784 |
| response bytes | 329,728 | 10,304 |
| candidates | 32,000,000 | 1,000,000 |
| logical candidate bytes | 2,048,000,000 | 64,000,000 |
| edges | 0 | 0 |
| merge entries | 5,120 | 160 |
| duplicates / score disagreements | 0 / 0 | 0 / 0 |

`candidate_bytes` is the coordinator's conservative logical accounting at 64
bytes per reported candidate; it is not peak live memory. The local dispatcher
uses an exact FP32 cosine scan, so every all-partition query reports the
complete 1M candidate corpus and zero HNSW edges.

## Reproduction

Run from the repository root at exact head
`09b2f0cfc5df95a9b918738c4237172cafff237b`. The fixture manifest from the
prior deterministic generation is retained at
`/mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/fixture`. It can instead be
regenerated with `generate-fixture` using 1,000,000 vectors, 32 queries,
16 dimensions, and seed 1.

Run each row separately so the modeled-work safety cap remains authoritative:

```sh
RUN_ROOT=$(mktemp -d /mnt/fast4tb/tmp/treedb_m6_1m_curve_XXXXXX)
for PROBES in 1 2 4 8 16; do
  ROW_ROOT="$RUN_ROOT/probe_$(printf '%02d' "$PROBES")"
  mkdir -p "$ROW_ROOT/evidence" "$ROW_ROOT/tmp"
  /usr/bin/time -v -o "$ROW_ROOT/resource_usage.txt" \
    env \
      TMPDIR="$ROW_ROOT/tmp" \
      BASE_SHA=93f48763467aefdf9b45ba0f7d22847f7f0c66ed \
      GITHUB_SHA=09b2f0cfc5df95a9b918738c4237172cafff237b \
      GITHUB_EVENT_PATH= \
      GOMAXPROCS=4 \
      GOMEMLIMIT=16GiB \
      GOWORK=off \
    go run ./cmd/treedb_vector_partition_bench \
      -dataset /mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/fixture \
      -stage distributed_simulation_or_cluster \
      -partitions 16 \
      -probes "$PROBES" \
      -overlap 0 \
      -top-k 10 \
      -seed 1 \
      -router-representatives 4 \
      -source-hnsw-degree 4 \
      -max-vectors 1000000 \
      -max-fixture-bytes 4294967296 \
      -format json \
      -out "$ROW_ROOT/evidence" \
    | tee "$ROW_ROOT/stdout.json"
done
```

Each artifact records `gomaxprocs=4`,
`go_memory_limit_bytes=17179869184`, and `source_hnsw_degree=4`; a row missing
those positive runtime controls fails result validation.

## Retained records

All records are under
`/mnt/fast4tb/tmp/treedb_m6_1m_curve_KHYYDr`. The canonical JSON files retain
the full fixture checksum, exact command, base/head commits, stage
attribution, counters, and latency distributions.

| Probes | Canonical JSON SHA-256 | Resource log SHA-256 |
| ---: | --- | --- |
| 1 | `b67e5fb0eab368772cfe539581376174ce4241b0dc7515b3080299a0aef67f8c` | `df7f0c43d1c6b18b9a1c72dd02b62bdb22eea50163d418faebaa8a5cbc3f5fb5` |
| 2 | `9badbddc408d8dab3adb32c9b2a8b7fd71474e3d8586f5a0475187a3d6ace560` | `85aecd33ecb954dbc184be4f29245f1ca4e8923eb3901a9c5c07c965e61f53c8` |
| 4 | `29c3a0ef67e4eb64942d7d3eeddb31e0b00836bbfb02c940701b58909acf6e03` | `73a481b3994ff93a0a9f60ff48b91e683f22dceef9d7f7ceb2ea5e2b2e3a09d3` |
| 8 | `d4d76a7dcd6b961758e51df3b7b939befe4f51a6407067e179336d8c712c7ce6` | `e6aac66ebc6a9b24e34baff5f6663301d1c953df618e7c01ec4fa5dfae983666` |
| 16 | `156411fd63113284d7f31d0efab71a555fb94443a9a3488757ca5e37952afcde` | `c76e3e71c48424ddbf06f08c4770afff840d13678e45ebcd824d5d1bc1034f5b` |

The checked-in ledger is a documentation-only successor to the measured code
head. It does not relabel the local artifacts as production evidence.
