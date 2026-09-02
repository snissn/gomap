# Vector partition M6 coordinator evidence ledger

Date: 2026-07-25

Measured code head: `d7f1e0f399b1a563b91177e93281f7e91ee55728`

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

The measured head also includes the latest review repairs: partition-local
HNSW traversal polls the coordinator's caller context, impossible router
fanout is rejected before opening the router, and active-manifest loading keeps
the effective request context through the lifecycle authority. Every selected
partition reserves a conservative HNSW search-scratch floor derived from its
row count and `ef_search`; only the remaining candidate bytes are weighted by
the pinned generation's home and overlap memberships. Candidate weighting
itself polls the effective request context.

Router open now observes that context throughout mapped-asset CRC and section
validation, row/reference/document scans, cancellable stable sorts, model
validation, digesting, and incremental canonical JSON emission. Cancellation
promptly releases the prepared view, and cleanup errors remain joined to the
primary failure. The benchmark memory model also includes retained membership,
router-vector, and seen-document state during the source/router build.
Coordinator response validation requires every successful shard partial to
contain exactly `min(top_k, candidates)` neighbors, and checked aggregate edge
accounting rejects cross-task `uint64` overflow before accepting any result.
The same checked aggregation now covers response bytes, candidates, and
logical candidate bytes, including the per-response `candidates * 64`
conversion; failures publish neither partial counters nor a partial response.
The exact shard fallback keeps a bounded worst-first top-k heap and polls
cancellation while draining it instead of performing an uninterruptible
full-partition sort. Scratch preflight covers the coexisting heap/output
buffers, and a final poll precedes success-stat publication.

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
| modeled benchmark-owned peak / cap | 421,256,080 / 4,294,967,296 bytes |

The source degree is an explicit bounded benchmark-builder control. The normal
default remains 16; this host-safe evidence uses 4. The modeled peak excludes
TreeDB engine/index internals and Go runtime/GC metadata, so the Go memory
limit and externally measured process RSS remain separate evidence.

The benchmark's hard work planner rejects all five 1M rows in one process:
the combined modeled corpus visits exceed its 200,000,000-unit cap. Each probe
point therefore ran as an isolated process with identical fixture, source,
seed, topology, code, and runtime controls. Every process rebuilt the same
persisted source/router; each result's query timings exclude that reported
build stage. Wall times were 2:08.19-2:16.03, maximum RSS was
2,526,816-2,865,528 KiB, and every process recorded zero swaps.

The cancellable stable-sort helper uses bounded scratch proportional to the
slice being sorted. At the absolute format caps, allocator retention across
the sequential node, representative, and membership sorts could temporarily
retain about 137 MiB beyond the prior in-place sort. This evidence uses 64
representatives, where that scratch is negligible, and makes no maximum-cap
router-open latency claim.

## Recall and latency curve

All rows completed 32 queries with zero retries, redirects, cancellations, or
failures.

| Probes | Recall@1 | Recall@10 | End-to-end p50 / p95 / p99 | QPS | Peak RSS | Placement + dedupe + merge / total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.093750 | 0.081250 | 1.844236 / 1.875103 / 1.883244 s | 0.541541 | 2,689,904,640 B | 0.076898% |
| 2 | 0.125000 | 0.156250 | 1.856194 / 1.913428 / 1.914313 s | 0.538569 | 2,934,300,672 B | 0.068903% |
| 4 | 0.281250 | 0.271875 | 1.845404 / 1.879081 / 1.889856 s | 0.541083 | 2,612,822,016 B | 0.064837% |
| 8 | 0.531250 | 0.503125 | 1.864198 / 1.916773 / 1.917950 s | 0.535676 | 2,625,609,728 B | 0.069350% |
| 16 | 1.000000 | 1.000000 | 1.938288 / 2.063461 / 2.082026 s | 0.512312 | 2,587,459,584 B | 0.067219% |

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
| persisted M4 router open | 1,871,976,114 ns | 1,999,620,743 ns | 2,006,136,130 ns |
| M4 router search | 9,628 ns | 14,315 ns | 19,488 ns |
| M1 placement/group planning | 1,233,170 ns | 1,494,447 ns | 1,972,001 ns |
| summed task queue | 330,719,352 ns | 390,050,962 ns | 390,155,513 ns |
| summed in-process dispatcher RPC | 358,638,969 ns | 516,027,231 ns | 521,235,977 ns |
| summed in-process residual | 12,408 ns | 21,648 ns | 24,395 ns |
| synthetic read proof | 0 ns | 0 ns | 0 ns |
| partition-local exact scan (512 samples) | 19,449,324 ns | 43,020,713 ns | 59,143,502 ns |
| response validation/materialization | 4,388 ns | 5,926 ns | 6,447 ns |
| stable-ID dedupe | 17,314 ns | 28,661 ns | 28,946 ns |
| top-k merge | 7,734 ns | 14,382 ns | 18,555 ns |
| end to end | 1,938,287,590 ns | 2,063,460,569 ns | 2,082,026,123 ns |

Membership-aware candidate planning scans the already-pinned generation
memberships and accounts for about 1.21 ms at p50 in the all-partition row.
This is included in the scoped coordinator-overhead ratio.

The all-partition harness reports 1,181,552,008 B/op and
2,067,936.625 allocs/op for the complete local operation. Those figures
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
`d7f1e0f399b1a563b91177e93281f7e91ee55728`. The fixture manifest from the
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
      GITHUB_SHA=d7f1e0f399b1a563b91177e93281f7e91ee55728 \
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
`/mnt/fast4tb/tmp/treedb_m6_1m_curve_final5_45Shnd`. The canonical JSON files retain
the full fixture checksum, exact command, base/head commits, stage
attribution, counters, and latency distributions.

| Probes | Canonical JSON SHA-256 | Resource log SHA-256 |
| ---: | --- | --- |
| 1 | `804ddf634f8afd05f5ca4d70154de0fc14aa972ba66ce81e9399287177cf9465` | `c1f01a48ff7076a654164def155396d34e6813850a142fead46a7b2caf24d021` |
| 2 | `3b0e7db5c33668dd68b2b89ab47663ba821c21cca1f930584cde5aa9f9515a81` | `50ba171de69c0f9c5e615140154ea22fa8e57125d3d9518dfe74b267f1c1d38c` |
| 4 | `96f1a915fde33bc5a70e4bfbcb8782374356a12a4886e2fdac01ee7294c7383f` | `e55327a84562279058bfcdb0928f5fc01773d1c006b4e069b2d6721ff5f9f668` |
| 8 | `7a2d65bcff053ab60750551915dc4b882c00ecfb256afe9fa90de2e18fb83f5b` | `1342c2ed4a8b3bcd5fd2d0c4bc86b57acd9db54f5582bec1714b72d6428781e2` |
| 16 | `7f1685a08b8905a30facf60d288d6ecdeac148cc31a2a9b5e0299bc4226a4ea8` | `b528ff4b837036b40e437667662c108cabef2d2f710a8caeea4ac4e21471ec8e` |

The checked-in ledger is a documentation-only successor to the measured code
head. It does not relabel the local artifacts as production evidence.
