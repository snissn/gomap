# Vector partition M6 coordinator evidence ledger

Date: 2026-07-24

Measured code head: `0ea66896889a96e005b8b8badfb0eb2a4bfcc1db`

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
fanout is rejected before opening the router, and every selected partition's
`ef_search` baseline is reserved before the remaining candidate bytes are
weighted by the pinned generation's home and overlap memberships. Candidate
weighting itself polls the effective request context.

Router open now observes that context throughout mapped-asset CRC and section
validation, row/reference/document scans, cancellable stable sorts, model
validation, digesting, and incremental canonical JSON emission. Cancellation
promptly releases the prepared view, and cleanup errors remain joined to the
primary failure. The benchmark memory model also includes retained membership,
router-vector, and seen-document state during the source/router build.

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
build stage. Wall times were 2:08.71-2:11.97, maximum RSS was
2,545,700-2,979,364 KiB, and every process recorded zero swaps.

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
| 1 | 0.093750 | 0.081250 | 1.872582 / 1.906417 / 1.933400 s | 0.532400 | 3,050,868,736 B | 0.075705% |
| 2 | 0.125000 | 0.156250 | 1.886421 / 1.945273 / 1.977914 s | 0.528652 | 2,739,494,912 B | 0.068769% |
| 4 | 0.281250 | 0.271875 | 1.897970 / 1.999101 / 2.041771 s | 0.524204 | 2,606,796,800 B | 0.066968% |
| 8 | 0.531250 | 0.503125 | 1.848452 / 1.888510 / 1.892579 s | 0.540180 | 2,688,237,568 B | 0.068589% |
| 16 | 1.000000 | 1.000000 | 1.897917 / 1.923833 / 1.925554 s | 0.527084 | 2,672,984,064 B | 0.066532% |

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
| persisted M4 router open | 1,826,786,522 ns | 1,855,809,474 ns | 1,863,167,242 ns |
| M4 router search | 9,174 ns | 10,290 ns | 17,899 ns |
| M1 placement/group planning | 1,215,509 ns | 1,356,009 ns | 1,389,207 ns |
| summed task queue | 298,117,642 ns | 395,190,071 ns | 480,365,625 ns |
| summed in-process dispatcher RPC | 372,510,136 ns | 509,485,543 ns | 530,713,887 ns |
| summed in-process residual | 12,026 ns | 20,080 ns | 21,744 ns |
| synthetic read proof | 0 ns | 0 ns | 0 ns |
| partition-local exact scan (512 samples) | 18,415,825 ns | 44,112,508 ns | 59,686,758 ns |
| response validation/materialization | 3,707 ns | 4,737 ns | 5,126 ns |
| stable-ID dedupe | 15,720 ns | 29,462 ns | 35,570 ns |
| top-k merge | 7,294 ns | 10,564 ns | 14,835 ns |
| end to end | 1,897,916,615 ns | 1,923,833,047 ns | 1,925,554,355 ns |

Membership-aware candidate planning scans the already-pinned generation
memberships and accounts for about 1.22 ms at p50 in the all-partition row.
This is included in the scoped coordinator-overhead ratio.

The all-partition harness reports 1,181,494,167.25 B/op and
2,067,928.75 allocs/op for the complete local operation. Those figures
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
`0ea66896889a96e005b8b8badfb0eb2a4bfcc1db`. The fixture manifest from the
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
      GITHUB_SHA=0ea66896889a96e005b8b8badfb0eb2a4bfcc1db \
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
`/mnt/fast4tb/tmp/treedb_m6_1m_curve_final2_uKQ7Zo`. The canonical JSON files retain
the full fixture checksum, exact command, base/head commits, stage
attribution, counters, and latency distributions.

| Probes | Canonical JSON SHA-256 | Resource log SHA-256 |
| ---: | --- | --- |
| 1 | `85c93525dabd270bbb6ae06d3d949257c9d3531e46f60f597af958a9802ba03c` | `a4c5f524e1cbe45c9222e67c4c1ffb2365bbc0d33f02e39db75ecc9416081f39` |
| 2 | `6aa04561634ee8b7e8811b125d3b3fa3c3acccc376f3b1294f2ab39f63c537d0` | `c2403604312e44c31f39a49a9e8af0fd7befc9b0de2922e2fa24f8f926994dc4` |
| 4 | `88728e928f5398e06e09dff182d31d6a92dc74c8ee0c99b14181089d231a715e` | `fa5783cff0dda70e37704e7a5e4bcd93f1a84142a88e12f446cec224afb89618` |
| 8 | `250b09371679e8c747857df6cfadddb3c72e7477779f0981e372451b96bb1582` | `fda783799adbdc9ec7079d1881d62cca6921f8a0298493bcdb036cf0ff6648a0` |
| 16 | `8555b33bf60cee427e036d74c4f94b1c960911d93deffa2e9910dd4fc3f3449b` | `3cb8d0eefc8e68b5253151bf1ef8777c8094b0973fcf2a14719ebc71eb633f71` |

The checked-in ledger is a documentation-only successor to the measured code
head. It does not relabel the local artifacts as production evidence.
