# Vector partition M6 coordinator evidence ledger

Date: 2026-07-24

Measured code head: `248d97d34bc83df32fa15a5e2f69008089070d1b`

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
build stage. Wall times were 2:05.26-2:10.87, maximum RSS was
2,512,592-2,850,984 KiB, and every process recorded zero swaps.

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
| 1 | 0.093750 | 0.081250 | 1.814151 / 1.840368 / 1.841994 s | 0.550157 | 2,919,407,616 B | 0.103336% |
| 2 | 0.125000 | 0.156250 | 1.810421 / 1.852126 / 1.857404 s | 0.550967 | 2,664,603,648 B | 0.097163% |
| 4 | 0.281250 | 0.271875 | 1.821100 / 1.863298 / 1.892165 s | 0.547043 | 2,572,894,208 B | 0.087504% |
| 8 | 0.531250 | 0.503125 | 1.827916 / 1.858411 / 1.902618 s | 0.546464 | 2,663,866,368 B | 0.088509% |
| 16 | 1.000000 | 1.000000 | 1.876839 / 1.907333 / 1.913753 s | 0.532703 | 2,843,308,032 B | 0.067582% |

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
| persisted M4 router open | 1,808,601,227 ns | 1,841,458,629 ns | 1,846,993,251 ns |
| M4 router search | 9,379 ns | 13,560 ns | 13,821 ns |
| M1 placement/group planning | 1,209,108 ns | 1,407,995 ns | 1,872,505 ns |
| summed task queue | 293,225,247 ns | 372,887,849 ns | 377,161,432 ns |
| summed in-process dispatcher RPC | 336,462,347 ns | 445,125,913 ns | 459,511,670 ns |
| summed in-process residual | 12,482 ns | 22,553 ns | 24,847 ns |
| synthetic read proof | 0 ns | 0 ns | 0 ns |
| partition-local exact scan (512 samples) | 14,998,720 ns | 43,076,374 ns | 55,134,161 ns |
| response validation/materialization | 3,556 ns | 4,376 ns | 4,867 ns |
| stable-ID dedupe | 15,644 ns | 28,526 ns | 28,879 ns |
| top-k merge | 7,382 ns | 9,082 ns | 19,137 ns |
| end to end | 1,876,838,725 ns | 1,907,332,913 ns | 1,913,752,573 ns |

Membership-aware candidate planning scans the already-pinned generation
memberships and accounts for about 1.21 ms at p50 in the all-partition row.
This is included in the scoped coordinator-overhead ratio.

The all-partition harness reports 1,181,543,247.25 B/op and
2,067,933.1875 allocs/op for the complete local operation. Those figures
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
`248d97d34bc83df32fa15a5e2f69008089070d1b`. The fixture manifest from the
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
      GITHUB_SHA=248d97d34bc83df32fa15a5e2f69008089070d1b \
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
`/mnt/fast4tb/tmp/treedb_m6_1m_curve_final4_sDw3Hj`. The canonical JSON files retain
the full fixture checksum, exact command, base/head commits, stage
attribution, counters, and latency distributions.

| Probes | Canonical JSON SHA-256 | Resource log SHA-256 |
| ---: | --- | --- |
| 1 | `07007e96bb96d5372b2755d710e56a1627ed1eb8be4f8988aa5ccca9bf0a2236` | `6d5a678cc01886c2a1e8e757502c0042b1cc6a53a9a21c3a15e11df491edfb84` |
| 2 | `5b57cd23976f91655b1b36ade1cac9e03c7e5b894332c30b9f24baad1bc9ab78` | `3b65130683d41843fee4ce38caf0307fe8150d20d8b90a279d406839057ccf1c` |
| 4 | `16aacacbebbc8aef555e7b028b5522cb37376f5dbbcd14d9b99796fe4290e858` | `637fbf9dbff67aed6e8b2c2ee264cc97e1c7d672ab688b87eb6c877de514e46a` |
| 8 | `1353af5217ae14601a7e922e72b218def1399fbc7fb638fc9260db5be5092292` | `12045f484f5ca4dd77a883d423a36852a0413bd00d1aa474847dfcc5331295fa` |
| 16 | `25ddb7b720db41846967947a3769c676b137e5666f095e50f025f5f04266c505` | `3eee68e5f98330d7b425491fc0a7cc59dabdffdf44c24330d4cdcce08c1d9140` |

The checked-in ledger is a documentation-only successor to the measured code
head. It does not relabel the local artifacts as production evidence.
