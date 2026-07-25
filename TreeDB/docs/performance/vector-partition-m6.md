# Vector partition M6 coordinator evidence ledger

Date: 2026-07-24

Measured code head: `95e5a7994a0852c895b99f23a94c42232e31c20c`

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
build stage. Wall times were 2:05.49-2:11.44, maximum RSS was
2,574,076-2,743,576 KiB, and every process recorded zero swaps.

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
| 1 | 0.093750 | 0.081250 | 1.821166 / 1.862120 / 1.867623 s | 0.548112 | 2,809,421,824 B | 0.075865% |
| 2 | 0.125000 | 0.156250 | 1.814909 / 1.848562 / 1.850521 s | 0.550172 | 2,635,853,824 B | 0.066580% |
| 4 | 0.281250 | 0.271875 | 1.835307 / 2.008795 / 2.057623 s | 0.538461 | 2,744,344,576 B | 0.064953% |
| 8 | 0.531250 | 0.503125 | 1.837858 / 1.875855 / 1.901427 s | 0.542214 | 2,687,205,376 B | 0.069304% |
| 16 | 1.000000 | 1.000000 | 1.880975 / 1.921939 / 1.928483 s | 0.531431 | 2,734,272,512 B | 0.065867% |

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
| persisted M4 router open | 1,811,766,948 ns | 1,862,130,014 ns | 1,866,282,998 ns |
| M4 router search | 9,915 ns | 12,555 ns | 22,551 ns |
| M1 placement/group planning | 1,202,158 ns | 1,289,491 ns | 1,300,540 ns |
| summed task queue | 294,417,439 ns | 400,703,508 ns | 461,809,813 ns |
| summed in-process dispatcher RPC | 375,112,234 ns | 485,997,752 ns | 541,235,305 ns |
| summed in-process residual | 10,928 ns | 24,117 ns | 25,918,421 ns |
| synthetic read proof | 0 ns | 0 ns | 0 ns |
| partition-local exact scan (512 samples) | 18,765,554 ns | 45,927,191 ns | 60,690,934 ns |
| response validation/materialization | 3,586 ns | 4,466 ns | 4,607 ns |
| stable-ID dedupe | 16,517 ns | 35,901 ns | 38,221 ns |
| top-k merge | 7,640 ns | 9,200 ns | 11,282 ns |
| end to end | 1,880,975,128 ns | 1,921,939,368 ns | 1,928,483,342 ns |

Membership-aware candidate planning scans the already-pinned generation
memberships and accounts for about 1.20 ms at p50 in the all-partition row.
This is included in the scoped coordinator-overhead ratio.

The all-partition harness reports 1,181,489,421 B/op and
2,067,926.8125 allocs/op for the complete local operation. Those figures
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
`95e5a7994a0852c895b99f23a94c42232e31c20c`. The fixture manifest from the
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
      GITHUB_SHA=95e5a7994a0852c895b99f23a94c42232e31c20c \
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
`/mnt/fast4tb/tmp/treedb_m6_1m_curve_final3_H3zIbY`. The canonical JSON files retain
the full fixture checksum, exact command, base/head commits, stage
attribution, counters, and latency distributions.

| Probes | Canonical JSON SHA-256 | Resource log SHA-256 |
| ---: | --- | --- |
| 1 | `f86d54785c2499921f66e46f8231f778ad3328149c9e642f300de17150e8cd45` | `94217a06e6efcabda83a99e4c05a5ff0e0dd8d858b4fa57759c53985d462eaef` |
| 2 | `f82231636bbcaffc276ba2a3f2c217639e1f13fe822d3d798f3f612f3a518004` | `58f0e0939e5d9b185b92ba92070edb4e5dae7be164055efa155349cb2f36b05d` |
| 4 | `c656fd1157f2fe467b87de05ce9759f1835abe0f7e5b7f163bd3fc0db32d2f1d` | `d58bab0f5ee39caf0b932cb1a871067aa0eea83af42d166263011ac712f3b443` |
| 8 | `132f68ede6e0e0eead62a3e35cf8ecea4b52ee8a4e3edc93bf220749cfed6429` | `5a2c567be3b930095d276b2f55f5ee51a87cb2c356be8c7e2d858cef369b3bfc` |
| 16 | `40802e040c4b6b05d96613ccfe0f606daa3f1a336e76263bdbb0561a9f3da2b1` | `508f265abf7e50b960cab2b357d0c7925040f8810ea660a4791c92f58bfcecdc` |

The checked-in ledger is a documentation-only successor to the measured code
head. It does not relabel the local artifacts as production evidence.
