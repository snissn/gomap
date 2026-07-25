# Vector partition M6 coordinator evidence ledger

Date: 2026-07-24

Measured code head: `89f618862e3cab8f92df6a4a476899a7227e326e`

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
fanout is rejected before opening the router, and shard candidate reservations
are weighted by the pinned generation's home and overlap memberships instead
of being divided equally by partition count. Router validation and canonical
digesting also observe the coordinator context throughout bounded scans and
incremental JSON emission, so a canceled maximum-size model does not retain the
collection storage barrier until a monolithic marshal completes.

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
build stage. Wall times were 2:07.63-2:21.29, maximum RSS was
2,388,188-2,618,268 KiB, and every process recorded zero swaps.

## Recall and latency curve

All rows completed 32 queries with zero retries, redirects, cancellations, or
failures.

| Probes | Recall@1 | Recall@10 | End-to-end p50 / p95 / p99 | QPS | Peak RSS | Placement + dedupe + merge / total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.093750 | 0.081250 | 1.835832 / 1.870110 / 1.872609 s | 0.544210 | 2,445,504,512 B | 0.079898% |
| 2 | 0.125000 | 0.156250 | 1.845797 / 1.888112 / 1.897177 s | 0.541133 | 2,548,928,512 B | 0.061851% |
| 4 | 0.281250 | 0.271875 | 1.840274 / 1.879044 / 1.890996 s | 0.542482 | 2,655,657,984 B | 0.061047% |
| 8 | 0.531250 | 0.503125 | 1.900081 / 1.992840 / 2.010155 s | 0.524501 | 2,671,886,336 B | 0.062923% |
| 16 | 1.000000 | 1.000000 | 1.969557 / 2.030505 / 2.210365 s | 0.504886 | 2,681,106,432 B | 0.065462% |

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
| persisted M4 router open | 1,897,002,517 ns | 1,954,877,773 ns | 2,151,266,256 ns |
| M4 router search | 8,792 ns | 12,443 ns | 13,459 ns |
| M1 placement/group planning | 1,240,218 ns | 1,463,268 ns | 1,611,268 ns |
| summed task queue | 323,750,687 ns | 409,781,798 ns | 410,920,730 ns |
| summed in-process dispatcher RPC | 374,867,651 ns | 512,897,811 ns | 530,874,248 ns |
| summed in-process residual | 14,161 ns | 24,978 ns | 33,919 ns |
| synthetic read proof | 0 ns | 0 ns | 0 ns |
| partition-local exact scan (512 samples) | 20,910,021 ns | 46,022,259 ns | 60,173,645 ns |
| response validation/materialization | 4,874 ns | 5,381 ns | 5,538 ns |
| stable-ID dedupe | 16,550 ns | 29,756 ns | 29,801 ns |
| top-k merge | 7,727 ns | 11,619 ns | 12,335 ns |
| end to end | 1,969,557,412 ns | 2,030,505,438 ns | 2,210,364,636 ns |

Membership-aware candidate planning scans the already-pinned generation
memberships and accounts for about 1.24 ms at p50 in the all-partition row.
This is included in the scoped coordinator-overhead ratio.

The all-partition harness reports 1,181,598,940.5 B/op and
2,068,044.5625 allocs/op for the complete local operation. Those figures
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
`89f618862e3cab8f92df6a4a476899a7227e326e`. The fixture manifest from the
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
      GITHUB_SHA=89f618862e3cab8f92df6a4a476899a7227e326e \
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
`/mnt/fast4tb/tmp/treedb_m6_1m_curve_digest_exact_LD4fDA`. The canonical JSON files retain
the full fixture checksum, exact command, base/head commits, stage
attribution, counters, and latency distributions.

| Probes | Canonical JSON SHA-256 | Resource log SHA-256 |
| ---: | --- | --- |
| 1 | `61565ecbb3d61c3e4dfbcec19fff3a1dce05ec1049c3ce850eb5338700d939e0` | `fa5df98f6980fffe0489de4c814fcb42cfd90c5daa48aed90fbeeb1afef2710e` |
| 2 | `0300a65d629f5b350abcdb8b958945c6521b24da656a51575d63e8d4e36234a9` | `e1222a481a692c9e8ac91eb72aecb473a6e0368acc2d8da88f19bd7065f5ce6f` |
| 4 | `3cf480af2a3be08ca178ff273e14a0c62d33a1b7f8f245e9f95d2babbd6af885` | `91fdc881dd95975c4409ee8d61bfc899970c88f35e64b1732a5f74e73a8cc4d2` |
| 8 | `c33df3ba0823eaf24bf133b9889354b6c01fb112172c3a14d3e3b65460cdc689` | `16d817ea443559de70c2f06eb0607e1a434df1c18b1ea0322d11405cba5ae6f2` |
| 16 | `212973c5506e121aa9ffda6af0dbfff61d45002021b8c81a484ef0b76383d96d` | `7dcb99ef3ec4d880de4910330d5ac3cf849bf3ee30a317a90b4233bb7c832d17` |

The checked-in ledger is a documentation-only successor to the measured code
head. It does not relabel the local artifacts as production evidence.
