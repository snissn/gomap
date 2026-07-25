# Vector partition M6 coordinator evidence ledger

Date: 2026-07-24

Measured code head: `c96a9b8b6b3a8aefe8257f26df10743d34ea6064`

Base M5 merge: `93f48763467aefdf9b45ba0f7d22847f7f0c66ed`

Status: experimental local-service evidence, not production evidence

## Claim boundary

This result composes the real persisted M4 router and production M6
coordinator with an in-process dispatcher that implements the M5 request and
response contract. It validates bounded planning/fanout, strict response
validation, stable-ID dedupe/merge, counters/timings, and exact
all-partition FP32 score/ID parity at the required 1M-vector shape.

It does **not** measure a network transport, remote service, serialization,
production Raft read-index/apply, or a production M5 service. The evidence
labels are:

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

## Exact shape and provenance

| Field | Value |
| --- | --- |
| fixture | `deterministic_1000000` |
| checksum | `71239d1335ddd724835d415f57acae7f8bb36a6af52642d1e710392a883b2d6f` |
| generator / arithmetic | `treedb_vector_partition_fixture_v2` / `ieee754_binary64_explicit_fma_v1` |
| corpus / queries / dimensions | 1,000,000 / 32 / 16 |
| metric / seed | cosine / 1 |
| partitions / probes / overlap | 16 / 16 / 0 |
| `top_k` | 10 |
| source `column_graph` HNSW degree | 4 |
| router representatives | 4 per partition, 64 total |
| `GOMAXPROCS` | 4 |
| Go memory limit | 17,179,869,184 bytes (16 GiB) |
| Go / platform | go1.26.0 / amd64/linux |
| modeled benchmark-owned peak / cap | 360,006,080 / 4,294,967,296 bytes |

The source degree is an explicit bounded benchmark-builder control. The
normal default remains 16; this host-safe acceptance row uses 4. The modeled
peak excludes TreeDB engine/index internals and Go runtime/GC metadata, so the
Go memory limit and externally measured process RSS remain separate evidence.

The process completed in 2:19.11 wall time with 149.52 seconds user CPU, 7.35
seconds system CPU, 112% average CPU, 2,655,364 KiB maximum RSS
(2,719,092,736 bytes), and zero swaps. The M4 build reported 16,070,272,461 ns
wall time, 78,254,753,000 ns process CPU, a 31,504-byte router asset, and zero
heap-copy bytes after mapped open.

## Correctness and acceptance result

The all-partition row passed exact global parity:

| Gate | Result |
| --- | ---: |
| exact all-partition parity checked | true |
| exact all-partition parity passed | true |
| recall@1 | 1.0 |
| recall@10 | 1.0 |
| end-to-end p50 / p95 / p99 | 1.953113 / 2.006014 / 2.071665 s |
| QPS | 0.511179 |
| placement + dedupe + merge / total | 0.0020718% |
| scoped coordinator-overhead ceiling | <15%: **PASS** |

The overhead ratio is exactly
`(placement + dedupe + merge) / coordinator total` over the 32 query samples.
It is a narrow M6 coordinator-bookkeeping gate. It is not a production
network-overhead ratio, and the large router-open component in this local
shape is part of its denominator.

Each query reopened and verified the pinned persisted M4 router. Router open
therefore dominates the end-to-end p50. This is visible rather than hidden in
shard-search or coordinator bookkeeping.

## Stage timing

Router, placement, queue, dispatcher, response, dedupe, merge, and end-to-end
percentiles use 32 per-query samples. Queue and dispatcher RPC values are sums
over that query's 16 fanout tasks, so they can exceed elapsed end-to-end time
under concurrency. Shard search uses 512 individual task samples.
`network_timing` is only the in-process dispatcher residual
`rpc_elapsed - reported_service_total`; it is not network measurement.

| Stage | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| persisted M4 router open | 1,884,563,437 ns | 1,942,644,401 ns | 1,983,347,639 ns |
| M4 router search | 8,460 ns | 10,030 ns | 12,314 ns |
| M1 placement/group planning | 13,538 ns | 20,699 ns | 21,531 ns |
| summed task queue | 341,004,505 ns | 396,918,399 ns | 464,908,101 ns |
| summed in-process dispatcher RPC | 365,564,080 ns | 524,328,186 ns | 596,995,160 ns |
| summed in-process residual | 12,935 ns | 28,332 ns | 29,634 ns |
| synthetic read proof | 0 ns | 0 ns | 0 ns |
| partition-local exact scan (512 samples) | 19,010,326 ns | 43,665,257 ns | 55,432,193 ns |
| response validation/materialization | 4,768 ns | 6,473 ns | 6,762 ns |
| stable-ID dedupe | 16,555 ns | 28,123 ns | 38,456 ns |
| top-k merge | 7,704 ns | 10,328 ns | 16,773 ns |
| end to end | 1,953,112,693 ns | 2,006,013,809 ns | 2,071,665,086 ns |

The harness reports 1,181,615,433 B/op and 2,067,611.71875 allocs/op for the
complete local operation. Those figures include the deliberately exhaustive
in-process partition scans and their response construction. They are evidence
for this simulator shape, not an allocation target for a production remote M5
implementation.

## Exact counters

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
uses an exact FP32 scan, so every all-partition query reports the complete 1M
candidate corpus and zero HNSW edges.

## Reproduction

Run from the repository root at exact head
`c96a9b8b6b3a8aefe8257f26df10743d34ea6064`. The retained artifact was produced
with:

```sh
RUN_ROOT=$(mktemp -d /mnt/fast4tb/tmp/treedb_m6_1m_safe_XXXXXX)
mkdir -p "$RUN_ROOT/fixture" "$RUN_ROOT/evidence" "$RUN_ROOT/tmp"

GOMAXPROCS=4 GOMEMLIMIT=16GiB GOWORK=off \
  go run ./cmd/treedb_vector_partition_bench generate-fixture \
    -out "$RUN_ROOT/fixture" \
    -vectors 1000000 \
    -queries 32 \
    -dimensions 16 \
    -seed 1

/usr/bin/time -v -o "$RUN_ROOT/resource_usage.txt" \
  env \
    TMPDIR="$RUN_ROOT/tmp" \
    BASE_SHA=93f48763467aefdf9b45ba0f7d22847f7f0c66ed \
    GITHUB_SHA=c96a9b8b6b3a8aefe8257f26df10743d34ea6064 \
    GITHUB_EVENT_PATH= \
    GOMAXPROCS=4 \
    GOMEMLIMIT=16GiB \
    GOWORK=off \
  go run ./cmd/treedb_vector_partition_bench \
    -dataset "$RUN_ROOT/fixture" \
    -stage distributed_simulation_or_cluster \
    -partitions 16 \
    -probes 16 \
    -overlap 0 \
    -top-k 10 \
    -seed 1 \
    -router-representatives 4 \
    -source-hnsw-degree 4 \
    -max-vectors 1000000 \
    -max-fixture-bytes 4294967296 \
    -format json \
    -out "$RUN_ROOT/evidence" \
  | tee "$RUN_ROOT/stdout.json"
```

The artifact records `gomaxprocs=4`,
`go_memory_limit_bytes=17179869184`, and `source_hnsw_degree=4`; a row missing
those positive runtime controls fails result validation.

## Retained records

| Record | Path | SHA-256 |
| --- | --- | --- |
| canonical JSON | `/mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/evidence_c96/m6_local_service_f71239d1335ddd724835d415f57acae7f8bb36a6af52642d1e710392a883b2d6f_s0000000000000001_n16_p16_o0000000000000000_t21906513a17b010cd9fba82b5262d0e75b4ed6ffb651548b6fb1632963c15d64_k10.json` | `cc33c29cd79d47c42b55064649fdad623b0d36321e9de076d29f75dfdf28ce42` |
| generated Markdown | same basename under `evidence_c96`, `.md` suffix | `2e398d334e7515274fdf9c6bec5cc4e5f62bd17bc3f11646c420616e3816f664` |
| captured stdout | `/mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/stdout_c96.json` | `38c5ba251396d32048b03f6aaa057c29c5251d3d97da30d8a2ae94d80995198d` |
| `/usr/bin/time -v` | `/mnt/fast4tb/tmp/treedb_m6_1m_safe_TEzTe1/resource_usage_c96.txt` | `e21109f2b143b8a9d7717434bb1d6556de062927e6b00a107ce7afa2326a6252` |

The checked-in ledger is a documentation-only successor to the measured head.
It does not relabel the local artifact as production evidence.

## Deliberately deferred curve

The all-partition row is the M6 acceptance/correctness row: it proves exact
score/ID parity without router-recall loss. A lower-probe recall/latency curve
remains future evidence.

The current command rebuilds the persisted 1M source and router for each
process invocation. Running one host-heavy rebuild per probe row would duplicate
and distort the evidence rather than isolate query behavior. A future curve
should reuse one validated persisted database/router across rows and keep the
same source generation, artifact identity, query order, and runtime controls.
