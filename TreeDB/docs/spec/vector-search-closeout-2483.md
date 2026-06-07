# TreeDB vector search closeout guidance (#2483)

Status: pre-alpha documentation closeout for the accepted exact FP32,
`scalar_u8`, `rabitq_1bit`, and `brq_1bit` work through #2507. This page is an
evidence index and route-boundary guide. It is not a claim that one route is a
universal replacement for another, and it is not a USearch parity claim. #2494
crossover evidence is pending.

## Dependency status

| Lane | Status for docs | Evidence boundary |
| --- | --- | --- |
| Exact FP32 #2445 | Closed no-promote; PR #2457 was closed unmerged. | Do not publish #2457 speedups. Keep the prior exact interpretation: remaining c=8 gaps are traversal/scoring/frontier work, not setup/materialization/allocation. |
| Quantized `scalar_u8` #2446/#2460-#2465 | Closed. #2456 and #2466 merged; #2460-#2464 are no-promote. | Use #2456/#2466 plus the #2487 current-main snapshot. Do not use no-promote candidate numbers as wins. |
| RaBitQ Sublane A #2475/#2476-#2479/#2482 | Closed. #2484, #2485, and #2486 merged; #2478/#2479 are no-promote. | `rabitq_1bit` v1 is current behavior. Use #2477/#2482 and #2487 evidence, with route/counter caveats. |
| Sublane B #2480/#2481 | Completed as design plus prototype. #2488 selected `brq_1bit` v1, #2489 added oracle goldens, and #2507 added lower-level BRQ asset/search runtime. | Treat BRQ as prototype evidence. It is not part of the #2487 current-main snapshot and has no promotion/crossover claim. |
| Unified current-main snapshot #2487 | Complete. | Artifact root `/tmp/gomap_2487_unified_snapshot_20260606_135941`; commit `32e143240dbffb24172e0ec91c5565ea7c84328a`; noisy/moderate Apple M3 host. |
| Crossover campaign #2490-#2494 | Pending. | Final scale-sensitive positioning versus exact FP32, quantized routes, USearch, and pgvector should consume #2494 when it lands or explicitly state that crossover evidence is pending. |

## Route boundary summary

- Exact/default mode uses authoritative `float32_vector` scoring and, for the
  current high-QPS no-document fast path, the prepared `hnsw_search_pack_v1`
  route. Keep exact route claims separate from quantized route claims.
- `scalar_u8` and `rabitq_1bit` are supported quantized score planes for
  `quantized_only` and `quantized_rerank`, including collection-level buffered
  rows. `quantized_only` must report zero exact vector/norm reads;
  `quantized_rerank` exact-reads only the configured shortlist.
- `brq_1bit` v1 is the selected #2480 bit-product codec contract and #2507
  lower-level prototype. It uses a new codec identity, `uint4` runtime query
  bit-products, and score label `brq_1bit_estimated_cosine_q4`. Do not
  reinterpret `rabitq_1bit` rows as BRQ rows, and do not promote BRQ as faster
  without a future same-host/crossover gate.
- Buffered no-document APIs target caller-owned result storage. Response-owned
  `SearchVectorIndex` convenience rows can be healthy while still allocating
  response result/ID storage.
- With-document materialization, projections, filters, and split fetch are
  separate measurement boundaries.

## #2487 unified current-main snapshot

The #2487 unified current-main snapshot is suitable for docs as absolute current-main rows with host
load caveats. It intentionally excludes no-promote/draft candidates (#2457,
#2460-#2464, #2478, #2479) and does not include BRQ Sublane B.

### Exact FP32 no-document rows

Fixture: 10,000 docs, 64 dims, M=16, efConstruction=128, efSearch=128, topK=10,
16-query stream, `BENCHTIME=1000x`, `COUNT=3`, `CPU_LIST=1,8`.

| Row | c | ns/op | ops/sec | B/op | allocs/op | Boundary |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `TreeDB_SearchWithBuffer` | 1 | 45,333 | 22,059 | 0 | 0 | reusable exact searcher, caller-owned buffer |
| `TreeDB_SearchWithBufferParallel` | 8 | 14,525 | 68,845 | 3 | 0 | reusable exact parallel row; nonzero B/op caveated in #2487 |
| `TreeDB_CollectionSearchVectorIndexWithBuffer` | 1 | 44,058 | 22,697 | 0 | 0 | collection buffered exact no-doc |
| `TreeDB_CollectionSearchVectorIndexWithBuffer` | 8 | 74,075 | 13,500 | 0 | 0 | collection buffered exact no-doc, serial row at GOMAXPROCS=8 |
| `TreeDB_CollectionSearchVectorIndexNoDocsOneShot` | 1 | 43,889 | 22,785 | 816 | 2 | response-owned no-doc convenience |
| `TreeDB_CollectionSearchVectorIndexNoDocsOneShot` | 8 | 49,471 | 20,214 | 816 | 2 | response-owned no-doc convenience |
| `USearch_Search` | 1 | 33,716 | 29,659 | 136 | 3 | pure in-memory external control |
| `USearch_SearchParallel` | 8 | 12,106 | 82,601 | 138 | 3 | pure in-memory external control |

Healthy exact rows reported `search_route_hnsw_search_pack/search=1`,
`docs_fetched/search=0`, no graph/vector/scratch fallbacks, and no document
bytes. The USearch rows are controls, not persistent TreeDB storage.

### `scalar_u8` quantized rows

Fixture: 1,024 rows, 128 dims, topK=10, efSearch=128, query ordinal 37,
`BENCHTIME=100000x`, `TIMING_COUNT=5`, `GOMAXPROCS=8`.

| Row | ns/op | ops/sec | B/op | allocs/op | recall@K | code B/vector | asset B/vector | exact reads/calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| lower `quantized_only`, c=1 | 9,800 | 102,038 | 0 | 0 | 100% | 128 | 169.7 | vector/norm `0/0`, exact calls `0` |
| lower `quantized_only`, c=8 | 2,762 | 362,100 | 0 | 0 | 100% | 128 | 169.7 | vector/norm `0/0`, exact calls `0` |
| lower `quantized_rerank`, c=1 | 11,417 | 87,588 | 0 | 0 | 100% | 128 | 169.7 | vector/norm `16,384/128`, exact calls `32` |
| lower `quantized_rerank`, c=8 | 3,116 | 320,907 | 0 | 0 | 100% | 128 | 169.7 | vector/norm `16,384/128`, exact calls `32` |
| collection `quantized_only`, c=1 | 11,291 | 88,569 | 0 | 0 | 100% | 128 | 169.7 | vector/norm `0/0`, exact calls `0` |
| collection `quantized_only`, c=8 | 3,288 | 304,117 | 0 | 0 | 100% | 128 | 169.7 | vector/norm `0/0`, exact calls `0` |
| collection `quantized_rerank`, c=1 | 11,621 | 86,054 | 0 | 0 | 100% | 128 | 169.7 | vector/norm `16,384/128`, exact calls `32` |
| collection `quantized_rerank`, c=8 | 4,025 | 248,455 | 0 | 0 | 100% | 128 | 169.7 | vector/norm `16,384/128`, exact calls `32` |

### `rabitq_1bit` quantized rows

Same quantized fixture as above.

| Row | ns/op | ops/sec | B/op | allocs/op | recall@K | code B/vector | asset B/vector | exact reads/calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| lower `quantized_only`, c=1 | 17,463 | 57,264 | 0 | 0 | 100% | 16 | 66.62 | vector/norm `0/0`, exact calls `0` |
| lower `quantized_only`, c=8 | 4,752 | 210,433 | 0 | 0 | 100% | 16 | 66.62 | vector/norm `0/0`, exact calls `0` |
| lower `quantized_rerank`, c=1 | 19,013 | 52,595 | 0 | 0 | 100% | 16 | 66.62 | vector/norm `16,384/128`, exact calls `32` |
| lower `quantized_rerank`, c=8 | 4,931 | 202,779 | 0 | 0 | 100% | 16 | 66.62 | vector/norm `16,384/128`, exact calls `32` |
| collection `quantized_only`, c=1 | 18,862 | 53,016 | 0 | 0 | 100% | 16 | 66.62 | vector/norm `0/0`, exact calls `0` |
| collection `quantized_only`, c=8 | 5,038 | 198,494 | 0 | 0 | 100% | 16 | 66.62 | vector/norm `0/0`, exact calls `0` |
| collection `quantized_rerank`, c=1 | 20,731 | 48,236 | 0 | 0 | 100% | 16 | 66.62 | vector/norm `16,384/128`, exact calls `32` |
| collection `quantized_rerank`, c=8 | 6,251 | 159,982 | 0 | 0 | 100% | 16 | 66.62 | vector/norm `16,384/128`, exact calls `32` |

## BRQ prototype evidence (#2507)

#2507 adds `brq_1bit` v1 lower-level quantized asset/search runtime. Its local
artifact root is `/tmp/2481_runtime_bench_20260606_165236`. The benchmark matrix
used a 1,024-row / 128-dim synthetic shape with `BENCHTIME=100000x`, `count=5`
for search and `BENCHTIME=100x`, `count=5` for rebuild/storage.

| BRQ row | median ns/op | ops/sec | B/op | allocs/op | recall@K | exact reads/calls | BRQ counters |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| lower `quantized_only`, c=1 | 20,671 | 48,377 | 0 | 0 | 100% | vector/norm `0/0`, exact calls `0` | `quantized_score_codec_brq_1bit/search=1`, `brq_1bit_bitproduct_passes/search=338` |
| lower `quantized_only`, c=8 | 4,841 | 206,585 | 0 | 0 | 100% | vector/norm `0/0`, exact calls `0` | same |
| lower `quantized_rerank`, c=1 | 21,236 | 47,089 | 0 | 0 | 100% | vector/norm `16,384/128`, exact calls `32` | same |
| lower `quantized_rerank`, c=8 | 5,581 | 179,172 | 0 | 0 | 100% | vector/norm `16,384/128`, exact calls `32` | same |

BRQ reports 16 logical code bytes/vector and about 74.47 quantized asset
bytes/vector in the 256-row rebuild/storage shape. Existing scalar_u8/RaBitQ
rows showed no same-host regression in the #2507 spot check, but the BRQ row is
prototype evidence only.

## Required evidence for future claims

Any future PR or doc update that changes route guidance must report the command,
commit, fixture, hardware/context, `ns/op`, `ops/sec`, `B/op`, `allocs/op`,
route/fallback counters, recall, code/asset bytes, and exact-read/rerank bounds.
Material speedup or crossover claims require same-host before/after evidence and
must consume #2494 once that synthesis is available.
