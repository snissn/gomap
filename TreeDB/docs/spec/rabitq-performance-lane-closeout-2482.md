# TreeDB RaBitQ performance lane closeout (#2482)

Status: pre-alpha closeout for the #2475 RaBitQ performance lane. This note
closes **Sublane A**, the `rabitq_1bit` v1 performance sweep, including the
semantics-preserving #2477 query-byte-table scorer evidence. At the original
#2482 closeout, **Sublane B** was deferred; it has since completed through #2480
and #2481 with a separate `brq_1bit` v1 contract/prototype. This file remains a
benchmark/evidence boundary, not a claim that RaBitQ universally replaces exact
FP32 or `scalar_u8`.

## Scope and hard invariants

Sublane A changed only the current `rabitq_1bit` v1 scorer implementation and
benchmark/profile workflow. No `rabitq_1bit` v1 storage, LSB-first bit order,
score formula, durable asset identity, codec identity, or fail-closed behavior
changed in Sublane A. The following did not change:

- `rabitq_1bit` v1 durable storage layout;
- LSB-first bit order;
- weighted sign-dot score formula;
- durable asset identity;
- codec name/version/config identity;
- fail-closed behavior for missing, stale, corrupt, mismatched, unsupported, or
  closed quantized assets.

The promoted #2477 change is semantics-preserving: it replaces per-bit scorer
work with query-local byte mismatch tables while computing the same weighted
sign-dot estimator from [`rabitq-1bit-v1.md`](rabitq-1bit-v1.md). It does not
change exact FP32, `scalar_u8`, graph traversal, result finalization, or hidden
fallback behavior.

## Lane result summary

| Issue | Result | Evidence / PR | Closeout interpretation |
| --- | --- | --- | --- |
| #2476 | Merged in PR #2484 | merge `435a1c06a7f5e9751f99a199ec3edae184787508`; runbook [`docs/benchmarks/treedb_rabitq_1bit_profile_gate.md`](../../../docs/benchmarks/treedb_rabitq_1bit_profile_gate.md); script [`scripts/treedb_rabitq_1bit_profile_gate.sh`](../../../scripts/treedb_rabitq_1bit_profile_gate.sh); starting smoke artifact `/tmp/gomap_2476_starting_smoke_20260606_102027/` | Established the required same-host baseline/candidate gate. Smoke rows are shape/counter evidence only, not speed claims. |
| #2477 | Merged in PR #2485 | merge `77d9f371832f2dbb51c02084f8c1876118eeca06`; final baseline `/tmp/gomap_2477_final2_baseline_main_435a1c06_20260606_112457`; final candidate `/tmp/gomap_2477_final2_candidate_17857e779_20260606_112935`; scalar interleaved guardrail `/tmp/gomap_2477_interleaved_scalar_lower_qonly_c8_20260606_114454` | Promoted the query byte-table scorer with strong same-host RaBitQ wins and unchanged allocation/recall/exact-read/route/fallback counters. |
| #2478 | Closed no-entry / no-promote | <https://github.com/snissn/gomap/issues/2478#issuecomment-4640631624> | Post-#2477 profiles did not show packed-code reader/stride/bounds/accessor overhead as a material bottleneck, so no code PR or AI review was opened. |
| #2479 | Closed skipped / no-promote | <https://github.com/snissn/gomap/issues/2479#issuecomment-4640644622> | Post-#2477/#2478 evidence did not show a credible batching/windowing opportunity, so no experiment branch, code PR, or AI review was opened. |
| #2480 | Merged in PR #2488 | merge `206a8fb3e5243de1b094e52746907771fee640ed`; spec [`brq-1bit-v1.md`](brq-1bit-v1.md) | Selected the separate `brq_1bit` v1 future-codec contract. It does not reinterpret `rabitq_1bit` v1. |
| #2481 | Completed via PR #2489 and PR #2507 | #2489 merge `f2016198b8d00824021b8e05420f1e939f751df7`; #2507 merge `8e7377cc995ffc928ac99a42ed8aec769f8f72fb`; BRQ artifacts `/tmp/2481_runtime_bench_20260606_165236` | Added BRQ oracle goldens, then lower-level BRQ asset/search runtime. This is prototype evidence, not a RaBitQ Sublane A promotion claim. |

#2482 closed when this closeout was merged because it recorded Sublane A's
promoted and no-promote outcomes. #2483 later reconciled Sublane B completion in
[`vector-search-closeout-2483.md`](vector-search-closeout-2483.md).

## Evidence boundary

Claim-quality #2477 evidence used the #2476 profile-gate script on the same
Apple M3 host with `go1.26.0 darwin/arm64`, `GOMAXPROCS=8`, `GOWORK=off`, and a
1024-row / 128-dimension deterministic `column_graph` fixture. Benchmark timing
used `BENCHTIME=100000x` and `TIMING_COUNT=5`; each claimed RaBitQ row captured
one CPU/alloc/block/mutex profile. The benchmark command shape was:

```sh
ROWS=claim_rerank \
PROFILE_ROWS=rabitq_lower_quantized_only_c1,rabitq_lower_quantized_only_c8,rabitq_collection_quantized_only_c1,rabitq_collection_quantized_only_c8,rabitq_lower_quantized_rerank32_c1,rabitq_lower_quantized_rerank32_c8,rabitq_collection_quantized_rerank32_c1,rabitq_collection_quantized_rerank32_c8 \
BENCHTIME=100000x TIMING_COUNT=5 PROFILE_COUNT=1 \
GOMAXPROCS=8 GOWORK=off \
scripts/treedb_rabitq_1bit_profile_gate.sh
```

Baseline: `origin/main` at `435a1c06a7f5e9751f99a199ec3edae184787508`.
Candidate benchmarked: `17857e779830e7d6f2f88a33f8a11ecea2817529`; the merged
PR head `69bcb950afb9efc5ddd321edb22f93b9b5103ea0` changed only comments /
docstrings after the benchmarked candidate.

## Final RaBitQ row matrix (#2477)

Medians below are copied from the generated `summary.md` files in the baseline
and candidate artifact directories. `quantized_only` rows have zero exact
vector/norm reads and zero exact rerank calls. `quantized_rerank` rows exact-read
only the 32-candidate shortlist. All rows reported `0 B/op`, `0 allocs/op`,
100% fixture recall, expected quantized route counters, and zero fallback / asset
unavailable counters.

| Row | Baseline ns/op | Baseline ops/s | Candidate ns/op | Candidate ops/s | ns/op delta | B/op | allocs/op | recall@K | exact reads / calls | route and fallback counters | code B/search | code B/vector | asset B/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: |
| lower `quantized_only`, c=1 | 40,721 | 24,557 | 17,790 | 56,210 | -56.3% | 0 | 0 | 100% | vector/norm `0/0`, exact calls `0` | `search_route_quantized_only/search=1`; `search_route_quantized_rerank/search=0`; fallback / unavailable counters `0` | 2,704 | 16 | 66.62 |
| lower `quantized_only`, c=8 | 9,977 | 100,229 | 7,991 | 125,147 | -19.9% | 0 | 0 | 100% | vector/norm `0/0`, exact calls `0` | `search_route_quantized_only/search=1`; `search_route_quantized_rerank/search=0`; fallback / unavailable counters `0` | 2,704 | 16 | 66.62 |
| lower `quantized_rerank` candidates=32, c=1 | 42,156 | 23,721 | 18,772 | 53,271 | -55.5% | 0 | 0 | 100% | vector/norm `16,384/128`, exact calls `32` | `search_route_quantized_only/search=0`; `search_route_quantized_rerank/search=1`; fallback / unavailable counters `0` | 2,704 | 16 | 66.62 |
| lower `quantized_rerank` candidates=32, c=8 | 10,652 | 93,883 | 6,852 | 145,936 | -35.7% | 0 | 0 | 100% | vector/norm `16,384/128`, exact calls `32` | `search_route_quantized_only/search=0`; `search_route_quantized_rerank/search=1`; fallback / unavailable counters `0` | 2,704 | 16 | 66.62 |
| collection `quantized_only`, c=1 | 42,805 | 23,362 | 19,348 | 51,686 | -54.8% | 0 | 0 | 100% | vector/norm `0/0`, exact calls `0` | `search_route_quantized_only/search=1`; `search_route_quantized_rerank/search=0`; fallback / unavailable counters `0`; `docs_fetched/search=0` | 2,704 | 16 | 66.62 |
| collection `quantized_only`, c=8 | 14,258 | 70,138 | 8,455 | 118,276 | -40.7% | 0 | 0 | 100% | vector/norm `0/0`, exact calls `0` | `search_route_quantized_only/search=1`; `search_route_quantized_rerank/search=0`; fallback / unavailable counters `0`; `docs_fetched/search=0` | 2,704 | 16 | 66.62 |
| collection `quantized_rerank` candidates=32, c=1 | 47,355 | 21,117 | 20,298 | 49,265 | -57.1% | 0 | 0 | 100% | vector/norm `16,384/128`, exact calls `32` | `search_route_quantized_only/search=0`; `search_route_quantized_rerank/search=1`; fallback / unavailable counters `0`; `docs_fetched/search=0` | 2,704 | 16 | 66.62 |
| collection `quantized_rerank` candidates=32, c=8 | 16,056 | 62,281 | 6,593 | 151,687 | -58.9% | 0 | 0 | 100% | vector/norm `16,384/128`, exact calls `32` | `search_route_quantized_only/search=0`; `search_route_quantized_rerank/search=1`; fallback / unavailable counters `0`; `docs_fetched/search=0` | 2,704 | 16 | 66.62 |

Interpretation: #2477 materially improves the current RaBitQ rows, especially
c=1 and rerank rows, while preserving score semantics and counters. It does not
make a universal acceleration claim. On this fixture the `scalar_u8` guardrail
rows are still faster than the optimized RaBitQ rows, while RaBitQ remains much
more compact: 16 logical code bytes/vector versus 128 for `scalar_u8`, and 66.62
asset bytes/vector versus 169.7 for `scalar_u8` in these search rows.

## Scalar and exact guardrails

The candidate run also emitted scalar_u8 guardrail rows with unchanged counters:
`0 B/op`, `0 allocs/op`, 100% recall, `quantized_only` exact reads at `0/0`,
`quantized_rerank` exact calls at `32/search`, `quantized_asset_unavailable=0`,
and fallback counters at zero. These scalar rows are counter/shape guardrails,
not #2477 promotion evidence, because #2477 did not touch the scalar scorer. The
generated scalar medians were:

| Scalar guardrail row | Baseline ns/op | Baseline ops/s | Candidate ns/op | Candidate ops/s | B/op | allocs/op | code B/search | code B/vector | asset B/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| lower `quantized_only`, c=1 | 11,462 | 87,247 | 10,235 | 97,706 | 0 | 0 | 21,632 | 128 | 169.7 |
| lower `quantized_only`, c=8 | 3,520 | 284,060 | 3,304 | 302,669 | 0 | 0 | 21,632 | 128 | 169.7 |
| lower `quantized_rerank` candidates=32, c=1 | 11,190 | 89,363 | 10,543 | 94,848 | 0 | 0 | 21,632 | 128 | 169.7 |
| lower `quantized_rerank` candidates=32, c=8 | 3,811 | 262,393 | 3,120 | 320,514 | 0 | 0 | 21,632 | 128 | 169.7 |
| collection `quantized_only`, c=1 | 11,454 | 87,303 | 10,997 | 90,937 | 0 | 0 | 21,632 | 128 | 169.7 |
| collection `quantized_only`, c=8 | 4,345 | 230,150 | 3,931 | 254,368 | 0 | 0 | 21,632 | 128 | 169.7 |
| collection `quantized_rerank` candidates=32, c=1 | 12,026 | 83,155 | 11,769 | 84,965 | 0 | 0 | 21,632 | 128 | 169.7 |
| collection `quantized_rerank` candidates=32, c=8 | 4,351 | 229,812 | 4,729 | 211,482 | 0 | 0 | 21,632 | 128 | 169.7 |

The host was noisy during #2477 (`context.txt` records Orca, fseventsd, openclaw,
and other activity), so the one noisy scalar lower `quantized_only` c=8 guardrail
was rerun interleaved: `/tmp/gomap_2477_interleaved_scalar_lower_qonly_c8_20260606_114454`.
That run reported 2.512 µs ±5% baseline versus 2.651 µs ±15% candidate,
`p=0.199`, `n=20`, with allocation and counter values unchanged; it was not a
significant regression.

Exact FP32 `hnsw_search_pack_v1` rows were not required for #2477 because the PR
touched only RaBitQ scorer internals. Exact/default FP32 search remains the
authoritative baseline and remains separate from `scalar_u8` and `rabitq_1bit`.
Future PRs that touch shared traversal, cache, result finalization, row-ref, or
exact scoring paths must include exact FP32 guardrails before making claims.

## CPU profile summary

The profile evidence explains why #2478/#2479 did not open code PRs. Before
#2477, the old per-bit RaBitQ scorer dominated the checked `quantized_only` CPU
profiles. After #2477, the byte-table score loop is low single digits to about
9% flat across the checked rows, and query table preparation is the visible
RaBitQ-local cost alongside traversal/frontier/search work.

| Row | Baseline old scorer flat | Candidate byte-table scorer flat | Candidate query table prep flat | Artifact |
| --- | ---: | ---: | ---: | --- |
| lower `quantized_only`, c=1 | 65.60% | 4.83% | 26.57% | `rabitq_lower_quantized_only_c1/cpu_top.txt` |
| lower `quantized_only`, c=8 | 65.31% | 8.53% | 24.42% | `rabitq_lower_quantized_only_c8/cpu_top.txt` |
| collection `quantized_only`, c=1 | 60.48% | 7.61% | 17.77% | `rabitq_collection_quantized_only_c1/cpu_top.txt` |
| collection `quantized_only`, c=8 | 62.17% | 4.32% | 24.82% | `rabitq_collection_quantized_only_c8/cpu_top.txt` |

Accessor/validation frames such as `CodeRowView.Rows`, `CodeRowView.Valid`, and
prepared direct-view checks were low single-digit and inconsistent, not a clear
post-#2477 target. Existing scoring seams already tile adjacency ordinals, and
the remaining costs did not show a credible batching/windowing opportunity
separate from query prep plus graph traversal.

## #2476 foundation smoke

#2476 merged the durable runbook and script used above. Its starting smoke
artifact `/tmp/gomap_2476_starting_smoke_20260606_102027/` was captured on
`origin/main` at `7c38b993ad20875f7307ab151229814d2bb62844` with the issue-required
`GOMAXPROCS=8 GOWORK=off`, `-benchtime=100x`, `-count=3`, `-benchmem` matrix.
The runbook labels this as smoke/shape evidence only. Representative medians:

| Smoke row | ns/op | ops/s | B/op median (max) | allocs/op median (max) | recall@K | code B/search | code B/vector | asset B/vector |
| --- | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: |
| RaBitQ lower `quantized_only`, c=1 | 37,942 | 26,356 | 0 (0) | 0 (0) | 100% | 2,704 | 16 | 66.62 |
| RaBitQ lower `quantized_only`, c=8 | 8,823 | 113,341 | 0 (0) | 0 (0) | 100% | 2,704 | 16 | 66.62 |
| RaBitQ collection `quantized_only`, c=1 | 48,398 | 20,662 | 0 (0) | 0 (0) | 100% | 2,704 | 16 | 66.62 |
| RaBitQ collection `quantized_only`, c=8 | 10,480 | 95,420 | 0 (0) | 0 (0) | 100% | 2,704 | 16 | 66.62 |
| scalar_u8 lower `quantized_only`, c=1 | 9,705 | 103,035 | 0 (0) | 0 (0) | 100% | 21,632 | 128 | 169.7 |
| scalar_u8 lower `quantized_only`, c=8 | 2,958 | 338,075 | 0 (1) | 0 (0) | 100% | 21,632 | 128 | 169.7 |
| scalar_u8 collection `quantized_only`, c=1 | 10,772 | 92,832 | 0 (0) | 0 (0) | 100% | 21,632 | 128 | 169.7 |
| scalar_u8 collection `quantized_only`, c=8 | 3,000 | 333,333 | 0 (0) | 0 (0) | 100% | 21,632 | 128 | 169.7 |

## Future work status

Sublane B is complete for design/prototype purposes: #2480 selected the
`brq_1bit` v1 contract and #2481 landed oracle plus lower-level runtime support.
BRQ remains a separate prototype route with its own evidence and counters; it is
not part of the #2477 RaBitQ promotion and not a universal speedup claim. Future
scale-sensitive positioning should consume #2494 crossover synthesis when it is
available.

Any future estimator, popcount/BRQ shape beyond v1, or multi-bit RaBitQ variant
that differs from `rabitq_1bit` v1 must use a new codec identity/version. It
must not reinterpret `rabitq_1bit` v1 storage, bit order, or score semantics for
speed.
