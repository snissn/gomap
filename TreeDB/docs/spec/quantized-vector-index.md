# Quantized Vector Index Score Planes (#1926)

Status: pre-alpha scalar `scalar_u8` v1 score-plane closeout for explicit
TreeDB `column_graph` query modes. Exact/default behavior remains the
production baseline. Non-scalar codecs such as BRQ, RaBitQ, PQ/OPQ, and SIMD
popcount variants are future work and are not part of this scalar closeout.

## User-visible query modes

`VectorIndexDefinition.QuantizedIndexes` declares one or more named derived score
planes for a `column_graph` vector index. Current normalized declarations default
to `codec="scalar_u8"` and `version=1`.

Search uses an explicit mode:

| Mode | Score plane | Returned scores | Exact vector/norm reads | Notes |
| --- | --- | --- | --- | --- |
| `exact` / zero value | Authoritative `float32_vector` plus inverse norms | Exact cosine | Yes, for scored candidates | Existing/default behavior. Quantized-only options are rejected in exact mode. |
| `quantized_only` | Selected prepared `scalar_u8` codes | Estimated scalar_u8 cosine | No | Traversal and final ranking use the quantized scorer. No hidden exact fallback, vector reads, or norm reads. |
| `quantized_rerank` | Selected prepared `scalar_u8` codes for traversal, authoritative vectors for rerank | Exact cosine | Yes, only for the trimmed quantized shortlist | Traversal still collects the normalized `ef_search` candidate pool, then trims to `QuantizedRerankCandidates` before exact rerank. |

`QuantizedRerankCandidates=0` means the normalized `ef_search` candidate set.
Non-zero values must be at least `TopK`. Returned `quantized_rerank` results are
ranked by exact cosine over the trimmed quantized candidate set; they are not a
silent full-exact search. Quantized route counters identify the selected query
mode and may be reported alongside the `column_graph` prepared physical route;
`hnsw_search_pack_v1` route/active/fallback counters remain zero for quantized
search.

## Durable asset model

Each declared scalar score plane rebuilds one TVIS vector-index-state asset with
role `quantized_codes`, asset id `quantized/<name>/codes`, logical type
`byte_vector`, and physical encoding `raw_fixed_bytes`. Rows are in graph ordinal
order and bind to the base graph identity (index, field, metric, dimensions, row
count, base manifest identity, graph schema hash, codec name/version/config, and
asset ref/checksum identity). For cosine, persisted scalar_u8 codes are built
from inverse-norm-normalized vector components so equivalent directions encode to
the same row.

The logical code payload is one byte per dimension. The typed-column asset also
contains part headers, primary-id/sort-key metadata, layout certification, and
padding, so benchmark evidence must report both logical `quantized_code_B/vector`
and actual `quantized_asset_B/vector`.

## Fail-closed behavior

Quantized modes validate the selected name, declaration, codec/version, prepared
asset, row count, dimensions, metric, source schema, base graph identity, asset
ref identity, and typed-column layout before traversal/scoring. Missing, stale,
corrupt, mismatched, unsupported, or unprepared assets return
`ErrVectorIndexSearchUnavailable`; they must not fall back to exact traversal or
document reconstruction. Fail-closed stats use codec-generic counters such as
`quantized_asset_missing`, `quantized_asset_invalid`, `quantized_asset_stale`,
and `quantized_asset_unavailable`. Exact mode rejects quantized-mode fields so
callers do not accidentally depend on no-op options.

## Benchmark and storage evidence

The permanent #1926 evidence harness compares exact, `quantized_only`, and
`quantized_rerank` on the same deterministic fixture and separately reports
rebuild/storage overhead:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^Benchmark(ColumnGraphScalarU8Quantized(ScorePlanes|RebuildStorage)1926|VectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414)$' \
  -benchmem -benchtime=500ms -count=3
```

`BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926` emits the per-query rows;
`BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414`
emits explicit lower-level buffered `VectorIndexSearcher.SearchWithBuffer` rows
for `route=quantized_only/c=1`, `route=quantized_only/c=8`,
`route=quantized_rerank/candidates=32/c=1`, and
`route=quantized_rerank/candidates=32/c=8`; and
`BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926` emits rebuild/storage
rows. The score-plane and buffered rows report `ns/op`, `ops/sec`, `B/op`,
`allocs/op`, `recall_at_k_pct` versus exact-mode topK on the same fixture,
`search_route_quantized_only/search`, `search_route_quantized_rerank/search`,
`quantized_scorer_active/search`, `quantized_asset_unavailable/search`,
`candidates/search`, `quantized_rerank_candidates/search`,
`quantized_rerank_exact_score_calls/search`, `quantized_code_B/search`,
`vector_B/search`, `norm_B/search`, logical code bytes/vector, and actual
`quantized_asset_B/vector`. The rebuild rows report build throughput, allocation
cost, total graph/vector-index-state storage, quantized asset bytes, and
quantized asset bytes/vector.

Representative local closeout run on Apple M3 / darwin arm64 / Go `go1.25.5`
used `/tmp/gomap_1926_quantized_scoreplanes_20260603_060146/` with the command
above. Medians from the three-count run:

| Benchmark row | Fixture | ns/op | ops/sec | B/op | allocs/op | recall@K vs exact | candidates/search | rerank exact calls/search | quantized code B/search | exact vector B/search | exact norm B/search | asset B/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `mode=exact` | 1024 rows, 128 dims, topK 10, ef 128 | 15,748 | 63,500 | 0 | 0 | 100% | 133 | 0 | 0 | 86,528 | 676 | 169.6 |
| `mode=quantized_only` | same | 28,114 | 35,570 | 0 | 0 | 100% | 133 | 0 | 21,632 | 0 | 0 | 169.6 |
| `mode=quantized_rerank/candidates=10` | same | 28,099 | 35,589 | 0 | 0 | 100% | 133 | 10 | 21,632 | 5,120 | 40 | 169.6 |
| `mode=quantized_rerank/candidates=32` | same | 28,417 | 35,190 | 0 | 0 | 100% | 133 | 32 | 21,632 | 16,384 | 128 | 169.6 |
| `mode=quantized_rerank/candidates=128` | same | 30,333 | 32,968 | 0 | 0 | 100% | 133 | 128 | 21,632 | 65,536 | 512 | 169.6 |

Rebuild/storage median rows from the same run:

| Rebuild row | Fixture | ns/op | ops/sec | B/op | allocs/op | graph total storage B/op | quantized asset B/op | quantized asset B/vector | logical code B/vector | exact vector+norm B/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `mode=exact_assets` | 256 rows, 128 dims, M 16 | 58,905,433 | 16.98 | 9,392,840 | 88,250 | 168,568 | 0 | 0 | 128 | 516 |
| `mode=scalar_u8_assets` | same | 62,804,454 | 15.92 | 9,740,738 | 88,814 | 213,224 | 44,656 | 174.4 | 128 | 516 |

Interpretation: this scalar Go scorer evidence proves mode behavior, counters,
allocation discipline, exact/norm read avoidance for `quantized_only`, exact
shortlist reads for `quantized_rerank`, and storage/rebuild accounting. It does
**not** claim a current speedup over exact prepared indexed scoring.

## Future work boundaries

Future quantization work should be separate from this scalar score-plane closeout:

- BRQ/RaBitQ/PQ/OPQ/residual-PQ codecs and packed popcount scorers need their
  own codec specs, tests, recall sweeps, and benchmark gates.
- Batch scorer kernels, SIMD/popcount integration, graph control-flow changes,
  block-planner/windowing changes, and traversal scheduling changes are not part
  of #1926 scalar acceptance. They must not be smuggled into scalar docs or used
  to reinterpret the current no-speedup evidence.
- Incremental quantized maintenance and stable-format migrations remain future
  pre-alpha work; current benchmark directories should be rebuilt across branch
  changes.
