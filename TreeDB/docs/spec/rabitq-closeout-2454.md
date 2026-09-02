# TreeDB RaBitQ closeout evidence (#2454)

Status: pre-alpha closeout for the landed pure-Go `rabitq_1bit` v1 score-plane
stack. This note is a benchmark/reproduction boundary, not a claim that RaBitQ
replaces exact FP32 or `scalar_u8` for every workload.

## Product boundaries

- Exact/default vector search remains authoritative FP32 cosine scoring.
- `scalar_u8` and `rabitq_1bit` are named derived score planes selected only by
  explicit `quantized_only` or `quantized_rerank` query modes.
- `rabitq_1bit` v1 search is pure Go. The #2453 go-highway acceleration
  investigation did **not** land: go-highway's fast bit-product shape does not
  match TreeDB's exact weighted RaBitQ cosine scorer or durable LSB-first packed
  asset contract without semantic changes or a more complex bit-plane/residual
  scorer.
- Quantized modes fail closed with `ErrVectorIndexSearchUnavailable` when the
  selected declaration, prepared asset, codec/version/config identity, graph
  identity, row count, or typed-column shape is missing, stale, corrupt,
  mismatched, unsupported, or closed. They must not silently fall back to exact
  traversal or document materialization.

## User-visible query modes

| Mode | Selected score plane | Traversal/final ranking | Exact reads | Returned scores |
| --- | --- | --- | --- | --- |
| `exact` / zero | none | Authoritative FP32 route | FP32 vector/pack reads for scored candidates | exact cosine |
| `quantized_only` | named `scalar_u8` or `rabitq_1bit` | selected quantized scorer for traversal and final topK | none | selected codec estimate |
| `quantized_rerank` | named `scalar_u8` or `rabitq_1bit` | selected quantized scorer over the normalized `ef_search` pool, then exact-rerank the trimmed `QuantizedRerankCandidates` shortlist | shortlist only | exact cosine over the quantized shortlist |

`QuantizedRerankCandidates=0` means the normalized `ef_search` candidate set;
non-zero values below `TopK` are rejected. Exact/default mode rejects quantized
mode fields so callers cannot accidentally rely on no-op options.

## RaBitQ v1 asset/storage recap

The durable `rabitq_1bit` score plane uses vector-index state role
`quantized_codes` and asset id `quantized/<name>/packed_codes`. The typed-column
part stores:

| Role | Type / encoding | Shape |
| --- | --- | --- |
| `packed_codes` | `packed_bit_vector` / `raw_packed_bit_vector` | `Rows=graph.RowCount`, `ElementsPerRow=next_power_of_two(VectorDimensions)`, `BitsPerElement=1` |
| `code_count` | `uint32` / `raw_uint32` | one scalar per graph ordinal |
| `quantized_dot_product_inv` | `float32` / `raw_float32` | one scalar per graph ordinal |

Logical bit `i` is LSB-first at byte `i/8`, bit `i%8`; unused high bits in the
last byte are zero. For the standard 128-dimensional fixture, the padded code is
128 bits, so `rabitq_1bit` logical code bytes/vector is `16`, compared with
`128` for scalar_u8 codes and `512 + 4` for the exact vector plus norm payload.

## Benchmark workflow

Capture hardware and commit first:

```sh
OUT=/tmp/gomap_2454_closeout_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"
{
  echo "date=$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo "commit=$(git rev-parse HEAD)"
  go version
  uname -a
  sysctl -n machdep.cpu.brand_string 2>/dev/null || true
} > "$OUT/environment.txt"
```

Run the closeout matrix:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^(BenchmarkColumnGraphScalarU8QuantizedScorePlanes1926|BenchmarkVectorIndexSearcherColumnGraphScalarU8QuantizedSearchWithBuffer2414|BenchmarkVectorIndexSearcherColumnGraphRabitQQuantizedSearchWithBuffer2451|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415|BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452|BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926|BenchmarkColumnGraphRabitQQuantizedRebuildStorage2450)$' \
  -benchmem -benchtime=100x -count=3 | tee "$OUT/bench_matrix.txt"
```

Fixture definitions:

- search rows: 1024 vectors, 128 dims, HNSW degree 16, `topK=10`,
  `efSearch=128`, deterministic synthetic rows, query ordinal 37;
- rebuild/storage rows: 256 vectors, 128 dims, degree 16;
- collection buffered rows warm the collection prepared cache and report
  `open_searcher_calls/op=0`, `open_setup_in_timed_loop=0`,
  `docs_fetched/search=0`, and `collection_searchvectorindex_with_buffer_seam=1`;
- lower-level rows open `VectorIndexSearcher` outside the timed loop and reuse
  `VectorIndexSearchBuffer`.

For RaBitQ collection rows where c=1/c=8 performance claims are published,
capture profile artifacts separately:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452/route=quantized_only/c=1$' \
  -benchmem -benchtime=2s -count=1 \
  -cpuprofile "$OUT/rabitq_collection_c1_cpu.pprof" \
  -memprofile "$OUT/rabitq_collection_c1_mem.pprof"

GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench '^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphRabitQQuantized2452/route=quantized_only/c=8$' \
  -benchmem -benchtime=2s -count=1 \
  -cpuprofile "$OUT/rabitq_collection_c8_cpu.pprof" \
  -memprofile "$OUT/rabitq_collection_c8_mem.pprof"
```

Repeat the same profile command with
`route=quantized_rerank/candidates=32/c={1,8}` if rerank rows are included in a
performance table. Keep profile top files next to the raw `.pprof` files:

```sh
go tool pprof -top "$OUT/rabitq_collection_c1_cpu.pprof" > "$OUT/rabitq_collection_c1_cpu_top.txt"
go tool pprof -top -alloc_space "$OUT/rabitq_collection_c1_mem.pprof" > "$OUT/rabitq_collection_c1_mem_top.txt"
```

## Representative local closeout run

Run context: Apple M3 / darwin arm64 / Go `go1.26.0`, `GOMAXPROCS=8`,
2026-06-06, repository code at `16be95544ee5bcff8726911f2e150f4e8e67d1cd`
before docs-only #2454 edits. Local artifact directory:
`/tmp/gomap_2454_closeout_20260606_072228/`.

Medians are selected from the three-count `-benchtime=100x` matrix. They are
fixture evidence, not a universal recall/speed guarantee.

### Search comparison rows

| Row / boundary | ns/op | ops/sec | B/op | allocs/op | recall@K vs exact | route counters | exact-read counters | selected score-plane code B/search | selected score-plane code B/vector | selected score-plane asset B/vector |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: |
| exact FP32, lower-level `SearchWithBuffer`, c=1 | 11,459 | 87,270 | 0 | 0 | 100% | `hnsw_search_pack=1`, quantized routes `0` | `vector_B/search=86,528`, `norm_B/search=0` | 0 | — | — |
| scalar_u8 `quantized_only`, lower-level `SearchWithBuffer`, c=1 | 9,850 | 101,519 | 0 | 0 | 100% | `quantized_only=1`, scorer active `1` | vector/norm `0/0` | 21,632 | 128 | 169.7 |
| RaBitQ pure-Go `quantized_only`, lower-level `SearchWithBuffer`, c=1 | 38,243 | 26,149 | 0 | 0 | 100% | `quantized_only=1`, scorer active `1` | vector/norm `0/0` | 2,704 | 16 | 66.62 |
| scalar_u8 collection buffered `quantized_only`, c=1 | 10,692 | 93,527 | 0 | 0 | 100% | `quantized_only=1`, scorer active `1` | vector/norm `0/0` | 21,632 | 128 | 169.7 |
| scalar_u8 collection buffered `quantized_only`, c=8 | 3,006 | 332,640 | 0 | 0 | 100% | `quantized_only=1`, scorer active `1` | vector/norm `0/0` | 21,632 | 128 | 169.7 |
| RaBitQ pure-Go collection buffered `quantized_only`, c=1 | 40,969 | 24,409 | 0 | 0 | 100% | `quantized_only=1`, scorer active `1` | vector/norm `0/0` | 2,704 | 16 | 66.62 |
| RaBitQ pure-Go collection buffered `quantized_only`, c=8 | 12,273 | 81,480 | 0 | 0 | 100% | `quantized_only=1`, scorer active `1` | vector/norm `0/0` | 2,704 | 16 | 66.62 |
| RaBitQ pure-Go collection buffered `quantized_rerank`, c=1 | 48,567 | 20,590 | 0 | 0 | 100% | `quantized_rerank=1`, scorer active `1` | `vector_B/search=16,384`, `norm_B/search=128`, exact calls `32` | 2,704 | 16 | 66.62 |
| RaBitQ pure-Go collection buffered `quantized_rerank`, c=8 | 26,389 | 37,895 | 0 | 0 | 100% | `quantized_rerank=1`, scorer active `1` | `vector_B/search=16,384`, `norm_B/search=128`, exact calls `32` | 2,704 | 16 | 66.62 |

The selected score-plane columns above are not a raw dump of every benchmark
fixture metric. The exact row's raw line still emits
`quantized_code_B/vector=128` and `quantized_asset_B/vector=169.7` because the
fixture declares a scalar_u8 asset, but exact mode selects no quantized score
plane; its route counters and `quantized_code_B/search=0` show that no quantized
scorer was active.

All rows above reported `quantized_asset_unavailable/search=0`,
`docs_fetched/search=0`, `graph_row_fallbacks/search=0`,
`typed_column_vector_fallbacks/search=0`, and `vector_scratch_decodes/search=0`
where those counters apply.

### Rebuild/storage rows

| Rebuild row | ns/op | ops/sec | B/op | allocs/op | graph total storage B/op | quantized asset B/op | quantized asset B/vector | logical code B/vector | exact vector+norm B/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| exact assets | 68,012,945 | 14.70 | 10,571,129 | 86,946 | 351,800 | 0 | 0 | 0 | 516 |
| scalar_u8 assets | 69,585,830 | 14.37 | 10,954,631 | 87,491 | 396,512 | 44,712 | 174.7 | 128 | 516 |
| RaBitQ assets | 69,606,614 | 14.37 | 10,793,037 | 87,575 | 370,864 | 19,064 | 74.47 | 16 | 516 |

Interpretation: on this small deterministic fixture, RaBitQ has much smaller
logical code bytes and lower asset bytes/vector than scalar_u8, but the pure-Go
RaBitQ weighted scorer is slower than exact FP32 and scalar_u8. Do not claim a
current RaBitQ speedup or accelerated backend. The useful landed properties are explicit
mode semantics, durable compact assets, fail-closed behavior, exact-read
accounting, 0 steady-state allocations in buffered rows, and reproducible recall
on this fixture.
