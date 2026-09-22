# scalar_u8 per-granule alpha optimization baseline (#2865)

Status: baseline/profile runbook for the #2864 optimization graph. This page is
benchmark-adjacent evidence, not a default-promotion decision. The current
promotion decision is the #2869 final **no-promote / explicit opt-in** gate in
`scalar-u8-alpha-default-gate-2845.md`.

## Baseline context

The #2845 default gate showed that explicit `scalar_u8` per-granule alpha keeps
hot collection rows at `0 B/op`, `0 allocs/op` and improves the 10k x 768
production gate recall from `81.25%` to `100%`, but it regressed several hot
collection runtimes versus legacy `scalar_u8`. Omitted `scalar_u8_calibration`
therefore still means legacy `scalar_u8` v1.

Latest-main #2845 count=10 production collection medians used as the starting
comparison point:

| Row | Legacy median ns/op | Alpha median ns/op | Recall notes |
| --- | ---: | ---: | --- |
| `quantized_only c=1` | 25,357 | 27,599 | alpha `100%`, legacy `81.25%` |
| `quantized_only c=8` | 4,555 | 4,592 | alpha `100%`, legacy `81.25%` |
| `quantized_rerank cand=32 c=1` | 29,076 | 31,427 | alpha `100%`, legacy `81.25%` |
| `quantized_rerank cand=32 c=8` | 5,166 | 5,591 | alpha `100%`, legacy `81.25%` |

Fresh local hot-loop profile artifacts captured after #2853 are under
`/tmp/alpha_profile_20260619_084353/`:

- `legacy_hot_qonly_c1_cpu.pprof`
- `alpha_hot_qonly_c1_cpu.pprof`
- `legacy_hot_qonly_c1.txt`
- `alpha_hot_qonly_c1.txt`
- `hot_diff_top.txt`

Focused hot-loop qonly c=1 snapshot from that run:

| Row | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| legacy `BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415/route=quantized_only/c=1` | 24,355 | 0 | 0 |
| alpha `BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415/route=quantized_only/c=1` | 26,211 | 0 | 0 |

Profile suspects to attribute before/after:

- `scoreFromDotRowID`: alpha `5.7%` vs legacy `1.6%`
- `AlphaForRow`: `2.3%`
- `granuleForRow`: `1.6%`
- `prepareScalarU8QuantizedScorer`: alpha `18.2%` vs legacy `13.6%`
- `columnVectorGraphScalarU8Code`: alpha `10.9%` vs legacy `7.0%`

## Required correctness smoke

Run before claiming an optimization PR is ready:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/internal/vectorops ./TreeDB/internal/quantizedasset ./TreeDB/collections -count=1
GOWORK=off go test ./TreeDB/docs -count=1
```

If the final default/config behavior changes, also run:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/nativewire ./TreeDB/documentservice -count=1
```

## Focused hot-loop profile rows

Use the same host, Go version, branch base, fixture shape, and benchtime for
before/after pairs. The profile env captures the benchmark's timed search loop,
not setup.

```sh
OUT=/tmp/alpha_profile_$(date +%Y%m%d_%H%M%S)
mkdir -p "$OUT"

TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_768 GOMAXPROCS=8 GOWORK=off \
  TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_CPU_PROFILE_PATH="$OUT/legacy_hot_qonly_c1_cpu.pprof" \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8Quantized2415/route=quantized_only/c=1$' \
  -benchmem -benchtime=10s -count=1 | tee "$OUT/legacy_hot_qonly_c1.txt"

TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_768 GOMAXPROCS=8 GOWORK=off \
  TREEDB_COLUMN_GRAPH_QUANTIZED_HOT_CPU_PROFILE_PATH="$OUT/alpha_hot_qonly_c1_cpu.pprof" \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionSearchVectorIndexWithBufferColumnGraphScalarU8QuantizedAlpha2415/route=quantized_only/c=1$' \
  -benchmem -benchtime=10s -count=1 | tee "$OUT/alpha_hot_qonly_c1.txt"

GOWORK=off go tool pprof -top "$OUT/legacy_hot_qonly_c1_cpu.pprof" > "$OUT/legacy_hot_top.txt"
GOWORK=off go tool pprof -top "$OUT/alpha_hot_qonly_c1_cpu.pprof" > "$OUT/alpha_hot_top.txt"
```

## Production gate subset and final gate

For each optimization issue, capture at least the affected production rows for
legacy and alpha. For the #2869 final gate, run the full matrix:

```sh
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_768 GOMAXPROCS=8 GOWORK=off \
  go test ./TreeDB/collections -run '^$' \
  -bench 'ScalarU8Quantized.*241|CollectionVectorQuantizedProductionGate2591' \
  -benchmem -benchtime=100000x -count=10
```

The final table must include legacy and alpha `quantized_only` c=1/c=8,
legacy and alpha `quantized_rerank` cand=32 c=1/c=8, exact FP32 reference rows
where present, recall@K, `ns/op`, `ops/sec`, `B/op`, `allocs/op`,
`candidates/search`, `edges/search`, `quantized_code_B/search`,
`quantized_score_codec_scalar_u8_alpha/search`, logical code bytes/vector, and
alpha asset bytes/vector.

## Regression gates

- Alpha must keep the recall/quality benefit from #2845 or explain any change.
- Legacy hot rows must not regress while optimizing alpha.
- Hot rows must remain `0 B/op`, `0 allocs/op`.
- No optimization may weaken alpha fail-closed behavior for missing, stale,
  mismatched, non-finite, or invalid alpha metadata.
- Omitted `scalar_u8_calibration` must remain legacy until the #2869 final gate
  explicitly proves default promotion is safe.
