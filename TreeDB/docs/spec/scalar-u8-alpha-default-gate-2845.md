# scalar_u8 per-granule alpha default gate (#2845)

Status: **no-promote / explicit opt-in** after the #2869 final gate run on
latest `main`.

Per-existing-storage-granule scalar alpha (`scalar_u8_calibration.mode =
"per_granule_alpha"`) remains supported as an explicit `scalar_u8` calibration
mode, but it is **not** the default for new `scalar_u8` declarations. Omitted
`scalar_u8_calibration` continues to mean legacy `scalar_u8` v1 with empty codec
config and config hash `0`; explicit `{"mode":"legacy"}` remains preserved.

## Decision rationale

The calibrated scorer keeps the material quality benefit on the 10k x 768
production gate fixture: legacy `scalar_u8` rows remain at `81.25%` recall while
per-granule-alpha rows reach `100%` recall. After #2866/#2867/#2868, alpha hot
rows are much closer to legacy and remain `0 B/op`, `0 allocs/op`, but the final
#2869 gate still shows persistent collection-row runtime regressions at c=1
(+4.9% qonly, +5.4% rerank) and smaller c=8 regressions/noise.

Because the promotion gate requires no unaccepted material runtime regression,
the default is not promoted. This is intentionally conservative. Reconsidering
the default should start from latest `main`, rerun the full gate, and
explain/optimize any remaining local hot-row regression before promotion.

## Evidence artifacts

Host/run context:

- Branch/worktree: `codex/2845-scalar-u8-alpha-default-gate`, after merging
  latest `origin/main` through #2850 (`efe052f2`).
- Host: linux/amd64, Intel i5-11400F, Go test reported `GOMAXPROCS=8`.
- Required correctness passed:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/internal/vectorops ./TreeDB/internal/quantizedasset ./TreeDB/collections -count=1
```

Required performance matrix run with required shape, benchtime, and count:

```sh
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_768 GOMAXPROCS=8 GOWORK=off \
  go test ./TreeDB/collections -run '^$' \
  -bench 'ScalarU8Quantized.*241|CollectionVectorQuantizedProductionGate2591' \
  -benchmem -benchtime=100000x -count=10
```

Local raw artifact: `/tmp/issue2853_required_count10_latest_main.txt`.

Rebuild/storage smoke from the earlier gate run:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926$' \
  -benchmem -benchtime=20x -count=3
```

## Median rows from latest-main count=10 gate

10k x 768 production gate, `Collection.SearchVectorIndexWithBuffer`:

| Mode | Route | c | median ns/op | recall@K | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| exact FP32 | exact | 1 | 26,723 | 100% | 0 | 0 |
| legacy `scalar_u8` | quantized_only | 1 | 25,357 | 81.25% | 0 | 0 |
| alpha `scalar_u8` | quantized_only | 1 | 27,599 | 100% | 0 | 0 |
| legacy `scalar_u8` | quantized_only | 8 | 4,555 | 81.25% | 0 | 0 |
| alpha `scalar_u8` | quantized_only | 8 | 4,592 | 100% | 0 | 0 |
| legacy `scalar_u8` | quantized_rerank cand=32 | 1 | 29,076 | 81.25% | 0 | 0 |
| alpha `scalar_u8` | quantized_rerank cand=32 | 1 | 31,427 | 100% | 0 | 0 |
| legacy `scalar_u8` | quantized_rerank cand=32 | 8 | 5,166 | 81.25% | 0 | 0 |
| alpha `scalar_u8` | quantized_rerank cand=32 | 8 | 5,591 | 100% | 0 | 0 |

Standalone #2414/#2415 collection scalar rows on the same shape showed 100%
recall for both legacy and alpha, with alpha hot rows also carrying
`QuantizedScoreCodecScalarU8Alpha/search=1` and
`quantized_score_codec_scalar_u8_alpha/search=1`.

Counters and storage:

- Alpha rows reported `per_granule_alpha_row=1` and `scalar_u8_alpha_row=1`.
- `quantized_only` rows kept `vector_B/search=0` and `norm_B/search=0`.
- `quantized_rerank` rows read only the configured shortlist (`vector_B/search=98304`, `norm_B/search=128` for cand=32, 768 dims).
- Hot rows remained `0 B/op`, `0 allocs/op` in the count=10 gate.
- Code assets remained mmap-backed where supported; the small alpha sidecar was
  measured as heap-copy prepared metadata.
- On the 10k x 768 gate, alpha quantized assets reported about
  `776.5 B/vector` total, including about `0.2256 B/vector` alpha metadata.
- Rebuild/storage smoke (256 x 128 shape): legacy `scalar_u8` asset bytes were
  142.8 B/vector; alpha total quantized assets were 151.5 B/vector, including
  8.766 B/vector of alpha metadata. Median rebuild time was roughly 89.4 ms
  legacy vs 91.9 ms alpha. Alpha distribution on that shape was one granule with
  alpha min/mean/max `0.1495` and code-boundary rate `0.1678%`.

## Final #2869 gate evidence

Host/run context:

- Branch/worktree: `snissn/2869-alpha-default-gate`, based on latest
  `origin/main` after #2868 merge (`6e207ed8d7bb900efe1b57bda3188d2c3cb6bae5`).
- Host: linux/amd64, Intel i5-11400F; `go version go1.25.7 linux/amd64`;
  `GOMAXPROCS=8`.
- Raw artifacts: `/tmp/alpha_2869_gate_20260619_105024/production_gate_count10.txt`
  and `/tmp/alpha_2869_gate_20260619_105024/benchstat.txt`.

Required correctness passed:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/internal/vectorops ./TreeDB/internal/quantizedasset ./TreeDB/collections -count=1
GOWORK=off go test ./TreeDB/docs -count=1
```

Required performance matrix passed:

```sh
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_768 GOMAXPROCS=8 GOWORK=off \
  go test ./TreeDB/collections -run '^$' \
  -bench 'ScalarU8Quantized.*241|CollectionVectorQuantizedProductionGate2591' \
  -benchmem -benchtime=100000x -count=10
```

10k x 768 production gate, `Collection.SearchVectorIndexWithBuffer` medians:

| Mode | Route | c | median ns/op | recall@K | ops/sec | B/op | allocs/op | candidates/search | edges/search | code B/vector | vector B/search | norm B/search | alpha asset B/vector | alpha scorer counter |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| exact FP32 | exact | 1 | 27,066 | 100% | 36,946.5 | 0 | 0 | 0 | 0 | 768 | 583,872 | 0 | 0 | 0 |
| exact FP32 | exact | 8 | 4,662 | 100% | 214,489.5 | 0 | 0 | 0 | 0 | 768 | 583,872 | 0 | 0 | 0 |
| legacy `scalar_u8` | quantized_only | 1 | 23,138 | 81.25% | 43,218 | 0 | 0 | 134.8 | 4,315 | 768 | 0 | 0 | 0 | 0 |
| legacy `scalar_u8` | quantized_only | 8 | 4,038 | 81.25% | 248,298 | 0 | 0 | 134.8 | 4,315 | 768 | 0 | 0 | 0 | 0 |
| legacy `scalar_u8` | quantized_rerank cand=32 | 1 | 27,048 | 81.25% | 36,971.5 | 0 | 0 | 134.8 | 4,315 | 768 | 98,304 | 128 | 0 | 0 |
| legacy `scalar_u8` | quantized_rerank cand=32 | 8 | 4,702 | 81.25% | 212,855 | 0 | 0 | 134.8 | 4,315 | 768 | 98,304 | 128 | 0 | 0 |
| alpha `scalar_u8` | quantized_only | 1 | 24,282 | 100% | 41,182 | 0 | 0 | 135 | 4,286 | 768 | 0 | 0 | 0.226 | 1 |
| alpha `scalar_u8` | quantized_only | 8 | 4,082 | 100% | 244,974.5 | 0 | 0 | 135 | 4,286 | 768 | 0 | 0 | 0.226 | 1 |
| alpha `scalar_u8` | quantized_rerank cand=32 | 1 | 28,506 | 100% | 35,079.5 | 0 | 0 | 135 | 4,286 | 768 | 98,304 | 128 | 0.226 | 1 |
| alpha `scalar_u8` | quantized_rerank cand=32 | 8 | 4,733 | 100% | 211,294.5 | 0 | 0 | 135 | 4,286 | 768 | 98,304 | 128 | 0.226 | 1 |

Alpha/legacy median runtime ratios for the production collection row were:

- quantized-only c=1: `1.049x` (`+4.9%`)
- quantized-only c=8: `1.011x` (`+1.1%`)
- rerank cand=32 c=1: `1.054x` (`+5.4%`)
- rerank cand=32 c=8: `1.006x` (`+0.6%`)

Counters remained healthy: alpha rows reported
`QuantizedScoreCodecScalarU8Alpha/search=1`, `per_granule_alpha_row=1`, and
`scalar_u8_alpha_row=1`; legacy rows reported those alpha counters as `0`.
There were no adjacency, graph-row, score-float64, or typed-vector fallbacks in
these rows. Quantized-only rows kept `vector_B/search=0` and `norm_B/search=0`;
rerank rows read only the configured shortlist (`vector_B/search=98304`,
`norm_B/search=128`).

## Required policy

- New declarations that omit `scalar_u8_calibration` MUST continue to normalize
  as legacy `scalar_u8` v1.
- Callers that want calibrated alpha MUST opt in explicitly with
  `mode="per_granule_alpha"`, `grouping="storage_layout_granule"`, and a
  supported `alpha_policy`.
- Explicit `mode="legacy"` MUST remain accepted and behaviorally identical to
  omission.
- Alpha assets/scorers MUST continue to fail closed on missing, stale,
  mismatched, closed, non-finite, or invalid alpha metadata; they MUST NOT fall
  back to exact traversal silently.
