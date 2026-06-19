# scalar_u8 per-granule alpha default gate (#2845)

Status: **no-promote / explicit opt-in** as of the latest-main #2845 gate run.

Per-existing-storage-granule scalar alpha (`scalar_u8_calibration.mode =
"per_granule_alpha"`) remains supported as an explicit `scalar_u8` calibration
mode, but it is **not** the default for new `scalar_u8` declarations. Omitted
`scalar_u8_calibration` continues to mean legacy `scalar_u8` v1 with empty codec
config and config hash `0`; explicit `{"mode":"legacy"}` remains preserved.

## Decision rationale

The calibrated scorer showed a material quality benefit on the 10k x 768
production gate fixture, but latest-main local evidence was mixed on hot
collection runtime: several collection buffered rows regressed versus legacy
`scalar_u8`. Because the promotion gate requires no unaccepted material runtime
regression, the default is not promoted.

This is intentionally conservative. Reconsidering the default should start from
latest `main`, rerun the full gate, and explain/optimize any local hot-row
regression before promotion.

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
