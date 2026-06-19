# scalar_u8 per-granule alpha default gate (#2845)

Status: **no-promote / explicit opt-in** as of the local #2845 gate run.

Per-existing-storage-granule scalar alpha (`scalar_u8_calibration.mode =
"per_granule_alpha"`) remains supported as an explicit `scalar_u8` calibration
mode, but it is **not** the default for new `scalar_u8` declarations. Omitted
`scalar_u8_calibration` continues to mean legacy `scalar_u8` v1 with empty codec
config and config hash `0`; explicit `{"mode":"legacy"}` remains preserved.

## Decision rationale

The calibrated scorer showed a material quality benefit on the 10k x 768
production gate fixture, but local evidence was mixed on hot collection runtime:
several collection buffered rows regressed by about 8-23% versus legacy
`scalar_u8`. Because the promotion gate requires no unaccepted material runtime
regression, the default is not promoted.

This is intentionally conservative. The run was `-count=3` local evidence, not
the full `-count=10` latest-main gate, and the branch is still based on the
#2844/#2848/#2849 predecessor snapshot. Revalidate after those predecessors
merge before reconsidering the default.

## Evidence artifacts

Host/run context:

- Branch/worktree: `codex/2845-scalar-u8-alpha-default-gate`, based on #2844
  snapshot `dec9d15e9`.
- Host: linux/amd64, Intel i5-11400F, Go test reported `GOMAXPROCS=8`.
- Required correctness passed:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/internal/vectorops ./TreeDB/internal/quantizedasset ./TreeDB/collections -count=1
```

Performance smoke used the required shape and benchtime with reduced count:

```sh
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_768 GOMAXPROCS=8 GOWORK=off \
  go test ./TreeDB/collections -run '^$' \
  -bench 'ScalarU8Quantized.*241|CollectionVectorQuantizedProductionGate2591' \
  -benchmem -benchtime=100000x -count=3
```

Artifact: `/tmp/issue2845_required_count3_20260618_162609.txt`.

Rebuild/storage smoke:

```sh
GOMAXPROCS=8 GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkColumnGraphScalarU8QuantizedRebuildStorage1926$' \
  -benchmem -benchtime=20x -count=3
```

Artifact: `/tmp/issue2845_rebuild_storage_20260618_163435.txt`.

## Median rows from local count=3 smoke

10k x 768 production gate, `Collection.SearchVectorIndexWithBuffer`:

| Mode | Route | c | median ns/op | recall@K | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| exact FP32 | exact | 1 | 26,574 | 100% | 0 | 0 |
| legacy `scalar_u8` | quantized_only | 1 | 24,777 | 81.25% | 0 | 0 |
| alpha `scalar_u8` | quantized_only | 1 | 26,627 | 100% | 0 | 0 |
| legacy `scalar_u8` | quantized_only | 8 | 4,045 | 81.25% | 0 | 0 |
| alpha `scalar_u8` | quantized_only | 8 | 4,578 | 100% | 0 | 0 |
| legacy `scalar_u8` | quantized_rerank cand=32 | 1 | 28,482 | 81.25% | 0 | 0 |
| alpha `scalar_u8` | quantized_rerank cand=32 | 1 | 34,984 | 100% | 0 | 0 |
| legacy `scalar_u8` | quantized_rerank cand=32 | 8 | 4,689 | 81.25% | 0 | 0 |
| alpha `scalar_u8` | quantized_rerank cand=32 | 8 | 5,507 | 100% | 0 | 0 |

Lower-level `VectorIndexSearcher.SearchWithBuffer` production rows were more
favorable to alpha, but the collection route is the production serving boundary
for this gate and showed material regressions.

Standalone #2414/#2415 scalar rows on the same shape showed 100% recall for both
legacy and alpha. Alpha still regressed several hot rows, including collection
`quantized_only/c=8` (4,243 ns/op legacy vs 5,244 ns/op alpha) and lower-level
`quantized_rerank/c=8` (4,369 ns/op legacy vs 5,328 ns/op alpha).

Counters and storage:

- Alpha rows reported `quantized_score_codec_scalar_u8_alpha/search=1`.
- `quantized_only` rows kept `vector_B/search=0` and `norm_B/search=0`.
- `quantized_rerank` rows read only the configured shortlist (`vector_B/search=98304`, `norm_B/search=128` for cand=32, 768 dims).
- Hot rows remained `0 B/op`, `0 allocs/op` in the count=3 smoke.
- Code assets remained mmap-backed where supported; the small alpha sidecar was
  measured as heap-copy prepared metadata.
- Rebuild/storage (256 x 128 shape): legacy `scalar_u8` asset bytes were
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
