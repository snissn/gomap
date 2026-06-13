# TreeDB scalar_u8 + fp32 detail query-time experiment

This opt-in experiment evaluates whether a selected high-detail fp32 tier can
improve scalar_u8 score quality on the same large vector shapes used by the
TreeDB quantized benchmark gate. It is intentionally a query-time viability
probe, not a production codec implementation.

## What it measures

For each query and candidate row, the experiment computes the normal TreeDB
scalar_u8 normalized-cosine score, then replaces selected per-dimension scalar_u8
contributions with normalized fp32 contributions:

```text
score = scalar_u8_score
      + sum selected_dim (
          q_norm[dim] * x_norm_fp32[dim]
        - scalar_u8_dim_contribution[dim]
        )
```

The report compares exact fp32 ranking against:

- `u8_base`: scalar_u8 only;
- `row_residual_topk`: query-independent row detail dims selected by largest
  scalar_u8 reconstruction residual;
- `query_score_error_topk`: query-time ceiling selector using the largest
  per-row, per-query score correction magnitudes.

The storage model is deliberately simple:

```text
base scalar_u8: dims bytes/vector
detail tier:    K * (uint16 dim_id + fp32 value) = 6K bytes/vector
```

For `10k_x_768`, that is `768 + 6K` bytes/vector.

## Non-goals

This experiment does not provide:

- a durable quantized codec;
- quantizedasset schema changes;
- mmap/direct prepared assets;
- HNSW traversal integration;
- a production hot-path scorer;
- speed evidence for a future implementation.

Use it to decide whether the accuracy/shortlist-quality tradeoff is worth
building as a real codec.

## Run

Default large shape:

```sh
OUT=$(mktemp -d /tmp/treedb_u8_fp32_detail_XXXXXX)
TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_EXPERIMENT=1 \
TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_OUT="$OUT" \
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_768 \
TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_DATASET=clustered \
GOMAXPROCS=8 GOWORK=off \
go test ./TreeDB/collections -run '^TestColumnGraphU8FP32DetailQueryExperiment$' -count=1
```

Useful overrides:

```sh
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_QUERIES=2
TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_KS=0,16,64,128
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_1536
TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_DATASET=rebuild_synthetic
```

Artifacts:

```text
$OUT/results.json
$OUT/report.md
```

## Initial smoke evidence

A short `10k_x_768`, two-query run completed successfully:

```text
/tmp/u8_fp32_detail_10k768_zOOAC7/report.md
```

That run is smoke evidence only. Larger/default query-count runs should be used
before making a production codec decision.
