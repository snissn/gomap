# TreeDB scalar_u4 + scalar_u8 detail query-time experiment

This opt-in experiment evaluates whether a packed scalar_u4 base plus selected
scalar_u8 detail dimensions is worth turning into a durable TreeDB quantized
codec. It is intentionally a query-time viability probe, not a production codec
or hot scorer.

The experiment uses the same large synthetic vector shapes as the scalar_u8
quantized benchmark gate. It stores u4 codes in unpacked test arrays for
simplicity, but reports the production-shaped packed storage model.

## What it measures

For each query and candidate row, the experiment computes a zero-preserving
signed scalar_u4 normalized-cosine score, then replaces selected per-dimension
u4 contributions with scalar_u8 contributions:

```text
score = scalar_u4_score
      + sum selected_dim (
          scalar_u8_dim_contribution[dim]
        - scalar_u4_dim_contribution[dim]
        )
```

Selectors:

- `u4z_base`: zero-preserving signed scalar_u4 only;
- `row_u4_u8_delta_topk`: query-independent row detail dims selected by largest
  scalar_u8-vs-scalar_u4 reconstruction delta;
- `query_score_error_topk`: query-time ceiling selector using largest per-row,
  per-query score correction magnitudes.

The storage model is:

```text
packed scalar_u4 base: dims/2 bytes/vector
scalar_u8 detail:      K * (uint16 dim_id + uint8 value) = 3K bytes/vector
```

For `10k_x_768`, that is `384 + 3K` bytes/vector. At `K=128`, the modeled row
payload equals full scalar_u8 (`768 B/vector`).

## Non-goals

This experiment does not provide:

- durable codec identity;
- quantizedasset schema changes;
- mmap/direct prepared assets;
- HNSW traversal integration;
- packed-code scorer throughput evidence.

## Run

```sh
OUT=$(mktemp -d /tmp/treedb_u4_u8_detail_XXXXXX)
TREEDB_COLUMN_GRAPH_U4_U8_DETAIL_EXPERIMENT=1 \
TREEDB_COLUMN_GRAPH_U4_U8_DETAIL_OUT="$OUT" \
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_768 \
TREEDB_COLUMN_GRAPH_U8_FP32_DETAIL_DATASET=clustered \
GOMAXPROCS=8 GOWORK=off \
go test ./TreeDB/collections -run '^TestColumnGraphU4U8DetailQueryExperiment$' -count=1
```

Useful overrides:

```sh
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_QUERIES=2
TREEDB_COLUMN_GRAPH_U4_U8_DETAIL_KS=0,16,64,128
TREEDB_COLUMN_GRAPH_QUANTIZED_BENCH_SHAPE=10k_x_1536
```

Artifacts:

```text
$OUT/results.json
$OUT/report.md
```

## Initial full-shape evidence

A `10k_x_768`, 16-query run completed successfully:

```text
/tmp/u4_u8_detail_10k768_zero_full_eOcq0u/report.md
```

The initial result was not promising for the tested scalar_u4 design: even at
`K=128` (`768 B/vector`, equal to full scalar_u8 payload), candidate gates stayed
well below the scalar_u8 baseline from the paired u8/fp32 experiment. Treat this
as evidence against implementing this exact u4/u8 detail codec without a better
u4 quantizer or selector.
