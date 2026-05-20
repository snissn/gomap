# Deep1B Float Quantization Recall Tournament

Status: implemented and measured as an opt-in Deep1B groundtruth-locality
extension.

Current measured run:

```text
date:          2026-05-20
queries:       official Deep1B public queries 0..99
regime:        official top100 local-neighborhood codec probe
source output: /tmp/gomap_deep1b_float_quant_q0_99_20260520_084512/report.md
```

This document is the standalone home for the next codec-only Deep1B vector
compression experiment. It defines the research framing, hypotheses, method
families, evaluation gates, and report shape. The executable tournament is
behind `COLUMN_VECTOR_DEEP1B_FLOAT_QUANT_TOURNAMENT=1` in
`TestColumnVectorGraphDeep1BGroundtruthLocality`; this file is the canonical
summary of the measured conclusions.

## Research Framing

The current Deep1B source vectors are fp32 values. The central question is not
"which integer codec is best?" but:

```text
Given fp32 source vectors, what lossy numeric representation preserves
retrieval behavior at the smallest information budget?
```

Integer scalar quantization is one way to quantize floats. It should compete
against other float-value quantizers:

```text
affine integer quantization
mantissa-truncated fp32
bf16 / fp16
block floating point
mixed-precision per-dimension and per-row policies
norm-explicit variants
```

This report deliberately separates the codec problem from routing and granule
construction. The initial experiment uses official Deep1B top100 nearest-neighbor
clouds as oracle local-neighborhood probes. These are useful for understanding
local ranking sensitivity and information budgets, but they are not production
TreeDB granules.

## Primary Hypotheses

1. Candidate generation needs much less information than final ranking.

   A representation may fail to preserve exact compressed top10 order while still
   preserving the exact winners inside an approximate top20/top50 shortlist.

2. Quantization is likely a stronger near-term lever than low-rank projection.

   Previous Deep1B top100 probes showed simple scalar u4/u8 lanes preserving
   candidate-survival gates more robustly than local PCA at similar or larger
   payloads. This experiment broadens the quantization arena instead of treating
   u4/u8 as special endpoints.

3. The right bit width is an empirical property of the retrieval gate.

   The tournament should be inverted:

   ```text
   not:    given N bits, what recall do we get?
   but:    given recall gate G, what is the minimum payload that passes?
   ```

4. Retrieval-aware allocation should beat uniform allocation when score margins
   are uneven.

   Some dimensions and rows should matter more near top-k decision boundaries.
   Per-dimension and per-row mixed precision should be evaluated as information
   allocation policies, not only as storage layouts.

5. Norm/magnitude handling is important enough to test explicitly.

   Norm-explicit quantization literature argues that norm error can dominate
   inner-product ranking error. Even when vectors are close to normalized,
   compressed reconstructions can introduce norm drift, so explicit norm or
   inverse-norm lanes should be tested.

## Non-Goals

This experiment does not try to prove production routing quality.

It should not claim production viability for:

```text
TreeDB granule construction
graph routing
IVF/k-means routing
PQ/OPQ/ScaNN/AVQ/QAQ/QINCo training
end-to-end latency under a final storage layout
```

Those are later experiments. This report answers the codec question first:

```text
How many bits, and what numeric shape, does retrieval actually need?
```

## Evaluation Regime

Initial regime:

```text
official Deep1B top100 local-neighborhood codec probe
```

For each selected query:

```text
1. load query q
2. read q's official top100 groundtruth row IDs
3. load the 100 fp32 database vectors
4. compute exact fp32 scores and exact rank order within the top100 cloud
5. compress the 100 vectors with each codec family
6. score q against compressed vectors
7. measure candidate survival and compressed final-ranking behavior
8. rerank approximate shortlists with exact fp32 vectors
```

The query remains fp32 unless a method explicitly defines a query transform.
This isolates the stored-vector codec.

Later regimes may reuse the same codec families on buildable blocks, but that is
out of scope for the first report.

## Run Command

Single-query smoke:

```sh
OUT=/tmp/gomap_deep1b_float_quant_q0_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_LOCALITY=1 \
COLUMN_VECTOR_DEEP1B_FLOAT_QUANT_TOURNAMENT=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B=1 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_QUERIES=0 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_SCAN_ITERS=4 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BGroundtruthLocality$' \
  -count 1 \
  -v
```

Wider report run:

```sh
OUT=/tmp/gomap_deep1b_float_quant_q0_99_$(date +%Y%m%d_%H%M%S)
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_LOCALITY=1 \
COLUMN_VECTOR_DEEP1B_FLOAT_QUANT_TOURNAMENT=1 \
COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 \
COLUMN_VECTOR_DEEP1B_DIR=/private/tmp/gomap-deep1b-cache \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_BASE1B=1 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_FETCH_CONCURRENCY=16 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_QUERIES=$(seq -s, 0 99) \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_PCA_RANKS=8,16,24,32,40,48,56,64,80,96 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_SCAN_ITERS=4 \
COLUMN_VECTOR_DEEP1B_GROUNDTRUTH_OUT="$OUT" \
GOWORK=off go test ./experiments/colgranule \
  -run '^TestColumnVectorGraphDeep1BGroundtruthLocality$' \
  -count 1 \
  -v
```

Outputs:

```text
$OUT/results.json
$OUT/report.md
```

## Measured q0..99 Result

These numbers are from the q0..99 run above with `SCAN_ITERS=1`. They are
official top100 oracle-neighborhood codec results. They do not prove that TreeDB
can build production granules with equivalent locality, and they do not validate
trained codebook methods such as PQ/OPQ/AVQ/QINCo.

### Gate Winners

Payload is the vector row-code payload only. Metadata is listed separately and
is not used as the primary ranking metric for this oracle-top100 regime.

| Gate | Cheapest all-query winner | Payload bits/vector | Payload B/vector | Metadata B/vector |
| --- | --- | ---: | ---: | ---: |
| compressed final top10 >= 9/10 | `float_quant_affine_u6_per_dim_reconstructed_norm` | 576 | 72 | 7.68 |
| compressed final top10 = 10/10 | `float_quant_block_float_u10_per_dim_reconstructed_norm` | 960 | 120 | 3.84 |
| compressed final top20 >= 19/20 | `float_quant_affine_u8_per_dim_f16_explicit_norm` | 768 | 96 | 9.68 |
| compressed final top20 = 20/20 | `float_quant_affine_u14_per_dim_f16_explicit_norm` | 1344 | 168 | 9.68 |
| exact top10 in approx@20 >= 9/10 | `float_quant_affine_u4_per_dim_reconstructed_norm` | 384 | 48 | 7.68 |
| exact top10 in approx@20 = 10/10 | `float_quant_affine_u4_per_dim_reconstructed_norm` | 384 | 48 | 7.68 |
| exact top10 in approx@50 = 10/10 | `float_quant_affine_u4_per_dim_reconstructed_norm` | 384 | 48 | 7.68 |
| exact top20 in approx@50 >= 19/20 | `float_quant_affine_u4_per_dim_reconstructed_norm` | 384 | 48 | 7.68 |
| exact top20 in approx@50 = 20/20 | `float_quant_affine_u4_per_dim_reconstructed_norm` | 384 | 48 | 7.68 |

The primary codec conclusion is therefore:

```text
For official Deep1B top100 local neighborhoods, u4 per-dimension affine
quantization is the conservative candidate-generation winner in this tournament.
It preserves all tested top10/top20 shortlist gates across queries 0..99 at
48 B/vector row payload.
```

This is not a final-ranking result. Compressed-only final ranking requires much
more precision. The cheapest all-query compressed-final winners were 72 B/vector
for top10 >= 9/10, 120 B/vector for exact top10, and 168 B/vector for exact
top20. That supports the cascade design:

```text
u4-ish compressed candidate scan
  -> exact or near-exact rerank
```

instead of trying to make the compressed codec own final top10 ordering.

### Per-Query Lower Bound

The generated report also lets each query choose its cheapest passing
float-quant method independently. This is not a deployable policy by itself, but
it shows how much headroom a better adaptive codec could have.

| Gate | Passing queries | p50 payload bits/vector | p90 payload bits/vector | Worst passing payload bits/vector |
| --- | ---: | ---: | ---: | ---: |
| compressed final top10 >= 9/10 | 100/100 | 384.0 | 394.6 | 480.0 |
| compressed final top10 = 10/10 | 100/100 | 384.0 | 480.0 | 768.0 |
| compressed final top20 >= 19/20 | 100/100 | 384.0 | 480.0 | 576.0 |
| compressed final top20 = 20/20 | 100/100 | 576.0 | 672.0 | 960.0 |
| exact top10 in approx@20 >= 9/10 | 100/100 | 192.0 | 288.0 | 384.0 |
| exact top10 in approx@20 = 10/10 | 100/100 | 288.0 | 384.0 | 384.0 |
| exact top10 in approx@50 = 10/10 | 100/100 | 108.0 | 192.0 | 288.0 |
| exact top20 in approx@50 >= 19/20 | 100/100 | 192.0 | 288.0 | 384.0 |
| exact top20 in approx@50 = 20/20 | 100/100 | 259.2 | 288.0 | 384.0 |

This says adaptive quantization is still worth studying, but the current simple
all-query policy to promote is not a mixed row/dimension oracle. It is u4
per-dimension affine.

### Method-Level Observations

Selected aggregate rows from the q0..99 report:

| Method | Payload B/vector | p50 compressed top10 | Worst compressed top10 | Worst top10@20 | Worst top10@50 | Worst top20@50 | Avg err/gap10 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `float_quant_affine_u3_per_dim_reconstructed_norm` | 36 | 8/10 | 5/10 | 7/10 | 9/10 | 18/20 | 16.57 |
| `float_quant_affine_u4_per_dim_reconstructed_norm` | 48 | 9/10 | 7/10 | 10/10 | 10/10 | 20/20 | 6.19 |
| `float_quant_random_rotation_affine_u4_per_dim_reconstructed_norm` | 48 | 9/10 | 7/10 | 9/10 | 10/10 | 20/20 | 6.22 |
| `float_quant_affine_u6_per_dim_reconstructed_norm` | 72 | 10/10 | 9/10 | 10/10 | 10/10 | 20/20 | 1.41 |
| `float_quant_block_float_u10_per_dim_reconstructed_norm` | 120 | 10/10 | 10/10 | 10/10 | 10/10 | 20/20 | 0.16 |
| `float_quant_bf16_reconstructed_norm` | 192 | 10/10 | 9/10 | 10/10 | 10/10 | 20/20 | 0.25 |
| `float_quant_fp16_reconstructed_norm` | 192 | 10/10 | 10/10 | 10/10 | 10/10 | 20/20 | 0.03 |
| `float_quant_fp32_reference_reconstructed_norm` | 384 | 10/10 | 10/10 | 10/10 | 10/10 | 20/20 | 0.00 |

Interpretation:

```text
u1/u2/u3 are useful diagnostics but not reliable conservative candidates.
u4 per-dim affine is the first all-query candidate-survival threshold.
u6 per-dim affine is the first all-query compressed-top10 >= 9/10 threshold.
u10-ish block-float is needed for exact compressed top10 in this run.
bf16/fp16/mantissa-style float formats are near-final/rerank lanes, not
candidate-generation winners by payload.
```

Explicit norm lanes did not become the candidate-generation winner. They helped
some higher-precision compressed-final gates, but the all-query candidate gates
were won by reconstructed-norm per-dimension affine u4.

## Size Accounting

The primary storage metric is:

```text
vector payload bits/vector
```

This intentionally ignores byte alignment and packing convenience. If 5.3
bits/value is the best information budget, the report should say that before we
translate it into an implementation layout.

Secondary metrics:

```text
scan-side tag bits/vector
metadata shape
metadata bytes/vector, when the denominator is meaningful
```

For official top100 probes, metadata amortization is not a promotion gate
because 100 rows is not a realistic TreeDB granule denominator. Shared scale
tables, bit plans, and codec headers should be described, but the headline result
is payload information budget.

## Method Families

The first implementation includes these executable lanes:

```text
affine per-dimension u1..u32 with reconstructed norms
affine per-dimension u1..u32 with f16 explicit norms
affine per-row/global selected widths: u1/u2/u3/u4/u5/u6/u8/u10/u12/u16/u20/u24/u32
block-float per-dimension/per-row selected widths: u1/u2/u3/u4/u5/u6/u8/u10/u12/u16/u20/u24/u32
fp32 mantissa truncation m0..m23
bf16, fp16, fp32 reference
random-rotation affine selected widths: u1/u2/u4/u8/u16/u32
greedy mixed per-dimension precision plans for candidate-survival gates
greedy mixed per-row precision classes for candidate-survival gates
base-u4 plus row-exception upgrades for candidate-survival gates
```

The mixed-precision lanes use exact row norms to isolate value-quantization
error. They are intentionally oracle codec probes: they answer whether
retrieval-aware bit allocation can reduce payload, not whether the metadata
layout is production-ready.

### 1. Uniform Affine Integer Quantization

Test every bit width:

```text
u1, u2, ..., u32
```

Scale/range policies:

```text
global / granule affine
per-dimension affine
per-row affine
per-dimension symmetric
per-row symmetric
```

For each value:

```text
x_hat = offset + scale * code
```

This family answers:

```text
How many absolute fixed-point bits over a chosen range does retrieval need?
```

### 2. Mantissa-Truncated FP32

Keep fp32 sign and exponent, but truncate or round the mantissa:

```text
fp32_m0, fp32_m1, ..., fp32_m23
```

Where `m23` is original fp32 significand precision, and lower `m` values retain
less relative precision.

This family answers:

```text
How many floating relative-precision bits does retrieval need?
```

Important comparison points:

```text
fp32_m7   roughly bf16 mantissa precision
fp32_m10  roughly fp16/tf32 mantissa precision
fp32_m23  fp32 reference mantissa precision
```

### 3. Standard Reduced Floats

Include actual reduced floating formats:

```text
bf16
fp16
fp32 reference
```

These are not equivalent to mantissa-truncated fp32 because exponent ranges
differ. They should be treated as separate numeric representations.

### 4. Block Floating Point

Use a shared exponent or scale with a low-bit mantissa/integer payload.

Candidate policies:

```text
per-row block float
per-dimension block float
per-cloud / granule block float
```

Mantissa ladder:

```text
m1, m2, ..., m23
```

This family bridges affine fixed-point and floating-point representations.

### 5. Per-Dimension Mixed Precision

Each dimension gets one bit width shared by all rows:

```text
dim 0 -> 3 bits
dim 1 -> 8 bits
dim 2 -> 5 bits
...
```

This preserves a columnar scan shape while allowing dimensions with higher
retrieval sensitivity to receive more precision.

Allocator sketch:

```text
1. start all dimensions at a low bit width
2. score compressed vectors
3. identify failed recall gates and boundary score errors
4. upgrade the dimension that reduces margin-normalized score error most per bit
5. repeat until the target gate passes or original fidelity is reached
```

The allocator should run per gate. The report should not assume a single bit
plan is optimal for every recall target.

### 6. Per-Row Precision Class

Each row gets one precision class:

```text
row i -> u2
row j -> u5
row k -> u12
```

This tests the LVQ-style intuition that some vectors are harder to represent
than others. Rows near top-k decision boundaries should be allowed to spend more
bits than obvious winners or obvious losers.

Allocator sketch:

```text
1. start all rows at a low bit width
2. score compressed vectors
3. identify true winners that fall below the shortlist or hard negatives that rise too high
4. upgrade rows involved in those boundary failures
5. repeat until the target gate passes or original fidelity is reached
```

Report:

```text
average payload bits/vector
worst-query payload bits/vector
row precision histogram
fraction of rows needing high precision
```

### 7. Base Codec Plus Exceptions

Start with a simple base representation:

```text
u2, u3, u4, mantissa m7, or block-float mN
```

Then add exception payloads for fragile rows or dimensions:

```text
row exceptions
dimension exceptions
residual sidecar
explicit norm sidecar
```

This family answers:

```text
Can a mostly cheap representation match high-precision candidate survival by
repairing only the boundary-sensitive cases?
```

### 8. Norm-Explicit Variants

For every major family, test:

```text
reconstructed norm
explicit f16 norm or inverse norm
explicit quantized norm
```

This is a lightweight NEQ-style probe. It is not a full NEQ implementation, but
it directly tests whether explicit magnitude correction reduces boundary flips.

## Recall Gates

The report should use a wide gate set and then invert each gate into a minimum
storage requirement.

Candidate-survival gates:

```text
top10 in approx@10 = 10/10
top10 in approx@20 >= 9/10
top10 in approx@20 = 10/10
top10 in approx@50 = 10/10
top20 in approx@20 = 20/20
top20 in approx@50 >= 19/20
top20 in approx@50 = 20/20
top50 in approx@75 >= 48/50
```

Exact-rerank gates:

```text
final recall@10 after exact rerank@20 >= 9/10
final recall@10 after exact rerank@50 = 10/10
final recall@20 after exact rerank@50 >= 19/20
```

Compressed-final gates:

```text
compressed final top10 recall >= 9/10
compressed final top10 recall = 10/10
compressed final top20 recall >= 19/20
compressed final top20 recall = 20/20
```

For top100-only probes, `approx@100` should be reported for continuity but not
used as a primary gate because the universe contains only 100 candidates.

## Score Diagnostics

Every method should report score error relative to rank margins:

```text
mean absolute score error
max absolute score error
score error / rank10-11 margin
score error / rank20-21 margin
score error / rank50-51 margin
pairwise inversion count near top10/top20/top50 boundaries
```

Margin-normalized error is the main diagnostic for why a method fails. A codec
with small average error can still fail if the error exceeds the local top-k
boundary gap.

## Primary Report Tables

### Gate Winner Table

This is the headline table.

```text
Gate                                  Cheapest method   Payload bits/vector   Notes
top10 in approx@20 >= 9/10            TBD               TBD                   TBD
top10 in approx@50 = 10/10            TBD               TBD                   TBD
top20 in approx@50 >= 19/20           TBD               TBD                   TBD
rerank@20 final recall@10 >= 9/10     TBD               TBD                   TBD
compressed final top10 >= 9/10        TBD               TBD                   TBD
compressed final top10 = 10/10        TBD               TBD                   TBD
```

### Method Curve Table

Each method family should also expose its Pareto curve.

```text
Method family   Parameter   Payload bits/vector   Candidate gates   Final gates   Score error/margin
uniform affine  u4          TBD                   TBD               TBD           TBD
uniform affine  u8          TBD                   TBD               TBD           TBD
fp32 mantissa   m10         TBD                   TBD               TBD           TBD
fp32 mantissa   m16         TBD                   TBD               TBD           TBD
block float     row_m8      TBD                   TBD               TBD           TBD
mixed dim       gate_X      TBD                   TBD               TBD           TBD
mixed row       gate_X      TBD                   TBD               TBD           TBD
```

### Mixed-Precision Diagnostics

For per-dimension mixed precision:

```text
bit-width histogram by dimension
dimensions upgraded most often
average bits/value
gate-specific bit plan
```

For per-row mixed precision:

```text
row precision histogram
rows upgraded most often by rank bucket
average payload bits/vector
worst-query payload bits/vector
high-precision fallback fraction
```

## Promotion Rules

A method is worth promoting only if it wins a real axis:

```text
same recall gate with fewer payload bits
same payload bits with better candidate survival
u8-like survival with materially less than 8 bits/value
fp16/fp32-like final ranking with materially fewer bits
better p90 or worst-query behavior
clearer diagnosis of which information matters
```

If a mixed-precision method does not beat a simple uniform lane, the conclusion
should say that directly. A simple codec is preferred unless the adaptive method
buys a real Pareto improvement.

## Relationship To Literature

This experiment is not a full implementation of ScaNN/AVQ, Query-Aware
Quantization, QUIP, NEQ, LVQ/SVS, RaBitQ, or TurboQuant. It uses their lessons to
design a TreeDB-specific scalar codec arena:

```text
ScaNN/AVQ, QAQ, QUIP:
  optimize score-relevant error rather than generic reconstruction error

NEQ:
  preserve norm/magnitude explicitly

LVQ/SVS:
  use locally or per-vector adaptive scalar compression

RaBitQ/TurboQuant:
  treat low-build quantization and score-estimation behavior as serious baselines
```

Reference links:

- Existing Deep1B groundtruth report:
  `docs/benchmarks/column_vector_deep1b_groundtruth_compression_report.md`
- Deep1B benchmark runbook:
  `docs/benchmarks/column_vector_deep1b.md`
- NEQ:
  https://ojs.aaai.org/index.php/AAAI/article/view/5333
- Intel SVS / LVQ:
  https://intel.github.io/ScalableVectorSearch/cpp/quantization/lvq.html
- ScaNN / AVQ:
  https://proceedings.mlr.press/v119/guo20h.html
- Query-Aware Quantization:
  https://ojs.aaai.org/index.php/AAAI/article/view/25613
- QUIP:
  https://proceedings.mlr.press/v51/guo16a.html
- RaBitQ:
  https://arxiv.org/abs/2405.12497
- TurboQuant:
  https://arxiv.org/abs/2504.19874

## Implementation Status

The tournament is implemented as one opt-in test/report path:

```text
env:  COLUMN_VECTOR_DEEP1B_FLOAT_QUANT_TOURNAMENT=1
test: TestColumnVectorGraphDeep1BGroundtruthLocality
file: experiments/colgranule/vector_graph_deep1b_float_quant_test.go
```

Implemented pieces:

```text
1. numeric quantizer helpers:
   - affine u1..u32
   - mantissa-truncated fp32 m0..m23
   - bf16/fp16 round-trip lanes
   - block-float lanes
   - random-rotation affine lanes

2. scoring/evaluation harness:
   - candidate survival gates
   - exact-rerank gates
   - compressed-final gates
   - score-error/margin diagnostics

3. gate inversion:
   - for each method family, find the minimum payload passing each gate
   - for adaptive methods, optimize separately per gate
   - report all-query winners and per-query p50/p90/worst lower bounds

4. mixed-precision allocators:
   - per-dimension greedy allocator
   - per-row precision-class allocator
   - base-plus-exceptions allocator

5. report:
   - measured gate-winner table
   - method Pareto curves
   - mixed-precision diagnostics
   - conclusions and recommended next implementation targets
```

Current limitations:

```text
1. mixed-precision metadata is intentionally rough and secondary
2. mixed row/dimension lanes are oracle codec probes, not production layouts
3. scan timings are research-scorer timings, not optimized packed-code kernels
4. results are official top100 locality probes, not buildable TreeDB granules
5. PQ/OPQ/AVQ/QINCo are intentionally excluded from this top100-only tournament
```

## Decision Output

Current policy-oriented conclusion:

```text
Candidate generation:
  promote u4 per-dimension affine as the conservative q0..99 top100 codec lane
  at 384 bits/vector = 48 B/vector row payload.

Near-final / compressed-only ranking:
  do not rely on u4.
  q0..99 required 576 bits/vector for compressed top10 >= 9/10,
  960 bits/vector for exact compressed top10,
  and 1344 bits/vector for exact compressed top20.

Adaptive quantization:
  still promising as a lower-bound signal, but the implemented mixed
  row/dimension oracle policies are not yet a production policy.

Mantissa/fp formats:
  useful near-final/rerank references, but not payload winners for candidate
  generation in this Deep1B top100 tournament.

Norm-explicit correction:
  not the primary candidate-generation win in this run.
```

This result should guide TreeDB codec priorities before investing in more
complex routing, trained codebooks, or production packing layouts.
