# Deep1B u2 Sparse Refinement Codec Plan

Status: proposed next experiment; not yet implemented or measured.

This plan follows the measured Deep1B float-quantization tournament in
`docs/benchmarks/column_vector_float_quantization_recall_tournament.md`.

The current top100 oracle result points at a specific next target:

```text
dense u2 base
+
sparse row-dimension correction sidecar
```

This document defines the next experiment. It intentionally treats wall-clock
throughput as non-gating. The current question is accuracy and plausible
on-disk storage, not optimized vector math.

## Research Question

Given official Deep1B top100 local-neighborhood clouds, can a uniform dense u2
base plus sparse correction sidecar satisfy candidate-survival gates with less
storage than dense u3 or dense u4 scalar quantization?

The output should be:

```text
fixed recall gate -> minimum vector payload
```

not:

```text
fixed bit budget -> observed recall
```

The practical decision point is whether sparse corrections stay sparse after
addressing overhead is modeled. If realistic sidecar addressing pushes the codec
near dense u3/u4, the simpler dense codec is probably the better near-term
candidate.

## Scope

Initial regime:

```text
official Deep1B top100 local-neighborhood upper-bound probe
queries: q0..q99 first, q0..q999 later if stable
dims:    D=96
```

For each query, the experiment uses:

```text
query vector q
official top100 database vectors
exact fp32 scores within the top100 cloud
```

This remains a codec-locality probe. It does not prove TreeDB can build
production granules with the same structure.

## Non-Goals

This experiment does not gate on:

```text
optimized SIMD throughput
packed-code kernel quality
production routing
graph/IVF granule construction
metadata implementation finality
PQ/OPQ/AVQ/QINCo-style trained codebooks
TurboQuant-style random-rotation codecs in the first implementation pass
```

It should still report proxy storage carefully enough that the result cannot be
mistaken for a pure oracle with free addressing.

## Codec Families

### 1. Dense Baselines

These establish the simple alternatives:

```text
dense u2 per-dimension affine
dense u3 per-dimension affine
dense u4 per-dimension affine
```

For Deep1B `D=96`, row-code payload is:

```text
u2: 192 bits = 24 B/vector
u3: 288 bits = 36 B/vector
u4: 384 bits = 48 B/vector
```

### 2. Hierarchical u2 + u3 Refinement Bit

The preferred sparse codec shape:

```text
u3_code = (u2_code << 1) | extra_bit
```

Every cell has a valid dense u2 value. Corrected cells store one extra bit that
refines the u2 value to a compatible u3 value.

Scoring is additive:

```text
dense_score[row] =
  sum_dim q[dim] * dequant_u2[dim][u2_code[row, dim]]

correction_score[row] =
  sum_corrected_cells q[dim] *
    (dequant_u3[dim][(u2_code << 1) | extra_bit] -
     dequant_u2[dim][u2_code])

score[row] = dense_score[row] + correction_score[row]
```

This preserves all four u2 codes and avoids variable-width decode in the dense
scan.

### 3. Independent u3 Exception

This sidecar stores a full independently selected u3 value or a compact delta
from the base u2 value.

This may be more accurate than hierarchical refinement, but it costs more:

```text
payload: 3-bit u3 override or small delta code
address: corrected row-dimension cell
```

This variant answers whether hierarchical compatibility is too restrictive.

### 4. Residual Sign Correction

This variant stores only a residual direction bit. The magnitude comes from a
per-dimension/per-code table:

```text
correction_delta = q[dim] * residual_step[dim][u2_code][sign]
```

This tests whether a cheap residual repair can capture most of the useful u3
effect without requiring a full compatible u3 table.

### 5. Sparse Exact-Delta Oracle

This is a research-only upper bound.

For selected corrected cells, store the best score-preserving delta allowed by
the oracle. This is not a production format. It measures how much benefit is
available if practical correction payloads are too weak.

## Deferred TurboQuant-Style Investigation

TurboQuant-style codecs remain a serious candidate, but they should not displace
the immediate u2 sparse-refinement experiment. The first pass should answer the
simpler scalar question:

```text
Can dense u2 plus sparse corrections beat dense u3/u4 at fixed recall gates?
```

Only after that should the arena add TurboQuant-style random-rotation scalar
quantization as an external comparison.

### Why It Is Deferred

TurboQuant introduces additional moving parts:

```text
random orthogonal rotation
distribution-derived scalar codebooks
per-vector correction scale
optional residual / QJL-style inner-product estimator
implementation language boundary
```

Those pieces are interesting, but they make the next experiment harder to
interpret if mixed into the u2 sidecar work too early.

### Candidate Implementations

Two repos are useful references:

```text
https://github.com/RyanCodrai/turbovec
https://github.com/0xSero/turboquant
```

The current read is:

```text
turbovec:
  Rust vector-search implementation with Python bindings.
  MIT licensed in the checked snapshot.
  Implements a vector-index-shaped TurboQuant-style codec:
    normalize
    random rotation
    scalar codebook
    packed 2/3/4-bit codes
    per-vector length-renormalization scale

0xSero/turboquant:
  PyTorch/Triton KV-cache-oriented implementation.
  Useful as a reference for Algorithm 1 / Algorithm 2 style pieces.
  Includes QJL residual signs and residual norms for the inner-product variant.
  Do not vendor it into TreeDB without a separate license/product review.
```

The important distinction is that `turbovec` is immediately shaped like a vector
search codec, while `0xSero/turboquant` is more useful as a mathematical
reference for the residual/QJL estimator.

### How To Evaluate Later

Do not port TurboQuant to Go first. Start with a sidecar probe:

```text
1. Export official Deep1B top100 query clouds from the Go harness.
2. Run a Python or Rust sidecar over the exported clouds.
3. Compare TQ2/TQ3/TQ4 against dense u2/u3/u4 and u2+sparse refinement.
4. Emit JSON/Markdown rows using the same recall gates and storage accounting.
```

For Deep1B `D=96`, the first storage model should count:

```text
TQ2:
  code bits: 96 * 2 = 192 bits = 24 B/vector
  plus per-row scale/correction scalar if used

TQ3:
  code bits: 96 * 3 = 288 bits = 36 B/vector
  plus per-row scale/correction scalar if used

TQ4:
  code bits: 96 * 4 = 384 bits = 48 B/vector
  plus per-row scale/correction scalar if used
```

Report row-code bytes separately from row-scale bytes and rotation/codebook
metadata. If the rotation is deterministic from a seed and dimension, report
that separately from a stored rotation matrix.

### TurboQuant Gates

The TurboQuant sidecar should use the same gates as this plan:

```text
exact top10 in approx@20/@50/@100
exact top20 in approx@50/@100
final recall@10 after exact rerank from approx@20/@50/@100
score-error / rank-boundary margin
```

It should be promoted only if it wins a real Pareto axis:

```text
same recall with fewer payload bits than u2+sparse refinement
same payload with better p90/worst-query behavior
simple dense scan shape that beats sparse sidecar complexity
useful near-final rerank quality at u3/u4-like payloads
```

If TurboQuant only matches dense u4 after adding per-row correction scalars, the
right conclusion is to defer it. If TQ2/TQ3 matches sparse-refinement candidate
survival with a cleaner dense layout, it becomes a high-priority codec candidate.

## Correction Selection Policies

The experiment should compare multiple policies because correction choice is the
main algorithmic question.

### A. Largest Absolute Score Error

Rank candidate cell repairs by:

```text
abs(q[dim] * cell_quantization_error[row, dim])
```

This is simple and query-specific. It is valid for the official top100 oracle
probe.

### B. Boundary Impact

Prefer corrections that affect rows near the target shortlist boundary:

```text
top10 / top20 winners
rank 11..50 hard negatives
rank 21..100 hard negatives
```

The aim is not to reduce mean error, but to stop true winners from falling below
the shortlist cutoff.

### C. Greedy Gate-Driven Selection

The most important policy:

```text
1. start with dense u2
2. add corrections in batches
3. rescore and test the recall gate
4. stop at the first passing payload
```

This directly supports:

```text
fixed recall gate -> minimum bits/vector
```

### D. Uniform Per-Row Cap

Limit how many corrections any one row can receive.

This tests whether a small number of fragile rows dominate the payload and
whether row-local correction bursts are necessary.

### E. Uniform Per-Dimension Cap

Limit how many corrections any dimension can receive.

This tests whether a few query-heavy or high-error dimensions dominate the
payload.

## Recall Gates

Use a broad gate set. At minimum:

```text
exact top10 in approx@20 = 10/10
exact top10 in approx@50 = 10/10
exact top10 in approx@100 = 10/10

exact top20 in approx@50 >= 19/20
exact top20 in approx@50 = 20/20
exact top20 in approx@100 = 20/20

final recall@10 after exact rerank from approx@20
final recall@10 after exact rerank from approx@50
final recall@10 after exact rerank from approx@100
```

Compressed-final top10/top20 should be reported, but it is not the primary
promotion metric for this codec.

## Storage Accounting

Report three layers separately.

### 1. Theoretical Value Bits

Only the scalar information content:

```text
dense u2 bits + correction payload bits
```

For hierarchical u3 refinement:

```text
bits/vector = 2 * D + corrections_per_vector
```

For Deep1B:

```text
bits/vector = 192 + corrections_per_vector
```

### 2. Addressed Sidecar Bits

Model practical ways to locate corrected cells.

Do not use one naive global address model as the only answer. Report multiple
models:

```text
global row-dim cell list
dimension-grouped row lists
tile-local sparse row lists
tile-local bitmasks
adaptive list-vs-mask per tile
```

For example, with 256-row tiles:

```text
sparse list entry:
  row offset: 8 bits
  extra bit:  1 bit
  total:      about 9 bits/correction

tile mask:
  mask:       256 bits/tile/dim when present
  extra bits: popcount(mask)
```

The experiment should report when mask mode beats list mode as correction
density increases.

### 3. Metadata-Amortized Bytes

Keep this secondary, but visible:

```text
per-dimension dequant tables
scale/offset or u2/u3 tables
tile directories
sidecar offsets/counts
```

Because the current regime is only a 100-row top100 cloud, metadata
amortization is not production-representative. The report must distinguish:

```text
top100 oracle payload
estimated production-granule payload
```

## Report Tables

The generated report should include these tables.

### Gate Winner Table

For each gate:

```text
gate
winning codec
selector policy
theoretical bits/vector
addressed bits/vector by sidecar model
corrections/vector p50/p90/worst
queries passed
```

### Method Pareto Table

For each codec family:

```text
codec
row-code bytes/vector
addressed bytes/vector
top10 in approx@20
top10 in approx@50
top20 in approx@50
final recall@10 after rerank@50
mean/max score error
score-error / rank10/11 margin
```

### Sidecar Density Table

For each winning sparse plan:

```text
corrections per vector
corrections per query cloud
corrections per dimension
corrections per row
list-vs-mask break-even
largest correction hotspots
```

### Dense Baseline Comparison

Every sparse result should be compared against:

```text
dense u2
dense u3
dense u4
```

The key question:

```text
Does sparse u2 refinement beat dense u3/u4 after sidecar addressing?
```

## Implementation Plan

### Step 1: Add a Dedicated Sparse-Refinement Result Section

Keep the current float-quantization tournament intact. Add a dedicated v3 output
section for sparse refinement plans rather than mixing them into the existing
mixed-precision tables.

### Step 2: Implement Hierarchical u2/u3 Tables

Add per-dimension compatible quantization tables:

```text
u2 parent code
u3 child code 0/1 under each u2 parent
delta_u2_to_u3[dim][u2_code][extra_bit]
```

The first implementation may use fp32 reference scoring. Packed-code throughput
is not a gate.

### Step 3: Implement Correction Candidate Generation

For each query cloud and codec:

```text
compute dense u2 score
compute exact fp32 score
enumerate candidate cell repairs
estimate score delta for each repair
```

### Step 4: Implement Selection Policies

Start with:

```text
largest absolute score error
greedy gate-driven selection
boundary-impact weighting
```

Add row/dim caps only after the first two selectors work.

### Step 5: Add Sidecar Storage Estimators

For every sparse plan, compute:

```text
theoretical payload bits
global cell-list bits
dim-grouped row-list bits
tile sparse-list bits
tile bitmask bits
adaptive tile bits
```

The storage estimator should be deterministic and independent of scorer
throughput.

### Step 6: Generate the Markdown Report

The generated report should be either:

```text
/tmp/gomap_deep1b_u2_sparse_refine_q0_99/report.md
```

or appended as a clearly labeled v3 section in the existing generated report.

Then summarize conclusions in:

```text
docs/benchmarks/column_vector_float_quantization_recall_tournament.md
```

### Step 7: Add TurboQuant Sidecar Only After u2 Sparse Results

After the u2 sparse-refinement report is measured, add an external comparison
stage if the sparse result leaves open a meaningful tradeoff:

```text
TQ2/TQ3/TQ4 row-code payload
TQ2/TQ3/TQ4 plus per-row correction scale
optional QJL/residual estimator prototype
```

This should be a sidecar experiment first. A Go port should wait until the
accuracy/storage result shows that TurboQuant beats either the sparse sidecar or
the simple dense scalar lanes on a real recall gate.

## Success Criteria

This experiment is successful if it answers:

```text
1. Can dense u2 plus sparse correction pass the key candidate gates?
2. How many corrected cells are needed per vector/query?
3. Does addressed sidecar storage preserve the win over dense u3/u4?
4. Which selector reaches gates with the fewest corrections?
5. Are wins stable across q0..q99 or concentrated in easy queries?
```

Promotion criterion:

```text
u2 sparse refinement is worth deeper implementation only if it beats dense u3
or dense u4 on storage at the same recall gate after sidecar addressing is
modeled.
```

If it does not beat the dense lanes, the correct conclusion is to prefer the
simpler dense scalar codec until production-granule evidence changes the tradeoff.

## Expected Interpretation

Possible outcomes:

```text
Sparse correction stays tiny:
  prioritize u2 dense base plus sparse sidecar as the main candidate-generation
  codec.

Sparse correction works only with free addressing:
  treat it as an oracle result; prefer dense u3/u4 for implementation.

Sparse correction needs many row/dim hotspots:
  consider row-class or dimension-class codecs before building a sparse sidecar.

Sparse correction fixes candidate survival but not compressed final ranking:
  keep it as candidate-generation only and rely on exact/fuller rerank.
```

The preferred near-term product-shaped result is:

```text
u2 dense base is the universal scan lane;
sidecar corrections are an optional candidate-generation repair layer;
fuller int8/fp16/fp32 vectors remain the final rerank authority.
```

## Concerns And Criticism

This plan is directionally useful only if it is treated as a progressive scalar
codec lower-bound study:

```text
How close can we get to the minimum value bits needed to preserve retrieval
gates in an oracle local neighborhood?
```

It should not be read as:

```text
This is nearly a production storage format.
```

The strongest critique is that the experiment can accidentally blur three
different things:

```text
1. theoretical payload lower bound
2. addressable sidecar storage format
3. deployable query-independent codec
```

Those must remain separate in the report.

### 1. Oracle Leakage

The official top100 clouds are valid for finding a lower bound, but correction
selection can leak future query information.

If the selector knows:

```text
the exact query
the exact top10/top20 rows
the hard negatives
the target gate being optimized
```

then a sparse sidecar can become query-conditioned ranking repair rather than a
storage codec. That is useful as an oracle bound, but it is close to storing an
answer key for the query.

The report must label query/gate-aware correction plans as:

```text
oracle per-query lower bound
```

not:

```text
codec candidate
```

A deployable codec would need query-independent repair rules, calibration-query
rules, self-query rules, or other non-oracle risk heuristics.

### 2. Sidecar Accounting May Erase The Win

The value-payload win is only meaningful after addressing is counted.

For Deep1B `D=96`:

```text
dense u2: 192 bits/vector = 24 B/vector
dense u3: 288 bits/vector = 36 B/vector
dense u4: 384 bits/vector = 48 B/vector
```

So the sparse sidecar has hard practical budgets:

```text
u2 -> u3 margin:  96 extra bits/vector = 12 B/vector
u2 -> u4 margin: 192 extra bits/vector = 24 B/vector
```

If row offsets, dim identifiers, tile directories, mode tags, alignment, and
extra bits cost more than those margins, the sparse format loses to dense u3 or
dense u4.

The key comparison is:

```text
u2 base + addressable correction sidecar
vs
dense u3/u4
```

not:

```text
u2 base + free extra value bits
vs
dense u4
```

The main report table should therefore include:

```text
Gate
Dense baseline
Sparse value bits
Sidecar-address bits
Total B/vector
Beats u3?
Beats u4?
```

### 3. Hierarchical u3 May Be Too Constrained

The proposed hierarchical refinement:

```text
u3_code = (u2_code << 1) | extra_bit
```

is operationally clean, but it constrains the u3 quantizer to refine the u2
levels. Independently optimized u2 and u3 quantizers may place reconstruction
levels differently.

The expectation to test is:

```text
hierarchical u3:
  cleanest physically

independent u3 exception:
  may score better, but costs more sidecar payload

residual sign correction:
  may be cheaper, but may be too weak when residual magnitudes vary
```

The gap between these methods estimates the accuracy cost of making the codec
physically neat.

### 4. Compressed-Final Gates Are Diagnostic Only

Compressed-final gates are useful, but they should not drive product
interpretation for this experiment.

If sparse corrections are chosen separately per gate, compressed-final gates can
be satisfied by repairing the exact ranking boundary. That again risks turning
the sidecar into an answer-aware repair layer.

The primary gates should remain candidate-generation gates:

```text
top10 in approx@20
top10 in approx@50
top20 in approx@50
final recall after exact rerank
```

Compressed-final top10/top20 should be reported as a diagnostic, not as the main
promotion signal.

### 5. Top100 Clouds May Overstate Sparsity

Official top100 clouds are highly local and small. Sparse correction may look
extremely efficient when the candidate universe is only 100 rows.

Production/buildable granules may have:

```text
4096 rows
8192 rows
16384 rows
```

with more ambiguous boundary rows and more hard negatives. Correction density
may not stay tiny.

The top100 result should be described as:

```text
local-theory floor
```

not:

```text
granule-scale sidecar density estimate
```

Granule-scale density requires a later production/buildable-block experiment.

### Numbers To Watch

The skeptical review should be answered by concrete numbers:

```text
corrections/value
corrections/vector
corrections/query
address bits/correction
total sidecar bits/vector
dense u3/u4 break-even comparison
p50/p90/worst-query stability
```

The most important output is not whether sparse corrections can pass an oracle
gate. It is whether the addressed sparse scheme still beats dense u3/u4.

### Method-Specific Critique

Dense u2/u3/u4 baselines:

```text
Required anchors. Dense u3 is especially important because it is the first
simple alternative if sparse addressing costs approach 36 B/vector.
```

Hierarchical u2 + u3 refinement:

```text
Best operational candidate. Clean and packable, but may lose accuracy versus
independent u3 levels.
```

Independent u3 exception:

```text
Good diagnostic for the cost of hierarchical compatibility. May be too
expensive once replacement payload and addressing are counted.
```

Residual sign correction:

```text
Potentially cheaper than independent u3. Risk is that sign-only repair is too
weak when residual magnitude varies by row/cell.
```

Sparse exact-delta oracle:

```text
Useful only as a ceiling. Do not overemphasize it as a codec result; it measures
how much an answer-aware repair layer could do.
```

### Honest Decision Point

The strongest useful conclusion would have this shape:

```text
u2 + sparse one-bit refinements can theoretically pass top100 candidate gates
at about 24 B/vector value payload.

After realistic sidecar addressing, the practical range is X-Y B/vector.

If X-Y remains below dense u3/u4, sparse refinement is a real codec direction.

If X-Y approaches or exceeds dense u3/u4, dense scalar quantization is the
cleaner implementation-shaped winner.
```
