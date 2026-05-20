# Deep1B Groundtruth Compression Report

This report extends the PR 1620 Deep1B compression work with the official
`groundtruth.public.10K.ibin` top100 nearest-neighbor file. The purpose is to
separate two questions:

1. Can true Deep1B nearest-neighbor clouds support strong granule-local
   compression?
2. Which compressed representations are plausible for final ranking versus
   candidate generation before exact rerank?

Deep1B vectors in this benchmark are `D=96`. The principal run below used
queries `0..99`, loaded each query's official top100 database rows from
`base.1B.fbin` by HTTP range request when the row was not present in the local
first-1M cache, fit local PCA on each top100 cloud, and ran the first
top100-only oracle compression tournament.

Run artifact:

```text
/tmp/gomap_deep1b_top100_tournament_ext_q0_99_20260519_202521/report.md
```

## What The Probe Establishes

The official top100 neighborhoods are genuinely local. Across queries `0..99`:

| Metric | Result |
| --- | ---: |
| Loaded groundtruth rows | `10000/10000` |
| Local first-1M rows | `9` |
| Remote base.1B rows | `9991` |
| Average centroid norm | `0.8902` |
| Centroid norm range | `0.7104-0.9878` |
| Average centroid cos(query) | `0.9427` |
| Centroid cos(query) range | `0.8716-0.9941` |
| Average pairwise cosine | `0.7927` |
| Pairwise cosine range | `0.4996-0.9756` |

This is much tighter than the earlier first-1M/top8192 smoke block. The premise
that true nearest-neighbor neighborhoods have exploitable local structure is now
supported.

The important limitation is equally clear: local variance preservation does not
automatically preserve final top-k ordering. The rank64 PCA path captures about
`99.24%` average variance, but only averages `7.79/10` exact top10 recall as a
final ranker.

| Rank | Approx code B/vector | Avg PCA variance | Avg final top10 | Worst final top10 | Avg top10 in approx@20 | Worst top10 in approx@20 | Avg top10 in approx@50 | Worst top10 in approx@50 | Avg top20 in approx@50 | Worst top20 in approx@50 | Avg score error |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | `8` | `51.26%` | `2.92/10` | `0/10` | `4.78/10` | `0/10` | `7.75/10` | `4/10` | `14.39/20` | `9/20` | `0.00987` |
| 16 | `16` | `72.08%` | `3.79/10` | `1/10` | `5.82/10` | `2/10` | `8.51/10` | `4/10` | `15.83/20` | `10/20` | `0.00863` |
| 24 | `24` | `83.53%` | `4.62/10` | `1/10` | `6.48/10` | `3/10` | `9.11/10` | `6/10` | `16.86/20` | `12/20` | `0.00769` |
| 32 | `32` | `90.27%` | `5.12/10` | `1/10` | `7.19/10` | `3/10` | `9.52/10` | `7/10` | `17.66/20` | `14/20` | `0.00682` |
| 40 | `40` | `94.36%` | `5.96/10` | `3/10` | `7.99/10` | `4/10` | `9.63/10` | `7/10` | `18.17/20` | `14/20` | `0.00605` |
| 48 | `48` | `96.87%` | `6.58/10` | `3/10` | `8.58/10` | `4/10` | `9.78/10` | `8/10` | `18.84/20` | `15/20` | `0.00520` |
| 56 | `56` | `98.38%` | `7.05/10` | `3/10` | `9.02/10` | `5/10` | `9.92/10` | `9/10` | `19.33/20` | `15/20` | `0.00432` |
| 64 | `64` | `99.24%` | `7.79/10` | `4/10` | `9.47/10` | `7/10` | `9.97/10` | `9/10` | `19.63/20` | `18/20` | `0.00341` |
| 80 | `80` | `99.91%` | `8.86/10` | `5/10` | `9.94/10` | `9/10` | `10.00/10` | `10/10` | `19.99/20` | `19/20` | `0.00158` |
| 96 | `96` | `100.00%` | `9.93/10` | `9/10` | `10.00/10` | `10/10` | `10.00/10` | `10/10` | `20.00/20` | `20/20` | `0.00014` |

`Approx code B/vector` is the per-row int8 coordinate payload only. It excludes
centroid, basis, scale, and rerank-store metadata because this official top100
probe is a locality/quality experiment, not a production granule layout.
Metadata amortization must be measured on TreeDB-buildable granule sizes.

The minimum-rank gate table is the most product-relevant output:

| Gate | Passed queries | Failed queries | p50 K | p90 K | Worst K |
| --- | ---: | ---: | ---: | ---: | ---: |
| Final top10 recall >= `9/10` | `100` | `0` | `80` | `96` | `96` |
| Final top10 recall = `10/10` | `96` | `4` | `96` | `96` | `96` |
| Exact top10 in approx@20 = `10/10` | `100` | `0` | `64` | `80` | `96` |
| Exact top10 in approx@50 = `10/10` | `100` | `0` | `24` | `56` | `80` |
| Exact top20 in approx@50 >= `19/20` | `100` | `0` | `40` | `64` | `80` |

The score margins explain the mismatch between small reconstruction error and
unstable final ordering:

| Margin metric | Result across queries `0..99` |
| --- | ---: |
| Average exact rank10/rank11 gap | `0.001109` |
| Minimum exact rank10/rank11 gap | `0.000020` |
| Average exact rank50/rank51 gap | `0.000350` |
| Minimum exact rank50/rank51 gap | `0.000004` |
| Rank64 average mean score error | `0.00341` |

Rank64 error is several times larger than the score gaps that decide top10
membership. That makes exact final top10 misses expected, even when the local
cloud is coherent. The same result is promising, but not decisive, for
candidate generation: rank64 placed the exact top10 inside approximate top50
for `97/100` queries and had worst-case `9/10`; rank80 placed exact top10
inside approximate top50 for `100/100` queries and exact top20 inside
approximate top50 at `19/20` or better for every query.

`approx@100` is a sanity ceiling in this top100-only probe because the whole
measured universe is the official top100. The actionable candidate-generation
signals here are `approx@20` and `approx@50`; production granules need the same
metric at wider row counts.

## Official Top100 Oracle Method Tournament

These rows are **official top100 local-neighborhood upper-bound probes**. They
are valid only for methods that need one query and its 100 official nearest
neighbor vectors. They are not production granule proof, and they do not
validate codebook or model-trained methods such as PQ, OPQ, residual PQ, LOPQ,
ScaNN/AVQ, query-aware quantization, QINCo, or Matryoshka. Metadata bytes below
are top100-local accounting; row-code bytes are the cleaner comparison until
TreeDB-buildable granules establish real amortization.

Selected aggregate rows across queries `0..99`:

| Method | Row-code B/vector | Metadata B/vector | Avg build ms | p50 compressed top10 | Worst compressed top10 | p50 top10@20 | Worst top10@20 | p50 top10@50 | Worst top10@50 | p50 top20@50 | Worst top20@50 | Avg score err | Avg err/gap10 | Avg scan ns/vector | Interpretation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `scalar_u8_affine_per_dim_reconstructed` | `96` | `7.68` | `0.251` | `10/10` | `9/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00011` | `0.36` | `1114` | Conservative full-dim SQ8 lane; strong candidate survival and near-final quality. |
| `scalar_u4_affine_per_dim_reconstructed` | `48` | `7.68` | `0.237` | `9/10` | `7/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00201` | `6.19` | `1077` | Strongest simple 48B top100 scalar lane; not a final ranker, but excellent candidate survival in this oracle universe. |
| `scalar_u2_affine_per_dim_reconstructed` | `24` | `7.68` | `0.230` | `6/10` | `2/10` | `8/10` | `5/10` | `10/10` | `7/10` | `18/20` | `13/20` | `0.02048` | `66.42` | `1052` | Too lossy; not safe even for top50 candidate survival. |
| `random_rotation_scalar_u4_affine_per_dim_reconstructed` | `48` | `7.76` | `1.653` | `9/10` | `7/10` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `19/20` | `0.00199` | `6.30` | `990` | Similar to plain u4; random rotation is worth keeping as a low-build challenger, but it is not a clear oracle win here. |
| `local_pca_i8_rank64` | `64` | `128.08` | `5.711` | `8/10` | `4/10` | `10/10` | `7/10` | `10/10` | `9/10` | `20/20` | `18/20` | `0.00341` | `11.26` | `96` | Good scan shape and useful candidate generation, but worse candidate robustness than simple u4 scalar in this top100 probe. |
| `boundary_weighted_pca_top20_hardneg_i8_rank64` | `64` | `128.08` | `4.451` | `9/10` | `6/10` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00342` | `11.68` | `70` | Recall-aware weighting improves candidate gates over variance PCA at the same rank, but it is oracle-trained on top100 boundaries. |
| `pairwise_diff_pca_top10_vs_11_100_i8_rank64` | `64` | `128.08` | `10.595` | `9/10` | `6/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `17/20` | `0.00327` | `10.91` | `64` | Boundary-separating directions improve top10 survival, but top20 worst-case is weaker than boundary-weighted PCA. Oracle only. |
| `query_axis_oracle_i8_projection_f16_norm` | `1` | `2.02` | `0.001` | `10/10` | `10/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00175` | `6.43` | `0.74` | Non-deployable query-specific upper bound. It proves score-aware projection has headroom, not that this exact method can ship. |

The top100-only tournament changes the immediate emphasis:

- Full-dim SQ8 remains the conservative compressed candidate/rerank lane.
- Plain per-dimension u4 scalar quantization is the most interesting simple
  48B oracle result. It keeps exact top10 inside approx@50 for all 100 queries
  and exact top20 inside approx@50 for all 100 queries in this top100 universe.
- u2 and binary/sign variants are not safe enough. The score error is one to
  two orders of magnitude larger than the ranking margins.
- Random rotation does not rescue u2/sign and does not clearly beat plain u4 on
  this sample, though it remains useful as a low-build challenger for
  production-scale tests.
- Boundary-weighted PCA and pairwise-difference PCA support the literature
  thesis: the basis objective matters. They improve rank64 candidate gates over
  plain variance PCA, but because they are trained on the oracle top100 ranking
  boundary, they are only upper-bound probes.
- The query-axis oracle is intentionally non-deployable. It estimates the
  amount of headroom available if a production method can learn a score-aware
  projection from real training data.

Current scalar scan times are from straightforward Go reconstruct-and-score
probes. They are useful for relative implementation shape, not as a final SIMD
kernel ceiling. The existing local-PCA scorer is much faster in Go because it
scores directly from low-rank coordinates instead of reconstructing every
dimension.

## Final Ranking Versus Candidate Generation

Final ranking needs high-fidelity scores. On this sample, low-rank PCA is not
safe as the final representation:

- Rank32: compact, but only `5.12/10` average final top10 recall.
- Rank64: much better, but still only `7.79/10` average final top10 recall.
- Rank96/full-rank int8 coordinates: `9.93/10` average final top10 recall, so
  even quantization and fp16 metadata can perturb a very tight boundary.

Candidate generation has a different gate: keep the real winners in a larger
shortlist, then rerank exactly. On this sample:

- Rank32 is weak but not useless: `9.52/10` average exact-top10 survival in
  approx@50, but worst query is only `7/10`.
- Rank64 is a credible but not fully safe local-PCA candidate generator:
  `9.97/10` average exact-top10 survival in approx@50, but one query misses one
  exact top10 row.
- Rank80 is the first rank in this ladder that clears exact top10 in approx@50
  for all 100 queries and keeps top20 in approx@50 at `19/20` or better.
- Rank96/full-dim int8 is the conservative compressed candidate lane and nearly
  a final-rank lane, but even it misses exact final top10 on 4 of 100 queries.

The product interpretation is:

```text
compressed representation should generate candidates
exact/full-fidelity representation should decide final rank
```

until same-byte quantizer baselines and production-buildable granule validation
prove otherwise.

## Design A-E

| Design | Representation | Best role | Current evidence | Recommendation |
| --- | --- | --- | --- | --- |
| A | Resident fp32 graph/search representation plus compressed storage columns | Final ranking and current product anchor | Existing resident fp32 graph search is `~58.7k` serial searches/s, `~289k` parallel searches/s, and `0 B/op` | Keep as the correctness and throughput baseline. Do not displace it with PCA yet. |
| B | Full-dimension granule-local int8 residual/scalar quantization, dim-major on disk | Conservative candidate generation; possible near-final rerank lane | Prior int8 tournament stores the 96D dim-major int8 matrix at `85.9-86.7 B/vector` with zstd-better/ClickHouse whole-granule `String CODEC(ZSTD(1))` | Build this first as the robust compressed hot/candidate representation. It has the simplest native scoring path and best measured storage-to-quality balance. |
| C | Low-rank local PCA int8 codes: centroid + basis + per-row coordinates | Compact candidate generation only | Prior 8192-row storage curve: rank32 `31.51 B/vector`, rank64 `59.65 B/vector`. Groundtruth top100, 100-query run: rank64 final top10 `7.79/10`, exact top10 in approx@50 `9.97/10`, and rank80 exact top10 in approx@50 `10.00/10` | Do not use for final ranking. Promote only as a candidate-generation experiment with exact-rerank gates. |
| D | Cascade: low-rank PCA prefilter, then full-dim int8/fp16 or fp32 rerank | Best near-term architecture | The groundtruth data says rank64/rank80 can preserve winners in a wider shortlist while failing exact final ranking | Most promising product path: scan `C`, rerank survivors with `B` or `A`, measure recall/cost at fixed rerank budgets. |
| E | PQ/residual-PQ/LUT scorer, ideally residualized by granule centroid or segment cluster | Candidate generation at fixed byte budgets | Not measured in this PR yet. It is the mandatory same-byte competitor to local PCA. The current Go harness does not yet train PQ/OPQ codebooks, and a top100-only cloud is too small for a fair 256-entry per-subquantizer training run | Add as the next tournament lane with a real train/eval split. Compare against rank32/rank48/rank64/rank80 PCA at matched bytes, not against fp32. |

## Literature-Backed Synthesis

The follow-on literature changes the roadmap more than the measured conclusion.
The state of practice is not "PCA but better." It is:

```text
compressed score estimation optimized for candidate recall
  -> larger shortlist
  -> higher-fidelity rerank
```

That matches the Deep1B groundtruth result. Rank64 local PCA is too noisy for
final top10 ordering because score margins are tiny, but it is interesting if it
keeps the exact winners inside a rerankable shortlist.

The dimensionality-reduction literature also gives a useful vocabulary for this
TreeDB design choice: a compressed representation can be an in-place replacement
for the stored vector, or it can be an out-of-place acceleration layer that only
generates candidates before a higher-fidelity rerank. The current Deep1B result
supports the second interpretation. Local PCA is a baseline acceleration layer,
not a validated final-ranking representation.

| Principle | Representative literature | TreeDB consequence |
| --- | --- | --- |
| Dimensionality reduction is a baseline, not the objective | ANN dimensionality-reduction surveys separate in-place transforms from out-of-place acceleration and compare PCA with vector-quantization and learned methods | Treat local PCA as one candidate-generation lane. Promote it only through shortlist containment and rerank quality. |
| Candidate generation and rerank is the default shape | Faiss `IndexRefine` explicitly combines a fast inaccurate index with a slower accurate shortlist search and treats recall at the shortlist size as the first-stage metric | Promote compressed lanes by `exact top10 in approx@50/@100`, not by final compressed top10 order alone. |
| Same-byte baselines are mandatory | PQ is the classic compact-code baseline; OPQ improves it by optimizing both the space decomposition and codebooks | Compare local PCA rank32/rank48/rank64 against PQ/OPQ 32/48/64 byte codes. Do not compare only against fp32. |
| Granules should encode residuals, not raw global vectors | LOPQ learns local product quantizers per coarse cell because residuals inside a cell are more unimodal than the original distribution | Test `granule centroid + residual encoder + compressed scan + exact rerank` as the main granule-native path. |
| Score-aware losses matter | ScaNN/AVQ, QUIP, NEQ, and query-aware quantization all move from reconstruction error toward inner-product or query-weighted error | Add score-error and margin-normalized diagnostics; later train projections/codebooks against score error or query logs. |
| Low-rank projection can be search-aware | LeanVec and LoRANN/RRR treat dimensionality reduction or low-rank factors as score-computation tools, not only reconstruction tools | Keep PCA as baseline, then test score-aware local projections such as pairwise-difference PCA, boundary-weighted PCA, or reduced-rank score approximation. |
| Supervised proximity objectives support boundary-aware projections | Metric learning and learning-to-hash methods train low-dimensional or compact representations to preserve task-specific proximity | If query logs, labels, or groundtruth pairs are available, add weighted PCA, pairwise-difference PCA, or local metric-learning projections as research lanes. |
| CPU-friendly scalar/low-build codecs are serious challengers | LVQ/SVS, RaBitQ, and TurboQuant target fast compressed scoring with less or no heavy codebook training | Add them as engineering challengers after the PQ/OPQ baseline, especially if training cost or graph random access dominates. |
| Neural and model-side compression are later tracks | QINCo/QINCo2 are promising learned residual compressors; Matryoshka is a model-side prefix-training strategy | Treat QINCo as a research ceiling and Matryoshka as relevant only when TreeDB owns or can choose the embedding model. |

For Deep1B `D=96`, the first same-byte tournament should be:

| Budget | Local basis lane | Quantizer lane | Conservative lane |
| ---: | --- | --- | --- |
| `32 B/vector` | local PCA `K=32` int8 | PQ/OPQ 32-byte code, e.g. `M=32`, 8-bit subcodes | - |
| `48 B/vector` | local PCA `K=48` int8 | PQ/OPQ 48-byte code, e.g. `M=48`, 8-bit subcodes | - |
| `64 B/vector` | local PCA `K=64` int8 | compatible 64-byte PQ/OPQ/residual-code layout | rank64 plus residual correction |
| `96 B/vector` | full-rank local coordinates | full int8/SQ8 | current robust compressed candidate lane |

For standard PQ, the subquantizer count must be compatible with `D=96` unless
the implementation uses OPQ/projection, uneven groups, residual/additive codes,
or another layout that permits the byte budget directly. The important benchmark
rule is same-byte comparison, not a specific `M` spelling.

The most product-useful output is inverted from a normal compression table:

```text
quality gate -> minimum bytes/vector
```

Examples:

```text
smallest bytes where exact top10 is contained in approx@50
smallest bytes where exact top10 is contained in approx@100
smallest bytes where final recall@10 after exact rerank meets target
```

That turns the benchmark into a storage policy: easy granules get small codes;
rank-fragile granules escalate to more bytes or to full-dim int8/fp16.

## Other Designs

- Spherical/JZIP remains a storage/cold-decode candidate. It wins vector-column
  bytes in the earlier report, but current decode+score is too slow for hot
  graph traversal unless a direct native scorer changes the economics.
- Cartesian byte-shuffle plus zstd is a useful non-trig cold block codec. It is
  faster than spherical reconstruction but still not the resident hot path.
- Matryoshka prefixes are attractive if the embedding model supports them. They
  are not proven by this Deep1B result, and should be benchmarked as a separate
  model-aware candidate lane.
- LVQ/SVS-style scalar compression, RaBitQ, and TurboQuant are useful
  low-build or CPU-friendly challengers. They should be measured after the
  local-PCA and PQ/OPQ rows establish the first Pareto frontier.
- QINCo/QINCo2 are useful research ceilings for very small codes, but they add
  neural training and decoder complexity. They should not be the first TreeDB
  production primitive.
- Binary/sign sketches and tail-norm bounds may still be useful as early
  rejection filters, but the previous safe-bound result pruned no candidates on
  the tested block.
- Granule construction matters. The official top100 cloud is a best-case
  locality probe, not a storage layout TreeDB can directly build at ingest time.
  Production-plausible builders need their own rows: IVF/k-means cluster
  blocks, graph-neighborhood blocks, row-id-adjacent blocks after graph sort,
  and actual graph visited sets.

## Actionable Gates

Use candidate-generation recall as the promotion metric:

```text
for each query and granule builder:
  exact winners = fp32 top10/top100 within the granule or evaluation universe
  approximate winners = compressed representation topM
  report exact top10 in approximate top20/top50/top100/top1000
  rerank approximate winners exactly
  report final recall@10 after rerank
```

Suggested gates before a compressed lane can be called promising:

| Gate | Target |
| --- | --- |
| Locality sample | At least 100 official groundtruth queries |
| Candidate survival | p50/p90/worst exact top10 in approx@M |
| Final quality | recall@10 after exact rerank at fixed M |
| Byte budget | Compare PCA and PQ at the same `B/vector` |
| Granule realism | Compare official top100 clouds with TreeDB-buildable granules |
| Margin diagnosis | Always report score error versus top-k boundary gaps |
| Adaptive policy | Report p50/p90/worst minimum bytes or rank needed to clear each gate |

## Concrete Research Tracks

Track A now has a first completed top100-only tournament for queries `0..99`.
It evaluated local PCA ranks `8,16,24,32,40,48,56,64,80,96`, adaptive rank
gates, full-dim SQ8, int4/int2/sign scalar variants, per-dimension/per-vector/
global scale policies, norm-explicit variants, boundary-weighted PCA,
pairwise-difference PCA, query-axis oracle projections, and random-rotation
scalar/sign probes. For the PCA ladder, it reported the minimum rank/bytes
needed for each gate:

```text
exact top10 in approx@50 = 10/10
exact top10 in approx@20 = 10/10
exact top20 in approx@50 >= 19/20
```

The headline result is that final compressed top10 order needs almost full rank
(`p50 K=80`, `p90 K=96` for >=9/10 final recall), while candidate generation is
much cheaper (`p50 K=24`, `p90 K=56`, worst `K=80` for exact top10 in
approx@50). For exact top20 in approx@50 at 19/20 or better, the gate is
`p50 K=40`, `p90 K=64`, worst `K=80`.

The scalar and basis-objective probes sharpen that conclusion. Full SQ8 is the
conservative compressed lane; plain per-dimension int4 is the strongest simple
48B top100 oracle candidate-survival result; int2/sign are too lossy; and
boundary-weighted or pairwise-difference PCA improves rank64 candidate gates
over variance PCA but remains an oracle-locality result. The remaining
top100-only probes worth adding, if this path remains decision-relevant, are
PCA plus tiny residual correction and low-rank-plus-tail progressive bound
tests.

Track B is the same-byte PQ/OPQ tournament. Compare local PCA `K=32/48/64`
against PQ/OPQ/residual-code layouts at the same byte budgets using the same
candidate-recall gates and exact rerank.

Track C is the granule-local residual encoding tournament. For each buildable
granule or cell:

```text
centroid
residuals = x - centroid
encode residuals with PCA K, PQ, OPQ/PQ, and PCA + residual correction
```

This is the closest TreeDB-native version of the LOPQ/RVQ lesson: compress the
local residual after the coarse locality unit, not the raw global vector.

## Recommended Next Work

1. Add production-plausible granule builders and run the same candidate-recall
   tables on their blocks. This is the next required step before any production
   compression claim.
2. Add PQ and OPQ as same-byte competitors to local PCA rank32/rank48/rank64
   and rank80, using a real train/eval split. Do not train codebooks on a
   single official top100 cloud.
3. Add granule-local residual encoders: local residual PQ/OPQ, then PCA plus
   residual correction if local PCA remains promising.
4. Benchmark the full cascade: compressed scan top50/top100, full-dim int8
   rerank, then optional exact fp32 rerank.
5. Finish the remaining top100-only probes only if they are still needed for
   method triage: PCA plus tiny residual correction and safe/progressive
   low-rank-plus-tail bounds.
6. Add CPU-friendly scalar/low-build challengers after the first production
   frontier: LVQ/SVS-style scalar compression, RaBitQ-inspired and
   TurboQuant-inspired lanes, with honest labels when the implementation is
   only an approximation.
7. Keep spherical and byte-shuffle codecs in the storage/cold-decode track
   unless direct scoring materially changes their throughput.

## Reading List

- ANN dimensionality-reduction survey: https://arxiv.org/html/2403.13491v2
- Faiss overview and `IndexRefine`: https://arxiv.org/html/2401.08281v4
- Product Quantization: https://inria.hal.science/inria-00514462/document
- OPQ: https://www.microsoft.com/en-us/research/publication/optimized-product-quantization-for-approximate-nearest-neighbor-search/
- LOPQ: https://openaccess.thecvf.com/content_cvpr_2014/papers/Kalantidis_Locally_Optimized_Product_2014_CVPR_paper.pdf
- Residual Vector Quantization: https://pmc.ncbi.nlm.nih.gov/articles/PMC3231071/
- Google ScaNN overview: https://research.google/blog/announcing-scann-efficient-vector-similarity-search/
- ScaNN/AVQ: https://proceedings.mlr.press/v119/guo20h.html
- Query-aware quantization: https://ojs.aaai.org/index.php/AAAI/article/view/25613/25385
- NEQ: https://ojs.aaai.org/index.php/AAAI/article/view/5333
- LeanVec: https://arxiv.org/html/2312.16335v2
- LoRANN/RRR: https://proceedings.neurips.cc/paper_files/paper/2024/hash/b939da3932e88ded5e9b08026e35069d-Abstract-Conference.html
- Learning to Hash survey: https://arxiv.org/abs/1509.05472
- Metric Learning survey: https://www.emerald.com/ftmal/article/5/4/287/1331280/Metric-Learning-A-Survey
- LVQ/SVS: https://www.vldb.org/pvldb/vol16/p3433-aguerrebere.pdf
- RaBitQ: https://arxiv.org/abs/2405.12497
- TurboQuant: https://arxiv.org/abs/2504.19874
- QINCo: https://proceedings.mlr.press/v235/huijben24a.html
- QINCo2: https://arxiv.org/html/2501.03078v1
- Matryoshka Representation Learning: https://arxiv.org/abs/2205.13147

The current result is therefore not "local PCA is enough." The sharper result
is:

```text
true Deep1B top100 neighborhoods are highly local,
rank64 local PCA is not a final ranker,
rank64/rank80 local PCA are plausible compact candidate generators,
and exact rerank remains mandatory.
```
