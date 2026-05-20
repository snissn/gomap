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
| `scalar_u8_affine_per_dim_reconstructed` | `96` | `7.68` | `0.235` | `10/10` | `9/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00011` | `0.36` | `1036` | Conservative full-dim SQ8 lane; strong candidate survival and near-final quality. |
| `scalar_u4_affine_per_dim_reconstructed` | `48` | `7.68` | `0.220` | `9/10` | `7/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00198` | `6.17` | `992` | Strongest simple 48B top100 scalar lane; not a final ranker, but excellent candidate survival in this oracle universe. |
| `scalar_u2_affine_per_dim_reconstructed` | `24` | `7.68` | `0.217` | `6/10` | `2/10` | `8/10` | `4/10` | `10/10` | `7/10` | `18/20` | `14/20` | `0.02002` | `65.96` | `973` | Too lossy; not safe even for top50 candidate survival. |
| `random_rotation_scalar_u4_affine_per_dim_reconstructed` | `48` | `7.76` | `1.565` | `9/10` | `7/10` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `19/20` | `0.00196` | `6.28` | `968` | Similar to plain u4; random rotation is worth keeping as a low-build challenger, but it is not a clear oracle win here. |
| `local_pca_i8_rank64` | `64` | `128.08` | `6.252` | `8/10` | `4/10` | `10/10` | `7/10` | `10/10` | `9/10` | `20/20` | `18/20` | `0.00347` | `11.45` | `104` | Good scan shape and useful candidate generation, but worse candidate robustness than simple u4 scalar in this top100 probe. |
| `boundary_weighted_pca_top20_hardneg_i8_rank64` | `64` | `128.08` | `5.114` | `9/10` | `6/10` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00350` | `11.94` | `74` | Recall-aware weighting improves candidate gates over variance PCA at the same rank, but it is oracle-trained on top100 boundaries. |
| `pairwise_diff_pca_top10_vs_11_100_i8_rank64` | `64` | `128.08` | `11.818` | `9/10` | `6/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `17/20` | `0.00332` | `11.11` | `69` | Boundary-separating directions improve top10 survival, but top20 worst-case is weaker than boundary-weighted PCA. Oracle only. |
| `local_pca_i8_rank64_residual_rp_i8_8` | `72` | `128.32` | `4.579` | `8/10` | `5/10` | `10/10` | `7/10` | `10/10` | `10/10` | `20/20` | `17/20` | `0.00330` | `11.07` | `67` | Tiny residual sketch improves score error and worst top10@50 versus rank64 PCA, but worsens worst top20@50 and costs extra row bytes. |
| `local_pca_i8_rank80_residual_rp_i8_8` | `88` | `159.36` | `4.811` | `9/10` | `5/10` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00152` | `5.92` | `83` | Repairs rank80's worst top20@50 and lowers score error, but does not beat the 80B boundary-weighted/pairwise oracle gates. |
| `query_axis_oracle_i8_projection_f16_norm` | `1` | `2.02` | `0.001` | `10/10` | `10/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00175` | `6.46` | `0.65` | Non-deployable query-specific upper bound. It proves score-aware projection has headroom, not that this exact method can ship. |

The harness now also emits a `pca_residual_random_projection` family for the
top100 oracle tournament. It scores local PCA plus a tiny deterministic int8
random-projection sketch of the PCA reconstruction residual at `+4`, `+8`, and
`+16` row-code bytes. This directly tests whether a small residual correction
can repair rank64/rank80 boundary flips without pretending to be a production
residual-PQ method. It is intentionally labeled as an official top100
local-neighborhood upper-bound probe.

The full residual-sketch top100 rerun was completed after the initial q0 smoke:

```text
/tmp/gomap_deep1b_top100_residual_sketch_q0_99_20260519_234135/report.md
/tmp/gomap_deep1b_top100_residual_sketch_q0_20260519_233746/report.md
```

The aggregate result is useful but not a promotion. `rank64 + residual_rp_i8_8`
improves worst exact top10-in-approx@50 from `9/10` to `10/10` and lowers mean
score error from `0.00347` to `0.00330`, but it worsens worst exact top20 in
approx@50 from `18/20` to `17/20` and spends `72 B/vector` instead of `64`.
`rank80 + residual_rp_i8_8` repairs rank80's worst exact top20-in-approx@50
from `19/20` to `20/20` and lowers mean score error from `0.00161` to
`0.00152`, but it spends `88 B/vector` and still does not beat the stronger
80B oracle basis-objective lanes on compressed-top10 robustness. Keep the lane
as evidence that small residual corrections can move boundary metrics, but do
not promote it above SQ8, u4 scalar, boundary-weighted PCA, or
pairwise-difference PCA from the top100 oracle frontier.

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

## Fixed-Budget Buildable-Granule PQ/Residual-PQ/OPQ Lanes

The production/buildable codebook lanes are measured separately from the
official top100 oracle probes. The current fixed-budget run uses deterministic
`ivf_kmeans` granules over a disjoint eval slice, trains global 8-bit PQ
codebooks, a global centroid-residual PQ variant, and OPQ-style learned
rotations on held-out base-prefix rows, and reports codec quality only
conditional on the centroid-routed candidate union. The residual-PQ row is a
global-centroid residual scout, not local LOPQ.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_fullbudget_ivf_q0_9_20260519_220119/report.md
```

Run shape:

| Parameter | Value |
| --- | ---: |
| Eval rows | `32768` |
| Held-out codebook train rows | `8192` |
| IVF granule target rows | `4096` |
| IVF granules | `8` |
| K-means iterations | `8` |
| Queries | `0..9` |
| Top centroid-routed granules | `1, 4` |
| Local PCA ranks | `32, 48, 64, 80, 96` |
| PQ budgets | `32, 48, 64, 80, 96 B/vector` |
| Residual PQ budgets | `32, 48, 64, 80, 96 B/vector` |
| OPQ budgets | `32, 48, 64, 80, 96 B/vector` |
| OPQ outer iterations | `3` |

PQ codebook metadata is counted as f16 centroids amortized over eval rows
(`1.5 B/vector`) plus per-row f16 inverse norms (`2 B/vector`), for
`3.5 B/vector` metadata in this run. Global residual PQ adds one f16 residual
centroid, bringing metadata to `3.51 B/vector`; OPQ adds f16 rotation metadata,
bringing method metadata to `4.06 B/vector` at this eval size. PQ training time
for 32B/48B/64B/80B/96B was `706 ms`, `944 ms`, `1058 ms`, `1123 ms`, and
`1222 ms`; global residual PQ took `704 ms`, `942 ms`, `1033 ms`, `1126 ms`,
and `1227 ms`; OPQ with 3 outer iterations took `3092 ms`, `4156 ms`,
`4765 ms`, `5381 ms`, and `6048 ms`.

Selected aggregate rows:

| Method | Top granules | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | Worst top10@50 | Worst top10@100 | Worst top20@100 | Avg score err | Avg scan ns/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| global PQ 32B x8 | 1 | `32` | `3.50` | `7/10` | `5/10` | `10/10` | `10/10` | `20/20` | `0.01829` | `16.06` |
| global residual PQ 32B x8 | 1 | `32` | `3.51` | `7/10` | `5/10` | `10/10` | `10/10` | `20/20` | `0.01830` | `15.88` |
| global OPQ 32B x8 | 1 | `32` | `4.06` | `7/10` | `4/10` | `10/10` | `10/10` | `20/20` | `0.01834` | `16.01` |
| local PCA rank32 int8 | 1 | `32` | `3.49` | `6/10` | `5/10` | `7/10` | `9/10` | `18/20` | `0.02206` | `25.01` |
| global PQ 48B x8 | 1 | `48` | `3.50` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00851` | `25.12` |
| global residual PQ 48B x8 | 1 | `48` | `3.51` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00851` | `25.36` |
| global OPQ 48B x8 | 1 | `48` | `4.06` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00845` | `25.09` |
| local PCA rank48 int8 | 1 | `48` | `4.22` | `8/10` | `6/10` | `10/10` | `10/10` | `20/20` | `0.01413` | `39.26` |
| scalar u4 reconstructed | 1 | `48` | `0.18` | `8/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00961` | `966.14` |
| global PQ 64B x8 | 1 | `64` | `3.50` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00686` | `34.19` |
| global residual PQ 64B x8 | 1 | `64` | `3.51` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00686` | `34.87` |
| global OPQ 64B x8 | 1 | `64` | `4.06` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00678` | `33.76` |
| local PCA rank64 int8 | 1 | `64` | `4.94` | `8/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00849` | `53.54` |
| global PQ 80B x8 | 1 | `80` | `3.50` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00506` | `45.12` |
| global residual PQ 80B x8 | 1 | `80` | `3.51` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00506` | `45.06` |
| global OPQ 80B x8 | 1 | `80` | `4.06` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00500` | `45.43` |
| local PCA rank80 int8 | 1 | `80` | `5.67` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00451` | `69.69` |
| global PQ 96B x8 | 1 | `96` | `3.50` | `9/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00161` | `55.02` |
| global residual PQ 96B x8 | 1 | `96` | `3.51` | `9/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00161` | `54.94` |
| global OPQ 96B x8 | 1 | `96` | `4.06` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00160` | `55.61` |
| local PCA rank96 int8 | 1 | `96` | `6.39` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00093` | `85.67` |
| scalar u8 reconstructed | 1 | `96` | `0.18` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00056` | `980.64` |
| global PQ 32B x8 | 4 | `32` | `3.50` | `6/10` | `5/10` | `10/10` | `10/10` | `20/20` | `0.01717` | `15.83` |
| global OPQ 48B x8 | 4 | `48` | `4.06` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00804` | `25.38` |
| global PQ 64B x8 | 4 | `64` | `3.50` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00662` | `33.99` |
| global PQ 80B x8 | 4 | `80` | `3.50` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00486` | `45.09` |
| local PCA rank80 int8 | 4 | `80` | `5.86` | `9/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00637` | `69.78` |
| global PQ 96B x8 | 4 | `96` | `3.50` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00146` | `57.36` |
| local PCA rank96 int8 | 4 | `96` | `6.62` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00082` | `86.16` |
| scalar u8 reconstructed | 4 | `96` | `0.19` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00056` | `962.58` |

This is the first same-byte production result that materially changes the
frontier:

- PQ32 beats local PCA rank32 at the same row-code budget on candidate survival
  and scan cost. It kept exact top10 inside approx@50 for every routed
  candidate set; PCA32 did not.
- PQ48/OPQ48 and scalar u4 all preserve the candidate gates in this run, but
  the PQ-family LUT scorer is about `25 ns/vector` while the current scalar
  lane reconstructs and scores at about `1 us/vector`.
- PQ64/OPQ64 are stronger same-byte competitors to local PCA rank64 on score
  error and scan cost. They are still candidate-generation lanes, not final-rank
  replacements.
- PQ80/PQ96 reduce score error and keep the candidate gates saturated in these
  aggregate rows, but compressed final top10 still has worst-query misses. The
  extra bytes improve the candidate generator; they do not justify removing the
  exact rerank stage.
- Global centroid-residual PQ does **not** move the frontier in this shape. Its
  candidate gates, score error, and scan cost essentially overlap global PQ,
  with only the extra f16 residual centroid in metadata. The residual-codebook
  question moves next to the local residual-PQ / LOPQ-lite section below.
- OPQ-style learned rotation is measured but not promoted. It slightly improves
  mean score error in some rows, but it also adds rotation metadata, costs about
  `3.9-4.3x` PQ training time, and does not deliver a decisive candidate-gate
  or worst-query win over global PQ on this sample.
- Routing remains the production bottleneck: top4 IVF routed all global top10
  for 9 of 10 queries, but query 6 routed only `6/10`. Codec recall cannot
  recover winners that never reach the selected granule union.

This fixed-budget global-codebook run does not validate ScaNN/AVQ, QINCo, or
Matryoshka. Local residual-PQ and local residual-OPQ are measured separately
below as per-buildable-granule lanes; they are buildable-granule evidence, not
official top100 oracle fits.

## Buildable Local Residual-PQ / LOPQ-Lite Lane

The next buildable-granule lane fits one residual PQ codebook per sealed IVF
granule and scores stored per-row codes. This is production-shaped because the
codebook belongs to the granule being stored; it is not trained on the official
top100 oracle cloud. It is still LOPQ-lite rather than full LOPQ because it
does not learn a local OPQ rotation or local subspace decomposition.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_local_residual_pq_ivf_q0_9_20260519_222818/report.md
```

Run shape:

| Parameter | Value |
| --- | ---: |
| Eval rows | `32768` |
| Eval row offset | `8192` |
| IVF granule target rows | `4096` |
| IVF granules | `8` |
| K-means iterations | `8` |
| Queries | `0..9` |
| Top centroid-routed granules | `1, 4` |
| Local PCA ranks | `32, 48, 64, 80, 96` |
| Global PQ budgets | `32, 48, 64, 80, 96 B/vector` |
| Local residual-PQ budgets | `32, 48, 64, 80, 96 B/vector` |
| PQ iterations | `4` |

Local residual-PQ trains per-granule codebooks for each budget. For this slice,
the five local budgets trained in `2814 ms`, `3763 ms`, `4146 ms`,
`4550 ms`, and `4896 ms`. Codebook metadata totals `394752 B`, or
`12.047 B/eval-vector`, before selected-row effects. The aggregate method rows
therefore show around `13.5-14.1 B/vector` metadata for selected granules,
versus `3.5 B/vector` for global PQ and roughly `3.5-6.6 B/vector` for local
PCA in this harness.

Selected aggregate rows:

| Method | Top granules | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | Worst top10@20 | Worst top10@50 | Worst top20@50 | Avg score err | Avg scan ns/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| global PQ 32B x8 | 1 | `32` | `3.50` | `7/10` | `5/10` | `7/10` | `10/10` | `19/20` | `0.01829` | `16.27` |
| local residual PQ 32B x8 | 1 | `32` | `13.51` | `8/10` | `7/10` | `9/10` | `10/10` | `20/20` | `0.01396` | `16.65` |
| local PCA rank32 int8 | 1 | `32` | `3.49` | `6/10` | `5/10` | `6/10` | `7/10` | `14/20` | `0.02206` | `25.30` |
| global PQ 32B x8 | 4 | `32` | `3.50` | `6/10` | `5/10` | `7/10` | `10/10` | `19/20` | `0.01717` | `15.89` |
| local residual PQ 32B x8 | 4 | `32` | `14.13` | `8/10` | `6/10` | `9/10` | `10/10` | `20/20` | `0.01374` | `16.23` |
| local PCA rank32 int8 | 4 | `32` | `3.57` | `6/10` | `4/10` | `7/10` | `9/10` | `13/20` | `0.02646` | `26.17` |
| global PQ 48B x8 | 1 | `48` | `3.50` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00851` | `25.54` |
| local residual PQ 48B x8 | 1 | `48` | `13.51` | `9/10` | `8/10` | `9/10` | `10/10` | `20/20` | `0.00673` | `25.84` |
| scalar u4 reconstructed | 1 | `48` | `0.18` | `8/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00961` | `984.61` |
| global PQ 48B x8 | 4 | `48` | `3.50` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00811` | `25.30` |
| local residual PQ 48B x8 | 4 | `48` | `14.13` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00661` | `25.49` |
| scalar u4 reconstructed | 4 | `48` | `0.19` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00957` | `971.21` |
| global PQ 64B x8 | 4 | `64` | `3.50` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `0.00662` | `34.48` |
| local residual PQ 64B x8 | 4 | `64` | `14.13` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00543` | `33.98` |
| local PCA rank64 int8 | 4 | `64` | `5.10` | `9/10` | `8/10` | `9/10` | `10/10` | `19/20` | `0.01114` | `52.89` |
| global PQ 96B x8 | 4 | `96` | `3.50` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00146` | `56.07` |
| local residual PQ 96B x8 | 4 | `96` | `14.13` | `9/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00126` | `55.66` |
| scalar u8 reconstructed | 4 | `96` | `0.19` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00056` | `976.13` |

Interpretation:

- Local residual-PQ moves the low-byte quality frontier, but not for free. At
  32B and 48B row-code budgets it improves candidate survival and score error
  versus global PQ and local PCA in the weakest rows, while keeping the same
  LUT-like scan cost profile as global PQ.
- The metadata cost is large at this granule count. A 32B local residual-PQ row
  is really about `46 B/vector` after selected-granule metadata here, so it
  should be compared against both same-row-code 32B methods and lower-metadata
  48B methods.
- The lane is most interesting when the product gate is strict top10@20 or
  top20@50 survival at 32B/48B/64B row-code budgets. It should not displace
  global PQ where global PQ already clears the gate at lower total bytes.
- At 80B/96B the quality edge narrows. Full-dim scalar and full-rank local
  coordinates remain the safer near-final rerank baselines, while exact rerank
  is still mandatory.
- This result upgrades Track C from pending to measured local residual coding.
  The follow-on section adds local OPQ/LOPQ-style evidence; larger train/eval
  coverage and actual graph/TreeDB granules still remain.

## Buildable Local OPQ / LOPQ-Style Scout

The follow-on run adds one learned residual OPQ rotation per buildable IVF
granule before the local PQ codebooks. This is the first LOPQ-style scout: the
rotation and codebooks belong to the sealed granule being stored, but the
granules are still IVF/k-means blocks over a 32K Deep1B slice rather than graph
neighborhoods or actual TreeDB granules.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_local_opq_ivf_q0_9_20260519_225007/report.md
```

Run shape:

| Parameter | Value |
| --- | ---: |
| Eval rows | `32768` |
| Eval row offset | `8192` |
| IVF granule target rows | `4096` |
| IVF granules | `8` |
| K-means iterations | `8` |
| Queries | `0..9` |
| Top centroid-routed granules | `1, 4` |
| Local PCA ranks | `32, 48, 64` |
| Global PQ budgets | `32, 48, 64 B/vector` |
| Local residual-PQ budgets | `32, 48, 64 B/vector` |
| Local residual-OPQ budgets | `32, 48, 64 B/vector` |
| PQ iterations | `4` |
| OPQ outer iterations | `3` |

Training and metadata:

| Lane | 32B train | 48B train | 64B train | Metadata B/eval-vector |
| --- | ---: | ---: | ---: | ---: |
| global PQ | `715 ms` | `952 ms` | `1045 ms` | `1.50` |
| local residual-PQ | `2860 ms` | `3830 ms` | `4163 ms` | `12.05` |
| local residual-OPQ | `12448 ms` | `16682 ms` | `19174 ms` | `16.55` |

Selected aggregate rows:

| Method | Top granules | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | Worst top10@20 | Worst top10@50 | Worst top20@50 | Avg score err | Avg scan ns/vector |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| global PQ 32B x8 | 1 | `32` | `3.50` | `7/10` | `5/10` | `7/10` | `10/10` | `19/20` | `0.01829` | `16.00` |
| local residual PQ 32B x8 | 1 | `32` | `13.51` | `8/10` | `7/10` | `9/10` | `10/10` | `20/20` | `0.01396` | `15.81` |
| local residual OPQ 32B x8 | 1 | `32` | `17.81` | `8/10` | `7/10` | `8/10` | `10/10` | `20/20` | `0.01380` | `15.79` |
| local residual PQ 48B x8 | 1 | `48` | `13.51` | `9/10` | `8/10` | `9/10` | `10/10` | `20/20` | `0.00673` | `25.46` |
| local residual OPQ 48B x8 | 1 | `48` | `17.81` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00661` | `25.34` |
| local residual PQ 64B x8 | 1 | `64` | `13.51` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00552` | `36.70` |
| local residual OPQ 64B x8 | 1 | `64` | `17.81` | `9/10` | `9/10` | `10/10` | `10/10` | `20/20` | `0.00540` | `36.52` |
| local residual PQ 32B x8 | 4 | `32` | `14.13` | `8/10` | `6/10` | `9/10` | `10/10` | `20/20` | `0.01374` | `16.21` |
| local residual OPQ 32B x8 | 4 | `32` | `18.66` | `8/10` | `6/10` | `9/10` | `10/10` | `19/20` | `0.01354` | `15.92` |
| local residual PQ 64B x8 | 4 | `64` | `14.13` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00543` | `37.18` |
| local residual OPQ 64B x8 | 4 | `64` | `18.66` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `0.00531` | `36.83` |

Interpretation:

- Local residual-OPQ reduces mean score error slightly versus local residual-PQ
  at matched budgets and improves the top1/64B worst compressed top10 row from
  `8/10` to `9/10`.
- It does not materially move the candidate-survival frontier. The important
  top10@50/top20@50 gates are already mostly cleared by local residual-PQ, and
  the top4/32B OPQ row slips to `19/20` top20@50.
- The added rotation is expensive: about `4-5x` the local residual-PQ training
  time in this run and about `4.5 B/eval-vector` more metadata.
- Conclusion: local OPQ/LOPQ-style coding is now measured, but it is not
  promoted yet. Local residual-PQ remains the better local-codebook lane until
  larger or actual-granule runs show OPQ wins that justify its cost.

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
| E | PQ/residual-PQ/LUT scorer, ideally residualized by granule centroid or segment cluster | Candidate generation at fixed byte budgets | Global 8-bit PQ, global centroid-residual PQ, OPQ-style lanes, per-buildable-granule local residual-PQ, and per-buildable-granule local residual-OPQ are measured on buildable IVF granules, IVF-sorted fixed blocks, IVF-window graph-neighborhood fixed blocks, and IVF-window graph-sorted row-adjacent fixed blocks. PQ32 beats same-byte local PCA rank32 on candidate survival and scan cost; PQ48/PQ64/PQ80/PQ96 are strong same-byte competitors where measured. OPQ does not clearly beat PQ on this sample after metadata and training cost. Global centroid-residual PQ overlaps global PQ. Local residual-PQ improves candidate gates and score error at 32/48/64B with about `12-14 B/eval-vector` extra codebook metadata. Local residual-OPQ slightly improves score error and some worst-query gates, but pays about `16.5-18.6 B/eval-vector` metadata and much higher training time. IVF-sorted fixed blocks improve buildable routing: top4 blocks routed all exact top10/top20 rows for queries `0..9`, while the graph-neighborhood and graph-sorted codebook runs both route p50 `10/10` exact top10 and worst `9/10` in top4 blocks. | Keep global PQ48/PQ64 as the simplest low-metadata codebook candidate-generator baseline. Retain local residual-PQ32/48 as the main low-byte-plus-metadata challenger because it can improve worst shortlist gates and score error. Do not promote local OPQ yet; it needs larger/actual-granule evidence to justify extra rotation metadata and build time. Next step is larger query/train/eval coverage and actual graph visited-set / TreeDB granule layouts, not more top100 codebook fits. |

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
| Same-byte baselines are mandatory | PQ is the classic compact-code baseline; OPQ improves it by optimizing both the space decomposition and codebooks | Compare local PCA ranks against PQ/OPQ at the same 32/48/64/80/96 byte budgets. Do not compare only against fp32. |
| Granules should encode residuals, not raw global vectors | LOPQ learns local product quantizers per coarse cell because residuals inside a cell are more unimodal than the original distribution | Test `granule centroid + residual encoder + compressed scan + exact rerank` as the main granule-native path. |
| Score-aware losses matter | ScaNN/AVQ, QUIP, NEQ, and query-aware quantization all move from reconstruction error toward inner-product or query-weighted error | Add score-error and margin-normalized diagnostics; later train projections/codebooks against score error or query logs. |
| Low-rank projection can be search-aware | LeanVec and LoRANN/RRR treat dimensionality reduction or low-rank factors as score-computation tools, not only reconstruction tools | Keep PCA as baseline, then test score-aware local projections such as pairwise-difference PCA, boundary-weighted PCA, or reduced-rank score approximation. |
| Supervised proximity objectives support boundary-aware projections | Metric learning and learning-to-hash methods train low-dimensional or compact representations to preserve task-specific proximity | If query logs, labels, or groundtruth pairs are available, add weighted PCA, pairwise-difference PCA, or local metric-learning projections as research lanes. |
| CPU-friendly scalar/low-build codecs are serious challengers | LVQ/SVS, RaBitQ, and TurboQuant target fast compressed scoring with less or no heavy codebook training | Add them as engineering challengers after the PQ/OPQ baseline, especially if training cost or graph random access dominates. |
| Neural and model-side compression are later tracks | QINCo/QINCo2 are promising learned residual compressors; Matryoshka is a model-side prefix-training strategy | Treat QINCo as a research ceiling and Matryoshka as relevant only when TreeDB owns or can choose the embedding model. |

For Deep1B `D=96`, the first same-byte tournament should be:

| Budget | Local basis lane | Quantizer lane | Conservative lane |
| ---: | --- | --- | --- |
| `32 B/vector` | local PCA `K=32` int8 | PQ/OPQ/residual-PQ 32-byte codes | - |
| `48 B/vector` | local PCA `K=48` int8 | PQ/OPQ/residual-PQ 48-byte codes | scalar u4 |
| `64 B/vector` | local PCA `K=64` int8 | PQ/OPQ/residual-PQ 64-byte codes | rank64 plus residual correction |
| `80 B/vector` | local PCA `K=80` int8 | PQ/OPQ/residual-PQ 80-byte codes | adaptive high-recall candidate lane |
| `96 B/vector` | full-rank local coordinates | PQ/OPQ/residual-PQ 96-byte codes | full int8/SQ8 |

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
  The current report now has rows for row-id-contiguous blocks, IVF/k-means
  variable-size clusters, IVF/k-means locality-sorted fixed blocks, and an
  IVF-window graph-neighborhood fixed-block proxy. The remaining
  production-plausible builders that still need their own rows are actual graph
  visited sets, row-id-adjacent blocks after graph sort, and actual TreeDB
  granules.

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

## First Buildable Granule Scout

The first production/buildable scout is now in the harness:
`TestColumnVectorGraphDeep1BBuildableGranuleScout`. The initial builder is
`row_id_contiguous`, which is a real storage unit TreeDB can build without
oracle labels, but it is intentionally a weak locality control. This result
should be read as a methodology checkpoint, not as proof that row-id granules
are good ANN granules.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_scout_q0_32768_20260519_204341/report.md
```

Run shape:

| Field | Value |
| --- | ---: |
| Regime | `buildable_granule_scout` |
| Builder | `row_id_contiguous` |
| Base rows | `32768` |
| Dims | `96` |
| Granule rows | `4096` |
| Granules | `8` |
| Queries | `0` |
| Top granules | `1,4` |

The important new accounting split is:

```text
routing recall:
  how many global exact winners reach the selected buildable granule union

conditional codec recall:
  among rows in that selected union, how well does the compressed scorer
  preserve the exact winners before rerank?
```

The routing result shows why row-id-contiguous granules are only a control:

| Query | Top granules | Candidate rows | Global top10 routed | Global top20 routed | Global top50 routed |
| ---: | ---: | ---: | ---: | ---: | ---: |
| `0` | `1` | `4096` | `2/10` | `2/20` | `8/50` |
| `0` | `4` | `16384` | `4/10` | `10/20` | `29/50` |

Inside the routed candidate union, the conditional codec rows are still useful
for comparing representation behavior:

| Selection | Method | Row-code B/vector | Metadata B/vector | Compressed top10 | Top10 in approx@20 | Top10 in approx@50 | Top20 in approx@50 | Scan ns/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `buildable_rowid_scalar_u4_affine_per_dim_reconstructed` | `48` | `0.19` | `10/10` | `10/10` | `10/10` | `20/20` | `980` |
| top1 | `buildable_rowid_local_pca_i8_rank64` | `64` | `5.08` | `9/10` | `10/10` | `10/10` | `20/20` | `60` |
| top4 | `buildable_rowid_scalar_u4_affine_per_dim_reconstructed` | `48` | `0.19` | `9/10` | `10/10` | `10/10` | `20/20` | `954` |
| top4 | `buildable_rowid_local_pca_i8_rank64` | `64` | `5.08` | `7/10` | `9/10` | `10/10` | `20/20` | `67` |
| top4 | `buildable_rowid_local_pca_i8_rank32` | `32` | `3.56` | `2/10` | `4/10` | `6/10` | `10/20` | `29` |

The interpretation is deliberately conservative:

- The production/buildable evaluation shape exists and separates routing from
  codec quality.
- Row-id-contiguous routing is poor, so this builder does not establish
  production viability.
- Conditional codec results remain aligned with the top100 oracle direction:
  scalar u4/u8 are robust but scan slowly in the current Go
  reconstruct-and-score kernel; low-rank PCA scans much faster but rank32 is
  fragile once the selected union is less coherent, while rank64 is a better
  candidate-generation lane.
- Codebook lanes need real train/eval splits, trained codebooks, and
  metadata-amortized bytes before they can make production claims. The
  fixed-budget PQ/OPQ/residual-PQ rows above add that first trained-codebook
  evidence; the later local residual-PQ section adds the first per-granule
  residual-codebook lane, and the local residual-OPQ section adds the first
  LOPQ-style per-granule rotation scout.

## IVF/K-Means Buildable Granule Scout

The harness now also supports `COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_kmeans`.
This trains deterministic cosine k-means centroids over the base prefix and
assigns rows to IVF-style granules. Unlike official top100 clouds, this is a
TreeDB-buildable locality probe. This specific scout is still not a
trained-codebook result by itself: PQ/OPQ require separate train/eval splits,
trained codebooks, and metadata accounting, which are covered by the fixed-budget
PQ/OPQ section above.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_ivf_q0_9_32768_20260519_205538/report.md
```

Run shape:

| Field | Value |
| --- | ---: |
| Regime | `buildable_granule_scout` |
| Builder | `ivf_kmeans` |
| Base rows | `32768` |
| Dims | `96` |
| Target granule rows | `4096` |
| Granules | `8` |
| K-means iterations | `8` |
| Queries | `0..9` |
| Top granules | `1,4` |

Routing improves sharply versus the row-id control, but it is not solved:

| Selection | Queries | Avg candidate rows | p50 top10 routed | Worst top10 routed | p50 top20 routed | Worst top20 routed | p50 top50 routed | Worst top50 routed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 IVF granule | `10` | `4294.5` | `9/10` | `3/10` | `14/20` | `6/20` | `34/50` | `17/50` |
| top4 IVF granules | `10` | `17304.9` | `10/10` | `8/10` | `20/20` | `17/20` | `50/50` | `41/50` |

Conditional on the routed candidate union, the codec comparison now looks like
this:

| Selection | Method | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | p50 top10@20 | Worst top10@20 | p50 top10@50 | Worst top10@50 | p50 top20@50 | Worst top20@50 | Scan ns/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `buildable_ivf_kmeans_local_pca_i8_rank32` | `32` | `3.67` | `7/10` | `4/10` | `9/10` | `8/10` | `10/10` | `9/10` | `20/20` | `16/20` | `25` |
| top1 | `buildable_ivf_kmeans_scalar_u4_affine_per_dim_reconstructed` | `48` | `0.20` | `9/10` | `8/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `972` |
| top1 | `buildable_ivf_kmeans_local_pca_i8_rank64` | `64` | `5.30` | `9/10` | `8/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `19/20` | `53` |
| top1 | `buildable_ivf_kmeans_scalar_u8_affine_per_dim_reconstructed` | `96` | `0.20` | `10/10` | `10/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `975` |
| top4 | `buildable_ivf_kmeans_local_pca_i8_rank32` | `32` | `3.50` | `6/10` | `5/10` | `9/10` | `6/10` | `10/10` | `9/10` | `18/20` | `16/20` | `26` |
| top4 | `buildable_ivf_kmeans_scalar_u4_affine_per_dim_reconstructed` | `48` | `0.18` | `9/10` | `8/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `972` |
| top4 | `buildable_ivf_kmeans_local_pca_i8_rank64` | `64` | `4.95` | `9/10` | `7/10` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `20/20` | `53` |
| top4 | `buildable_ivf_kmeans_scalar_u8_affine_per_dim_reconstructed` | `96` | `0.18` | `10/10` | `9/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `980` |

The production interpretation is:

- IVF/k-means is a much stronger buildable locality builder than row-id order on
  this prefix, but top1 routing has bad worst-query misses and top4 routing
  scans roughly half of the 32K prefix.
- Full SQ8 and u4 scalar are robust candidate-generation lanes once the exact
  winners have reached the routed union. The current Go kernel is slow because
  it reconstructs/scans scalar codes naively.
- Local PCA rank64 is the first fast buildable-lane candidate: around `64`
  row-code B/vector plus `~5` metadata B/vector, with `~53 ns/vector` scan in
  this harness. It preserves top10@50 and top20@50 well, but compressed final
  top10 is still not reliable.
- Local PCA rank32 is too fragile on these less coherent buildable unions unless
  paired with a refinement/residual stage.
- The later local residual-PQ and local residual-OPQ sections start the
  residual-stage comparison. The next production tournament step is to broaden
  global PQ, OPQ, local residual-PQ, and local residual-OPQ to more queries and
  actual graph/TreeDB granules, then compare them against scalar u4/u8 and
  PCA64 at matched total bytes.

## IVF/K-Means Sorted-Block Buildable Granule Scout

The harness now supports
`COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_kmeans_sorted_blocks`. It trains the
same deterministic cosine k-means model, assigns every row to a centroid, sorts
rows by assigned centroid and within-centroid similarity, and then chunks that
storage order into fixed-size blocks. This is a TreeDB-buildable
locality-sorted storage-block proxy. It is not an official top100 oracle cloud,
and it is not a graph-neighborhood proof.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_sorted_blocks_q0_9_20260519_230642/report.md
```

Run shape:

| Field | Value |
| --- | ---: |
| Regime | `buildable_granule_scout` |
| Builder | `ivf_kmeans_sorted_blocks` |
| Eval rows | `32768` |
| Eval offset | `8192` |
| PQ train rows | `8192` |
| Dims | `96` |
| Granule rows | `4096` |
| Granules | `8` |
| K-means iterations | `8` |
| Queries | `0..9` |
| Top granules | `1,4` |

Routing is the main reason to add this builder. Top1 fixed blocks are still
uneven, but top4 locality-sorted blocks route all exact top10 and top20 rows for
the ten-query sample:

| Selection | Queries | Candidate rows | p50 top10 routed | Worst top10 routed | p50 top20 routed | Worst top20 routed | p50 top50 routed | Worst top50 routed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 sorted block | `10` | `4096` | `8/10` | `3/10` | `16/20` | `4/20` | `38/50` | `9/50` |
| top4 sorted blocks | `10` | `16384` | `10/10` | `10/10` | `20/20` | `20/20` | `50/50` | `48/50` |

The codebook training/accounting remains separate from the top100 oracle probe:

| Lane | Bytes | Train ms | Metadata B/eval-vector |
| --- | ---: | ---: | ---: |
| global PQ | `32/48/64` | `713/947/1046` | `1.500` |
| global residual PQ | `32/48/64` | `711/948/1039` | `1.506` |
| global OPQ | `32/48/64` | `3102/4172/4779` | `2.062` |
| local residual-PQ | `32/48/64` | `2838/3787/4171` | `12.047` |
| local residual-OPQ | `32/48/64` | `12419/16665/19110` | `16.547` |

Selected conditional codec rows:

| Selection | Method | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | p50 top10@20 | Worst top10@20 | p50 top20@50 | Worst top20@50 | Avg score err | Scan ns/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `global_pq_32B` | `32` | `3.50` | `8/10` | `5/10` | `9/10` | `6/10` | `20/20` | `19/20` | `0.01823` | `16.97` |
| top1 | `local_residual_pq_32B` | `32` | `14.05` | `8/10` | `7/10` | `10/10` | `8/10` | `20/20` | `19/20` | `0.01432` | `16.68` |
| top1 | `local_residual_opq_32B` | `32` | `18.55` | `8/10` | `7/10` | `10/10` | `9/10` | `20/20` | `20/20` | `0.01396` | `16.68` |
| top1 | `local_pca_rank32` | `32` | `3.56` | `7/10` | `6/10` | `9/10` | `6/10` | `20/20` | `17/20` | `0.02251` | `27.79` |
| top4 | `global_pq_32B` | `32` | `3.50` | `6/10` | `5/10` | `9/10` | `7/10` | `20/20` | `19/20` | `0.01717` | `16.47` |
| top4 | `local_residual_pq_32B` | `32` | `14.05` | `8/10` | `7/10` | `10/10` | `9/10` | `20/20` | `19/20` | `0.01414` | `16.63` |
| top4 | `local_residual_opq_32B` | `32` | `18.55` | `8/10` | `7/10` | `10/10` | `9/10` | `20/20` | `19/20` | `0.01391` | `16.22` |
| top4 | `local_pca_rank32` | `32` | `3.56` | `6/10` | `4/10` | `7/10` | `5/10` | `17/20` | `12/20` | `0.02843` | `27.99` |
| top1 | `global_pq_48B` | `48` | `3.50` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00855` | `25.95` |
| top1 | `local_residual_pq_48B` | `48` | `14.05` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00684` | `27.51` |
| top1 | `local_residual_opq_48B` | `48` | `18.55` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00672` | `25.45` |
| top1 | `scalar_u4` | `48` | `0.19` | `9/10` | `8/10` | `10/10` | `9/10` | `20/20` | `20/20` | `0.00972` | `979.91` |
| top4 | `global_pq_48B` | `48` | `3.50` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00811` | `26.71` |
| top4 | `local_residual_pq_48B` | `48` | `14.05` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00681` | `26.22` |
| top4 | `local_residual_opq_48B` | `48` | `18.55` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00669` | `26.34` |
| top4 | `scalar_u4` | `48` | `0.19` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00977` | `977.96` |
| top1 | `global_pq_64B` | `64` | `3.50` | `8/10` | `7/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00696` | `35.57` |
| top1 | `local_residual_pq_64B` | `64` | `14.05` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00560` | `34.19` |
| top1 | `local_residual_opq_64B` | `64` | `18.55` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00549` | `34.98` |
| top4 | `global_pq_64B` | `64` | `3.50` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00662` | `34.57` |
| top4 | `local_residual_pq_64B` | `64` | `14.05` | `9/10` | `7/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00559` | `34.97` |
| top4 | `local_residual_opq_64B` | `64` | `18.55` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00547` | `35.01` |
| top1 | `local_pca_rank80` | `80` | `5.84` | `9/10` | `7/10` | `10/10` | `9/10` | `20/20` | `20/20` | `0.00519` | `86.53` |
| top4 | `local_pca_rank80` | `80` | `5.84` | `9/10` | `8/10` | `10/10` | `10/10` | `20/20` | `20/20` | `0.00641` | `84.48` |

The interpretation is consistent with the variable-size IVF scout, but the
fixed-block layout makes the storage tradeoff clearer:

- Sorting rows by buildable IVF locality improves top4 routing enough that all
  exact top10/top20 rows survive routing for queries `0..9`; top1 still has
  severe worst-query misses.
- This is a routing/layout win, not a codec win by itself, because top4 scans
  exactly four fixed 4096-row blocks.
- At 32B, global PQ and local residual-PQ beat local PCA rank32 on both
  candidate survival and scan cost. Local residual-PQ is the better codebook
  lane when its `~14 B/vector` selected-metadata cost is acceptable.
- Local residual-OPQ slightly lowers score error and sometimes improves a
  worst gate, but the extra `~4.5 B/vector` metadata and roughly `4-5x`
  training cost versus local residual-PQ are not justified yet.
- Scalar u4 remains a strong candidate-survival baseline at 48B row-code bytes,
  but this Go scorer is reconstruction-heavy and about `~1 us/vector`.
- Local PCA rank80 can match candidate-survival gates, but it scans at
  `~85 ns/vector` and still needs exact rerank. PQ/local residual-PQ are now the
  stronger 32/48/64B buildable-lane candidates.
- The report renderer now summarizes p50/worst exact-rerank recall@10 from
  approx@20, approx@50, and approx@100 in the aggregate tables. Those columns
  make the cascade contract explicit: compressed codes generate a shortlist,
  then the full-fidelity lane decides final rank.
- Buildable aggregate tables also estimate selected-candidate row-code
  KiB/query and metadata-amortized total KiB/query. This is a storage-footprint
  estimate for the routed candidate union, not a cache-aware physical I/O model.

## IVF Graph-Neighborhood Buildable Granule Scout

The harness now supports
`COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_graph_neighborhood_blocks`. It
trains deterministic cosine k-means centroids, sorts rows by assigned centroid
locality, builds a query-independent local nearest-neighbor graph inside each
IVF-sorted window, and forms fixed-size storage blocks by graph BFS. This is a
production/buildable graph-neighborhood proxy. It is not an official top100
oracle cloud, and it is still not a full HNSW/TreeDB graph-visited-set result.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_graph_blocks_q0_9_20260519_235519/report.md
```

Run shape:

| Field | Value |
| --- | ---: |
| Regime | `buildable_granule_scout` |
| Builder | `ivf_graph_neighborhood_blocks` |
| Eval rows | `32768` |
| Dims | `96` |
| Granule rows | `4096` |
| Granules | `8` |
| K-means iterations | `6` |
| Graph degree | `16` |
| Queries | `0..9` |
| Top granules | `1,4` |
| Scan iterations | `4` |

Routing is improved versus row-id order but not solved. Top1 graph-neighborhood
blocks are uneven; top4 graph-neighborhood blocks route almost all winners but
still miss one query's exact top10 and several top20/top50 rows:

| Selection | Queries | Candidate rows | p50 top10 routed | Worst top10 routed | p50 top20 routed | Worst top20 routed | p50 top50 routed | Worst top50 routed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 graph-neighborhood block | `10` | `4096` | `7/10` | `2/10` | `14/20` | `5/20` | `38/50` | `13/50` |
| top4 graph-neighborhood blocks | `10` | `16384` | `10/10` | `9/10` | `20/20` | `17/20` | `50/50` | `42/50` |

This run intentionally did not enable PQ/OPQ/local-residual codebooks. It is a
builder/locality scout for scalar and local-PCA lanes; trained-codebook lanes
need a follow-up run on the same builder with held-out train/eval accounting.

Selected conditional codec rows:

| Selection | Method | Row-code B/vector | Metadata B/vector | p50 compressed top10 | Worst compressed top10 | p50 top10@20 | Worst top10@20 | p50 top10@50 | Worst top10@50 | p50 top20@50 | Worst top20@50 | p50 rerank@50 recall@10 | Worst rerank@50 recall@10 | Scan ns/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `local_pca_rank32` | `32` | `3.56` | `7/10` | `4/10` | `9/10` | `7/10` | `10/10` | `9/10` | `19/20` | `15/20` | `1.00` | `0.90` | `28.90` |
| top1 | `scalar_u4` | `48` | `0.19` | `9/10` | `8/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `1.00` | `1.00` | `977.39` |
| top1 | `local_pca_rank64` | `64` | `5.08` | `9/10` | `8/10` | `10/10` | `9/10` | `10/10` | `10/10` | `20/20` | `18/20` | `1.00` | `1.00` | `69.68` |
| top1 | `local_pca_rank80` | `80` | `5.84` | `10/10` | `7/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `1.00` | `1.00` | `88.76` |
| top1 | `scalar_u8` | `96` | `0.19` | `10/10` | `9/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `1.00` | `1.00` | `974.72` |
| top4 | `local_pca_rank32` | `32` | `3.56` | `7/10` | `3/10` | `9/10` | `7/10` | `10/10` | `9/10` | `18/20` | `14/20` | `1.00` | `0.90` | `28.13` |
| top4 | `scalar_u4` | `48` | `0.19` | `9/10` | `7/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `1.00` | `1.00` | `970.97` |
| top4 | `local_pca_rank64` | `64` | `5.08` | `9/10` | `7/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `19/20` | `1.00` | `1.00` | `69.03` |
| top4 | `local_pca_rank80` | `80` | `5.84` | `9/10` | `8/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `1.00` | `1.00` | `86.40` |
| top4 | `scalar_u8` | `96` | `0.19` | `10/10` | `9/10` | `10/10` | `10/10` | `10/10` | `10/10` | `20/20` | `20/20` | `1.00` | `1.00` | `974.07` |

Interpretation:

- The graph-neighborhood proxy adds a buildable graph-locality row to the
  tournament, but the routing result is not yet a production win. Top4 still
  scans `16K` rows and has one top10 routing miss in the ten-query sample.
- Conditional codec behavior is consistent with the IVF and sorted-block
  scouts: scalar u4 is a robust 48B candidate-survival lane in the current Go
  implementation but scans around `1 us/vector`; local PCA rank64/rank80 are
  much faster candidate generators but still require exact rerank.
- Rank32 is too fragile on less coherent buildable unions. Its p50 gates look
  useful, but the worst-query top20@50 and rerank@50 rows are not good enough
  for promotion.
- This section should be treated as builder coverage, not a trained-codebook
  result. The follow-on run below enables global PQ, local residual-PQ, and
  local residual-OPQ with the same train/eval discipline used in the
  IVF/sorted-block sections.

## IVF Graph-Neighborhood Codebook Tournament

The follow-on run enables the trained codebook lanes on the same
`ivf_graph_neighborhood_blocks` builder. This is production/buildable proxy
evidence: global PQ/OPQ/residual-PQ use held-out codebooks trained on base rows
before the eval slice, and local residual-PQ/local residual-OPQ train per sealed
graph-neighborhood block with metadata amortized over block rows. It is still
not an official top100 oracle fit, not a full HNSW visited-set result, and not
an actual TreeDB granule layout.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_graph_codebooks_q0_9_20260520_000218/report.md
```

Run shape:

| Field | Value |
| --- | ---: |
| Regime | `buildable_granule_scout` |
| Builder | `ivf_graph_neighborhood_blocks` |
| Eval rows | `32768` |
| Eval row offset | `8192` |
| Dims | `96` |
| Granule rows | `4096` |
| Granules | `8` |
| K-means iterations | `6` |
| Graph degree | `16` |
| Queries | `0..9` |
| Top granules | `1,4` |
| PQ train rows | `8192` |
| PQ iterations | `4` |
| OPQ outer iterations | `3` |
| Scan iterations | `4` |

Training cost and metadata:

| Method family | Budgets | Train ms | Metadata B/eval-vector | Interpretation |
| --- | --- | ---: | ---: | --- |
| global PQ | `32/48/64/80/96` | `711.6-1233.3` | `1.50` | simplest trained codebook baseline |
| global residual-PQ | `32/48/64/80/96` | `708.4-1232.4` | `1.51` | overlaps global PQ in this run |
| global OPQ | `32/48/64/80/96` | `3107.0-6165.7` | `2.06` | extra rotation cost, no decisive win yet |
| local residual-PQ | `32/48/64` | `2846.6-4159.0` | `12.05` | strongest local low-byte challenger |
| local residual-OPQ | `32/48/64` | `12435.8-19375.7` | `16.55` | slightly lower error, high metadata/build cost |

Routing is the first hard limit. With top4 graph-neighborhood blocks, the codec
can only rank rows inside the routed union; this run still misses some global
winners before compression:

| Selection | Queries | Candidate rows | p50 top10 routed | Worst top10 routed | p50 top20 routed | Worst top20 routed | p50 top50 routed | Worst top50 routed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 graph-neighborhood block | `10` | `4096` | `8/10` | `3/10` | `17/20` | `4/20` | `38/50` | `9/50` |
| top4 graph-neighborhood blocks | `10` | `16384` | `10/10` | `9/10` | `20/20` | `18/20` | `50/50` | `45/50` |

Selected aggregate codec rows:

| Selection | Method | Row-code B/vector | Metadata B/vector | Avg total KiB/query | Worst top10@20 | Worst top10@50 | Worst top20@50 | Worst rerank@20 recall@10 | Avg score err | Scan ns/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `global_pq_32B` | `32` | `3.50` | `142.0` | `6/10` | `10/10` | `19/20` | `0.60` | `0.01823` | `17.03` |
| top1 | `local_residual_pq_32B` | `32` | `14.05` | `184.2` | `9/10` | `10/10` | `19/20` | `0.90` | `0.01431` | `19.01` |
| top1 | `local_pca_rank32` | `32` | `3.56` | `142.2` | `8/10` | `9/10` | `18/20` | `0.80` | `0.02216` | `29.74` |
| top4 | `global_pq_32B` | `32` | `3.50` | `568.0` | `7/10` | `10/10` | `19/20` | `0.70` | `0.01715` | `17.43` |
| top4 | `local_residual_pq_32B` | `32` | `14.05` | `736.8` | `9/10` | `10/10` | `19/20` | `0.90` | `0.01412` | `17.23` |
| top4 | `local_pca_rank32` | `32` | `3.56` | `569.0` | `6/10` | `7/10` | `12/20` | `0.60` | `0.02789` | `29.98` |
| top1 | `global_pq_48B` | `48` | `3.50` | `206.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00856` | `25.98` |
| top4 | `global_pq_48B` | `48` | `3.50` | `824.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00810` | `26.72` |
| top4 | `local_residual_pq_48B` | `48` | `14.05` | `992.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00679` | `26.92` |
| top4 | `scalar_u4` | `48` | `0.19` | `771.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00977` | `1019.62` |
| top4 | `global_pq_64B` | `64` | `3.50` | `1080.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00662` | `35.54` |
| top4 | `local_residual_pq_64B` | `64` | `14.05` | `1248.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00560` | `35.77` |
| top4 | `local_pca_rank64` | `64` | `5.08` | `1105.2` | `9/10` | `10/10` | `20/20` | `0.90` | `0.01149` | `72.20` |
| top4 | `global_pq_80B` | `80` | `3.50` | `1336.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00487` | `49.15` |
| top4 | `local_pca_rank80` | `80` | `5.84` | `1373.4` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00643` | `91.35` |
| top4 | `global_pq_96B` | `96` | `3.50` | `1592.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00145` | `58.48` |
| top4 | `scalar_u8` | `96` | `0.19` | `1539.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00057` | `1013.58` |

Interpretation:

- Top4 routing is much stronger than top1, but still not complete: query `0`
  routes `9/10` exact top10 and `18/20` exact top20, and query `9` routes
  `9/10` exact top10 and `19/20` exact top20. The codec rows are therefore
  conditional on the routed union, not proof that the whole search found every
  global winner.
- Global PQ48 is the clean simple frontier point in this run. It passes the
  measured candidate-survival gates for both top1 and top4 routed unions with
  low metadata and about `26 ns/vector` scan cost.
- Local residual-PQ32 is the interesting low-row-code challenger: it improves
  worst top10@20 and score error versus global PQ32, but it costs about
  `14 B/vector` selected metadata, so it is closer to a `46 B/vector` total
  lane than a pure 32B lane.
- Local residual-OPQ is still not promoted. It slightly lowers score error, but
  the extra rotation metadata and `12-19 s` local training cost in this small
  scout do not buy a new gate.
- Local PCA rank64/rank80 remain useful baselines, but PQ dominates them on
  scan cost at matching gates in this graph-neighborhood proxy.
- Scalar u4/u8 remain quality references and possible rerank lanes, but the
  current reconstructed Go scorer is about `1 us/vector`, so they are not the
  current compressed hot-scan frontier.

## IVF Graph-Sorted Row-Adjacent Codebook Tournament

The harness now also supports
`COLUMN_VECTOR_DEEP1B_BUILDABLE_BUILDER=ivf_graph_sorted_blocks`. It trains
deterministic cosine k-means centroids, builds the same query-independent local
nearest-neighbor graph inside IVF-sorted windows, materializes one deterministic
graph traversal order, and then chunks adjacent rows in that graph order into
fixed-size storage blocks. This is the buildable proxy for "row-id-adjacent
blocks after graph sort." It is not an official top100 oracle cloud and still
is not a full HNSW/TreeDB graph-visited-set result.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_graph_sorted_codebooks_q0_9_20260520_002224/report.md
```

Run shape:

| Field | Value |
| --- | ---: |
| Regime | `buildable_granule_scout` |
| Builder | `ivf_graph_sorted_blocks` |
| Eval rows | `32768` |
| Eval row offset | `8192` |
| Dims | `96` |
| Granule rows | `4096` |
| Granules | `8` |
| K-means iterations | `6` |
| Graph degree | `16` |
| Queries | `0..9` |
| Top granules | `1,4` |
| PQ train rows | `8192` |
| PQ iterations | `4` |
| OPQ outer iterations | `3` |
| Scan iterations | `4` |

Training cost and metadata:

| Method family | Budgets | Train ms | Metadata B/eval-vector | Interpretation |
| --- | --- | ---: | ---: | --- |
| global PQ | `32/48/64` | `705.6-1034.2` | `1.50` | simplest trained codebook baseline |
| global residual-PQ | `32/48/64` | `706.4-1035.0` | `1.51` | overlaps global PQ in this run |
| global OPQ | `32/48/64` | `3093.9-4820.1` | `2.06` | extra rotation cost, no decisive win yet |
| local residual-PQ | `32/48/64` | `2817.2-4137.5` | `12.05` | local low-byte challenger |
| local residual-OPQ | `32/48/64` | `12418.2-19352.1` | `16.55` | slightly lower error, high metadata/build cost |

Routing is close to the graph-neighborhood BFS-block builder, not a new routing
breakthrough. Top4 graph-sorted blocks usually route all exact winners, but the
routed union still misses winners on the hard queries before any codec is
applied:

| Selection | Queries | Candidate rows | p50 top10 routed | Worst top10 routed | p50 top20 routed | Worst top20 routed | p50 top50 routed | Worst top50 routed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 graph-sorted block | `10` | `4096` | `8/10` | `3/10` | `17/20` | `4/20` | `38/50` | `9/50` |
| top4 graph-sorted blocks | `10` | `16384` | `10/10` | `9/10` | `20/20` | `18/20` | `50/50` | `45/50` |

Selected aggregate codec rows:

| Selection | Method | Row-code B/vector | Metadata B/vector | Avg total KiB/query | Worst top10@20 | Worst top10@50 | Worst top20@50 | Worst rerank@20 recall@10 | Avg score err | Scan ns/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `global_pq_32B` | `32` | `3.50` | `142.0` | `6/10` | `10/10` | `19/20` | `0.60` | `0.01823` | `16.86` |
| top1 | `local_residual_pq_32B` | `32` | `14.05` | `184.2` | `9/10` | `10/10` | `19/20` | `0.90` | `0.01422` | `16.66` |
| top1 | `local_pca_rank32` | `32` | `3.56` | `142.2` | `8/10` | `9/10` | `18/20` | `0.80` | `0.02216` | `27.81` |
| top4 | `global_pq_32B` | `32` | `3.50` | `568.0` | `7/10` | `10/10` | `19/20` | `0.70` | `0.01715` | `16.86` |
| top4 | `local_residual_pq_32B` | `32` | `14.05` | `736.8` | `8/10` | `10/10` | `18/20` | `0.80` | `0.01407` | `16.64` |
| top4 | `local_residual_opq_32B` | `32` | `18.55` | `808.8` | `9/10` | `10/10` | `20/20` | `0.90` | `0.01384` | `16.56` |
| top4 | `local_pca_rank32` | `32` | `3.56` | `569.0` | `6/10` | `7/10` | `12/20` | `0.60` | `0.02789` | `28.94` |
| top1 | `global_pq_48B` | `48` | `3.50` | `206.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00856` | `26.14` |
| top4 | `global_pq_48B` | `48` | `3.50` | `824.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00810` | `26.58` |
| top4 | `local_residual_pq_48B` | `48` | `14.05` | `992.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00679` | `26.79` |
| top4 | `scalar_u4` | `48` | `0.19` | `771.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00977` | `1001.60` |
| top4 | `global_pq_64B` | `64` | `3.50` | `1080.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00662` | `35.52` |
| top4 | `local_residual_pq_64B` | `64` | `14.05` | `1248.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00559` | `35.51` |
| top4 | `local_pca_rank64` | `64` | `5.08` | `1105.2` | `9/10` | `10/10` | `20/20` | `0.90` | `0.01149` | `68.26` |
| top4 | `local_pca_rank80` | `80` | `5.84` | `1373.4` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00643` | `89.12` |
| top4 | `scalar_u8` | `96` | `0.19` | `1539.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00057` | `1001.37` |

Interpretation:

- The graph-sorted row-adjacent builder is important coverage because it is a
  storage-order proxy, but it does not beat the previous graph-neighborhood
  builder as a routing mechanism. Top4 still misses exact winners before
  compression on the hard queries.
- Global PQ48 remains the cleanest trained-codebook frontier point: all measured
  candidate-survival and rerank gates pass, metadata is small, and scan cost is
  about `26 ns/vector`.
- Local residual-PQ32 is useful mostly when the row-code byte budget is the hard
  constraint. It improves top1 worst top10@20 from `6/10` to `9/10` and lowers
  score error versus global PQ32, but selected metadata raises the effective
  lane to about `46 B/vector`.
- Local residual-OPQ32 improves the top4 32B worst gates further, but with about
  `18.55 B/vector` metadata and `12-19 s` local training in this run. That is
  not yet a production promotion.
- Local PCA rank32 is not competitive on this less coherent buildable union.
  Rank64/rank80 are still useful baselines, but PQ gives better scan cost at
  similar candidate-survival gates.

## Graph-Visited Block Routing Scout

The harness now has a separate selection mode:
`COLUMN_VECTOR_DEEP1B_BUILDABLE_SELECTION=graph_visited_blocks`. This keeps the
same sealed `ivf_graph_sorted_blocks` storage blocks, but routes queries by
starting from static IVF centroid entry rows, greedily expanding the
query-independent row graph using exact query-to-row scores, and then reading
the sealed graph-sorted storage blocks that contain the visited rows.

This is a **production/buildable granule scout**, not an official top100 oracle
cloud. It is closer to an actual graph visited-set route than centroid-ranked
block selection, but it is still not a full production HNSW/TreeDB graph or
actual TreeDB granule layout. The codec metrics below are conditional on the
selected blocks; routing misses still bound global recall before compression.

Run artifact:

```text
/tmp/gomap_deep1b_buildable_graph_visited_full_ladder_q0_9_20260520_012048/report.md
```

Run shape:

| Field | Value |
| --- | ---: |
| Regime | `buildable_granule_scout` |
| Builder | `ivf_graph_sorted_blocks` |
| Selection | `graph_visited_blocks` |
| Eval rows | `32768` |
| Eval row offset | `8192` |
| Dims | `96` |
| Granule rows | `4096` |
| Granules | `8` |
| K-means iterations | `6` |
| Graph degree | `16` |
| Graph entry clusters | `4` |
| Queries | `0..9` |
| Top granules | `1,4` |
| PQ train rows | `8192` |
| Local PCA ranks | `32,48,64,80,96` |
| PQ / residual-PQ / OPQ budgets | `32,48,64,80,96 B/vector` |
| Local residual-PQ / local residual-OPQ budgets | `32,48,64,80,96 B/vector` |
| PQ iterations | `4` |
| OPQ outer iterations | `3` |
| Scan iterations | `4` |

Training cost and metadata now cover the full fixed-byte ladder:

| Method family | Budgets | Train ms | Metadata B/eval-vector | Interpretation |
| --- | --- | ---: | ---: | --- |
| global PQ | `32/48/64/80/96` | `773.2-1235.3` | `1.50` | simplest trained codebook baseline |
| global residual-PQ | `32/48/64/80/96` | `715.0-1426.2` | `1.51` | overlaps global PQ in this run |
| global OPQ | `32/48/64/80/96` | `3305.9-7065.1` | `2.06` | extra rotation cost, no decisive win yet |
| local residual-PQ | `32/48/64/80/96` | `3035.0-4945.4` | `12.05` | local low-byte challenger |
| local residual-OPQ | `32/48/64/80/96` | `12593.6-25047.3` | `16.55` | slightly lower error, high metadata/build cost |

Routing is the main finding. The graph-visited selector is now measured, but it
does not outperform centroid-ranked graph-sorted top4 routing on this slice.
Top4 graph-visited blocks route all exact top10/top20 winners for most queries,
but query `6` routes only `6/10` top10 and `12/20` top20, and query `9` routes
`9/10` top10 and `19/20` top20. Those misses happen before any codec is scored.

| Selection | Queries | Candidate rows | p50 top10 routed | Worst top10 routed | p50 top20 routed | Worst top20 routed | p50 top50 routed | Worst top50 routed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 graph-visited block | `10` | `4096` | `3/10` | `0/10` | `5/20` | `1/20` | `10/50` | `5/50` |
| top4 graph-visited blocks | `10` | `16384` | `10/10` | `6/10` | `20/20` | `12/20` | `50/50` | `39/50` |

Selected aggregate codec rows:

| Selection | Method | Row-code B/vector | Metadata B/vector | Avg total KiB/query | Worst top10@20 | Worst top10@50 | Worst top20@50 | Worst rerank@20 recall@10 | Avg score err | Scan ns/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `global_pq_32B` | `32` | `3.50` | `142.0` | `6/10` | `10/10` | `19/20` | `0.60` | `0.01757` | `17.24` |
| top1 | `local_residual_pq_32B` | `32` | `14.05` | `184.2` | `10/10` | `10/10` | `19/20` | `1.00` | `0.01401` | `16.98` |
| top1 | `global_pq_48B` | `48` | `3.50` | `206.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00827` | `27.45` |
| top4 | `global_pq_32B` | `32` | `3.50` | `568.0` | `7/10` | `10/10` | `19/20` | `0.70` | `0.01713` | `16.44` |
| top4 | `local_residual_pq_32B` | `32` | `14.05` | `736.8` | `8/10` | `10/10` | `18/20` | `0.80` | `0.01412` | `16.97` |
| top4 | `local_residual_opq_32B` | `32` | `18.55` | `808.8` | `9/10` | `10/10` | `19/20` | `0.90` | `0.01387` | `16.55` |
| top4 | `global_pq_48B` | `48` | `3.50` | `824.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00809` | `26.35` |
| top4 | `local_residual_pq_48B` | `48` | `14.05` | `992.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00681` | `27.40` |
| top4 | `local_residual_opq_48B` | `48` | `18.55` | `1064.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00671` | `26.86` |
| top4 | `scalar_u4` | `48` | `0.19` | `771.0` | `10/10` | `10/10` | `19/20` | `1.00` | `0.00979` | `1010.28` |
| top4 | `global_pq_64B` | `64` | `3.50` | `1080.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00660` | `38.10` |
| top4 | `local_residual_pq_64B` | `64` | `14.05` | `1248.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00561` | `38.53` |
| top4 | `local_residual_opq_64B` | `64` | `18.55` | `1320.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00548` | `38.09` |
| top4 | `global_pq_80B` | `80` | `3.50` | `1336.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00485` | `47.25` |
| top4 | `local_pca_rank80` | `80` | `5.84` | `1373.4` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00645` | `83.42` |
| top4 | `local_residual_pq_80B` | `80` | `14.05` | `1504.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00414` | `48.23` |
| top4 | `global_pq_96B` | `96` | `3.50` | `1592.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00144` | `58.35` |
| top4 | `local_pca_rank96` | `96` | `6.59` | `1641.5` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00084` | `105.47` |
| top4 | `local_residual_opq_96B` | `96` | `18.55` | `1832.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00124` | `58.46` |
| top4 | `scalar_u8` | `96` | `0.19` | `1539.0` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00057` | `1005.98` |

The same artifact now includes a staged cascade cost estimate: compressed scan,
materialized approximate topK selection, and resident fp32 rerank of the
selected shortlist. This is still not full end-to-end query latency because it
excludes I/O, decompression, cache effects, and a fused scan+topK executor.

| Selection | Method | Selected-code KiB/query | p50 scan us | p50 topK@50 us | p50 measured cascade@50 us | Avg cascade@50 KiB | p50 measured cascade@100 us | Avg cascade@100 KiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `global_pq_80B` | `334.0` | `192.93` | `16.22` | `214.79` | `352.9` | `242.84` | `371.9` |
| top1 | `local_residual_pq_80B` | `376.2` | `189.58` | `16.52` | `209.40` | `395.1` | `235.90` | `414.1` |
| top1 | `global_pq_96B` | `398.0` | `240.58` | `16.19` | `259.80` | `416.9` | `281.01` | `435.9` |
| top1 | `local_pca_rank96` | `410.4` | `432.18` | `16.18` | `454.53` | `429.3` | `482.40` | `448.3` |
| top4 | `global_pq_80B` | `1336.0` | `768.76` | `33.41` | `798.72` | `1354.9` | `825.23` | `1373.9` |
| top4 | `local_pca_rank80` | `1373.4` | `1381.71` | `33.90` | `1416.62` | `1392.3` | `1437.91` | `1411.3` |
| top4 | `global_pq_96B` | `1592.0` | `957.34` | `33.31` | `993.56` | `1610.9` | `1013.78` | `1629.9` |
| top4 | `local_residual_opq_96B` | `1832.8` | `958.80` | `33.29` | `997.81` | `1851.7` | `1012.64` | `1870.6` |

Interpretation:

- This fills the graph-visited-set scout gap, but it is not a routing win. The
  graph-visited selector is weaker than centroid-ranked graph-sorted top4 on
  hard queries `6` and `9`, so it should be treated as a stress test rather
  than a production route.
- Conditional on the routed union, the codec conclusions are stable: global
  PQ48/PQ64 remain the cleanest low-byte trained-codebook frontier points, and
  the 80B/96B ladder mostly buys lower score error and better compressed-top10
  stability after the wider candidate gates are already saturated.
- Global PQ80/PQ96 have low selected metadata (`3.50 B/vector`) and scan at
  about `47-59 ns/vector`; they are the cleanest high-byte codebook baselines.
- Local PCA96 has the lowest codebook-family score error in the top4 row
  (`0.00084`) but scans at about `105 ns/vector`, so it is a quality baseline,
  not the speed frontier.
- Local residual-PQ80 improves score error versus global PQ80, but spends about
  `14 B/vector` selected metadata; keep it as a challenger rather than the
  default.
- Local residual-OPQ96 scans like global PQ96 but spends about
  `18.55 B/vector` selected metadata and much more build time, so it is not
  promoted yet.
- The apparent perfect rerank@50 rows do not repair routing misses. If the
  exact winner never enters the routed storage-block union, no compressed scan
  or rerank lane can recover it.

## Exact In-Cluster Graph-Visited Buildable Smoke

The harness now also supports exact in-cluster graph builders:
`ivf_exact_graph_neighborhood_blocks` and `ivf_exact_graph_sorted_blocks`.
These train deterministic cosine k-means centroids, build exact kNN adjacency
inside each IVF cluster, then either form BFS neighborhood blocks or materialize
a graph traversal order and chunk adjacent rows into fixed-size blocks. This is
a stronger buildable graph-locality proxy than the earlier IVF-window graph,
but it is still not a production HNSW/TreeDB graph build.

The exact graph builder is intentionally expensive: the smoke below builds
exact in-cluster adjacency over only `8192` eval rows and already spends about
`5.37 s` on storage-block construction plus `5.35 s` on the separate routing
graph. Treat this as scout evidence for locality/routing behavior, not as a
proposed production graph-construction path.

Run artifact:

```text
/tmp/gomap_deep1b_exact_graph_smoke_20260520_020621/report.md
```

Run shape:

| Field | Value |
| --- | ---: |
| Regime | `buildable_granule_scout` |
| Builder | `ivf_exact_graph_sorted_blocks` |
| Selection | `graph_visited_blocks` |
| Eval rows | `8192` |
| Eval row offset | `1024` |
| Dims | `96` |
| Granule rows | `4096` |
| Granules | `2` |
| Granule build ms | `5372.352` |
| Routing graph build ms | `5347.940` |
| Graph degree | `16` |
| Graph entry clusters | `4` |
| Queries | `0,1` |
| Top granules | `1,2` |
| PQ train rows | `1024` |
| PQ budgets | `32,48 B/vector` |
| PQ iterations | `2` |
| Scan iterations | `2` |

Routing still remains the first gate. Top1 reads one 4096-row block and misses
query `0` winners before any codec is applied; top2 reads all `8192` eval rows
in this smoke and therefore routes the measured winners, which is useful as a
codec sanity row but not a production routing win.

| Selection | Queries | Candidate rows | p50 top10 routed | Worst top10 routed | p50 top20 routed | Worst top20 routed | p50 top50 routed | Worst top50 routed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 exact-graph-visited block | `2` | `4096` | `6/10` | `6/10` | `10/20` | `10/20` | `28/50` | `28/50` |
| top2 exact-graph-visited blocks | `2` | `8192` | `10/10` | `10/10` | `20/20` | `20/20` | `50/50` | `50/50` |

Selected conditional codec rows:

| Selection | Method | Row-code B/vector | Metadata B/vector | Avg total KiB/query | Worst top10@20 | Worst top10@50 | Worst top20@50 | Worst exact rerank@50 recall@10 | Avg score err | Scan ns/vector |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top1 | `global_pq_32B` | `32` | `8.00` | `160.0` | `10/10` | `10/10` | `19/20` | `1.00` | `0.01814` | `15.92` |
| top1 | `local_pca_rank32` | `32` | `3.56` | `142.2` | `7/10` | `10/10` | `18/20` | `1.00` | `0.03078` | `27.46` |
| top1 | `global_pq_48B` | `48` | `8.00` | `224.0` | `9/10` | `10/10` | `20/20` | `1.00` | `0.00903` | `24.86` |
| top1 | `scalar_u4` | `48` | `0.19` | `192.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.01000` | `959.77` |
| top1 | `local_pca_rank64` | `64` | `5.08` | `276.3` | `10/10` | `10/10` | `20/20` | `1.00` | `0.01262` | `66.47` |
| top1 | `scalar_u8` | `96` | `0.19` | `384.8` | `10/10` | `10/10` | `20/20` | `1.00` | `0.00058` | `959.62` |

The staged cascade rows reinforce the same kernel lesson as the earlier smoke:
global PQ32 scans and reranks much faster than local PCA32 in this Go harness,
and both are far faster than reconstructed scalar u4/u8 scanning. For top1
exact-graph-visited blocks, `global_pq_32B` has p50 scan `64.15 us`,
topK@50 `9.42 us`, fp32 cascade@50 `78.55 us`, and int8 cascade@50
`122.57 us`; `local_pca_rank32` has p50 scan `112.04 us`, topK@50 `9.42 us`,
fp32 cascade@50 `126.72 us`, and int8 cascade@50 `169.82 us`.

Interpretation:

- Exact in-cluster graph blocks are valuable as a stricter buildable locality
  proxy, but the current exact graph construction cost is not production
  viable.
- The smoke does not prove production routing: top1 misses winners, and top2
  is effectively "read all blocks" at this tiny eval size.
- Conditional on the routed union, global PQ32/PQ48 are again stronger scan
  baselines than local PCA at matched row-code bytes in the current Go harness.
- Scalar u4/SQ8 remain quality references, but the reconstructed Go scorer is
  still around `1 us/vector`, so they need an optimized kernel before they are
  hot-scan candidates.

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
over variance PCA but remains an oracle-locality result. The PCA plus tiny
residual-correction lane is now present in the harness as
`pca_residual_random_projection`, and the full `0..99` rerun shows it can move
individual boundary metrics but does not become the top100 oracle frontier:
rank64+8 fixes worst top10@50 while worsening worst top20@50, and rank80+8
fixes worst top20@50 only by spending `88 B/vector`. The remaining top100-only
probe worth adding, if this path remains decision-relevant, is a
low-rank-plus-tail progressive bound test.

Track A.5 is now started: buildable-granule scouts over row-id-contiguous
blocks, IVF/k-means variable-size clusters, IVF/k-means locality-sorted fixed
blocks, IVF-window graph-neighborhood fixed blocks, IVF-window graph-sorted
row-adjacent fixed blocks, a graph-visited block-routing scout over the sealed
graph-sorted storage blocks, and an exact in-cluster graph-visited smoke over
exact graph-sorted blocks. Row-id order is a weak control. IVF/k-means gives
the first buildable locality signal, the sorted-block variants show that a
buildable locality order can feed fixed TreeDB-style storage blocks, and the
graph proxies add query-independent graph locality without claiming full
HNSW/TreeDB graph behavior. The graph-visited scouts now make the routing
failure mode explicit: they are closer to visited-set paths, but still miss
winners on hard queries before compression. The exact in-cluster graph smoke
also shows that stronger locality proxies must carry build-cost accounting.
These results show why codebook stages need real train/eval splits rather than
top100 oracle fits.

Track B now covers the fixed Deep1B budget ladder. Global PQ, global
centroid-residual PQ, and OPQ-style lanes have been compared against local PCA
`K=32/48/64/80/96` at 32/48/64/80/96-byte budgets on buildable IVF/k-means
granules, IVF-sorted fixed blocks, and the IVF-window graph-neighborhood
and graph-sorted row-adjacent fixed-block proxies. PQ is currently the simpler
trained codebook baseline; OPQ is a measured challenger without a decisive
Pareto win yet; and global residual PQ does not beat global PQ in this shape.
Per-buildable-granule local residual-PQ and local residual-OPQ are now measured
on sorted fixed blocks, graph-neighborhood blocks, graph-sorted row-adjacent
blocks, and graph-visited selections over sealed graph-sorted blocks. Local
residual-PQ improves candidate gates at extra metadata cost; local residual-OPQ
slightly lowers score error but has not yet justified the additional rotation
metadata and build time. Track B still needs larger query coverage, larger
train/eval slices, production-grade local OPQ/LOPQ amortization, full
HNSW/TreeDB graph visited sets, and actual TreeDB granule layouts.

Track C is the granule-local residual encoding tournament. The first measured
lanes are local residual-PQ and local residual-OPQ. For each buildable granule
or cell, the remaining tournament should compare:

```text
centroid
residuals = x - centroid
encode residuals with PCA K, PQ, OPQ/PQ, and PCA + residual correction
```

This is the closest TreeDB-native version of the LOPQ/RVQ lesson: compress the
local residual after the coarse locality unit, not the raw global vector.

## Full-Int8 Cascade Instrumentation Smoke

The buildable-granule renderer now emits the next cascade lane:

```text
compressed scan
  -> approximate topK selection
  -> full-dim SQ8/int8 rerank
```

This is a staged kernel estimate over resident materialized data, not full
end-to-end query latency. It measures the compressed scorer, approximate topK
selection over materialized scores, and a resident full-dim SQ8 rerank scorer;
it still excludes I/O, decompression, cache effects, and a fused scan+topK
executor.

Instrumentation smoke:

```text
builder:        ivf_kmeans_sorted_blocks
queries:        0,1
eval rows:      8192
granule rows:   4096
top granules:   1
PCA ranks:      32,64,96
PQ budgets:     32,48 B/vector
PQ train rows:  1024
PQ train iters: 2
scan iters:     2
```

The smoke confirms the new columns render and distinguishes the byte lane from
the current kernel lane. In this Go prototype, full-dim SQ8 rerank is currently
much slower than the resident fp32 rerank kernel:

```text
avg exact-fp32 rerank:   75.73 ns/vector
avg full-int8 rerank:   972.66 ns/vector
```

So this result should be read as a storage/cascade instrumentation result, not
as an optimized int8 scoring result. It lowers rerank bytes but does not lower
rerank latency until the full-int8 scorer is replaced by a production SIMD or
otherwise optimized kernel.

Representative `global_pq_32B_x8` row from the smoke:

```text
selected compressed code bytes/query: 160.0 KiB
fp32 cascade@50 p50:                   76.99 us
int8 cascade@50 p50:                  121.41 us
fp32 cascade@50 bytes:                178.9 KiB
int8 cascade@50 bytes:                164.7 KiB
full-int8 rerank@50 recall@10:          1.00
```

This fills the report's instrumentation gap for a full-dim int8 rerank stage.
It does not yet settle the production cascade question, because the measured
path is still staged and resident rather than an integrated TreeDB executor.

## Evidence Contract Audit

This report is now a tournament artifact, not a single-method benchmark note.
The current evidence map is:

| Requirement | Current evidence | Status |
| --- | --- | --- |
| Separate official top100 oracle locality from production/buildable locality | Top100 sections are labeled as oracle local-neighborhood upper-bound probes. Buildable sections are separated into row-id, IVF/k-means, IVF-sorted fixed blocks, graph-neighborhood fixed blocks, graph-sorted row-adjacent fixed blocks, graph-visited block-routing over sealed graph-sorted blocks, and an exact in-cluster graph-visited smoke. | Satisfied for current report wording. |
| Primary metric is candidate survival, not compressed final top10 order | Tables report exact top10/top20 containment at approx@20/@50 and rerank@20/@50 recall@10. The conclusion keeps exact rerank mandatory. | Satisfied for measured rows. |
| Fixed byte-budget comparisons | Top100 and buildable runs cover the 32/48/64/80/96-byte ladder where the lane exists. The graph-visited full-ladder scout covers 32/48/64/80/96 for PQ, residual-PQ, OPQ-style, local residual-PQ, and local residual-OPQ lanes, and compares them against local PCA and scalar lanes at the same row-code budgets. The exact in-cluster graph smoke currently covers only 32/48/64/96 selected rows, so it is locality/routing instrumentation rather than a full budget ladder. | Satisfied for the measured scout lanes; still partial for production because larger/full TreeDB granules remain unmeasured. |
| Metadata-amortized accounting | Buildable codebook tables split row-code bytes and metadata bytes. Top100 oracle rows are explicitly row-code payload probes; their metadata amortization is not production evidence. | Satisfied for buildable codebook lanes; top100 metadata remains intentionally unproven. |
| Train/eval discipline for PQ/OPQ/residual-PQ | PQ, residual-PQ, OPQ-style, local residual-PQ, and local residual-OPQ rows are trained on buildable train samples and evaluated on held-out eval/query slices, not on a single top100 cloud. | Satisfied for measured codebook lanes. |
| Top100-only method coverage | Measured local PCA int8, adaptive rank, full-dim SQ8, scalar low-bit lanes, scale-policy probes, norm-explicit correction, boundary-weighted PCA, pairwise-difference PCA, query-centered oracle projection, random-rotation scalar/sign probes, and PCA plus tiny residual correction. | Broadly satisfied; low-rank-plus-tail progressive bounds remain open. |
| Production/buildable granule coverage | Measured row-id controls, IVF clusters, IVF-sorted fixed blocks, graph-neighborhood fixed blocks, graph-sorted row-adjacent fixed blocks, a graph-visited block-routing scout over sealed graph-sorted storage blocks, and an exact in-cluster graph-visited smoke over exact graph-sorted blocks. | Partial; full HNSW/TreeDB graph visited sets and actual TreeDB granules are not measured. |
| Cascade architecture | Existing rows report compressed shortlist containment and exact rerank@20/@50 recall. The buildable scout renderer now emits staged cascade measurements: measured compressed scan cost, measured approximate topK selection over materialized scores, a measured resident-fp32 row-id rerank kernel, and a measured resident full-dim SQ8/int8 rerank kernel at top20/top50/top100, with selected-code bytes plus fp32 or full-int8 rerank bytes. | Partial; this is still not full end-to-end latency because I/O, decompression, cache effects, fused scan+topK executor effects, optimized int8 scoring, and optional fp32-after-int8 rerank are not measured. |
| p50/p90/worst-query behavior | Top100 adaptive-rank tables include p50/p90/worst K. The buildable scout renderer now emits aggregate routing and conditional-codec p50/p90/worst tables for new artifacts. The curated historical sections in this report still emphasize p50/worst unless regenerated from those artifacts. | Partial; larger buildable runs should be regenerated or summarized with the new p90 tables before final promotion. |
| Production promotion criteria | Current report does not promote spherical hot scoring, local PCA as a final ranker, OPQ as a winner, graph-visited block routing as a production route, exact in-cluster graph construction as a production builder, or top100-only oracle projections as production methods. It promotes global PQ48/PQ64 as the simplest measured low-byte trained-codebook candidate-generator baseline, global PQ80/PQ96 as clean high-byte error-reduction baselines, and local residual-PQ as a metadata-heavy challenger. | Satisfied for current evidence, but not final until full graph visited sets and TreeDB granules are measured. |

The most important remaining gap is production locality. The report has measured
several buildable fixed-block proxies and a graph-visited block-routing scout,
but it still has not shown that actual TreeDB storage granules, full graph
visited sets, or production graph-neighborhood layouts preserve the same
candidate-survival frontier. Until that is measured, the current result is a
strong research frontier, not a production format decision.

## Recommended Next Work

1. Extend the trained graph-neighborhood, graph-sorted, and graph-visited
   block-routing runs from this fixed-byte scout to larger query coverage and
   larger train/eval slices, then add full HNSW/TreeDB graph-visited-set and
   actual TreeDB granule builders. This is the next required step before any
   production compression claim.
2. Repeat the PQ/residual-PQ/OPQ/local-residual-PQ/local-residual-OPQ
   same-byte tournament with broader train/eval coverage and stricter metadata
   amortization checks. Do not train codebooks on a single official top100
   cloud.
3. Extend local OPQ/LOPQ to production-plausible graph/TreeDB granules and add
   PCA plus residual correction if local PCA remains promising.
4. Extend the cascade benchmark beyond the current staged scan-plus-topK-plus
   fp32/full-int8 rerank measurements: add an optimized full-dim int8 rerank
   kernel, optional exact fp32 rerank after int8, p50/p95 end-to-end latency,
   decompression/I/O/cache effects, a fused scan+topK executor, and cache-aware
   bytes read/query.
5. Finish safe/progressive low-rank-plus-tail bounds only if the top100 oracle
   path remains needed for method triage; the tiny residual sketch has now been
   rerun on queries `0..99` and is not promoted above the current oracle
   frontier.
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
global PQ is the strongest measured trained-codebook buildable baseline so far,
graph-sorted and graph-visited block proxies are measured but not a new routing frontier,
exact in-cluster graph proxies are useful stronger locality scouts but too expensive as built,
global PQ48/PQ64 are the cleanest low-byte codebook frontier points,
global PQ80/PQ96 are clean high-byte error-reduction baselines,
and exact rerank remains mandatory.
```
