# Deep1B Groundtruth Compression Report

This report extends the PR 1620 Deep1B compression work with the official
`groundtruth.public.10K.ibin` top100 nearest-neighbor file. The purpose is to
separate two questions:

1. Can true Deep1B nearest-neighbor clouds support strong granule-local
   compression?
2. Which compressed representations are plausible for final ranking versus
   candidate generation before exact rerank?

Deep1B vectors in this benchmark are `D=96`. The live sample below used queries
`0..4`, loaded each query's official top100 database rows from `base.1B.fbin`
by HTTP range request, and fit local PCA on each top100 cloud.

## What The Probe Establishes

The official top100 neighborhoods are genuinely local. Across queries `0..4`:

| Metric | Result |
| --- | ---: |
| Loaded groundtruth rows | `500/500` |
| Average centroid norm | `0.8779` |
| Average centroid cos(query) | `0.9297` |
| Centroid cos(query) range | `0.8960-0.9711` |
| Average pairwise cosine | `0.7708` |
| Pairwise cosine range | `0.6359-0.8791` |

This is much tighter than the earlier first-1M/top8192 smoke block. The premise
that true nearest-neighbor neighborhoods have exploitable local structure is now
supported.

The important limitation is equally clear: local variance preservation does not
automatically preserve final top-k ordering. The rank64 PCA path captures about
`99.05%` average variance, but only averages `6.8/10` exact top10 recall as a
final ranker.

| Rank | Avg PCA variance | Avg exact top10 in approx@10 | Avg exact top10 in approx@20 | Avg exact top10 in approx@50 | Avg mean score error |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | `45.87%` | `2.2/10` | `4.2/10` | `7.0/10` | `0.00704` |
| 16 | `68.03%` | `3.2/10` | `4.8/10` | `7.8/10` | `0.00656` |
| 32 | `88.39%` | `4.2/10` | `6.0/10` | `9.2/10` | `0.00599` |
| 64 | `99.05%` | `6.8/10` | `8.8/10` | `10.0/10` | `0.00403` |
| 96 | `100.00%` | `9.8/10` | `10.0/10` | `10.0/10` | `0.00005` |

The score margins explain the mismatch between small reconstruction error and
unstable final ordering:

| Margin metric | Result across queries `0..4` |
| --- | ---: |
| Average exact rank10/rank11 gap | `0.000922` |
| Minimum exact rank10/rank11 gap | `0.000057` |
| Average p90 adjacent top100 gap | `0.000924` |
| Rank64 average mean score error | `0.00403` |

Rank64 error is several times larger than the score gaps that decide top10
membership. That makes exact final top10 misses expected, even when the local
cloud is coherent. The same result is promising for candidate generation:
rank64 placed the exact top10 inside approximate top50 for all five sampled
queries.

`approx@100` is a sanity ceiling in this top100-only probe because the whole
measured universe is the official top100. The actionable candidate-generation
signals here are `approx@20` and `approx@50`; production granules need the same
metric at wider row counts.

## Final Ranking Versus Candidate Generation

Final ranking needs high-fidelity scores. On this sample, low-rank PCA is not
safe as the final representation:

- Rank32: compact, but only `4.2/10` average final top10 recall.
- Rank64: much better, but still only `6.8/10` average final top10 recall.
- Rank96/full-rank int8 coordinates: `9.8/10` average final top10 recall, so
  even quantization and fp16 metadata can perturb a very tight boundary.

Candidate generation has a different gate: keep the real winners in a larger
shortlist, then rerank exactly. On this sample:

- Rank32 is weak but not useless: `9.2/10` average exact-top10 survival in
  approx@50.
- Rank64 is the first credible local-PCA candidate generator: `10/10` exact
  top10 survival in approx@50 for every sampled query.
- Rank96/full-dim int8 is the conservative compressed candidate lane.

The product interpretation is:

```text
compressed representation should generate candidates
exact/full-fidelity representation should decide final rank
```

until broader query and production-granule validation proves otherwise.

## Design A-E

| Design | Representation | Best role | Current evidence | Recommendation |
| --- | --- | --- | --- | --- |
| A | Resident fp32 graph/search representation plus compressed storage columns | Final ranking and current product anchor | Existing resident fp32 graph search is `~58.7k` serial searches/s, `~289k` parallel searches/s, and `0 B/op` | Keep as the correctness and throughput baseline. Do not displace it with PCA yet. |
| B | Full-dimension granule-local int8 residual/scalar quantization, dim-major on disk | Conservative candidate generation; possible near-final rerank lane | Prior int8 tournament stores the 96D dim-major int8 matrix at `85.9-86.7 B/vector` with zstd-better/ClickHouse whole-granule `String CODEC(ZSTD(1))` | Build this first as the robust compressed hot/candidate representation. It has the simplest native scoring path and best measured storage-to-quality balance. |
| C | Low-rank local PCA int8 codes: centroid + basis + per-row coordinates | Compact candidate generation only | Prior 8192-row storage curve: rank32 `31.51 B/vector`, rank64 `59.65 B/vector`. Groundtruth top100: rank64 final top10 `6.8/10`, but exact top10 in approx@50 is `10/10` on queries `0..4` | Do not use for final ranking. Promote to a candidate-generation experiment with explicit candidate-recall gates. |
| D | Cascade: low-rank PCA prefilter, then full-dim int8/fp16 or fp32 rerank | Best near-term architecture | The groundtruth data says rank64 can preserve winners in a wider shortlist while failing exact final ranking | Most promising product path: scan `C`, rerank survivors with `B` or `A`, measure recall/cost at fixed rerank budgets. |
| E | PQ/residual-PQ/LUT scorer, ideally residualized by granule centroid or segment cluster | Candidate generation at fixed byte budgets | Not measured in this PR yet. It is the relevant same-byte competitor to local PCA | Add as the next tournament lane. Compare against rank32/rank64 PCA at `~32-64 B/vector`, not against fp32. |

## Other Designs

- Spherical/JZIP remains a storage/cold-decode candidate. It wins vector-column
  bytes in the earlier report, but current decode+score is too slow for hot
  graph traversal unless a direct native scorer changes the economics.
- Cartesian byte-shuffle plus zstd is a useful non-trig cold block codec. It is
  faster than spherical reconstruction but still not the resident hot path.
- Matryoshka prefixes are attractive if the embedding model supports them. They
  are not proven by this Deep1B result, and should be benchmarked as a separate
  model-aware candidate lane.
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

## Recommended Next Work

1. Expand the groundtruth run from queries `0..4` to at least 100 queries and
   report p50, p90, and worst-query candidate survival.
2. Add production-plausible granule builders and run the same candidate-recall
   tables on their blocks.
3. Add PQ/residual-PQ as a same-byte competitor to local PCA rank32/rank64.
4. Benchmark the full cascade: rank64 PCA top50/top100, full-dim int8 rerank,
   then exact fp32 rerank.
5. Keep spherical and byte-shuffle codecs in the storage/cold-decode track
   unless direct scoring materially changes their throughput.

The current result is therefore not "local PCA is enough." The sharper result
is:

```text
true Deep1B top100 neighborhoods are highly local,
rank64 local PCA is not a final ranker,
rank64 local PCA may be a compact candidate generator,
and exact rerank remains mandatory.
```
