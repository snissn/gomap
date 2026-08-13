# TreeDB vector server matched-recall matrix

Run date: 2026-08-12. Source revision:
`21a57f937f88ff7b3b2746848efa40433a84389d` with
`vcs.modified=false`.

## Selected results

Each engine's selected row is the lowest tested `efSearch` whose median
recall@10 is at least `.90`.

| system | corpus | selected EF | recall@10 | c=1 QPS / p95 | c=32 QPS / p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| TreeDB single daemon / four groups | 100k | 64 | .9525 | 2,198.0 / 0.537 ms | 10,852.5 / 4.866 ms |
| pgvector | 100k | 64 | .9251 | 1,255.6 / 0.965 ms | 3,745.6 / 15.390 ms |
| Milvus Standalone | 100k | 64 | .9210 | 644.8 / 1.931 ms | 2,256.7 / 25.410 ms |
| TreeDB single daemon / four groups | 250k | 128 | .9580 | 1,525.3 / 0.791 ms | 6,220.6 / 8.466 ms |
| pgvector | 250k | 128 | .9229 | 385.5 / 3.580 ms | 1,712.9 / 36.690 ms |
| Milvus Standalone | 250k | 128 | .9336 | 559.8 / 1.997 ms | 2,141.3 / 26.670 ms |

TreeDB QPS ratios at those selected rows:

| corpus | vs pgvector c=1 / c=32 | vs Milvus c=1 / c=32 |
| --- | ---: | ---: |
| 100k | 1.75x / 2.90x | 3.41x / 4.81x |
| 250k | 3.96x / 3.63x | 2.73x / 2.91x |

## Method

- Deterministic 128-dimensional cosine corpora and the same exact top-10 truth.
- 1,000 measured queries per cell at concurrency 1 and 32.
- Three serialized, counterbalanced repetitions; tables report medians.
- Search QPS excludes index construction.
- TreeDB: one OS daemon, one public coordinator, four logical serving groups,
  retained `M=18` / `efConstruction=256` graph, and probes=2.
- pgvector: one PostgreSQL 16 container with pgvector HNSW
  `M=16` / `efConstruction=128`.
- Milvus: Standalone plus etcd and MinIO with HNSW
  `M=16` / `efConstruction=128`.
- Linux/amd64 on a 6-core/12-thread Intel Core i5-11400F. Runs used CPUs 0-11,
  a 20 GiB aggregate memory envelope, and no swap.

Pinned external images:

- `pgvector/pgvector:pg16@sha256:84a355869251af1a3379cfc9fa7b4dbf962c03f642a4bb7b339a203925071c43`
- `milvusdb/milvus:v2.6.20@sha256:e514fced2aa26cf3b94e7de20986fe9e535159fde08f9934d245d0e1a909c18c`
- `quay.io/coreos/etcd:v3.5.25@sha256:dc2bdc588d2adc5272204a1fff7f1d89f31e8caacea78fdf509fd409d7162a9d`
- `minio/minio:RELEASE.2024-12-18T13-15-44Z@sha256:34c8e2f52a5984492555427fee07254c80036bdb7079bb91679232abd7a4fa20`

TreeDB executable SHA-256:
`1e934694ebda13005a9d234413ead0ba6636e70f82056a5b424074fdeba166c4`.

## Repetition ranges at selected EF

| system | corpus | c | QPS min-max | p95 ms min-max |
| --- | ---: | ---: | ---: | ---: |
| TreeDB | 100k | 1 | 2,051.9-2,200.3 | 0.524-0.625 |
| TreeDB | 100k | 32 | 10,174.0-10,976.8 | 4.852-5.951 |
| pgvector | 100k | 1 | 789.4-1,360.0 | 0.861-1.713 |
| pgvector | 100k | 32 | 3,540.8-5,021.5 | 11.269-17.105 |
| Milvus | 100k | 1 | 537.0-664.7 | 1.823-2.214 |
| Milvus | 100k | 32 | 2,214.6-2,421.7 | 23.632-26.805 |
| TreeDB | 250k | 1 | 1,515.0-1,581.7 | 0.744-0.813 |
| TreeDB | 250k | 32 | 6,069.4-7,529.4 | 6.817-8.931 |
| pgvector | 250k | 1 | 348.9-395.4 | 3.556-4.037 |
| pgvector | 250k | 32 | 1,698.5-1,884.9 | 33.072-37.427 |
| Milvus | 250k | 1 | 559.6-579.3 | 1.944-2.074 |
| Milvus | 250k | 32 | 2,014.1-2,217.8 | 24.826-28.921 |

## Full median EF curves

### 100k

| system | EF | recall@10 | c=1 QPS / p95 ms | c=32 QPS / p95 ms |
| --- | ---: | ---: | ---: | ---: |
| TreeDB | 64 | .9525 | 2,198.0 / 0.537 | 10,852.5 / 4.866 |
| TreeDB | 128 | .9910 | 1,721.2 / 0.729 | 7,917.1 / 6.238 |
| TreeDB | 256 | .9971 | 1,305.6 / 1.013 | 5,673.2 / 9.053 |
| pgvector | 16 | .6247 | 2,008.8 / 0.665 | 5,789.4 / 9.922 |
| pgvector | 32 | .7906 | 1,630.8 / 0.844 | 5,231.1 / 11.196 |
| pgvector | 64 | .9251 | 1,255.6 / 0.965 | 3,745.6 / 15.390 |
| pgvector | 128 | .9793 | 776.0 / 1.799 | 3,596.3 / 18.114 |
| pgvector | 256 | .9905 | 562.2 / 2.410 | 2,957.1 / 19.746 |
| pgvector | 512 | .9953 | 436.8 / 3.078 | 2,418.9 / 22.702 |
| Milvus | 16 | .6061 | 712.6 / 1.835 | 2,381.5 / 23.518 |
| Milvus | 32 | .7749 | 682.8 / 1.729 | 2,397.7 / 23.976 |
| Milvus | 64 | .9210 | 644.8 / 1.931 | 2,256.7 / 25.410 |
| Milvus | 128 | .9837 | 526.5 / 2.596 | 2,154.4 / 26.819 |
| Milvus | 256 | .9967 | 520.4 / 2.325 | 2,208.2 / 25.543 |
| Milvus | 512 | .9988 | 456.7 / 2.737 | 1,969.5 / 28.429 |

### 250k

| system | EF | recall@10 | c=1 QPS / p95 ms | c=32 QPS / p95 ms |
| --- | ---: | ---: | ---: | ---: |
| TreeDB | 64 | .8647 | 2,090.0 / 0.550 | 10,215.6 / 5.268 |
| TreeDB | 128 | .9580 | 1,525.3 / 0.791 | 6,220.6 / 8.466 |
| TreeDB | 256 | .9894 | 1,096.6 / 1.199 | 4,525.0 / 11.111 |
| pgvector | 16 | .4744 | 1,402.8 / 1.054 | 5,245.5 / 12.776 |
| pgvector | 32 | .6458 | 1,054.8 / 1.285 | 4,121.3 / 15.607 |
| pgvector | 64 | .8122 | 606.8 / 2.285 | 2,771.7 / 24.377 |
| pgvector | 128 | .9229 | 385.5 / 3.580 | 1,712.9 / 36.690 |
| pgvector | 256 | .9712 | 242.5 / 5.927 | 1,239.7 / 50.080 |
| pgvector | 512 | .9866 | 171.7 / 8.350 | 859.4 / 68.531 |
| Milvus | 16 | .5117 | 679.8 / 1.840 | 2,289.9 / 25.347 |
| Milvus | 32 | .6762 | 695.8 / 1.671 | 2,354.4 / 24.896 |
| Milvus | 64 | .8280 | 628.0 / 2.022 | 2,157.6 / 26.236 |
| Milvus | 128 | .9336 | 559.8 / 1.997 | 2,141.3 / 26.670 |
| Milvus | 256 | .9789 | 478.2 / 2.666 | 1,983.8 / 28.171 |
| Milvus | 512 | .9913 | 408.8 / 2.719 | 1,805.5 / 31.424 |

## Interpretation limits

- EF is engine-specific. The selected points compare achieved recall rather
  than assuming equal numeric EF means equal work.
- The EF sweep is discrete, so selected recalls are close rather than exactly
  equal; TreeDB has the highest selected recall in both corpora.
- TreeDB uses its retained production graph while the external engines build
  fresh indexes per repetition. Build time is excluded for all search rows.
- This is a single-host serving comparison, not a multi-host distributed
  topology comparison.
- The host had modest background activity. Runs were serialized and
  counterbalanced; the repetition ranges above expose observed noise.

The [machine-readable result](treedb_vector_server_matched_recall_2026-08-12.json)
contains every curve point, selected range, source revision, and SHA-256 digest
of the 30 reduced search inputs. Its SHA-256 is
`e9e6076ff3232a84458b97551652af16939e17b9028a5206477c505228a82ea6`.
