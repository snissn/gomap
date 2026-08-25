# #4327 1M retrieval-only requalification

Three serialized, complete retrieval-only repetitions. Each uses 1,000,000 rows, 25 warm samples/row, candidate limit 65,536, commit `48db2eb621c5824b20b7ca2ecbfbde4651873ae6`, base `6b07740e25bf663b2df3594ed74532601c23ac96`, Go 1.26.0, Apple M3. Raw sample nanoseconds are retained in each JSON report.

| row | median p50 (ms) | median p95 (ms) | median p99 (ms) |
| --- | ---: | ---: | ---: |
| `text_common_score_only` | 56.830 | 59.517 | 60.008 |
| `text_rare_score_only` | 25.647 | 27.384 | 27.631 |
| `text_multi_term_and_score_only` | 96.900 | 100.945 | 101.384 |
| `text_multi_term_or_score_only` | 64.969 | 68.243 | 68.898 |
| `hybrid_text_only_no_docs` | 64.905 | 68.355 | 68.727 |
| `hybrid_text_scalar_no_docs` | 161.096 | 170.988 | 186.610 |
| `hybrid_text_vector_no_docs` | 155.967 | 159.304 | 161.584 |
| `hybrid_text_vector_scalar_no_docs` | 182.433 | 186.305 | 187.680 |

Text rows retain exact pruning: common/AND/OR score only tens of candidates while skipping posting blocks. No historical broad-text gap reproduced; no text product optimization target is activated. The high-cost scalar/hybrid vector rows have fixed exact-bound-insufficient budgets and are directional/out of scope pending a separately owned hybrid-budget seam/profile.
