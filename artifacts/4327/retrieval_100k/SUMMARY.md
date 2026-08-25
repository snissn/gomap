# #4327 current-main 100k retrieval qualification

Three serialized, complete bounded retrieval-only repetitions. Each has 100,000 rows, 25 warm samples per row, candidate limit 65,536, exact harness commit `34df61e5d4a67ac570061c6c8059a78e03090efd`, base `6b07740e25bf663b2df3594ed74532601c23ac96`, and Go 1.26.0. `SHA256SUMS` covers raw JSON, reports, commands, logs, and context.

| row | median p50 (ms) | median p95 (ms) | median p99 (ms) |
| --- | ---: | ---: | ---: |
| `text_common_score_only` | 4.855 | 5.785 | 5.823 |
| `text_rare_score_only` | 1.934 | 2.594 | 2.869 |
| `text_multi_term_and_score_only` | 5.696 | 6.716 | 6.982 |
| `text_multi_term_or_score_only` | 5.719 | 6.576 | 7.010 |
| `hybrid_text_only_no_docs` | 5.738 | 6.650 | 6.748 |
| `hybrid_text_scalar_no_docs` | 13.552 | 14.332 | 16.662 |
| `hybrid_text_vector_no_docs` | 71.745 | 74.441 | 75.489 |
| `hybrid_text_vector_scalar_no_docs` | 23.416 | 24.328 | 24.512 |

All reports select only `load`, `queries`, and `reopen`; every report is complete, has the complete eight-row matrix, retains 25 raw latency samples per row, and all query/reopen guardrails pass. Text common/AND/OR rows retain zero document fetches/fallbacks/fail-closed and block skipping, so this validates the prior low-work identity rather than a broad-text performance target. Hybrid scalar/vector rows retain their fixed candidate budget and are profiled separately; these directional fixed-budget rows are not optimization gates.
