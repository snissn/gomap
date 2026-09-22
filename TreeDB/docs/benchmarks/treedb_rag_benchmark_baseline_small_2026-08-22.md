# TreeDB RAG Retrieval Benchmark (treedb_rag_benchmark/v1)

Generated at 2026-08-22T12:41:05Z.

Issue: #4267. Host: Michaels-Laptop.local (darwin/arm64, 8 CPUs, go1.26.0).

## Fixture

- corpus `treedb-rag-corpus/v1` fingerprint `86bb3e342f463185…`
- docs=512 chunks=1024 (2/doc) dims=64 queries=24 top_k=10 candidate_limit=64 reps=3 vector_m=8 ef_search=128 vector_mode=exact (column_graph exact score plane)

## Ingest / storage

| embed s | ingest docs/s | index build s | storage B | B/chunk |
|---:|---:|---:|---:|---:|
| 0.007 | 11771 | 0.087 | 4317776 | 4217 |

## Rows

| route | mode | filter | sel% | recall@5 | recall@10 | recall@100 | mrr@10 | p50 ms | p99 ms | mean ms | docs_fetched | postings_scanned | candidates_fused |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hybrid | fetch_topk | narrow_25pct | 25.00 | 0.0187 | 0.0375 | 0.0375 | 0.2500 | 0.149 | 0.592 | 0.291 | 3.96 | 252.4 | 33.7 |
| hybrid | fetch_topk | none_100pct | 100.00 | 0.0886 | 0.1761 | 0.1761 | 1.0000 | 0.447 | 0.638 | 0.459 | 10.00 | 252.4 | 128.0 |
| hybrid | fetch_topk | rare_06pct | 6.25 | 0.0131 | 0.0252 | 0.0252 | 0.1250 | 0.367 | 0.466 | 0.381 | 10.00 | 252.4 | 72.0 |
| hybrid | score_only | narrow_25pct | 25.00 | 0.0187 | 0.0375 | 0.0375 | 0.2500 | 0.139 | 0.257 | 0.164 | 0.00 | 252.4 | 33.7 |
| hybrid | score_only | none_100pct | 100.00 | 0.0886 | 0.1761 | 0.1761 | 1.0000 | 0.150 | 0.222 | 0.158 | 0.00 | 252.4 | 128.0 |
| hybrid | score_only | rare_06pct | 6.25 | 0.0131 | 0.0252 | 0.0252 | 0.1250 | 0.068 | 0.227 | 0.081 | 0.00 | 252.4 | 72.0 |
| text_only | fetch_topk | narrow_25pct | 25.00 | 0.0187 | 0.0375 | 0.0375 | 0.2500 | 0.102 | 0.540 | 0.205 | 2.50 | 252.4 | 16.0 |
| text_only | fetch_topk | none_100pct | 100.00 | 0.0876 | 0.1761 | 0.1761 | 0.9792 | 0.378 | 0.573 | 0.387 | 10.00 | 252.4 | 10.0 |
| text_only | fetch_topk | rare_06pct | 6.25 | 0.0121 | 0.0242 | 0.0242 | 0.1042 | 0.042 | 0.480 | 0.090 | 1.25 | 252.4 | 8.0 |
| text_only | score_only | narrow_25pct | 25.00 | 0.0187 | 0.0375 | 0.0375 | 0.2500 | 0.105 | 0.232 | 0.130 | 0.00 | 252.4 | 16.0 |
| text_only | score_only | none_100pct | 100.00 | 0.0876 | 0.1761 | 0.1761 | 0.9792 | 0.096 | 0.261 | 0.103 | 0.00 | 252.4 | 10.0 |
| text_only | score_only | rare_06pct | 6.25 | 0.0121 | 0.0242 | 0.0242 | 0.1042 | 0.042 | 0.109 | 0.051 | 0.00 | 252.4 | 8.0 |
| vector_only | fetch_topk | narrow_25pct | 25.00 | 0.0187 | 0.0375 | 0.0375 | 0.2500 | 0.060 | 0.522 | 0.181 | 3.96 | 0.0 | 17.7 |
| vector_only | fetch_topk | none_100pct | 100.00 | 0.0886 | 0.1771 | 0.1771 | 1.0000 | 0.325 | 0.498 | 0.336 | 10.00 | 0.0 | 10.0 |
| vector_only | fetch_topk | rare_06pct | 6.25 | 0.0131 | 0.0263 | 0.0263 | 0.1250 | 0.326 | 0.362 | 0.329 | 10.00 | 0.0 | 64.0 |
| vector_only | score_only | narrow_25pct | 25.00 | 0.0187 | 0.0375 | 0.0375 | 0.2500 | 0.054 | 0.064 | 0.054 | 0.00 | 0.0 | 17.7 |
| vector_only | score_only | none_100pct | 100.00 | 0.0886 | 0.1771 | 0.1771 | 1.0000 | 0.028 | 0.037 | 0.029 | 0.00 | 0.0 | 10.0 |
| vector_only | score_only | rare_06pct | 6.25 | 0.0131 | 0.0263 | 0.0263 | 0.1250 | 0.030 | 0.037 | 0.031 | 0.00 | 0.0 | 64.0 |

## Measurement boundary

- Query timing: per-query wall time around SearchHybrid only; fixture build, ingest, index build, checkpoint, and warmup queries are excluded
- Embedding: embedding runs at fixture build; embed_seconds/embed_docs_per_sec are reported separately and excluded from ingest_docs_per_sec
- Ingest: ingest_docs_per_sec covers InsertBatch + Flush only

## Counter validations

54 checks: **PASS**

- ok score_only_zero_doc_fetch — text_only/score_only/none_100pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — text_only/score_only/none_100pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — text_only/score_only/none_100pct: fail_closed/search=0.0000 want 0
- ok score_only_zero_doc_fetch — text_only/score_only/rare_06pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — text_only/score_only/rare_06pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — text_only/score_only/rare_06pct: fail_closed/search=0.0000 want 0
- ok score_only_zero_doc_fetch — text_only/score_only/narrow_25pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — text_only/score_only/narrow_25pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — text_only/score_only/narrow_25pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — text_only/fetch_topk/none_100pct: documents_fetched/search=10.0000 want <=10
- ok zero_full_scan_fallbacks — text_only/fetch_topk/none_100pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — text_only/fetch_topk/none_100pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — text_only/fetch_topk/rare_06pct: documents_fetched/search=1.2500 want <=10
- ok zero_full_scan_fallbacks — text_only/fetch_topk/rare_06pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — text_only/fetch_topk/rare_06pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — text_only/fetch_topk/narrow_25pct: documents_fetched/search=2.5000 want <=10
- ok zero_full_scan_fallbacks — text_only/fetch_topk/narrow_25pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — text_only/fetch_topk/narrow_25pct: fail_closed/search=0.0000 want 0
- ok score_only_zero_doc_fetch — vector_only/score_only/none_100pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — vector_only/score_only/none_100pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — vector_only/score_only/none_100pct: fail_closed/search=0.0000 want 0
- ok score_only_zero_doc_fetch — vector_only/score_only/rare_06pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — vector_only/score_only/rare_06pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — vector_only/score_only/rare_06pct: fail_closed/search=0.0000 want 0
- ok score_only_zero_doc_fetch — vector_only/score_only/narrow_25pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — vector_only/score_only/narrow_25pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — vector_only/score_only/narrow_25pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — vector_only/fetch_topk/none_100pct: documents_fetched/search=10.0000 want <=10
- ok zero_full_scan_fallbacks — vector_only/fetch_topk/none_100pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — vector_only/fetch_topk/none_100pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — vector_only/fetch_topk/rare_06pct: documents_fetched/search=10.0000 want <=10
- ok zero_full_scan_fallbacks — vector_only/fetch_topk/rare_06pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — vector_only/fetch_topk/rare_06pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — vector_only/fetch_topk/narrow_25pct: documents_fetched/search=3.9583 want <=10
- ok zero_full_scan_fallbacks — vector_only/fetch_topk/narrow_25pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — vector_only/fetch_topk/narrow_25pct: fail_closed/search=0.0000 want 0
- ok score_only_zero_doc_fetch — hybrid/score_only/none_100pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — hybrid/score_only/none_100pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — hybrid/score_only/none_100pct: fail_closed/search=0.0000 want 0
- ok score_only_zero_doc_fetch — hybrid/score_only/rare_06pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — hybrid/score_only/rare_06pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — hybrid/score_only/rare_06pct: fail_closed/search=0.0000 want 0
- ok score_only_zero_doc_fetch — hybrid/score_only/narrow_25pct: documents_fetched/search=0.0000 want 0
- ok zero_full_scan_fallbacks — hybrid/score_only/narrow_25pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — hybrid/score_only/narrow_25pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — hybrid/fetch_topk/none_100pct: documents_fetched/search=10.0000 want <=10
- ok zero_full_scan_fallbacks — hybrid/fetch_topk/none_100pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — hybrid/fetch_topk/none_100pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — hybrid/fetch_topk/rare_06pct: documents_fetched/search=10.0000 want <=10
- ok zero_full_scan_fallbacks — hybrid/fetch_topk/rare_06pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — hybrid/fetch_topk/rare_06pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — hybrid/fetch_topk/narrow_25pct: documents_fetched/search=3.9583 want <=10
- ok zero_full_scan_fallbacks — hybrid/fetch_topk/narrow_25pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — hybrid/fetch_topk/narrow_25pct: fail_closed/search=0.0000 want 0
