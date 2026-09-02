# TreeDB RAG Retrieval Benchmark (treedb_rag_benchmark/v1)

Generated at 2026-08-22T12:41:07Z.

Issue: #4267. Host: Michaels-Laptop.local (darwin/arm64, 8 CPUs, go1.26.0).

## Fixture

- corpus `treedb-rag-corpus/v1` fingerprint `3435163361e2e52e…`
- docs=4096 chunks=8192 (2/doc) dims=64 queries=24 top_k=10 candidate_limit=64 reps=3 vector_m=8 ef_search=128 vector_mode=exact (column_graph exact score plane)

## Ingest / storage

| embed s | ingest docs/s | index build s | storage B | B/chunk |
|---:|---:|---:|---:|---:|
| 0.040 | 12557 | 0.591 | 32703901 | 3992 |

## Rows

| route | mode | filter | sel% | recall@5 | recall@10 | recall@100 | mrr@10 | p50 ms | p99 ms | mean ms | docs_fetched | postings_scanned | candidates_fused |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| hybrid | fetch_topk | narrow_25pct | 25.00 | 0.0024 | 0.0047 | 0.0047 | 0.2500 | 0.875 | 2.287 | 1.156 | 2.54 | 2019.5 | 32.0 |
| hybrid | fetch_topk | none_100pct | 100.00 | 0.0111 | 0.0222 | 0.0222 | 1.0000 | 1.004 | 1.624 | 1.030 | 10.00 | 2019.5 | 128.0 |
| hybrid | fetch_topk | rare_06pct | 6.25 | 0.0016 | 0.0031 | 0.0031 | 0.1250 | 0.317 | 1.264 | 0.436 | 1.25 | 2019.5 | 12.4 |
| hybrid | score_only | narrow_25pct | 25.00 | 0.0024 | 0.0047 | 0.0047 | 0.2500 | 0.867 | 1.872 | 1.068 | 0.00 | 2019.5 | 32.0 |
| hybrid | score_only | none_100pct | 100.00 | 0.0111 | 0.0222 | 0.0222 | 1.0000 | 0.671 | 1.333 | 0.688 | 0.00 | 2019.5 | 128.0 |
| hybrid | score_only | rare_06pct | 6.25 | 0.0016 | 0.0031 | 0.0031 | 0.1250 | 0.325 | 1.067 | 0.393 | 0.00 | 2019.5 | 12.4 |
| text_only | fetch_topk | narrow_25pct | 25.00 | 0.0024 | 0.0047 | 0.0047 | 0.2500 | 0.822 | 2.195 | 1.089 | 2.50 | 2019.5 | 16.0 |
| text_only | fetch_topk | none_100pct | 100.00 | 0.0110 | 0.0220 | 0.0220 | 0.9792 | 0.871 | 1.375 | 0.877 | 10.00 | 2019.5 | 10.0 |
| text_only | fetch_topk | rare_06pct | 6.25 | 0.0015 | 0.0030 | 0.0030 | 0.1042 | 0.272 | 1.323 | 0.379 | 1.25 | 2019.5 | 8.0 |
| text_only | score_only | narrow_25pct | 25.00 | 0.0024 | 0.0047 | 0.0047 | 0.2500 | 0.823 | 1.728 | 1.005 | 0.00 | 2019.5 | 16.0 |
| text_only | score_only | none_100pct | 100.00 | 0.0110 | 0.0220 | 0.0220 | 0.9792 | 0.577 | 1.224 | 0.582 | 0.00 | 2019.5 | 10.0 |
| text_only | score_only | rare_06pct | 6.25 | 0.0015 | 0.0030 | 0.0030 | 0.1042 | 0.271 | 0.773 | 0.327 | 0.00 | 2019.5 | 8.0 |
| vector_only | fetch_topk | narrow_25pct | 25.00 | 0.0024 | 0.0047 | 0.0047 | 0.2500 | 0.196 | 0.694 | 0.315 | 2.54 | 0.0 | 16.0 |
| vector_only | fetch_topk | none_100pct | 100.00 | 0.0111 | 0.0222 | 0.0222 | 1.0000 | 0.446 | 0.574 | 0.454 | 10.00 | 0.0 | 10.0 |
| vector_only | fetch_topk | rare_06pct | 6.25 | 0.0016 | 0.0032 | 0.0032 | 0.1250 | 0.078 | 0.873 | 0.139 | 1.25 | 0.0 | 4.4 |
| vector_only | score_only | narrow_25pct | 25.00 | 0.0024 | 0.0047 | 0.0047 | 0.2500 | 0.186 | 0.395 | 0.193 | 0.00 | 0.0 | 16.0 |
| vector_only | score_only | none_100pct | 100.00 | 0.0111 | 0.0222 | 0.0222 | 1.0000 | 0.028 | 0.157 | 0.037 | 0.00 | 0.0 | 10.0 |
| vector_only | score_only | rare_06pct | 6.25 | 0.0016 | 0.0032 | 0.0032 | 0.1250 | 0.070 | 0.083 | 0.071 | 0.00 | 0.0 | 4.4 |

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
- ok fetch_topk_bounded_by_topk — vector_only/fetch_topk/rare_06pct: documents_fetched/search=1.2500 want <=10
- ok zero_full_scan_fallbacks — vector_only/fetch_topk/rare_06pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — vector_only/fetch_topk/rare_06pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — vector_only/fetch_topk/narrow_25pct: documents_fetched/search=2.5417 want <=10
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
- ok fetch_topk_bounded_by_topk — hybrid/fetch_topk/rare_06pct: documents_fetched/search=1.2500 want <=10
- ok zero_full_scan_fallbacks — hybrid/fetch_topk/rare_06pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — hybrid/fetch_topk/rare_06pct: fail_closed/search=0.0000 want 0
- ok fetch_topk_bounded_by_topk — hybrid/fetch_topk/narrow_25pct: documents_fetched/search=2.5417 want <=10
- ok zero_full_scan_fallbacks — hybrid/fetch_topk/narrow_25pct: full_document_scan_fallbacks/search=0.0000 want 0
- ok zero_fail_closed — hybrid/fetch_topk/narrow_25pct: fail_closed/search=0.0000 want 0
