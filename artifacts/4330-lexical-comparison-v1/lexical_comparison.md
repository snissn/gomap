# Same-corpus lexical comparison

Schema: `treedb_lexical_comparison/v1`. Manifest: `a268cb2e59f4a63d8bcc7a0047fe85fe2195bef62a536f9b54bcae6f4b568d8f`. Corpus: `10000` documents, `fe5b9d106a099d5c29d3327a8424b7f6d57f248b8802225b2d7bb3719062a227`.

Source: `d036bd777300521243cdf4aab1ce8993739d3359` / tree `8dfec3426e736f7fe6429a5b31415793e864a7bc`; retained qualification eligible: **TRUE**.

Completed engines: `lucene, treedb_text_v2`; partial engines: `bleve, sqlite_fts5`.

Only pinned-scoring, validator-accepted rows enter the headline table. Native-scoring rows with different formulas are retained separately as directional context. Times are warm single-query latency on one host; they are not timing assertions.

## Headline query latency

| engine | case | p50 | p95 | p99 | result digest |
| --- | --- | ---: | ---: | ---: | --- |
| Apache Lucene | common | 0.416 ms | 1.654 ms | 2.031 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Apache Lucene | rare | 0.215 ms | 0.282 ms | 0.912 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Apache Lucene | and | 0.272 ms | 1.159 ms | 3.387 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Apache Lucene | or | 0.251 ms | 1.026 ms | 2.565 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Apache Lucene | phrase | 0.369 ms | 0.869 ms | 3.714 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| Apache Lucene | scalar_filtered | 0.196 ms | 0.220 ms | 0.833 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| TreeDB text-v2 | common | 0.245 ms | 0.465 ms | 0.781 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| TreeDB text-v2 | rare | 0.018 ms | 0.028 ms | 0.836 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| TreeDB text-v2 | and | 0.016 ms | 0.033 ms | 0.040 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| TreeDB text-v2 | or | 0.018 ms | 0.020 ms | 0.023 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| TreeDB text-v2 | phrase | 0.037 ms | 0.062 ms | 0.074 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| TreeDB text-v2 | scalar_filtered | 0.038 ms | 0.292 ms | 0.316 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |

## Directional native-scoring latency (not semantically equivalent)

| engine | case | p50 | p95 | p99 | disposition |
| --- | --- | ---: | ---: | ---: | --- |
| Bleve | common | 0.369 ms | 0.427 ms | 0.450 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | rare | 0.003 ms | 0.024 ms | 0.035 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | and | 0.006 ms | 0.008 ms | 0.053 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | or | 0.008 ms | 0.011 ms | 0.018 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | phrase | 0.022 ms | 0.044 ms | 0.241 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| SQLite FTS5 | common | 1.978 ms | 1.999 ms | 2.058 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | rare | 0.018 ms | 0.019 ms | 0.020 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | and | 0.021 ms | 0.022 ms | 0.024 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | or | 0.031 ms | 0.031 ms | 0.032 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | phrase | 0.028 ms | 0.030 ms | 0.030 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | scalar_filtered | 0.021 ms | 0.021 ms | 0.021 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |

## Build resources and checkpointed storage

| engine | build repetitions (s) | docs/s | CPU per repetition | peak RSS per repetition | durable bytes per repetition | WAL bytes per repetition | transient bytes per repetition |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Bleve | 0.680, 0.645, 0.666 | 14709.4, 15514.0, 15017.5 | 0.625 s, 0.585 s, 0.597 s | 329.188 MiB, 323.734 MiB, 334.562 MiB | 10573205, 10573350, 10573573 | 0, 0, 0 | 0, 0, 0 |
| Apache Lucene | 1.224, 1.241, 1.228 | 8169.8, 8060.7, 8141.2 | 1.160 s, 1.222 s, 1.211 s | unsupported: Standard Java process APIs do not expose process-lifetime peak RSS, unsupported: Standard Java process APIs do not expose process-lifetime peak RSS, unsupported: Standard Java process APIs do not expose process-lifetime peak RSS | 335311, 335311, 335311 | 0, 0, 0 | 0, 0, 0 |
| SQLite FTS5 | 0.059, 0.071, 0.071 | 168814.2, 141614.6, 141406.2 | 0.046 s, 0.048 s, 0.049 s | 50.906 MiB, 50.938 MiB, 50.969 MiB | 3915776, 3915776, 3915776 | 0, 0, 0 | 0, 0, 0 |
| TreeDB text-v2 | 0.201, 0.202, 0.178 | 49657.7, 49456.8, 56108.5 | 0.134 s, 0.120 s, 0.135 s | 84.422 MiB, 87.969 MiB, 86.797 MiB | 6553974, 6553974, 6553974 | 0, 0, 0 | 0, 0, 0 |

## Equivalence and availability ledger

| engine | case | status | detail |
| --- | --- | --- | --- |
| Bleve | common | directional | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | rare | directional | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | and | directional | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | or | directional | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | phrase | directional | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | scalar_filtered | unsupported | Bleve v2.4.4 exposes the tenant predicate only as a scoring Boolean clause; exact non-scoring scalar-filter semantics unavailable |
| Apache Lucene | common | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| Apache Lucene | rare | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| Apache Lucene | and | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| Apache Lucene | or | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| Apache Lucene | phrase | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| Apache Lucene | scalar_filtered | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| SQLite FTS5 | common | directional | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | rare | directional | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | and | directional | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | or | directional | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | phrase | directional | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | scalar_filtered | directional | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| TreeDB text-v2 | common | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| TreeDB text-v2 | rare | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| TreeDB text-v2 | and | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| TreeDB text-v2 | or | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| TreeDB text-v2 | phrase | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |
| TreeDB text-v2 | scalar_filtered | equivalent | pinned scoring semantics, reference IDs/order, reopen, and intended route proven |

## Exact commands, versions, and configuration

### Bleve

- Versions: `{"bleve": "v2.4.4", "go": "go1.26.0", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "standard", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "index_type": "scorch", "phrase_fields": ["title", "body"], "phrase_scoring": "native TF-IDF title boost 3, body boost 1", "scoring_contract": "native_directional", "stored_source_fields": ["id", "title", "body", "tenant"], "tenant_analyzer": "keyword", "term_vectors": true, "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase native scoring", "working_directory": "$RUN/dependency_work/bleve_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `$HOME/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r1.json --repetition 1 --index $RUN/indexes/bleve-r1`
- Command: `$HOME/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r2.json --repetition 2 --index $RUN/indexes/bleve-r2`
- Command: `$HOME/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r3.json --repetition 3 --index $RUN/indexes/bleve-r3`

### Apache Lucene

- Versions: `{"java": "26.0.2.1", "lucene": "9.12.1", "platform": "Mac OS X/aarch64", "vm": "OpenJDK 64-Bit Server VM"}`
- Configuration: `{"analyzer": "StandardAnalyzer", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "compound_file": false, "jvm_execution": "-XX:ActiveProcessorCount=1 -XX:+UseSerialGC -XX:-TieredCompilation -XX:CICompilerCount=1 -Xbatch", "merge_scheduler": "SerialMergeScheduler", "phrase_filter_fields": ["title", "body"], "phrase_scoring": "weighted terms scored; title/body phrase OR applied as FILTER", "scoring_contract": "pinned_bm25f", "similarity": "BM25(k1=1.2,b=0.75)", "stored_source_fields": ["id", "title", "body", "tenant"], "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for term scoring, including phrase-qualified rows", "working_directory": "$RUN/dependency_work/lucene_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "jvm_max_heap_bytes": 4294901760, "matches_runner_detected": true}}`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r1.json --repetition 1 --index $RUN/indexes/lucene-r1`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r2.json --repetition 2 --index $RUN/indexes/lucene-r2`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r3.json --repetition 3 --index $RUN/indexes/lucene-r3`

### SQLite FTS5

- Versions: `{"platform": "Darwin-25.2.0-arm64-arm-64bit-Mach-O", "python": "3.14.6 (main, Jun 10 2026, 10:03:53) [Clang 21.0.0 (clang-2100.0.123.102)]", "sqlite": "3.53.2"}`
- Configuration: `{"build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "fts_content_mode": "contentless", "generated_weighted_field_storage": "FTS index only", "journal_mode": "WAL", "phrase_fields": ["title", "body"], "phrase_scoring": "native bm25 title weight 3, body weight 1", "scoring_contract": "native_directional", "source_table": "docs", "sqlite_auxiliary_threads": 0, "stored_source_fields": ["id", "title", "body", "tenant"], "synchronous": "FULL", "tie_break": "score,id", "tokenizer": "unicode61 remove_diacritics 2", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase native scoring", "working_directory": "$REPO"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 -E -s -B $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r1.json --repetition 1 --db $RUN/indexes/sqlite_fts5-r1.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 -E -s -B $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r2.json --repetition 2 --db $RUN/indexes/sqlite_fts5-r2.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 -E -s -B $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r3.json --repetition 3 --db $RUN/indexes/sqlite_fts5-r3.sqlite3`

### TreeDB text-v2

- Versions: `{"go": "go1.26.0", "module": "d036bd777300521243cdf4aab1ce8993739d3359", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "simple", "bm25f": {"b": 0.75, "k1": 1.2}, "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "candidate_limit": 10000, "index_version": "v2", "max_postings_scanned": 80000, "result_mode": "score_only", "route_proof": "one untimed explained query per case", "scoring_contract": "pinned_bm25f", "store_positions": true, "stored_source_fields": ["id", "title", "body", "tenant"], "timed_explain": false, "top_k": 10, "weights": {"body": 1, "title": 3}, "working_directory": "$REPO"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `$HOME/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r1.json --repetition 1 --db $RUN/indexes/treedb_text_v2-r1`
- Command: `$HOME/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r2.json --repetition 2 --db $RUN/indexes/treedb_text_v2-r2`
- Command: `$HOME/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r3.json --repetition 3 --db $RUN/indexes/treedb_text_v2-r3`
