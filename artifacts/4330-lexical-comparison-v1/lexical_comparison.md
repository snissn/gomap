# Same-corpus lexical comparison

Schema: `treedb_lexical_comparison/v1`. Manifest: `a268cb2e59f4a63d8bcc7a0047fe85fe2195bef62a536f9b54bcae6f4b568d8f`. Corpus: `10000` documents, `fe5b9d106a099d5c29d3327a8424b7f6d57f248b8802225b2d7bb3719062a227`.

Source: `deafa8e97c16d87e7fd92b1bd562d7fd234bec3a` / tree `efe6a357adab6b3feb0e45863b5f6610870b671e`; retained qualification eligible: **TRUE**.

Completed engines: `lucene, treedb_text_v2`; partial engines: `bleve, sqlite_fts5`.

Only pinned-scoring, validator-accepted rows enter the headline table. Native-scoring rows with different formulas are retained separately as directional context. Times are warm single-query latency on one host; they are not timing assertions.

## Headline query latency

| engine | case | p50 | p95 | p99 | result digest |
| --- | --- | ---: | ---: | ---: | --- |
| Apache Lucene | common | 0.415 ms | 1.847 ms | 2.084 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Apache Lucene | rare | 0.216 ms | 0.314 ms | 0.871 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Apache Lucene | and | 0.274 ms | 1.179 ms | 3.589 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Apache Lucene | or | 0.257 ms | 1.035 ms | 2.769 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Apache Lucene | phrase | 0.363 ms | 0.886 ms | 3.945 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| Apache Lucene | scalar_filtered | 0.196 ms | 0.224 ms | 0.982 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| TreeDB text-v2 | common | 0.253 ms | 0.538 ms | 1.449 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| TreeDB text-v2 | rare | 0.013 ms | 0.034 ms | 0.080 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| TreeDB text-v2 | and | 0.015 ms | 0.036 ms | 0.060 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| TreeDB text-v2 | or | 0.022 ms | 0.042 ms | 0.077 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| TreeDB text-v2 | phrase | 0.037 ms | 0.056 ms | 0.059 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| TreeDB text-v2 | scalar_filtered | 0.042 ms | 0.282 ms | 0.653 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |

## Directional native-scoring latency (not semantically equivalent)

| engine | case | p50 | p95 | p99 | disposition |
| --- | --- | ---: | ---: | ---: | --- |
| Bleve | common | 0.355 ms | 0.397 ms | 0.407 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | rare | 0.003 ms | 0.022 ms | 0.038 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | and | 0.006 ms | 0.010 ms | 0.024 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | or | 0.008 ms | 0.010 ms | 0.028 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| Bleve | phrase | 0.021 ms | 0.027 ms | 0.034 ms | Bleve v2.4.4 native TF-IDF scorer does not implement the pinned BM25F formula |
| SQLite FTS5 | common | 4.315 ms | 6.097 ms | 7.491 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | rare | 0.020 ms | 0.023 ms | 0.025 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | and | 0.024 ms | 0.024 ms | 0.024 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | or | 0.040 ms | 0.041 ms | 0.049 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | phrase | 0.030 ms | 0.031 ms | 0.033 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |
| SQLite FTS5 | scalar_filtered | 0.025 ms | 0.026 ms | 0.028 ms | SQLite FTS5 native bm25() IDF and floor do not implement the pinned BM25F formula |

## Build resources and checkpointed storage

| engine | build repetitions (s) | docs/s | CPU per repetition | peak RSS per repetition | durable bytes per repetition | WAL bytes per repetition | transient bytes per repetition |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Bleve | 0.611, 0.611, 0.615 | 16378.2, 16377.9, 16263.1 | 0.563 s, 0.561 s, 0.560 s | 334.297 MiB, 337.844 MiB, 336.031 MiB | 10573591, 10573596, 10573800 | 0, 0, 0 | 0, 0, 0 |
| Apache Lucene | 1.243, 1.251, 1.276 | 8041.9, 7994.5, 7837.4 | 1.174 s, 1.230 s, 1.232 s | unsupported: Standard Java process APIs do not expose process-lifetime peak RSS, unsupported: Standard Java process APIs do not expose process-lifetime peak RSS, unsupported: Standard Java process APIs do not expose process-lifetime peak RSS | 335311, 335311, 335311 | 0, 0, 0 | 0, 0, 0 |
| SQLite FTS5 | 0.068, 0.072, 0.072 | 147162.3, 139685.2, 139135.7 | 0.055 s, 0.059 s, 0.058 s | 50.875 MiB, 49.828 MiB, 51.141 MiB | 9252864, 9252864, 9252864 | 0, 0, 0 | 0, 0, 0 |
| TreeDB text-v2 | 0.174, 0.156, 0.225 | 57548.3, 64235.6, 44488.7 | 0.116 s, 0.111 s, 0.118 s | 81.469 MiB, 81.047 MiB, 83.344 MiB | 6553974, 6553974, 6553974 | 0, 0, 0 | 0, 0, 0 |

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
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r1.json --repetition 1 --index $RUN/indexes/bleve-r1`
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r2.json --repetition 2 --index $RUN/indexes/bleve-r2`
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r3.json --repetition 3 --index $RUN/indexes/bleve-r3`

### Apache Lucene

- Versions: `{"java": "26.0.2.1", "lucene": "9.12.1", "platform": "Mac OS X/aarch64", "vm": "OpenJDK 64-Bit Server VM"}`
- Configuration: `{"analyzer": "StandardAnalyzer", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "compound_file": false, "jvm_execution": "-XX:ActiveProcessorCount=1 -XX:+UseSerialGC -XX:-TieredCompilation -XX:CICompilerCount=1 -Xbatch", "merge_scheduler": "SerialMergeScheduler", "phrase_filter_fields": ["title", "body"], "phrase_scoring": "weighted terms scored; title/body phrase OR applied as FILTER", "scoring_contract": "pinned_bm25f", "similarity": "BM25(k1=1.2,b=0.75)", "stored_source_fields": ["id", "title", "body", "tenant"], "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for term scoring, including phrase-qualified rows", "working_directory": "$RUN/dependency_work/lucene_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "jvm_max_heap_bytes": 4294901760, "matches_runner_detected": true}}`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r1.json --repetition 1 --index $RUN/indexes/lucene-r1`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r2.json --repetition 2 --index $RUN/indexes/lucene-r2`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r3.json --repetition 3 --index $RUN/indexes/lucene-r3`

### SQLite FTS5

- Versions: `{"platform": "Darwin-25.2.0-arm64-arm-64bit-Mach-O", "python": "3.14.6 (main, Jun 10 2026, 10:03:53) [Clang 21.0.0 (clang-2100.0.123.102)]", "sqlite": "3.53.2"}`
- Configuration: `{"build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "journal_mode": "WAL", "phrase_fields": ["title", "body"], "phrase_scoring": "native bm25 title weight 3, body weight 1", "scoring_contract": "native_directional", "sqlite_auxiliary_threads": 0, "stored_source_fields": ["id", "title", "body", "tenant"], "synchronous": "FULL", "tie_break": "score,id", "tokenizer": "unicode61 remove_diacritics 2", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase native scoring", "working_directory": "$REPO"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r1.json --repetition 1 --db $RUN/indexes/sqlite_fts5-r1.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r2.json --repetition 2 --db $RUN/indexes/sqlite_fts5-r2.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r3.json --repetition 3 --db $RUN/indexes/sqlite_fts5-r3.sqlite3`

### TreeDB text-v2

- Versions: `{"go": "go1.26.0", "module": "deafa8e97c16d87e7fd92b1bd562d7fd234bec3a", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "simple", "bm25f": {"b": 0.75, "k1": 1.2}, "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "candidate_limit": 10000, "index_version": "v2", "max_postings_scanned": 80000, "result_mode": "score_only", "route_proof": "one untimed explained query per case", "scoring_contract": "pinned_bm25f", "store_positions": true, "stored_source_fields": ["id", "title", "body", "tenant"], "timed_explain": false, "top_k": 10, "weights": {"body": 1, "title": 3}, "working_directory": "$REPO"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r1.json --repetition 1 --db $RUN/indexes/treedb_text_v2-r1`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r2.json --repetition 2 --db $RUN/indexes/treedb_text_v2-r2`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r3.json --repetition 3 --db $RUN/indexes/treedb_text_v2-r3`
