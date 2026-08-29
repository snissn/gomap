# Same-corpus lexical comparison

Schema: `treedb_lexical_comparison/v1`. Manifest: `a2a69522bb3b0533bcc9030bc48286e4e727262a9a361605fd53c41ca86cf8d1`. Corpus: `10000` documents, `fe5b9d106a099d5c29d3327a8424b7f6d57f248b8802225b2d7bb3719062a227`.

Source: `18770a0f0908fcb8405238de7285695d7d156c3c` / tree `805d04696efc4792c2503cc7f6a1441dcefdfdc2`; retained qualification eligible: **TRUE**.

Completed engines: `lucene, sqlite_fts5, treedb_text_v2`; partial engines: `bleve`.

Only exact, validator-accepted rows enter the headline table. Times are warm single-query latency on one host; they are not timing assertions.

## Headline query latency

| engine | case | p50 | p95 | p99 | result digest |
| --- | --- | ---: | ---: | ---: | --- |
| Bleve | common | 0.357 ms | 1.220 ms | 33.702 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Bleve | rare | 0.004 ms | 0.014 ms | 0.020 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Bleve | and | 0.008 ms | 0.029 ms | 0.058 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Bleve | or | 0.010 ms | 0.024 ms | 0.038 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Bleve | phrase | 0.036 ms | 0.046 ms | 0.087 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| Apache Lucene | common | 0.824 ms | 1.777 ms | 5.807 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Apache Lucene | rare | 0.390 ms | 0.449 ms | 0.586 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Apache Lucene | and | 0.417 ms | 0.981 ms | 1.384 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Apache Lucene | or | 0.416 ms | 0.613 ms | 1.317 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Apache Lucene | phrase | 0.303 ms | 0.537 ms | 1.418 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| Apache Lucene | scalar_filtered | 0.185 ms | 0.221 ms | 1.045 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| SQLite FTS5 | common | 4.321 ms | 4.548 ms | 4.883 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| SQLite FTS5 | rare | 0.020 ms | 0.021 ms | 0.022 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| SQLite FTS5 | and | 0.024 ms | 0.029 ms | 0.036 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| SQLite FTS5 | or | 0.039 ms | 0.040 ms | 0.042 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| SQLite FTS5 | phrase | 0.030 ms | 0.031 ms | 0.031 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| SQLite FTS5 | scalar_filtered | 0.025 ms | 0.026 ms | 0.026 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| TreeDB text-v2 | common | 0.252 ms | 0.341 ms | 0.505 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| TreeDB text-v2 | rare | 0.015 ms | 0.030 ms | 0.042 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| TreeDB text-v2 | and | 0.018 ms | 0.033 ms | 0.034 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| TreeDB text-v2 | or | 0.021 ms | 0.040 ms | 0.086 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| TreeDB text-v2 | phrase | 0.053 ms | 0.069 ms | 0.106 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| TreeDB text-v2 | scalar_filtered | 0.110 ms | 0.285 ms | 0.354 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |

## Build resources and checkpointed storage

| engine | build repetitions (s) | docs/s | CPU per repetition | peak RSS per repetition | durable bytes per repetition | WAL bytes per repetition | transient bytes per repetition |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Bleve | 0.602, 0.565, 0.606 | 16617.2, 17695.5, 16496.4 | 0.559 s, 0.526 s, 0.552 s | 323.688 MiB, 342.031 MiB, 344.422 MiB | 9963097, 9962949, 9963139 | 0, 0, 0 | 0, 0, 0 |
| Apache Lucene | 0.353, 0.309, 0.298 | 28360.3, 32406.8, 33505.5 | 0.714 s, 0.682 s, 0.675 s | unsupported: Standard Java process APIs do not expose process-lifetime peak RSS, unsupported: Standard Java process APIs do not expose process-lifetime peak RSS, unsupported: Standard Java process APIs do not expose process-lifetime peak RSS | 310455, 310455, 310455 | 0, 0, 0 | 0, 0, 0 |
| SQLite FTS5 | 0.061, 0.056, 0.058 | 164213.7, 179234.0, 171506.6 | 0.054 s, 0.053 s, 0.054 s | 48.203 MiB, 48.250 MiB, 48.359 MiB | 9252864, 9252864, 9252864 | 0, 0, 0 | 0, 0, 0 |
| TreeDB text-v2 | 0.202, 0.143, 0.143 | 49429.1, 70168.4, 70170.3 | 0.120 s, 0.114 s, 0.113 s | 83.406 MiB, 79.203 MiB, 74.625 MiB | 6553974, 6553974, 6553974 | 0, 0, 0 | 0, 0, 0 |

## Equivalence and availability ledger

| engine | case | status | detail |
| --- | --- | --- | --- |
| Bleve | common | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | rare | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | and | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | or | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | phrase | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | scalar_filtered | unsupported | Bleve v2.4.4 exposes the tenant predicate only as a scoring Boolean clause; exact non-scoring scalar-filter semantics unavailable |
| Apache Lucene | common | equivalent | reference IDs and order match; reopen and intended route proven |
| Apache Lucene | rare | equivalent | reference IDs and order match; reopen and intended route proven |
| Apache Lucene | and | equivalent | reference IDs and order match; reopen and intended route proven |
| Apache Lucene | or | equivalent | reference IDs and order match; reopen and intended route proven |
| Apache Lucene | phrase | equivalent | reference IDs and order match; reopen and intended route proven |
| Apache Lucene | scalar_filtered | equivalent | reference IDs and order match; reopen and intended route proven |
| SQLite FTS5 | common | equivalent | reference IDs and order match; reopen and intended route proven |
| SQLite FTS5 | rare | equivalent | reference IDs and order match; reopen and intended route proven |
| SQLite FTS5 | and | equivalent | reference IDs and order match; reopen and intended route proven |
| SQLite FTS5 | or | equivalent | reference IDs and order match; reopen and intended route proven |
| SQLite FTS5 | phrase | equivalent | reference IDs and order match; reopen and intended route proven |
| SQLite FTS5 | scalar_filtered | equivalent | reference IDs and order match; reopen and intended route proven |
| TreeDB text-v2 | common | equivalent | reference IDs and order match; reopen and intended route proven |
| TreeDB text-v2 | rare | equivalent | reference IDs and order match; reopen and intended route proven |
| TreeDB text-v2 | and | equivalent | reference IDs and order match; reopen and intended route proven |
| TreeDB text-v2 | or | equivalent | reference IDs and order match; reopen and intended route proven |
| TreeDB text-v2 | phrase | equivalent | reference IDs and order match; reopen and intended route proven |
| TreeDB text-v2 | scalar_filtered | equivalent | reference IDs and order match; reopen and intended route proven |

## Exact commands, versions, and configuration

### Bleve

- Versions: `{"bleve": "v2.4.4", "go": "go1.26.0", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "standard", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "index_type": "scorch", "phrase_field_weights": {"body": 1, "title": 3}, "phrase_fields": ["title", "body"], "store_fields": false, "tenant_analyzer": "keyword", "term_vectors": true, "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase scoring only", "working_directory": "$RUN/dependency_work/bleve_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r1.json --repetition 1 --index $RUN/indexes/bleve-r1`
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r2.json --repetition 2 --index $RUN/indexes/bleve-r2`
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r3.json --repetition 3 --index $RUN/indexes/bleve-r3`

### Apache Lucene

- Versions: `{"java": "26.0.2.1", "lucene": "9.12.1", "platform": "Mac OS X/aarch64", "vm": "OpenJDK 64-Bit Server VM"}`
- Configuration: `{"analyzer": "StandardAnalyzer", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "compound_file": false, "phrase_field_weights": {"body": 1, "title": 3}, "phrase_fields": ["title", "body"], "similarity": "BM25(k1=1.2,b=0.75)", "stored_fields": ["id"], "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase scoring only", "working_directory": "$RUN/dependency_work/lucene_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "jvm_max_heap_bytes": 4294901760, "matches_runner_detected": true}}`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r1.json --repetition 1 --index $RUN/indexes/lucene-r1`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r2.json --repetition 2 --index $RUN/indexes/lucene-r2`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r3.json --repetition 3 --index $RUN/indexes/lucene-r3`

### SQLite FTS5

- Versions: `{"platform": "Darwin-25.2.0-arm64-arm-64bit-Mach-O", "python": "3.14.6 (main, Jun 10 2026, 10:03:53) [Clang 21.0.0 (clang-2100.0.123.102)]", "sqlite": "3.53.2"}`
- Configuration: `{"build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "journal_mode": "WAL", "phrase_field_weights": {"body": 1, "title": 3}, "phrase_fields": ["title", "body"], "sqlite_auxiliary_threads": 0, "synchronous": "FULL", "tie_break": "score,id", "tokenizer": "unicode61 remove_diacritics 2", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase scoring only", "working_directory": "$REPO"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r1.json --repetition 1 --db $RUN/indexes/sqlite_fts5-r1.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r2.json --repetition 2 --db $RUN/indexes/sqlite_fts5-r2.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r3.json --repetition 3 --db $RUN/indexes/sqlite_fts5-r3.sqlite3`

### TreeDB text-v2

- Versions: `{"go": "go1.26.0", "module": "18770a0f0908fcb8405238de7285695d7d156c3c", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "simple", "bm25f": {"b": 0.75, "k1": 1.2}, "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "candidate_limit": 10000, "index_version": "v2", "max_postings_scanned": 80000, "result_mode": "score_only", "route_proof": "one untimed explained query per case", "store_positions": true, "timed_explain": false, "top_k": 10, "weights": {"body": 1, "title": 3}, "working_directory": "$REPO"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r1.json --repetition 1 --db $RUN/indexes/treedb_text_v2-r1`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r2.json --repetition 2 --db $RUN/indexes/treedb_text_v2-r2`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r3.json --repetition 3 --db $RUN/indexes/treedb_text_v2-r3`
