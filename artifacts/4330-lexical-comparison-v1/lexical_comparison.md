# Same-corpus lexical comparison

Schema: `treedb_lexical_comparison/v1`. Manifest: `4552fed21bd0afc563c3f3492773c08227b5efbc3adb4b50d55e5fb3aff990a9`. Corpus: `10000` documents, `fe5b9d106a099d5c29d3327a8424b7f6d57f248b8802225b2d7bb3719062a227`.

Source: `0bc76c31e42cac294f2686550104f73d07dd25a9` / tree `becbb79eb17a537c781d7150131908b05c7988b4`; retained qualification eligible: **TRUE**.

Completed engines: `lucene, sqlite_fts5, treedb_text_v2`; partial engines: `bleve`.

Only exact, validator-accepted rows enter the headline table. Times are warm single-query latency on one host; they are not timing assertions.

## Headline query latency

| engine | case | p50 | p95 | p99 | result digest |
| --- | --- | ---: | ---: | ---: | --- |
| Bleve | common | 0.356 ms | 0.385 ms | 0.411 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Bleve | rare | 0.003 ms | 0.017 ms | 0.039 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Bleve | and | 0.005 ms | 0.006 ms | 0.007 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Bleve | or | 0.008 ms | 0.010 ms | 0.011 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Bleve | phrase | 0.019 ms | 0.023 ms | 0.035 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| Apache Lucene | common | 0.425 ms | 2.134 ms | 2.789 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Apache Lucene | rare | 0.218 ms | 0.386 ms | 1.548 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Apache Lucene | and | 0.300 ms | 1.317 ms | 3.722 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Apache Lucene | or | 0.299 ms | 1.406 ms | 3.293 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Apache Lucene | phrase | 0.379 ms | 2.974 ms | 4.573 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| Apache Lucene | scalar_filtered | 0.186 ms | 0.517 ms | 1.212 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| SQLite FTS5 | common | 4.335 ms | 7.006 ms | 9.491 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| SQLite FTS5 | rare | 0.020 ms | 0.021 ms | 0.021 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| SQLite FTS5 | and | 0.024 ms | 0.024 ms | 0.024 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| SQLite FTS5 | or | 0.040 ms | 0.042 ms | 0.046 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| SQLite FTS5 | phrase | 0.031 ms | 0.032 ms | 0.033 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| SQLite FTS5 | scalar_filtered | 0.025 ms | 0.025 ms | 0.026 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| TreeDB text-v2 | common | 0.242 ms | 0.478 ms | 0.590 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| TreeDB text-v2 | rare | 0.020 ms | 0.029 ms | 0.046 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| TreeDB text-v2 | and | 0.016 ms | 0.034 ms | 0.037 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| TreeDB text-v2 | or | 0.018 ms | 0.020 ms | 0.028 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| TreeDB text-v2 | phrase | 0.037 ms | 0.040 ms | 0.057 ms | `8088cbb0b34ee919fde96c27663d90a54e722216b390e032842cdc7ae04d5696` |
| TreeDB text-v2 | scalar_filtered | 0.037 ms | 0.647 ms | 0.673 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |

## Build resources and checkpointed storage

| engine | build repetitions (s) | docs/s | CPU per repetition | peak RSS per repetition | durable bytes per repetition | WAL bytes per repetition | transient bytes per repetition |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Bleve | 0.667, 0.634, 0.682 | 14982.4, 15774.8, 14664.8 | 0.595 s, 0.559 s, 0.614 s | 348.844 MiB, 347.828 MiB, 342.547 MiB | 10573579, 10573543, 10573991 | 0, 0, 0 | 0, 0, 0 |
| Apache Lucene | 1.407, 1.321, 1.462 | 7107.0, 7571.6, 6838.6 | 1.215 s, 1.267 s, 1.352 s | unsupported: Standard Java process APIs do not expose process-lifetime peak RSS, unsupported: Standard Java process APIs do not expose process-lifetime peak RSS, unsupported: Standard Java process APIs do not expose process-lifetime peak RSS | 335311, 335311, 335311 | 0, 0, 0 | 0, 0, 0 |
| SQLite FTS5 | 0.128, 0.172, 0.203 | 78158.1, 58169.8, 49344.1 | 0.085 s, 0.130 s, 0.124 s | 50.453 MiB, 50.516 MiB, 50.609 MiB | 9252864, 9252864, 9252864 | 0, 0, 0 | 0, 0, 0 |
| TreeDB text-v2 | 0.199, 0.166, 0.183 | 50358.2, 60227.9, 54497.1 | 0.127 s, 0.118 s, 0.119 s | 81.203 MiB, 79.953 MiB, 79.578 MiB | 6553974, 6553974, 6553974 | 0, 0, 0 | 0, 0, 0 |

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
- Configuration: `{"analyzer": "standard", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "index_type": "scorch", "phrase_field_weights": {"body": 1, "title": 3}, "phrase_fields": ["title", "body"], "stored_source_fields": ["id", "title", "body", "tenant"], "tenant_analyzer": "keyword", "term_vectors": true, "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase scoring only", "working_directory": "$RUN/dependency_work/bleve_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r1.json --repetition 1 --index $RUN/indexes/bleve-r1`
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r2.json --repetition 2 --index $RUN/indexes/bleve-r2`
- Command: `go run . --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/bleve-r3.json --repetition 3 --index $RUN/indexes/bleve-r3`

### Apache Lucene

- Versions: `{"java": "26.0.2.1", "lucene": "9.12.1", "platform": "Mac OS X/aarch64", "vm": "OpenJDK 64-Bit Server VM"}`
- Configuration: `{"analyzer": "StandardAnalyzer", "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "compound_file": false, "jvm_execution": "-XX:ActiveProcessorCount=1 -XX:+UseSerialGC -XX:-TieredCompilation -XX:CICompilerCount=1 -Xbatch", "merge_scheduler": "SerialMergeScheduler", "phrase_field_weights": {"body": 1, "title": 3}, "phrase_fields": ["title", "body"], "similarity": "BM25(k1=1.2,b=0.75)", "stored_source_fields": ["id", "title", "body", "tenant"], "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase scoring only", "working_directory": "$RUN/dependency_work/lucene_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "jvm_max_heap_bytes": 4294901760, "matches_runner_detected": true}}`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r1.json --repetition 1 --index $RUN/indexes/lucene-r1`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r2.json --repetition 2 --index $RUN/indexes/lucene-r2`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/lucene-r3.json --repetition 3 --index $RUN/indexes/lucene-r3`

### SQLite FTS5

- Versions: `{"platform": "Darwin-25.2.0-arm64-arm-64bit-Mach-O", "python": "3.14.6 (main, Jun 10 2026, 10:03:53) [Clang 21.0.0 (clang-2100.0.123.102)]", "sqlite": "3.53.2"}`
- Configuration: `{"build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "journal_mode": "WAL", "phrase_field_weights": {"body": 1, "title": 3}, "phrase_fields": ["title", "body"], "sqlite_auxiliary_threads": 0, "stored_source_fields": ["id", "title", "body", "tenant"], "synchronous": "FULL", "tie_break": "score,id", "tokenizer": "unicode61 remove_diacritics 2", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body for non-phrase scoring only", "working_directory": "$REPO"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r1.json --repetition 1 --db $RUN/indexes/sqlite_fts5-r1.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r2.json --repetition 2 --db $RUN/indexes/sqlite_fts5-r2.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 $REPO/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/sqlite_fts5-r3.json --repetition 3 --db $RUN/indexes/sqlite_fts5-r3.sqlite3`

### TreeDB text-v2

- Versions: `{"go": "go1.26.0", "module": "0bc76c31e42cac294f2686550104f73d07dd25a9", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "simple", "bm25f": {"b": 0.75, "k1": 1.2}, "build_timing_boundary": "after frozen TSV parse; includes engine document materialization, index setup, checkpoint, and close", "candidate_limit": 10000, "index_version": "v2", "max_postings_scanned": 80000, "result_mode": "score_only", "route_proof": "one untimed explained query per case", "store_positions": true, "stored_source_fields": ["id", "title", "body", "tenant"], "timed_explain": false, "top_k": 10, "weights": {"body": 1, "title": 3}, "working_directory": "$REPO"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r1.json --repetition 1 --db $RUN/indexes/treedb_text_v2-r1`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r2.json --repetition 2 --db $RUN/indexes/treedb_text_v2-r2`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest $REPO/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus $RUN/corpus.tsv --out $RUN/raw/treedb_text_v2-r3.json --repetition 3 --db $RUN/indexes/treedb_text_v2-r3`
