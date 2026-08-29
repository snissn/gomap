# Same-corpus lexical comparison

Schema: `treedb_lexical_comparison/v1`. Manifest: `12bcce9db9564c4d6b31206ce00bf16289fb9fd5d6f479eec8510e7dc8203728`. Corpus: `10000` documents, `7e31d754a2c94b1fa6e4d1cf6d75d2f0df595fc8a9cdf80eb12a3d0b7650485e`.

Source: `3def6c723dc0c12a566a2518e7f706e1fbfc8687` / tree `bbc7880549e39daa2324a0e19670169cf23e905b`; retained qualification eligible: **TRUE**.

Only exact, validator-accepted rows enter the headline table. Times are warm single-query latency on one host; they are not timing assertions.

## Headline query latency

| engine | case | p50 | p95 | p99 | result digest |
| --- | --- | ---: | ---: | ---: | --- |
| Bleve | common | 0.342 ms | 0.357 ms | 0.363 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Bleve | rare | 0.003 ms | 0.007 ms | 0.019 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Bleve | and | 0.005 ms | 0.022 ms | 0.032 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Bleve | or | 0.008 ms | 0.021 ms | 0.032 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Bleve | phrase | 0.018 ms | 0.031 ms | 0.042 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| Bleve | scalar_filtered | 0.005 ms | 0.006 ms | 0.007 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| Apache Lucene | common | 0.844 ms | 1.929 ms | 2.636 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Apache Lucene | rare | 0.377 ms | 0.412 ms | 0.447 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Apache Lucene | and | 0.375 ms | 0.522 ms | 1.200 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Apache Lucene | or | 0.395 ms | 0.482 ms | 1.173 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Apache Lucene | phrase | 0.335 ms | 0.388 ms | 1.196 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| Apache Lucene | scalar_filtered | 0.200 ms | 0.372 ms | 0.379 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| SQLite FTS5 | common | 3.923 ms | 4.144 ms | 4.317 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| SQLite FTS5 | rare | 0.019 ms | 0.023 ms | 0.027 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| SQLite FTS5 | and | 0.022 ms | 0.025 ms | 0.027 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| SQLite FTS5 | or | 0.038 ms | 0.041 ms | 0.042 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| SQLite FTS5 | phrase | 0.022 ms | 0.024 ms | 0.026 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| SQLite FTS5 | scalar_filtered | 0.026 ms | 0.028 ms | 0.034 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| TreeDB text-v2 | common | 0.267 ms | 0.469 ms | 0.908 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| TreeDB text-v2 | rare | 0.015 ms | 0.032 ms | 0.072 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| TreeDB text-v2 | and | 0.022 ms | 0.052 ms | 0.078 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| TreeDB text-v2 | or | 0.029 ms | 0.050 ms | 0.052 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| TreeDB text-v2 | phrase | 0.045 ms | 0.065 ms | 0.068 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| TreeDB text-v2 | scalar_filtered | 0.086 ms | 0.420 ms | 0.503 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |

## Build resources and checkpointed storage

| engine | build repetitions (s) | docs/s | CPU per repetition | peak RSS per repetition | durable bytes | WAL bytes | transient bytes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Bleve | 0.342, 0.348, 0.347 | 29221.8, 28723.9, 28828.6 | 0.305 s, 0.317 s, 0.319 s | 227.734 MiB, 227.656 MiB, 234.234 MiB | [7354589, 7354590, 7354525] | [0, 0, 0] | [0, 0, 0] |
| Apache Lucene | 0.276, 0.262, 0.262 | 36201.0, 38226.6, 38158.1 | 0.578 s, 0.608 s, 0.605 s | unsupported: Java 17 standard APIs do not expose process lifetime peak RSS, unsupported: Java 17 standard APIs do not expose process lifetime peak RSS, unsupported: Java 17 standard APIs do not expose process lifetime peak RSS | [260735, 260735, 260735] | [0, 0, 0] | [0, 0, 0] |
| SQLite FTS5 | 0.036, 0.036, 0.039 | 277704.8, 274797.6, 259288.5 | 0.034 s, 0.034 s, 0.034 s | 46.484 MiB, 46.594 MiB, 46.562 MiB | [5861376, 5861376, 5861376] | [0, 0, 0] | [0, 0, 0] |
| TreeDB text-v2 | 0.151, 0.136, 0.133 | 66336.6, 73773.1, 75228.5 | 0.105 s, 0.105 s, 0.103 s | 67.703 MiB, 74.125 MiB, 68.969 MiB | [6553974, 6553974, 6553974] | [0, 0, 0] | [0, 0, 0] |

## Equivalence and availability ledger

| engine | case | status | detail |
| --- | --- | --- | --- |
| Bleve | common | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | rare | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | and | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | or | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | phrase | equivalent | reference IDs and order match; reopen and intended route proven |
| Bleve | scalar_filtered | equivalent | reference IDs and order match; reopen and intended route proven |
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
- Configuration: `{"analyzer": "standard", "index_type": "scorch", "store_fields": false, "tenant_analyzer": "keyword", "term_vectors": true, "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body", "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/dependency_work/bleve_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/bleve-r1.json --repetition 1 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/bleve-r1`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/bleve-r2.json --repetition 2 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/bleve-r2`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/bleve-r3.json --repetition 3 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/bleve-r3`

### Apache Lucene

- Versions: `{"java": "26.0.2.1", "lucene": "9.12.1", "platform": "Mac OS X/aarch64", "vm": "OpenJDK 64-Bit Server VM"}`
- Configuration: `{"analyzer": "StandardAnalyzer", "compound_file": false, "similarity": "BM25(k1=1.2,b=0.75)", "stored_fields": ["id"], "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body", "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/dependency_work/lucene_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "jvm_max_heap_bytes": 4294901760, "matches_runner_detected": true}}`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/lucene-r1.json --repetition 1 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/lucene-r1`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/lucene-r2.json --repetition 2 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/lucene-r2`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/lucene-r3.json --repetition 3 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/lucene-r3`

### SQLite FTS5

- Versions: `{"platform": "Darwin-25.2.0-arm64-arm-64bit-Mach-O", "python": "3.14.6 (main, Jun 10 2026, 10:03:53) [Clang 21.0.0 (clang-2100.0.123.102)]", "sqlite": "3.53.2"}`
- Configuration: `{"journal_mode": "WAL", "sqlite_auxiliary_threads": 1, "synchronous": "FULL", "tie_break": "score,id", "tokenizer": "unicode61 remove_diacritics 2", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body", "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/sqlite_fts5-r1.json --repetition 1 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/sqlite_fts5-r1.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/sqlite_fts5-r2.json --repetition 2 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/sqlite_fts5-r2.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/sqlite_fts5-r3.json --repetition 3 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/sqlite_fts5-r3.sqlite3`

### TreeDB text-v2

- Versions: `{"go": "go1.26.0", "module": "3def6c723dc0c12a566a2518e7f706e1fbfc8687", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "simple", "bm25f": {"b": 0.75, "k1": 1.2}, "candidate_limit": 10000, "index_version": "v2", "max_postings_scanned": 80000, "result_mode": "score_only", "store_positions": true, "top_k": 10, "weights": {"body": 1, "title": 3}, "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "runner output, corpus, index, and result artifact must have the same decimal POSIX st_dev identity", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/treedb_text_v2-r1.json --repetition 1 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/treedb_text_v2-r1`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/treedb_text_v2-r2.json --repetition 2 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/treedb_text_v2-r2`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/treedb_text_v2-r3.json --repetition 3 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/treedb_text_v2-r3`
