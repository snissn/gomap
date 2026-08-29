# Same-corpus lexical comparison

Schema: `treedb_lexical_comparison/v1`. Manifest: `02d2cdbf4fa1899638bec595e31f36ebf9ba0facc08df19c56898eab9b02f3a6`. Corpus: `10000` documents, `7e31d754a2c94b1fa6e4d1cf6d75d2f0df595fc8a9cdf80eb12a3d0b7650485e`.

Source: `cd74b3d44116059cd9e92b6ea86d00ddedd9a506` / tree `3f2ab05f76ec83c6676bdd44c906b39358fe8f1a`; retained qualification eligible: **TRUE**.

Only exact, validator-accepted rows enter the headline table. Times are warm single-query latency on one host; they are not timing assertions.

## Headline query latency

| engine | case | p50 | p95 | p99 | result digest |
| --- | --- | ---: | ---: | ---: | --- |
| Bleve | common | 0.348 ms | 0.362 ms | 0.367 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Bleve | rare | 0.003 ms | 0.009 ms | 0.017 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Bleve | and | 0.006 ms | 0.024 ms | 0.034 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Bleve | or | 0.008 ms | 0.020 ms | 0.053 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Bleve | phrase | 0.018 ms | 0.033 ms | 0.042 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| Bleve | scalar_filtered | 0.005 ms | 0.006 ms | 0.012 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| Apache Lucene | common | 0.818 ms | 2.024 ms | 2.382 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| Apache Lucene | rare | 0.351 ms | 0.406 ms | 0.436 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| Apache Lucene | and | 0.389 ms | 0.443 ms | 1.266 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Apache Lucene | or | 0.389 ms | 0.438 ms | 1.208 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Apache Lucene | phrase | 0.319 ms | 0.393 ms | 1.124 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| Apache Lucene | scalar_filtered | 0.190 ms | 0.260 ms | 0.270 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| SQLite FTS5 | common | 3.892 ms | 4.053 ms | 4.157 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| SQLite FTS5 | rare | 0.019 ms | 0.021 ms | 0.026 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| SQLite FTS5 | and | 0.023 ms | 0.025 ms | 0.026 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| SQLite FTS5 | or | 0.040 ms | 0.041 ms | 0.044 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| SQLite FTS5 | phrase | 0.022 ms | 0.024 ms | 0.027 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| SQLite FTS5 | scalar_filtered | 0.026 ms | 0.029 ms | 0.031 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| TreeDB text-v2 | common | 0.284 ms | 0.524 ms | 0.864 ms | `32055f75762e1bc6d5caf948100d7005401d8ea6d53bfda1e0d45ad4f13efd40` |
| TreeDB text-v2 | rare | 0.015 ms | 0.024 ms | 0.033 ms | `d1e8f7e714d80caa330129cd3ef396355c46b9eda93e8ceeaae231865bd1aee0` |
| TreeDB text-v2 | and | 0.021 ms | 0.031 ms | 0.033 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| TreeDB text-v2 | or | 0.024 ms | 0.037 ms | 0.040 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| TreeDB text-v2 | phrase | 0.042 ms | 0.049 ms | 0.059 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| TreeDB text-v2 | scalar_filtered | 0.115 ms | 0.326 ms | 0.371 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |

## Build resources and checkpointed storage

| engine | build repetitions (s) | docs/s | CPU per repetition | peak RSS per repetition | durable bytes | WAL bytes | transient bytes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Bleve | 0.363, 0.351, 0.334 | 27542.9, 28499.6, 29902.0 | 0.326 s, 0.320 s, 0.303 s | 234.516 MiB, 234.516 MiB, 228.234 MiB | [7354448, 7354443, 7354415] | [0, 0, 0] | [0, 0, 0] |
| Apache Lucene | 0.299, 0.258, 0.254 | 33445.7, 38728.1, 39447.1 | 0.625 s, 0.584 s, 0.579 s | unsupported: Java 17 standard APIs do not expose process lifetime peak RSS, unsupported: Java 17 standard APIs do not expose process lifetime peak RSS, unsupported: Java 17 standard APIs do not expose process lifetime peak RSS | [260735, 260735, 260735] | [0, 0, 0] | [0, 0, 0] |
| SQLite FTS5 | 0.037, 0.037, 0.040 | 271867.5, 273369.1, 252188.5 | 0.035 s, 0.035 s, 0.035 s | 46.438 MiB, 46.594 MiB, 46.781 MiB | [5861376, 5861376, 5861376] | [0, 0, 0] | [0, 0, 0] |
| TreeDB text-v2 | 0.143, 0.135, 0.129 | 70140.9, 73960.5, 77466.9 | 0.103 s, 0.106 s, 0.101 s | 66.812 MiB, 74.688 MiB, 69.344 MiB | [6553974, 6553974, 6553974] | [0, 0, 0] | [0, 0, 0] |

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
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "corpus, index, and result artifact must resolve to the runner output filesystem", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/bleve-r1.json --repetition 1 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/bleve-r1`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/bleve-r2.json --repetition 2 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/bleve-r2`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/bleve-r3.json --repetition 3 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/bleve-r3`

### Apache Lucene

- Versions: `{"java": "26.0.2.1", "lucene": "9.12.1", "platform": "Mac OS X/aarch64", "vm": "OpenJDK 64-Bit Server VM"}`
- Configuration: `{"analyzer": "StandardAnalyzer", "compound_file": false, "similarity": "BM25(k1=1.2,b=0.75)", "stored_fields": ["id"], "tie_break": "score,id", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body", "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/dependency_work/lucene_adapter"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "corpus, index, and result artifact must resolve to the runner output filesystem", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "/dev/disk3s5|apfs", "index_store_id": "/dev/disk3s5|apfs", "result_store_id": "/dev/disk3s5|apfs", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "jvm_max_heap_bytes": 4294901760, "matches_runner_detected": true}}`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/lucene-r1.json --repetition 1 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/lucene-r1`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/lucene-r2.json --repetition 2 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/lucene-r2`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/lucene-r3.json --repetition 3 --index /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/lucene-r3`

### SQLite FTS5

- Versions: `{"platform": "Darwin-25.2.0-arm64-arm-64bit-Mach-O", "python": "3.14.6 (main, Jun 10 2026, 10:03:53) [Clang 21.0.0 (clang-2100.0.123.102)]", "sqlite": "3.53.2"}`
- Configuration: `{"journal_mode": "WAL", "sqlite_auxiliary_threads": 1, "synchronous": "FULL", "tie_break": "score,id", "tokenizer": "unicode61 remove_diacritics 2", "top_k": 10, "weighted_field_materialization": "title repeated 3x then body", "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "corpus, index, and result artifact must resolve to the runner output filesystem", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/sqlite_fts5-r1.json --repetition 1 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/sqlite_fts5-r1.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/sqlite_fts5-r2.json --repetition 2 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/sqlite_fts5-r2.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/sqlite_fts5-r3.json --repetition 3 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/sqlite_fts5-r3.sqlite3`

### TreeDB text-v2

- Versions: `{"go": "go1.26.0", "module": "cd74b3d44116059cd9e92b6ea86d00ddedd9a506", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "simple", "bm25f": {"b": 0.75, "k1": 1.2}, "candidate_limit": 10000, "index_version": "v2", "max_postings_scanned": 80000, "result_mode": "score_only", "store_positions": true, "top_k": 10, "weights": {"body": 1, "title": 3}, "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison"}`
- Environment: `{"contract": {"build_cpu_metric": "process CPU nanoseconds", "engine_process_concurrency": 1, "filesystem_identity_policy": "corpus, index, and result artifact must resolve to the runner output filesystem", "latency_clock": "monotonic wall clock", "memory_limit_policy": "inherit one detected host address-space limit; adapters do not raise or lower it", "peak_rss_metric": "process lifetime peak resident bytes when the runtime exposes it; otherwise typed unsupported", "query_concurrency": 1, "resource_scope": "single adapter process per retained repetition", "runtime_cpu_parallelism": 1}, "execution": {"engine_process_concurrency": 1, "query_concurrency": 1, "runtime_cpu_parallelism": 1}, "filesystem": {"corpus_store_id": "16777232", "index_store_id": "16777232", "result_store_id": "16777232", "runner_device_id": "16777232", "same_filesystem": true}, "memory": {"adapter_changed_limit": false, "detected_address_space_limit": "unlimited", "detection_source": "runner_rlimit", "matches_runner_detected": true}}`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/treedb_text_v2-r1.json --repetition 1 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/treedb_text_v2-r1`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/treedb_text_v2-r2.json --repetition 2 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/treedb_text_v2-r2`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/raw/treedb_text_v2-r3.json --repetition 3 --db /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/artifacts/4330-lexical-comparison-v1/indexes/treedb_text_v2-r3`
