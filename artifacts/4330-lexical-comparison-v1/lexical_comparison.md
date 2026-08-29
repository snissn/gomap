# Same-corpus lexical comparison

Schema: `treedb_lexical_comparison/v1`. Manifest: `3907a39a5f43e9df611d69848773b9e79175919542bced3cce13d1034ef7347c`. Corpus: `10000` documents, `f0956e2758bdc3fc0fe0b2deea88170e72c00ce26c394c7189b4f15c93133130`.

Source: `d3fbb7ba7a20924cc865f802fbc956b152b10196` / tree `0f3c83d9f85f39ae259be4371853722e03e5c873`; retained qualification eligible: **TRUE**.

Only exact, validator-accepted rows enter the headline table. Times are warm single-query latency on one host; they are not timing assertions.

## Headline query latency

| engine | case | p50 | p95 | p99 | result digest |
| --- | --- | ---: | ---: | ---: | --- |
| Bleve | common | 0.410 ms | 0.428 ms | 0.440 ms | `6860686faff70c354f6ee78209cf4b521a71c5d4abbd454ee73b56efcc727b55` |
| Bleve | rare | 0.004 ms | 0.005 ms | 0.006 ms | `6860686faff70c354f6ee78209cf4b521a71c5d4abbd454ee73b56efcc727b55` |
| Bleve | and | 0.008 ms | 0.009 ms | 0.021 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Bleve | or | 0.010 ms | 0.012 ms | 0.016 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Bleve | phrase | 0.019 ms | 0.024 ms | 0.031 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| Bleve | scalar_filtered | 0.006 ms | 0.007 ms | 0.008 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| Apache Lucene | common | 0.771 ms | 1.612 ms | 1.858 ms | `6860686faff70c354f6ee78209cf4b521a71c5d4abbd454ee73b56efcc727b55` |
| Apache Lucene | rare | 0.272 ms | 0.360 ms | 0.394 ms | `6860686faff70c354f6ee78209cf4b521a71c5d4abbd454ee73b56efcc727b55` |
| Apache Lucene | and | 0.331 ms | 0.412 ms | 1.866 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| Apache Lucene | or | 0.300 ms | 0.372 ms | 0.464 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| Apache Lucene | phrase | 0.275 ms | 0.423 ms | 0.467 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| Apache Lucene | scalar_filtered | 0.216 ms | 0.395 ms | 0.512 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| SQLite FTS5 | common | 3.202 ms | 3.239 ms | 3.256 ms | `6860686faff70c354f6ee78209cf4b521a71c5d4abbd454ee73b56efcc727b55` |
| SQLite FTS5 | rare | 0.017 ms | 0.019 ms | 0.020 ms | `6860686faff70c354f6ee78209cf4b521a71c5d4abbd454ee73b56efcc727b55` |
| SQLite FTS5 | and | 0.021 ms | 0.021 ms | 0.021 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| SQLite FTS5 | or | 0.035 ms | 0.036 ms | 0.040 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| SQLite FTS5 | phrase | 0.020 ms | 0.021 ms | 0.023 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| SQLite FTS5 | scalar_filtered | 0.022 ms | 0.022 ms | 0.023 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |
| TreeDB text-v2 | common | 0.256 ms | 0.433 ms | 0.760 ms | `6860686faff70c354f6ee78209cf4b521a71c5d4abbd454ee73b56efcc727b55` |
| TreeDB text-v2 | rare | 0.014 ms | 0.017 ms | 0.019 ms | `6860686faff70c354f6ee78209cf4b521a71c5d4abbd454ee73b56efcc727b55` |
| TreeDB text-v2 | and | 0.020 ms | 0.022 ms | 0.023 ms | `ec83fcd96024576cbb2628ca36dd60d1bdf129216c6d71b8d5e7a44a5065953d` |
| TreeDB text-v2 | or | 0.024 ms | 0.028 ms | 0.033 ms | `d3a21b7de5119f9bf360bf76da508e0778a2ddb502717efdcc4d4abd0b0ca9d8` |
| TreeDB text-v2 | phrase | 0.037 ms | 0.040 ms | 0.043 ms | `3f12e436d210de561f67fb0131f637b024ed2bce2d328478c4d7bded8a2fff34` |
| TreeDB text-v2 | scalar_filtered | 0.039 ms | 0.233 ms | 0.353 ms | `efbd9f605d29312de95b30ea2d42e875a7e9c79acbfabe73748ed2b115672fa0` |

## Build and checkpointed storage

| engine | build repetitions (s) | docs/s | durable bytes | WAL bytes | transient bytes |
| --- | --- | --- | --- | --- | --- |
| Bleve | 0.133, 0.128, 0.120 | 75007.1, 78311.5, 83315.1 | [2635305, 2635263, 2635322] | [0, 0, 0] | [0, 0, 0] |
| Apache Lucene | 0.255, 0.227, 0.227 | 39201.8, 43979.8, 44108.1 | [126452, 126452, 126452] | [0, 0, 0] | [0, 0, 0] |
| SQLite FTS5 | 0.018, 0.018, 0.018 | 566888.1, 559316.7, 559679.3 | [1761280, 1761280, 1761280] | [0, 0, 0] | [0, 0, 0] |
| TreeDB text-v2 | 0.131, 0.104, 0.099 | 76439.0, 95920.5, 100653.4 | [5767542, 5767542, 5767542] | [0, 0, 0] | [0, 0, 0] |

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
- Configuration: `{"analyzer": "standard", "index_type": "scorch", "store_fields": false, "tenant_analyzer": "keyword", "term_vectors": true, "tie_break": "score,id", "top_k": 10, "weights": {"body": 1, "title": 3}, "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/dependency_work/bleve_adapter"}`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/bleve-r1.json --repetition 1 --index /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/bleve-r1`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/bleve-r2.json --repetition 2 --index /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/bleve-r2`
- Command: `go run . --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/bleve-r3.json --repetition 3 --index /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/bleve-r3`

### Apache Lucene

- Versions: `{"java": "26.0.2.1", "lucene": "9.12.1", "platform": "Mac OS X/aarch64", "vm": "OpenJDK 64-Bit Server VM"}`
- Configuration: `{"analyzer": "StandardAnalyzer", "compound_file": false, "similarity": "BM25(k1=1.2,b=0.75)", "stored_fields": ["id"], "tie_break": "score,id", "top_k": 10, "weights": {"body": 1.0, "title": 3.0}, "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/dependency_work/lucene_adapter"}`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/lucene-r1.json --repetition 1 --index /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/lucene-r1`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/lucene-r2.json --repetition 2 --index /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/lucene-r2`
- Command: `mvn -q compile exec:java -Dexec.args=--manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/lucene-r3.json --repetition 3 --index /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/lucene-r3`

### SQLite FTS5

- Versions: `{"platform": "Darwin-25.2.0-arm64-arm-64bit-Mach-O", "python": "3.14.6 (main, Jun 10 2026, 10:03:53) [Clang 21.0.0 (clang-2100.0.123.102)]", "sqlite": "3.53.2"}`
- Configuration: `{"journal_mode": "WAL", "synchronous": "FULL", "tie_break": "score,id", "tokenizer": "unicode61 remove_diacritics 2", "top_k": 10, "weights": {"body": 1.0, "title": 3.0}, "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison"}`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/sqlite_fts5-r1.json --repetition 1 --db /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/sqlite_fts5-r1.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/sqlite_fts5-r2.json --repetition 2 --db /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/sqlite_fts5-r2.sqlite3`
- Command: `/opt/homebrew/opt/python@3.14/bin/python3.14 /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/sqlite_fts5_bench.py --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/sqlite_fts5-r3.json --repetition 3 --db /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/sqlite_fts5-r3.sqlite3`

### TreeDB text-v2

- Versions: `{"go": "go1.26.0", "module": "d3fbb7ba7a20924cc865f802fbc956b152b10196", "platform": "darwin/arm64"}`
- Configuration: `{"analyzer": "simple", "bm25": {"b": 0.75, "k1": 1.2}, "candidate_limit": 10000, "index_version": "v2", "max_postings_scanned": 80000, "result_mode": "score_only", "store_positions": true, "top_k": 10, "weights": {"body": 1, "title": 3}, "working_directory": "/Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison"}`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/treedb_text_v2-r1.json --repetition 1 --db /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/treedb_text_v2-r1`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/treedb_text_v2-r2.json --repetition 2 --db /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/treedb_text_v2-r2`
- Command: `go run ./benchmarks/text_hybrid_scoreboard/treedb_adapter --manifest /Users/michaelseiler/orca/workspaces/gomap/4330-lexical-comparison/benchmarks/text_hybrid_scoreboard/lexical_manifest.json --corpus /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/corpus.tsv --out /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/raw/treedb_text_v2-r3.json --repetition 3 --db /Users/michaelseiler/orca/workspaces/gomap/4330-retained-v1/indexes/treedb_text_v2-r3`
