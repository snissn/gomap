# Retained same-corpus lexical comparison (#4330)

Validator-accepted retained evidence for TreeDB text-v2, Apache Lucene 9.12.1, Bleve 2.4.4, and SQLite FTS5 on one deterministic 10,000-document corpus.

## Identity

- source commit: `d3fbb7ba7a20924cc865f802fbc956b152b10196`
- source tree: `0f3c83d9f85f39ae259be4371853722e03e5c873`
- TreeDB subtree: `4f8d7ad883a41991739cdf35896305c3185422f0`
- comparator harness subtree: `4fbcb1dfa30fb5efe8905c826063c45cc28f1e30`
- manifest SHA-256: `3907a39a5f43e9df611d69848773b9e79175919542bced3cce13d1034ef7347c`
- corpus SHA-256: `f0956e2758bdc3fc0fe0b2deea88170e72c00ce26c394c7189b4f15c93133130`
- source dirty: false
- qualification eligible: true

## Contract and result

Each engine completed three serial build/checkpoint/reopen repetitions and 20 retained samples for common, rare, AND, OR, exact phrase, and scalar-filtered cases. The validator accepted exact ordered result agreement, corpus/document counts, intended index routes, reopen parity, and no fallback/timeout. All four engines completed all six equivalent cases.

The headline is bounded to this synthetic fixture and host. It does not establish general relevance quality, production-cache behavior, or broad industry parity. Use `lexical_comparison.md` for the compact matrix and `lexical_comparison.json` for raw consolidated samples, commands, versions, configs, storage classes, source identity, and equivalence ledger.

## Contents and integrity

- `raw/`: twelve shared-schema engine artifacts, three per engine;
- `logs/`: exact setup/adapter commands and bounded output;
- `corpus.tsv`, `lexical_manifest.json`, `lexical_result.schema.json`: consumed data and contracts;
- `lexical_comparison.json`, `lexical_comparison.md`: consolidated report;
- `SHA256SUMS`: digest ledger for every retained file except itself.

Verify from this directory:

```sh
shasum -a 256 -c SHA256SUMS
```
