# Retained same-corpus lexical comparison (#4330)

Validator-accepted retained evidence for TreeDB text-v2, Apache Lucene 9.12.1, Bleve 2.4.4, and SQLite FTS5 on one deterministic 10,000-document corpus.

## Identity

- source commit: `5920f1209dcb52ab5339e7fa39e9b2f38080a68b`
- source tree: `d670eeab60465e2a72ab7f479a8e5de21261d371`
- TreeDB subtree: `670740f244cf42afffbe517c3a514da6895decfd`
- comparator harness subtree: `358a947926579070563e882b3bdb82751f6fb096`
- tracked-diff SHA-256 before and after run: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- untracked sources, including ignored inputs, before and after run: none
- manifest SHA-256: `a268cb2e59f4a63d8bcc7a0047fe85fe2195bef62a536f9b54bcae6f4b568d8f`
- corpus SHA-256: `fe5b9d106a099d5c29d3327a8424b7f6d57f248b8802225b2d7bb3719062a227`
- source dirty: false
- post-run source identity reverified: true
- qualification eligible: true

## Contract and result

TreeDB and Lucene completed six pinned-scoring, exact headline cases across three serial build/checkpoint/query-reopen/durability-reopen repetitions with 20 retained samples per case. Bleve retained five native-TF-IDF rows as directional context and typed its scalar-filtered row unsupported; SQLite retained all six native-FTS5-scoring rows as directional context. The validator accepted exact-row ordered reference agreement, directional predicate/filter/phrase eligibility and complete top-K cardinality, corpus/document counts, scoring-sensitive probes, concrete engine-specific route proof, reopen parity, exact runner-bound filesystem/memory/concurrency conditions, and no fallback/timeout. The runner reverified HEAD, tree/subtree OIDs, tracked diff, and ordinary plus ignored untracked source identities after every adapter completed.

Every durable-footprint row includes the complete logical `id`, `title`,
`body`, and `tenant` source fields; generated weighted fields remain
index-only.

The headline is bounded to this synthetic fixture and host. It does not establish general relevance quality, production-cache behavior, or broad industry parity. Use `lexical_comparison.md` for the compact matrix and `lexical_comparison.json` for raw consolidated samples, commands, versions, configs, CPU/RSS evidence, storage classes, source identity, and equivalence ledger.

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
