# Retained same-corpus lexical comparison (#4330)

Validator-accepted retained evidence for TreeDB text-v2, Apache Lucene 9.12.1, Bleve 2.4.4, and SQLite FTS5 on one deterministic 10,000-document corpus.

## Identity

- source commit: `4dba389e1a72a02744d0e5eb02edc4a203911e06`
- source tree: `c09e332dd1f2c7de13af542ca733d011acf9a11f`
- TreeDB subtree: `4f8d7ad883a41991739cdf35896305c3185422f0`
- comparator harness subtree: `d9493c935a3ff1d5a228098a20fb094fc91d7540`
- tracked-diff SHA-256 before and after run: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- untracked nonignored sources before and after run: none
- manifest SHA-256: `12bcce9db9564c4d6b31206ce00bf16289fb9fd5d6f479eec8510e7dc8203728`
- corpus SHA-256: `7e31d754a2c94b1fa6e4d1cf6d75d2f0df595fc8a9cdf80eb12a3d0b7650485e`
- source dirty: false
- post-run source identity reverified: true
- qualification eligible: true

## Contract and result

Each engine completed three serial build/checkpoint/reopen repetitions and 20 retained samples for common, rare, AND, OR, exact phrase, and scalar-filtered cases. The validator accepted exact ordered result agreement, corpus/document counts, scoring-sensitive title-weight/term-frequency/length-normalization probes, concrete engine-specific route proof, reopen parity, exact runner-bound filesystem/memory/concurrency conditions, and no fallback/timeout. The runner reverified HEAD, tree/subtree OIDs, tracked diff, and nonignored untracked source identities after every adapter completed. All four engines completed all six equivalent cases.

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
