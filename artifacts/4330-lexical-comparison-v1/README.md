# Retained same-corpus lexical comparison (#4330)

Validator-accepted retained evidence for TreeDB text-v2, Apache Lucene 9.12.1, Bleve 2.4.4, and SQLite FTS5 on one deterministic 10,000-document corpus.

## Identity

- source commit: `cd74b3d44116059cd9e92b6ea86d00ddedd9a506`
- source tree: `3f2ab05f76ec83c6676bdd44c906b39358fe8f1a`
- TreeDB subtree: `4f8d7ad883a41991739cdf35896305c3185422f0`
- comparator harness subtree: `2ba29ecaa0e9f88359b3877952b071ff0b84b1c4`
- manifest SHA-256: `02d2cdbf4fa1899638bec595e31f36ebf9ba0facc08df19c56898eab9b02f3a6`
- corpus SHA-256: `7e31d754a2c94b1fa6e4d1cf6d75d2f0df595fc8a9cdf80eb12a3d0b7650485e`
- source dirty: false
- qualification eligible: true

## Contract and result

Each engine completed three serial build/checkpoint/reopen repetitions and 20 retained samples for common, rare, AND, OR, exact phrase, and scalar-filtered cases. The validator accepted exact ordered result agreement, corpus/document counts, scoring-sensitive title-weight/term-frequency/length-normalization probes, intended index routes, reopen parity, frozen filesystem/memory/concurrency conditions, and no fallback/timeout. All four engines completed all six equivalent cases.

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
