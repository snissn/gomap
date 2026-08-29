# Retained same-corpus lexical comparison (#4330)

Validator-accepted retained evidence for TreeDB text-v2, Apache Lucene 9.12.1, Bleve 2.4.4, and SQLite FTS5 on one deterministic 10,000-document corpus.

## Identity

- source commit: `3def6c723dc0c12a566a2518e7f706e1fbfc8687`
- source tree: `bbc7880549e39daa2324a0e19670169cf23e905b`
- TreeDB subtree: `4f8d7ad883a41991739cdf35896305c3185422f0`
- comparator harness subtree: `bcfcb3840be9fea1c3d21a6b17801a409c249914`
- tracked-diff SHA-256 before and after run: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- manifest SHA-256: `12bcce9db9564c4d6b31206ce00bf16289fb9fd5d6f479eec8510e7dc8203728`
- corpus SHA-256: `7e31d754a2c94b1fa6e4d1cf6d75d2f0df595fc8a9cdf80eb12a3d0b7650485e`
- source dirty: false
- post-run source identity reverified: true
- qualification eligible: true

## Contract and result

Each engine completed three serial build/checkpoint/reopen repetitions and 20 retained samples for common, rare, AND, OR, exact phrase, and scalar-filtered cases. The validator accepted exact ordered result agreement, corpus/document counts, scoring-sensitive title-weight/term-frequency/length-normalization probes, concrete engine-specific route proof, reopen parity, exact runner-bound filesystem/memory/concurrency conditions, and no fallback/timeout. The runner reverified source identity and the tracked diff after every adapter completed. All four engines completed all six equivalent cases.

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
