# Retained same-corpus lexical comparison (#4330)

Validator-accepted retained evidence for TreeDB text-v2, Apache Lucene 9.12.1, Bleve 2.4.4, and SQLite FTS5 on one deterministic 10,000-document corpus.

## Identity

- source commit: `18770a0f0908fcb8405238de7285695d7d156c3c`
- source tree: `805d04696efc4792c2503cc7f6a1441dcefdfdc2`
- TreeDB subtree: `0f6b09c0cee1990959f473571e64e30ac1a816fe`
- comparator harness subtree: `bdacbf27d97e0ea084781955b9339d3859b9cfb5`
- tracked-diff SHA-256 before and after run: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- untracked nonignored sources before and after run: none
- manifest SHA-256: `a2a69522bb3b0533bcc9030bc48286e4e727262a9a361605fd53c41ca86cf8d1`
- corpus SHA-256: `fe5b9d106a099d5c29d3327a8424b7f6d57f248b8802225b2d7bb3719062a227`
- source dirty: false
- post-run source identity reverified: true
- qualification eligible: true

## Contract and result

TreeDB, Lucene, and SQLite completed three serial build/checkpoint/query-reopen/durability-reopen repetitions and 20 retained samples for common, rare, AND, OR, exact phrase, and scalar-filtered cases. Bleve completed the five exactly equivalent lexical cases; its scalar-filtered row is typed unsupported because Bleve v2.4.4 cannot express the tenant predicate as a non-scoring filter. The validator accepted exact ordered result agreement, corpus/document counts, scoring-sensitive title-weight/term-frequency/length-normalization probes, adversarial phrase-position probes, concrete engine-specific route proof, reopen parity, exact runner-bound filesystem/memory/concurrency conditions, and no fallback/timeout. The runner reverified HEAD, tree/subtree OIDs, tracked diff, and nonignored untracked source identities after every adapter completed.

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
