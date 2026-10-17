# P1 native MVCC frozen18 evidence

This non-PR evidence branch preserves local correctness, raw-path guard, feature-enabled and diagnostic evidence for [P1 PR #5146](https://github.com/snissn/gomap/pull/5146). It is not final graph qualification and this branch is not an implementation PR head.

- Baseline: 0149d78dba2574b09d7119d2631ec78a144e088d.
- Original measured source: c23338b31cd1c3af42356d4d68a7a85a22498eb4.
- Exact source tree: bed3920afec44abf68dfb5736a846255c2e04f27.
- Published implementation: bdd08a129ee202c582aaf4b37356b0cba9f14f5a (same tree and baseline parent; different commit metadata).
- Archive SHA256: 79501aaf9db7d9f8c369be2dfbd2d868b31263e68591143a80587adb7e7d1458.
- Archive Git blob: 0e09f3bb4750fb60db1d90f8f89ff25fc7ccf7e3.
- Archive: frozen18-evidence.tar.gz, 2,828,157 bytes, 223 files plus its per-file SHA256 manifest.

Read performance-disposition-18.md for the coordinator's scoped acceptance and unresolved tail limitations. After extracting the archive, packet-18.md and handoff-18.json index the full completion packet; qualification-18.json and build/capture manifests provide exact argv, source hashes, build/runtime environment, embedded raw output and file hashes. Earlier failed raw-gate15/17 evidence is retained beside the passing gate18.

The archive omits compiled binaries and reproducible large parsed trace/event expansions. It retains original compact traces, profiles, shared diagnostic overlays, parsers, exact binary hashes and reproducible build commands. No diagnostic overlay is part of product source. Source inputs were verified from the recorded exact runtime. Extraction does not rerun tests or benchmarks.

```sh
sha256sum frozen18-evidence.tar.gz
tar -xzf frozen18-evidence.tar.gz
# Verify extracted entries against evidence-18-archive-manifest.json.
```

Publishing classification: live inventory of 271 open PR heads (including fork/draft/stacked heads) showed no overlap with codex/native-mvcc-evidence-5139-frozen18. This commit uses [skip ci] for a non-PR evidence publication; implementation PR5146 retains its normal required CI and review path. No checks are waived by this archive.
