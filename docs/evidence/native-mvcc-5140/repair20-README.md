# Native MVCC P1 repair20 evidence

Implementation PR: https://github.com/snissn/gomap/pull/5146
Remote head: ea4f5bc6294245488526df6015f59e0b98b89674
Local frozen head: f42f532ad0e63cab3c99296ea27140ce22f59859
Identical source tree: 98068799cc4ea5cf7ea733f2db00c64a68f74dfa

Archive: repair20-evidence.tar.gz (929262 bytes; 86 files plus MANIFEST.json).
SHA256: 8d43ca9c5e9eb400c7bca7394f13ffc7660757cf5877433a9773ffc0f6e877c6
Git blob: c8dc494b505c2312ea22a35fe7851d87db4d49e4

The packet includes valid failing-before/passing-after history controls, exact-source focused/race/CI-contract checks, captured-source and binary bindings, three enabled processes ×30 shapes, ordinary baseline/candidate three-pair diagnostics, raw output verification, review dispositions and the source delta. It also retains repair19 hosted raw/PRIMARY summaries, original vacuum and one retry outputs, and bounded baseline/candidate retry controls. Older CI is explicitly not relabeled as repair20. The initial fence test's synchronous blocked snapshot is a retained harness diagnostic, not a product red.

Ordinary allocation counts match baseline; timing medians are +1.27% to +39.53% and B/op +0.32% to +3.00%. Coordinator intermediate P1-only acceptance is recorded in packet-20.md and the PR body; no speedup, sustained parity, final Q acceptance or mandatory CI waiver is claimed. Current-head CI and review were pending when archived.

Earlier evidence: https://github.com/snissn/gomap/tree/a0343d2e84ec6a09c6017f96dc354c0410b0ebc6/docs/evidence/native-mvcc-5140

This branch is an evidence-only checkpoint, not an implementation PR head. Exact destination mapping was checked before publication; [skip ci] applies to the evidence commit only. Runtime/harness provenance remains bound to the measured source above. No generated binary or large hosted artifact is duplicated here; the manifest and original hosted artifacts provide their identity.
