# Final repaired descendant applicability

The measured runtime is `974d1ce9bd5cd91d17f5851d53663bf71643b288`. The coordinator compared all 7,330 frozen Git paths with the staged publication and independently rehashed every unchanged original worktree file. Only four owning-document links and the CI discovery fingerprint differ. Runtime, modules, tests and benchmark fixtures remain byte-identical; no original paths are deleted, no compiled Go artifacts are added, and the earlier publication stays unchanged.

[The JSON proof](final-descendant-applicability.json) records the exact staged snapshot before this proof, final independent review and manifest. Those later files are artifact-only descendants; this snapshot is not relabeled as the final publication tree. The proof verifies all 634 byte-preserved provenance entries and that CI changes only `discovery_source_sha256`. Current hosted strict default performance, Windows runtime, required CI and current-head review remain merge gates.
