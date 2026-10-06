# Independent main-edbd composition review

**ACCEPT bounded C2 source composition and retained-evidence applicability. Final integrated-head proof and hosted gates remain HELD.** No concrete composition blocker was found. Review used immutable Git objects only; no Go, remote jobs, source/index changes or GitHub mutations.

Accepted C2 head: `0142519b2cc4c097f50b4e7a64a5b6993ff0d9c6`. Main delta: `40dc31f44e7bb14a4f85dab254131af029ef820b` → `edbd68d34b0fdf7152a498b87d373576de029721`. Their common ancestor is that exact main base. The read-only auto-merge tree is `ae8f7c61c30ddb7395bd29825debf32cd5238b36` and its sole conflict is `.github/ci/ci_impact.json`. All three CI JSON inputs have identical fields/policy/owners/rules except `discovery_source_sha256`; root must refresh that fingerprint after the coherent integration.

All 34 main paths have exact before/after/C2/auto-merge Git-blob and SHA bindings in `edbd-main-composition-bindings.json` (SHA `6dbcc7510645c6f13c2ad7035770167a71ddebe169d50994706a2a79bba6ea2c`). The auto-merge changes exactly those 34 paths versus014. Non-overlapping source blobs match main directly; four shared docs and the CI fingerprint are the only automatic-composition surfaces.

C2 caching/native DB/public DB, MVCC/fences/adapters, WAL, canonical planning/finalization/native reader authority, rootpublication/resource identities and value-log persistent GC/rewrite files are unchanged. Module files and both benchmarks are unchanged: raw fixture SHA `7f3b0a1fa48e41ce8dca25df6406fe8480ae69429cc5b562792282f2422e65be`; public MVCC fixture SHA `243a3049303d017a5e68a4eb8782287c707cc91aa23f23199e910ed6db5e4b18`. No M7 adapter changes; existing shared authority methods and registries remain unchanged despite the additive manager getter.

The shared manager addition is **`ActiveHandles`**, not ActiveResourceCount, at new-main `TreeDB/internal/mappedresource/manager.go:453`. It returns zero for nil, locks the existing mutex, reads the existing scalar counter and unlocks. Removing that exact added method reproduces the complete old file byte for byte. It changes no type layout, global state, acquisition/release path, pin registry, PinSummary or GlobalPinSummary authority. Production calls added by main are only `collections/column_asset_manager.go:2816` lifecycle diagnostics and `collections/document_materializer.go:597` asset counters, replacing full Stats copying. Neither C2 timed fixture invokes those collection methods.

The remaining production edits are collection GetInto/index-range full typed-row materialization and same-snapshot locator/read-view handling. Main adds its R1 lifecycle/mutation/read tests, benchmark, docs, capture/validation scripts and example. This audit checks composition, not an independent qualification of that already-landed R1 feature. It introduces no new COW mode/pool/ledger, native publication authority or C3 fence removal. The shared contracts/readme changes concern R1 and dedicated harness usage, preserving C2 packet identities and standalone-fixture distinctions.

**Compiled closure is not claimed byte-identical.** Existing TreeDB root test files already import collections (for example command_wal_group_commit_test.go:17 and production_authority_matrix_test.go:26). Thus a rebuilt raw public benchmark test binary can compile changed collection/mappedresource dependencies even though the measured C2 Open/Set/capture/read paths do not execute those changed methods. Existing C2 runtime callers/methods remain identical; link layout, rebuilt executable identity and new numerical results do not follow from source sameness. Final-head strict performance/CI catches that separate risk.

The retained974d648-row cost packet keeps its complete original source/binary/environment identities; it is not relabeled as the new full tree. Source/lifetime/refusal/maintenance and actual functional conclusions apply to unchanged C2 surfaces. No new45-run qualification is required merely by a SHA change, but new-head hosted default performance, actual Windows runtime, required CI and review remain separate gates. The original full7330 source manifest must not be called the new whole-source manifest.

Local retained014 evidence confirms the clean Codex comment [6012465384](https://github.com/snissn/gomap/pull/5069#issuecomment-6012465384) and all five accepted strict raw-path cases. The inspected root-fetched status snapshot has 26 successful CheckRuns, with pending/queued rows and no completed failures; this is retained014 evidence, not a live refresh or new-head approval.

Before finalization, root must resolve/refresh the discovery-only CI conflict, verify the final merged source/old packet correspondence to the enumerated delta, check auto-merged owning docs/Store fences/links, append a precise main-integration caveat/proof, regenerate the literal publication manifest and bind the exact new committed head. Fresh hosted gates/review must be assessed at that final head. No default promotion, sustained C3/C4 qualification or parent completion is inferred.

Exact main changed paths:

- `M .github/ci/ci_impact.json`
- `M TreeDB/collections/api.go`
- `M TreeDB/collections/column_asset_manager.go`
- `M TreeDB/collections/document_materializer.go`
- `A TreeDB/collections/r1_lifecycle_5060_bench_test.go`
- `A TreeDB/collections/r1_lifecycle_5060_maintenance_test.go`
- `A TreeDB/collections/r1_lifecycle_5060_test.go`
- `A TreeDB/collections/r1_mutation_5059_test.go`
- `A TreeDB/collections/r1_range_history_5065_test.go`
- `A TreeDB/collections/r1_reads_5058_test.go`
- `M TreeDB/collections/typed_storage_naming_test.go`
- `A TreeDB/docs/evidence/r1-row-store-5061/README.md`
- `A TreeDB/docs/evidence/r1-row-store-5061/publication-manifest.template.json`
- `M TreeDB/docs/guides/README.md`
- `M TreeDB/docs/guides/collections-quickstart.md`
- `A TreeDB/docs/guides/typed-row-store.md`
- `M TreeDB/docs/guides/typed-storage-performance.md`
- `M TreeDB/docs/spec/README.md`
- `M TreeDB/docs/spec/contracts.md`
- `A TreeDB/docs/spec/r1-indexed-mutations.md`
- `M TreeDB/docs/spec/r1-indexed-row-contract.md`
- `A TreeDB/docs/spec/r1-row-lifecycle.md`
- `A TreeDB/docs/spec/r1-row-reads.md`
- `M TreeDB/internal/mappedresource/manager.go`
- `M TreeDB/internal/mappedresource/manager_test.go`
- `M cmd/benchprof/README.md`
- `M cmd/unified_bench/README.md`
- `A examples/typed_rows/README.md`
- `A examples/typed_rows/main.go`
- `A scripts/r1_lifecycle_capture.py`
- `A scripts/r1_lifecycle_capture.sh`
- `A scripts/r1_lifecycle_capture_test.py`
- `A scripts/r1_lifecycle_validate.py`
- `A scripts/r1_lifecycle_validate_test.py`
