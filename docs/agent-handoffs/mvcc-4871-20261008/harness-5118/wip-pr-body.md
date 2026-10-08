Adds an opt-in `-treedb-index-primary-directory` selector using the adapter's existing optional reflection boundary. Supported builds receive the actual boolean option; unsupported enablement fails before Open. Resolved text distinguishes requested, supported, and configured enablement read from built options. README documents configuration-versus-runtime proof and enabled-directory comparison limits.

WIP migration handoff for #5118 within #4871. This is not a mature, reviewed or mergeable candidate. Root owns review and merge; no manual hosted AI review requested and no retained performance collection performed.

Source: base `3c33dd77d2457a8e477ec0e7cd50bb8fde80fcc3`; branch `codex/5118-primary-harness-selector`; WIP head `20e3c50511a0aeec02efd648d4257ccd312aac2c`; tree `a5cc7e55087813cf470375c7629202944ab24729`. Changes are limited to unified-bench adapter/README/tests and the CI-impact discovery fingerprint. All 57 original CI members, workflows, ownership and harness bindings are preserved. Benchmark names/profile artifact filenames/structured schemas are unchanged. No TreeDB product edits.

Validation so far:

- `git diff --cached --check`: passed before commit.
- `uv run --with pyyaml python .github/scripts/refresh_ci_impact_inventory.py`: passed; `--check` passed.
- `PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_ci_impact.py`: 21 tests passed.
- `PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_treedb_ci_contract.py`: 6 tests passed.
- `PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_treedb_windows_core_headroom.py`: 16 tests passed.
- `uv run --with pyyaml python .github/scripts/test_refresh_ci_impact_inventory.py`: 15 tests, 2 failures/11 errors due to `missing-harness-input`: its fixture omits already-bound `scripts/cow_c3_read/*`. No selector relationship established. Exact-base .github reproduction emitted the same failure pattern but was stopped before a complete result for migration.
- Focused Go test (`go test -p 2 ./cmd/unified_bench -run 'Test(TreeDBPrimaryDirectoryOptionalOption|BuildTreeDBOptionsPrimaryDirectory|BuildTreeDBOptions.*Profile|BuildTreeDBOptions.*Unsafe|.*CommandWAL)' -count=1`) stopped at setup under GOPROXY=off because the original module cache lacked dependencies. This is infrastructure-only, not a compiler/test result.
- Authorized task-owned external module fetch was safely stopped for migration (exit143), alongside base reproduction and slow git-status command. No Go checks reached compilation.

Required next actions:

1. Reuse the exact five-line missing-fixture hunk, including comments, from external C3 #5080 head `04fd2951`, `.github/scripts/test_refresh_ci_impact_inventory.py` blob `8e42fbb82e3ff94ff4661f0ee8a043007d27fa89`. This repair was authorized by the root coordinator but is not yet applied; do not redesign policy or remove bindings. It iterates declared harness inputs and copies missing fixture paths from ROOT.
2. Complete external cached dependency preflight with cached Go1.26, then focused real adapter checks and complete `cmd/unified_bench` vet/test. Preserve supported bool/unsupported/default/invalid-type coverage and command-WAL/profile/unsafe checks. Real-builder tests adapt to actual option availability; current main lacks the option, and supported bool tests use synthetic option structs.
3. Stage intended fixture repair and refresh/check CI-impact again before the next coherent commit/push. Monitor required exact-head CI and all actual platform findings.
4. Root arranges independent fresh-context GPT-6.1 Sol exact-candidate review of provenance, isolation/concurrency, fail-closed selection and wording. No merge or expensive retained #5111 collection until review/landing and exact product/harness identity freeze.

Local paths retained for handoff: worktree `/Volumes/FlashDrive/gomap-5118-primary-harness-root-20261008`; outputs `/Volumes/FlashDrive/gomap-5118-primary-harness-outputs-20261008` (logs, partial module-cache, gocache, tmp, uv-cache and small base-ci-repro archive). No writer remains after spin-down. Preserve logs until the coordinator accepts durable handoff; caches/base fixture are disposable once consumers release them. Worktree/source are recoverable from the pushed branch after verification; cleanup belongs to the root coordinator.
