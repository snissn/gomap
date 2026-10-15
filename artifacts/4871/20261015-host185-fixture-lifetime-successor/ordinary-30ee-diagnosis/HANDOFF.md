# Ordinary 30ee fixture repair handoff

Parent worktree: `/mnt/fast4tb/gomap-mvcc-4871-20261015/ordinary-repair`.
Starting/preserved HEAD: `30ee954b974077b8968cdfb836e03058d9934ed1`.
Scope: five test files only. No production, benchmark, gate, workflow, disk-format, allocation-bound or refund-oracle change. Root owns Git/publication/CI.

## Repair and causal limits

1. **Original asynchronous grower scheduling.** The DB primary-owner test and its exact primaryarena sibling repeatedly poll bounded original Pager close. The production close signals and polls its original async grower; the previous bounded fixture spin could consume all turns without scheduling that worker. Added `runtime.Gosched` only after a paused turn, preserving original 4096 turns, 32 records, 1MiB grants and every custody/refund assertion. Terminal failure prints bounded stacks and actual receipt/Arena/Pager phase, growDone identity and owner state. The artifact-only SAME-grower control deliberately withholds the original worker and consumes the original bound in receipt0/Arena1/Pager1, then releases that same worker and finishes within the original bound. Exact hosted failing phase remains unattributed; an initial 20-repetition diagnostic did not reproduce the hosted failure. New diagnostic token fields are not instantiated in this fixture's original close object graph; no production bound repair was justified.

   Exact sibling: `primaryarena.TestArenaPhysicalCloseWorkOriginalCaptureRemains` uses the same bounded close of an actual asynchronously growing pager. Other inspected drivers: dependency sticky/nonlast release and runtime disposal do not drive this successful final grower; read-only mapping test has no grower; owned-child pager test already yields; physical-fault tests stop on error. No wider expansion.

2. **Permanent failure process lifetime.** Actual Linux descriptor inspection after both fixtures' visible teardown found a retained duplicate namespace-parent directory FD. Their failed DATA cleanup intentionally retains the original PRIMARY owner edge and admission; Windows correctly cannot remove the directory while that owner is alive. The two named fixtures now execute all original substantive and cleanup assertions in a subprocess, with every directory owned by the parent. Child completion must succeed before parent TempDir removal. The child uses the same test binary (including race instrumentation), exact test/subtest regex, and parent test deadline through CommandContext, retaining output on failure. No original failed-owner completion/refund is forced. The separate genuine zombie value-log manager is now under a parent-owned subdirectory. Other successful variants remain in process. Windows runtime remains a hosted successor requirement; local Windows compilation alone cannot establish this behavior.

3. **Dictionary budget basis.** Trace identifies the SAME enrolled default DATA registry owner, not a dictionary owner or PRIMARY pager: asynchronous seed publisher Prepare -> CaptureStableIndexFileResource -> newStablePagerResourceToken -> IdentityPinRegistry.Observe -> stateLocked grows identity/table metadata (including observed 32+2048 bytes), plus separately traced original construction storage. Six external leases stay unchanged while that existing owner's bytes change. Artifact-only SAME Observe gating fails the unchanged dictionary exact refund assertion when the seed publisher completes after the baseline (registry6560->5536; External19232->18208). Its paired positive passes when SAME seed publication completes before enrollment. This discriminates baseline contamination; it does not reproduce the exact hosted +2080/+2096 magnitude. Shared `cowCutFixture` now invokes original `backend.Checkpoint()` after original seed Set and before `newCOWCache` enrollment. All 15 callers were inspected; none requires pending seed publication. Two other exact-baseline consumers share the same vulnerability. `cow-cut-fixture-callers.md` records the finite map. All whole-budget, lease, generation and dictionary release assertions remain exact.

## Evidence inventory

All entries are under this directory. `logs/<label>/{command,before,after,result}.json` retain argv/environment, full source identities and exit; `raw.txt` is complete output. Overlay sources/identities are retained in their named control directories. Intentionally failing controls and build errors remain intact.

- `arena-finish-diagnostic-01`: initial artifact phase diagnostics, count20, did not reproduce failure.
- `grower-control-01`: SAME original grower withheld/released control; original bounds.
- `grower-repair-normal-01`, `grower-repair-race-01`: earlier two-file packet; historical once later test files changed.
- `windows-holder-diagnostic-01`: actual retained directory FD evidence after original visible teardown.
- `dictionary-owner-negative-01`: count20 local race did not reproduce; source drifted only in unrelated DB test files during this early trace, so not exact final-source proof.
- `dictionary-resize-diagnostic-01`: retained overlay import build error.
- `dictionary-resize-diagnostic-02`: exact-source trace of SAME registry owner resize by original publisher.
- `dictionary-publication-negative-01`: deliberate unchanged exact refund assertion failure from SAME publisher crossing baseline.
- `dictionary-publication-positive-01`: retained overlay import build error.
- `dictionary-publication-positive-02`: SAME publisher completed before enrollment; original dictionary test passes.
- `coherent-normal-01`, `coherent-race-01`, `coherent-windows-build-01`: final five-file validation; see validation-final.json for actual results.

Final packet focused normal validation passed five repetitions (135 top-level pass events across three packages); race validation passed three repetitions (81 top-level pass events, zero DATA RACE reports). Both final runtime checks have identical full source identities before and after. Final artifacts record the Windows build outcome and exact freeze. Hosted30ee raw/PRIMARY pass is parent-owned evidence for 30ee only (Get paired -0.42%, PRIMARY -50.088%). No new performance work or gate claim. Exact successor review, CI platform/race execution and complete root-owned failed-check disposition remain required. Historical RED evidence and unattributed hosted grower phase remain explicit.

## Final freeze and lease release

All five source files are frozen. Source/runtime writer lease is released; no child test, compiler, diagnostic or benchmark remains active. HEAD is unchanged. `freeze-final.json` contains all 10,246 tracked/untracked source-file SHA256 values and HEAD; `changed-files-final.json` binds each of the five test-only deltas to HEAD. `validation-final.json` contains exact commands, environment, exits, source equality, raw identities and all three Windows test executable hashes. All three final checks exit0 and match the same frozen manifest. Windows amd64 cross-compilation succeeded for DB, primaryarena and caching. `final.patch` and `frozen-source/` provide reviewable copies. `git diff --check` and gofmt are clean. The packet changes94 lines added and7 removed, all in tests; product and harness bytes equal HEAD.

Root may regenerate CI inventory, commit and obtain exact review. Successor hosted execution remains required. There is no acceptance, mergeability, successor performance or Windows runtime claim. Historical RED logs, exact dictionary control limitations and the unattributed hosted grower phase remain retained.
