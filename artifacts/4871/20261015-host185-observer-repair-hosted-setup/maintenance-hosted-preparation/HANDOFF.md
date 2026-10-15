# Hosted maintenance diagnostic — frozen harness packet

Source/runtime lease is RELEASED to root. No active child/process remains. No Go command, compiler, Go test body, benchmark, module fetch, Git mutation, GitHub request or remote dispatch was executed by this worker. Root owns commit/publication/independent review and the explicit remote runtime grant.

Worktree: `/mnt/fast4tb/gomap-mvcc-4871-20261015/maintenance-hosted-runner`.
Parent: `72e01efd767ef5d766f070e531fb53ea3f22674d`.
Branch: `codex/4871-maintenance-diagnostic-185-20261015`.
The harness commit is intentionally PENDING ROOT and will differ from its exact product candidate. `freeze-final.json` freezes all10251 current files, the six changed-file hashes and patch hash; it does not mislabel the dirty harness packet as a committed product SHA.

## Scope / invariants

Only `.github/workflows/treedb-vacuum-m0.yml` and five `.github/scripts/mvcc_maintenance_*` files changed (four Python scripts plus README). All product/benchmark/native bytes equal72e01; actual ordinary-repair and its CI workflows were untouched.

This branch's workflow is workflow_dispatch ONLY, contents:read, fixed Ubuntu24.04, existing pinned checkout/setup-go/upload SHAs, no PR/push/self-hosted/input-ref execution. The job and bootstrap reject another branch/event/repository/architecture. Checkout records the actual reviewed harness `github.sha`. Bootstrap creates separate clean detached worktrees for literal baseline5bca87f and candidate72e01e; neither arm contains the diagnostic branch changes.

Collector schedule/parser/quietness/control algorithms are unchanged: fourteen calls, three AB/BA pairs each of original durable pointer Checkpoint16x and production foreground-churn Vacuum1x, then candidate-only cohort8/16 at128x. Exact selectors, GOMAXPROCS2, affinity0,1, ten-second2% preflight/250ms sampling,120s Go child timeout,150s outer cap, original metric/oracle requirements, no retries. A missing affinity or noisy hosted runner refuses/fails rather than broadening eligibility. No existing raw/PRIMARY/M0 acceptance gate changes.

## Complete adaptations versus reviewed02

1. **Collector:** exactly two textual replacements: hardcoded host185 D becomes RUNNER_TEMP/mvcc-maintenance-72e01; authorization path becomes that root's authorization.json. Reversing these replacements yields byte-identical reviewed collect-02.py. schedule/parse/selftest ASTs are identical.
2. **Preparer:** same D relocation. Same sources/heads/roots/build argv/environment and four600s build limits. Replace subprocess.run(timeout600) with a fresh process group +600s wait + kill/join on timeout/interruption, retaining original argv/exit/raw hash/interruption receipt before refusal. This improves failed-build evidence/control; no benchmark body or measured boundary changes. Exact differential retained.
3. **Hosted bootstrap:** new platform-only orchestration. Checks manual context/clean exact harness; records harness SHA/files separately from both product SHAs. Verifies given Go1.26.3 executable hash `d68b7abbc40d0844f673f6cf06ae3cded225c50437c6454fa37ef178d079fe65`; retains driver/compile/link bytes and full toolchain archive. Creates fresh task cache/tmp paths, clean exact product worktrees, downloads modules with normal proxy/sumdb verification, verifies and records graphs, rejects source drift, then runs original build/freeze and collector with GOPROXY/GOSUMDBoff. Drivers run within bootstrap so a second supervisor cannot kill them before their own child-group cleanup. The manual dispatch produces a schedule/collector-hash-bound authorization receipt; this is not an independent review verdict and is valid only after root's external review/grant prerequisites.
4. **Failure custody:** bootstrap command/stage receipts and final available source inventories persist on failure. Always-upload includes all available manifests, SDK/compiler, four ELFs/build logs/info, harness source (hidden .github included), raw attempts and quiet-process telemetry. Expendable caches/product trees are represented by full manifests rather than uploading them wholesale. A failure before an artifact exists cannot produce it; setup-action failure retains the initial scope/SHA marker and platform action logs. A forcibly terminated runner can prevent always-upload; no success is inferred.
5. **Infrastructure:** ephemeral hosted Ubuntu CPU/storage/image replace185; actual image/CPU/disk/memory and process telemetry retained. No persistent runner/task cache is claimed and no actions/cache used. setup-go may use hosted tool cache; exact compiler hash is mandatory. Workflow120-minute outer job bound includes network preparation,4x600s build caps,14x150s child envelope and overhead/upload. Individual network/bootstrap commands also have bounded receipts; no retry loops.
6. **Local controls:** pure syntax/receipt/manual-context/workflow tests and independent YAML parse added; README states finite sample and infrastructure limits. No product test/benchmark body or production instrumentation added.

## Validation / exact artifacts

`validation-final.json`: seven pure test groups PASS (original positive/six negative parser controls plus checkpoint/cohort completeness, fixed schedule/inputs, nine manual-context refusals and eight workflow-negative controls); independent PyYAML BaseLoader structure parse PASS; syntax and git diff whitespace check PASS. Source before=after=freeze,10251 files. No Go/build/test/benchmark/network/dispatch runtime. The earlier first static-check attempt caught an overly broad word match on the step title “inputs”; it was fixed to reject input expressions, then final checks passed. No runtime attempt occurred in that development failure.

- `freeze-final.json`, `changed-files-final.json`, `before.json`, `after.json`: exact source identities.
- `final.patch`, `frozen-source/`: complete six-file review packet including new files.
- `equivalence.json`, `collect-vs-reviewed.diff`, `prepare-vs-reviewed.diff`, `reviewed-inputs/`: original reviewed hashes and complete adaptation evidence.
- `static-tests.raw.txt`, `workflow-parsed.json`, `validation-final.json`: final local controls/argv/results.

Prior review inputs remain `ordinary-review/maintenance-harness-5699.md`, `maintenance-driver-5699.md`, `review-platform-successor.md`. Existing host185 strict preflight refused before any benchmark (executed0); that refusal is preserved, not relabeled. This packet grants no remote execution; root must freeze a real harness commit, obtain independent exact-source review and explicitly authorize dispatch. No PR/merge/new release is requested.

A quiet successful future packet remains bounded diagnostics with timings_qualify=false/root disposition pending. It is not sustained/universal cost acceptance, M0 stability, native qualification, current-head CI or mergeability. Three matched pairs, one cohort event per size,250ms sampling limits, vacuum's missing full digest/physical-bytes/shrink metrics and hardware differences remain explicit. Current product CI/cost status stays root-owned.
