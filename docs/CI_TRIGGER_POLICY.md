# CI events and native qualification

An ordinary PR is tested on GitHub's merge candidate. It is not the same checkout as a raw push to its source branch. The following rules avoid running both as if they were interchangeable, without claiming that an ordinary smoke is a full qualification.

| Workflow | Ordinary working-branch push | Pull request | Main push, tag push, manual dispatch |
|---|---|---|---|
| Format, Windows HashDB | Run on the branch head; superseded ordinary heads can be cancelled | No extra run (upstream push-only behavior unchanged) | Full |
| Root: vet+test | No extra run | Linux tests, docs, Python and native-client integration; macOS on admitted changes | Full Linux and macOS |
| unified_bench: vet+test | No extra run | Linux tests and pinned normal/race steady-rusage evidence; macOS on admitted changes | Full Linux and macOS |
| redisserver: protocol bench | No | No Docker comparison; Root's ordinary native TCP tests still exercise HashDB and TreeDB | Full five-server benchmark and results archive |
| unified_bench: suites (gates) | No | Regular unified-bench tests execute the 2k-key flushthrash, longmix and bigkeys smoke cases | Full suites, including 1M-key guard with its wall/RSS limits and all three profile CLI runs |

Other workflows, including **TreeDB: vet+test**, keep their own events, shards, permissions, files and required gate. No setting or hardware permission is changed. An ordinary bare feature branch now has fewer checks until a PR is opened; named exact qualifications need not wait for a PR.

## Which native jobs can be omitted?

An ordinary PR can avoid the **two Root macOS reservations** or the **one unified-bench macOS reservation** only when the accepted base policy positively recognizes the complete change. Compiled dependencies matter: shared modules, non-test TreeDB implementations, tooling, workflow changes and unknown inputs are not a blanket macOS exemption. A Markdown suffix alone is not proof, either. Root and unified use separate scopes because their tests and runtime fixtures differ. Linux coverage and TreeDB's own Darwin checks are not selected by this rule.

The current exemptions apply only to edits of existing regular files:
- Both lanes: `CHANGELOG.md`, `TODO.md`, `TENTATIVE_DELETIONS.md`, or ordinary `TreeDB/**/*_test.go` in the root Go module. The Root workflow's initial package listing does parse TreeDB test headers. The unchanged TreeDB macOS PR shards still load and test those packages. That includes Go package-header validation that might otherwise fail during dependency loading in Unified. Root’s portable `make docs-check` still runs on Linux, including its recursive TreeDB source scan. Neither is evidence that an omitted macOS job ran.
- Unified only: equivalent `HashDB/**/*_test.go` edits (Root still loads and tests HashDB on macOS), or `TreeDB/docs/spec/backup-restore.md`, `TreeDB/docs/spec/no-wal-fast-audit.md` and `docs/benchmarks/ycsb_mongodb_treedb_current.md`. The known prose files are not Unified runtime inputs; Root's docs check still runs. If a native suite starts consuming a previously exempt file, update the accepted scope policy with that consumer change.
- File additions, removals, renames, mode/type changes, fixture and vendor paths, nested-module test sources, all production sources, workflow/harness changes and mixed changes containing anything unknown keep native coverage. These are not path filters on the PR workflow.

For a same-repository PR, the short Ubuntu router verifies that the checkout is exactly the event's two-parent merge, then runs the base commit's classifier against its first parent and tested merge tree. It records `full` or `skip` in the run summary. No changed-files API or truncated file list participates. On a fork, deliberately retained PR, main, tag or manual run, the native jobs run without consulting a candidate policy. A missing helper (including the first rollout PR), shallow/inconsistent history, failed checker, unexpected output or unrecognized edit retains native coverage. A positive `skip` means **not run**, never native success evidence.

The existing visible check names stay literal: Root `ubuntu-latest-1/2`, `macos-latest-1/2`; unified `ubuntu-latest`, `macos-latest`. There is no new synthetic success or serial aggregate. The existing `TreeDB required gate` is unchanged. These source rules do not install or change GitHub branch protection.

## Exact evidence and cancellation

For full qualification at a chosen revision, dispatch the relevant workflow at a branch or tag that already contains its `workflow_dispatch` trigger, after that workflow exists on the default branch. Verify the recorded run SHA. Pushing a new tag can exercise the full **push** route if the tagged revision already contains that workflow’s tag filter. An old tag does not gain a trigger retroactively; keep its archived result or use its original qualification route. For example: `gh workflow run redis-bench.yml --ref <retained-tag>` and `gh workflow run unified-bench-suites.yml --ref <retained-tag>`. These produce real new measurements, not a replay of prior evidence. Existing archives and manual single-use/dedicated qualification workflows are untouched.

Ordinary Root and unified PR runs share a concurrency group only with other events for the same PR **in that workflow**. For a deliberately retained qualification, include `[ci retain]` in the PR title **before** opening or synchronizing it: that run gets a unique group and full macOS execution. Editing a title does not retroactively reclassify or trigger an existing run. On the two remaining all-branch push checks, default/protected branches, tag runs, manual dispatches and retained evidence are isolated by run ID. Raw coalescing applies only to the ordinary working prefixes `fix/`, `feature/`, `feat/`, `docs/` and `chore/`, or a source commit explicitly marked `[ci routine]`. Other branch families, including `codex/`, `ci-proof/` and `codex-proof/`, retain raw results by default. Adding `[ci retain]` overrides either ordinary rule. Neither marker expands the benchmark schedule; use the explicit full route for that. Deletions and incomplete push identity also remain isolated. A later eligible routine run on the same branch can cancel its predecessor. GitHub schedules by admission to the concurrency group, not guaranteed chronological commit order. A unique group on retained runs is important: even a noncancelling shared group can evict an older pending run.

GitHub treats concurrency groups as case-insensitive. Avoid creating routine branch names differing only by case; mark an exact head `[ci retain]` when its independent result is needed. Verify the recorded SHA before relying on a supersession result.
