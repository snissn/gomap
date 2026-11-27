# Repository agent notes

## TreeDB storage and compatibility

- TreeDB is pre-alpha: APIs and disk formats may change without backward
  compatibility; rebuild experimental databases when needed. Update documentation
  and tests for format changes rather than adding migration scaffolding now.
- The value log is persistent storage, distinct from the redo WAL; it is not an
  ephemeral journal. Large values may be referenced by long-lived index pointers.
  Never truncate or delete a value-log segment because of age: remove it only
  when unreachable or safely replaced by rewrite/compaction. These rules hold
  even when WAL is disabled and value-log writes are deferred to a flush.
- TreeDB has no legacy slab value storage; update or remove documentation that
  says otherwise. HashDB still uses slab segments.
- Test pointer durability across checkpoint or synchronous write and reopen, and
  GC removal of unreferenced segments while retaining referenced ones. Existing
  tests are in `TreeDB/reopen_verify_test.go` and `TreeDB/db/vlog_gc_test.go`.
  `TreeDB/node/leaf_density_test.go` benchmarks prefix-compressed leaf density;
  adjacent tests enforce compression effectiveness. See `TreeDB/AGENTS.md` and
  the [TreeDB specs](TreeDB/docs/spec/README.md) for storage and recovery details.

## Benchmark profiling contract

Use `unified-bench -profile-dir` and the [profile workflow](cmd/unified_bench/README.md#standard-profile-workflow-benchprof);
`benchprof` runs automatically and can also be run separately. Preserve the
results, CPU, allocation, checkpoint CPU, block/mutex, and trace artifacts.
When benchmark names, profile names, or defaults change, update parsers and tests
in `cmd/unified_bench/main.go`, `cmd/benchprof/main.go`,
`cmd/benchprof/main_test.go`, and `cmd/unified_bench/profile_artifact_dir_test.go`, plus both
`cmd/unified_bench/README.md` and `cmd/benchprof/README.md`. Standalone package
profiling may use a dedicated fresh-process script; document its benchmark names,
artifacts, and reproduction in both READMEs and do not label its Go test profiles
as benchprof inputs. The focused collection insert compression recipe is in the
[unified-bench README](cmd/unified_bench/README.md#focused-collection-insert-compression-profile).

## Pre-push gate & CI tiers

Before every `git push` to a PR branch, run this local pre-push gate so hosted
runners are never burned on avoidable format, lint, or doc-contract failures
(passing `-trimpath` explicitly so parallel worktrees share compiled actions in
a persistent host `GOCACHE` without setting global `GOFLAGS`):

```sh
unformatted="$(gofmt -l $(git ls-files '*.go'))" && test -z "$unformatted"
go vet -trimpath <affected-packages>
go test -trimpath <affected-packages>
# If any docs, README, or spec files changed:
make docs-check && go test -trimpath ./TreeDB/docs && go test -trimpath ./TreeDB/collections -run '^TestTypedStorage'
```

CI uses a 3-tier pre-alpha pipeline (`.github/workflows/ci.yml` + `.github/workflows/ci-full-sweep.yml`)
reporting a single fail-closed `CI Gate` check:

- **Tier 0 (Docs-only PRs)**: `docs-check`, `TreeDB/docs`, and `TestTypedStorage*` (`<1 min`).
- **Tier 0.5 (Draft PRs)**: Preflight-only (`gofmt`, affected `go vet`, and compile check, `~1 min`) until marked Ready for Review. Keep WIP iterations in Draft PRs and batch review fixes + base rebases into one coherent push (hold non-urgent pushes while `@codex review` is still running).
- **Tier 1 (Surgical Ready PRs)**: Fast `preflight` (`gofmt` fails fast before fan-out), parallel `go vet` and `CGO_ENABLED=0` Linux cross-compile checks for `darwin`/`windows`, dynamically sharded affected-cone Linux tests, and split-sharded directly-touched package `-race` (`~3–6 min`). Platform/durability/mmap/`go.mod` changes automatically escalate targeted affected-package tests to `macos-latest` and `windows-latest`.
- **Tier 2 (Full Cross-OS + Full `-race` + Suites/Perf Sweep)**: Runs on an every-8h schedule (skips when `main` is unchanged), manual `workflow_dispatch`, or on PRs opted in via the `ci:full` label or `[ci full]` in the PR title. Apply the `ci:full` label to trigger Tier 2 immediately on an already-open PR without pushing (or include `[ci full]` in the title at open, push, or ready-for-review; `pull_request: edited` is intentionally not subscribed so PR body edits never re-trigger CI or overwrite `CI Gate`) when touching high-risk storage/OS/durability internals or running post-sprint qualification.
