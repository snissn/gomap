# Contributing

TreeDB is the repository's main product; HashDB is the older experimental and
comparison engine. Start with [README](README.md) for usage and
[AGENTS](AGENTS.md) for the code map and critical boundaries.

## Setup and common commands

Use the Go version declared in `go.mod` (CI reads that file), Git, Make and a
working dependency download path. Python 3 runs the workflow contract tests.
Linux/macOS are convenient local environments; CI also exercises Windows.
See [getting started](docs/GETTING_STARTED.md) for examples.

```sh
make deps                 # download root-module dependencies
make build-native-server  # build just the native server into bin/
make check-nativewire     # build + vet + existing native-wire/CLI package tests
make workflow-check       # Makefile orchestration tests; no Go downloads
make docs-check           # documentation invariants
make test                 # go test ./... once, from the root module
make vet                  # go vet ./... once, from the root module
```

`make help` lists other commands. `make build` includes both TreeDB servers and
the existing tools. `make fmt` formats tracked Go files; prefer formatting only
changed files when other work is present. `make hooks` is an optional local
formatting hook, not a prerequisite. Do not run `make tidy` as unrelated cleanup.

TreeDB and `cmd/unified_bench` use the root module, so repeating `go test ./...`
from those directories does not add coverage. Their focused Make targets remain
available. Separate nested modules (for example `TreeDB/integration/gethethdb`)
are not included by root-module `./...`; follow that module's guidance when it
changes. Tagged/external-service tests likewise need their documented harness.

## Completion and review

Before editing, state the user-visible outcome and a small reproducer or
acceptance test. Inspect the existing code, applicable instructions and open
work first. Trace the necessary caller/interface, handler/service, persistence,
worker and external-service boundaries; mark absent layers as not applicable.
A local library test does not by itself verify a running server or a cluster.

One owner carries the change through implementation, integration and evidence.
Delegate only independent, bounded work that saves effort; keep integration
with that owner and avoid recursive delegation. Reuse an existing issue/PR and
its evidence rather than creating a parallel tracking system for a small fix.

Match verification to risk:

| Change | Start with | Broaden when needed |
| --- | --- | --- |
| Documentation | `make docs-check`; verify edited links/commands | Validate any behavior claims against code/tests |
| Makefile/workflow | `make workflow-check` | Run the real affected command and inspect its CI result |
| Native-wire feature | `make check-nativewire` | Affected collection/storage tests; standalone/TCP test for startup or transport changes |
| Storage, durability, ownership | Affected invariant and reopen/recovery tests | Race tests, crash/corrupt-tail coverage, relevant platform jobs |
| Performance | Correctness tests plus matched before/after benchmark | Same workload, profile, hardware and retained raw evidence |

`make test-race` covers selected engine packages only. CI contains broader
platform/race shards and dedicated gates; `.github/workflows/` is the source of
truth for their selection and commands. Keep existing regression protection.
Removing a check needs evidence that its protection is obsolete or duplicated,
not merely that it is inconvenient or red.

Review the diff once against its outcome, invariants and test evidence. Load
only applicable [review playbooks](review-prompts/README.md). Distinguish
blocking defects from optional improvements; do not turn unrelated cleanup into
a completion gate. When a failure repeats, inspect its logs and root cause,
compare with the base when relevant, and change the approach instead of
repeating the same attempt without new evidence.

In the PR, report the outcome, affected path, exact commands and tested commit,
pass/fail results, and what remains unverified. A pre-existing CI failure is
still a blocker to claiming all-green verification, not permission to suppress
it. Re-resolve the live head, checks, reviews and unresolved threads before
claiming merge readiness. Stop when the bounded outcome is sufficiently
verified; merging and deployment still require authorization.

## First feature pilot: native-wire publication and reopen

User outcome: inserted BSON documents and their unique secondary-index entries
remain readable before flush, after explicit checkpoint, and after reopening
the same database directory. `make check-nativewire` builds the shipped command
and runs the existing native-wire and CLI package tests without the test cache.

The CLI wiring is in `cmd/treedb-native-server/main.go`: it opens the cached
backend, constructs the collection manager/document service and serves native
wire. The persistence pilot is
[`TestNativewireYCSBForcedPointerPublicationReadability`](TreeDB/nativewire/forced_pointer_readability_test.go):
concurrent in-process clients -> native-wire handlers -> collection/index
operations -> cached/backend storage -> persistent value-log pointers ->
`FlushAll`/`Checkpoint` -> close/reopen -> document and index readback.
The same file also tests current-writable read barriers and injected EOF errors.

This pilot uses `command_wal_relaxed` with an explicit checkpoint. It does not
prove ordinary durable-profile acknowledgements, standalone daemon startup or
signal shutdown, abrupt power-loss recovery, Mongo compatibility, cluster
behavior, or an external benchmark. No MongoDB or other external service is
required. Use the existing relevant tests/harnesses when changing those paths.

## Code and storage contracts

Keep the stable surface small: `github.com/snissn/gomap/TreeDB` and
`github.com/snissn/gomap/HashDB`; see [API stability](docs/API_STABILITY.md).
For changes to durability, iteration, locking or ownership, update the relevant
`docs/contracts/` and [TreeDB specs](TreeDB/docs/spec/README.md) with their tests.
On-disk changes need reopen/crash and truncated/corrupt-tail coverage, explicit
format/version decisions, and an API stability/changelog note for intentional
breaks. TreeDB remains pre-alpha; do not add speculative migration machinery.

## Benchmark Profiling Workflow

Prefer the existing capture and analysis tools:

```sh
make unified-bench benchprof
OUT=$(mktemp -d /tmp/gomap_profiles_XXXXXX)
# Supply the workload arguments being investigated:
./bin/unified-bench -profile-dir "$OUT"
./bin/benchprof -profiles-dir "$OUT"
```

The profiling contract includes `benchprof_results.json/md`,
`cpu_<test>_<db>.pprof`, `allocs_<test>_<db>.pprof`,
`checkpoint_cpu_checkpoint_<test>_<db>.pprof`, `block.pprof`, `mutex.pprof` and
`trace.out`. Changes to names, flags or defaults must keep producer and parser
in sync: `cmd/unified_bench`, `cmd/benchprof`, their READMEs,
`profile_artifact_dir_test.go` and `cmd/benchprof/main_test.go`.

For collection insert/compression profiling, use the existing
`scripts/treedb_insert_compression_profile.sh`, not a new harness:

```sh
RUN_DIR=/tmp/treedb_insert_compression_profile_$(date +%Y%m%d_%H%M%S) \
  scripts/treedb_insert_compression_profile.sh
```

It captures template-v1 `BenchmarkCollectionShapeInsertBatch` under `auto/`,
optionally compares `off/`, and emits `benchstat_auto_vs_off.txt` when benchstat
is installed. Inspect `auto/collections_report.md`, CPU/memory `.pprof` files
and their `_top.txt` summaries. Useful overrides are
`COUNT=1 BENCHTIME=1000x RUN_COMPRESSION_OFF=false` for smoke,
`INDEXES_REGEX='0|1|2|3'` for all shape index counts, and `RUN_TIMED_CPU=true`
for the timed CPU-only insert profile. Do not claim performance improvement
without matched measurements.
