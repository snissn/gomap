# COW successor workspace diagnostic (#5214)

This packet investigates the actual Dgraph point-read scratch allocation; it
does not establish final server throughput, RSS, recovery or merge acceptance.
The source anchor is `857890de8dec2cae5d9047b99bc7d5373c748990`.
Whole-process cumulative Dgraph allocation samples put 39.91MB flat at
`cowReadWorkspace.pointScratch`. That sample includes setup and diagnostics and
is not timed bytes/op, live memory, or an estimated throughput improvement.
The separate 26.92MB owned-iterator allocation is outside this implementation.

## Ownership and selected maintained reuse

Public `Store.GetAt` captures and closes one physical read cut per call. It uses
the captured successor, not the direct raw `DB.Get` optimization. Previously,
a nonempty backend basis required a point scratch allocation on each fresh cut.
The original `cowBackendBasis.physicalWorkspaceLocked` and `pointScratch` seam
already supports admitted traversal reuse for direct raw points. Successors
now try that owner's mutex; a busy owner uses the unchanged per-cut path.
Copy-out finishes before unlocking. Snappy/LZ4 stay on the maintained bounded
block-decoder path, while generic codecs and dictionary authority retain their
existing policies. The exact basis owns the lazy scratch and any block decoder
until final release; multiple retained bases remain separately budget charged.

Batch arenas have the wrong last-consumer boundary. The ordinary tree iterator
pool does not carry COW admission or successor state. Reusing exported snapshot
identities would let stale closed aliases refer to a new owner. None is used.
No new arena, pool, publisher, on-disk format or durability boundary is added.

This is a representation optimization: baseline logical behavior should already
pass. Activation and allocation evidence replace a fabricated behavioral red.
The original independent-reader test is preserved, including its successor case.

## Predeclared early A/B

Compile the exact same `cow_successor_workspace_external_test.go` harness against
the anchor and candidate using a test overlay. Record the file hash, both exact
source heads, toolchain, binaries/hashes, environment and commands. Prebuilds,
correctness tests and profiles are outside the measurement slot. No samples were
collected at initial construction time; the later hosted result is recorded below.

`BenchmarkCOWSuccessorWorkspace` has ten leaves: inline/forced-pointer GetAt,
depth 1/8, concurrency 1/8 (eight cells), and unchanged prefix iteration at depth
8/concurrency 1 for both value layouts (two cells). Each point operation includes
one complete public `GetAt` and a deterministic result oracle. Concurrent N is
the total number of calls, not N for each worker. One prefix operation consumes
all 32 keys x eight versions, including iterator Close; it is a control, not a
claim of point improvement. The functional fixture also checks exact-key scans.

Use fixed `-test.benchtime=1024x -test.count=1` with identical GOMAXPROCS=8.
Preload/checkpoint, resolved `no_wal_fast/cow_ordered`, drained mutable/queue
state, nonzero physical handoff and actual pointer-layout checks prove route
activation. Compression is off to isolate traversal; focused correctness tests
separately cover cold compressed leaves and dictionary fallback. Every point
and both iterator paths warm outside timing. There is no read/write operation
deferred past the timed call; setup, checkpoint, warmup, stats and Close of the DB
are excluded equally. Automatic checkpoint and periodic vacuum are disabled on
both arms; foreground pruning behavior is unchanged.

Execute three alternating pairs **A/B, B/A, A/B**, where A is the anchor and B
is the candidate: six process invocations, ten leaves each, **60 logical rows**.
Retain all stdout/stderr, failed rows and nulls. Report each row and paired
direction in ns/op, B/op and allocs/op, with ops/s derived from ns/op. These
short runs are a discriminator, not statistical/final performance acceptance.
Do not rerun to obtain significance. Reject material concurrent/time regression
even if bytes improve. Inconclusive or nonadvancing evidence returns to source
comparison; it does not waive the original gates.

After permission for jobs, focused candidate commands are:

```sh
go test ./TreeDB/caching -run 'TestCOW(SuccessorBasisWorkspace|RetainedReadersOwnPhysicalScratch|RetainedPhysicalScratchAdmissionAndRelease|OrderedIteratorDictionaryLeafFallback|InlineSuccessorUsesOwnedWinner|PhysicalLeafCache)' -count=1
go test ./TreeDB/mvcc -run '^TestCOWSuccessorWorkspacePublicGetAtAndHistory$' -count=1
go test -race ./TreeDB/caching ./TreeDB/mvcc -run 'TestCOW(SuccessorBasisWorkspace|RetainedReadersOwnPhysicalScratch|OrderedIteratorDictionaryLeafFallback|SuccessorWorkspacePublicGetAtAndHistory)' -count=1
go test -c ./TreeDB/mvcc -o "$ARTIFACTS/candidate.test"
```

For each prebuilt arm, in the assigned quiet slot:

```sh
GOMAXPROCS=8 "$BINARY" -test.run='^$' -test.bench='^BenchmarkCOWSuccessorWorkspace$' -test.benchtime=1024x -test.count=1
```

Set TMPDIR and all artifact/output paths under the assigned `/mnt/fast4tb` task
directory. These standalone Go benchmark results/profiles are not benchprof
inputs. A viable discriminator still needs unchanged strict PRIMARY/ordinary/COW
guards, applicable races, exact-head review/CI and final public-server evidence.

## Hosted discriminator and pre-execution policy

The local `preflight-01` exhausted the original quiet acquisition with **zero
timed rows**, so its result remains NULL. No historical timing rows are replaced.
The issue and initial plan used “material” without numeric limits. Before any
timing, the coordinator accepted the following explicit policy on 2026-10-23:

- For **each** of the eight GetAt cells and two prefix controls, the median of
  its three paired head/base time ratios must be at most 1.05, and median B/op
  and allocs/op must each be no greater than baseline.
- Both serial depth-eight GetAt cells, inline and forced-pointer separately,
  must remove at least one median alloc/op. Unseparated or smaller improvement
  is insufficient, even if the other cells pass.

These are this diagnostic's pre-execution limits. They do not change PRIMARY
or any final qualification threshold. Three pairs provide a bounded discriminator,
not statistical or product acceptance. A per-cell failure is visible; an average
across cells cannot hide concurrent or prefix regression.

`scripts/cow_successor_workspace_diagnostic.py` runs on an ephemeral hosted
Linux runner using the maintained PRIMARY quiet acquisition, process observation,
ELF/runtime binding and checkout guards. The original hosted comparison used
baseline `857890de8dec2cae5d9047b99bc7d5373c748990`, candidate
`030a377b2513930a59c48116165fdcdbd6320e01` and frozen product
`786d74430bc67b0b71af6b6eccf461ec23948eee`. Only the runner, its Python controls,
scoped workflow, generated inventory and this document differed from that product.
Runtime and all original Go correctness controls were byte-identical. The sole
baseline overlay is the frozen public fixture, SHA-256
`2226735b88900fe2b33178498172d946de6d536e793f045c5b2f8c1a38544d08`;
no new candidate helper or product source is copied into baseline.

The original ten cells, 1024 fixed calls, GOMAXPROCS=8 and six fresh benchmark
processes in AB/BA/AB order remain unchanged. No CPU0 affinity is imposed: the
runner records its actual allowed CPUs and each child's affinity. If fewer than
eight CPUs are available, oversubscription is explicit and the result is not an
eight-core scaling claim. GC and sampler settings are inherited and bound, not
disabled by the driver; the original fixture's checkpoint/vacuum configuration is
unchanged on both arms.

Builds occur before quiet acquisition. One six-minute total deadline covers source
checks, both builds, quiet acquisition, measurement, identity checks and cleanup;
five seconds of that budget are reserved for cleanup. Interrupted child process
groups are killed and their direct children joined. The original full five-second
quiet predicate, 60-second acquisition cap and 2% external-process CPU threshold
remain unchanged. Process observation continues across all six invocations.
Quiet failure, timeout, source/runtime/affinity drift, malformed or missing rows,
command failure and cleanup failure produce NULL and no available guard. A clean
complete run violating any cell or improvement limit produces FAIL. Only complete,
quiet, identity-bound rows satisfying all limits produce diagnostic PASS.

The existing Linux MVCC job runs the diagnostic only for PR head ref
`codex/5214-cow-successor-workspace` with successor implementation, fixture or
runner/observer changes. Later unrelated PRs do not inherit this frozen-product
diagnostic, including later changes to the same source files. Its 15-minute job
budget and existing raw/PRIMARY
commands are unchanged. The diagnostic's six-minute allowance has limited headroom
after those gates; job or command timeout supplies no evidence. Always-run artifact
upload and summary steps retain failures and absent-evidence NULLs. This is not a
new blanket gate for unrelated changes.

## Original hosted result and main integration

Hosted artifact **12509276552**, retained as `ci-030/cow-successor`, contains all
60 original rows and returned diagnostic **PASS**, with quiet and source/runtime
identity checks satisfied. Both serial depth-eight GetAt cells improved:

| Layout | Median B/op, base to candidate | Median allocs/op | Median paired time change |
| --- | --- | --- | --- |
| Inline | 11,720 to 648 | 12 to 8 | -58.916% |
| Forced pointer | 16,880 to 5,976 | 16 to 14 | -25.457% |

Every cell met the unchanged policy. The largest median paired time increase was
parallel inline depth eight, **+4.286%**, within the predeclared +5% limit. Both
prefix controls had unchanged median allocations and bytes. Ratios are paired
per cell before taking the median; they are not ratios of the displayed medians.
The original local NULL and all original hosted samples remain retained. This
result applies to the original heads above, not to the later main integration.

Main `8c392a8e27b8f23e795d07edfb3782ca32d46976` was merged once into that candidate.
The new frozen integration product is
`3dff6da9663bd9fc1f56558926e9aee2127ffb46`: every Go file is byte-identical to
main or, for the scratch product's changed paths, original product `786d74430`.
Both the existing public Stats diagnostic and scoped successor diagnostic remain
in the shared workflow. The following tooling descendant binds baseline to that
exact main commit and product to this merge; it permits the same **five tooling
paths only**. It does not permit runtime or Go fixture/control changes after the
merge. Fixture hash, ten cells, 60 rows, ordering, budgets and numeric limits are
unchanged. The workflow uses the same exact baseline as the runner.

**Integration qualification is pending.** Python/inventory controls and a clean
runtime-union check do not transfer the earlier hosted PASS to the integrated
source. Latest-head ordinary/PRIMARY, successor, applicable races and review/CI
must qualify the integration independently. No integrated Go or timing campaign
was run during source integration.

```sh
python3 scripts/cow_successor_workspace_diagnostic.py \
  --root "$GITHUB_WORKSPACE" \
  --baseline 8c392a8e27b8f23e795d07edfb3782ca32d46976 \
  --candidate "$HEAD_SHA" \
  --output "$RUNNER_TEMP/mvcc-raw-path-gate/cow-successor"
```

Artifacts include the immutable plan, both binary hashes and Go build information,
source/fixture/observer hashes, actual CPU affinity, environment hash, before/after
runtime bindings, all command receipts and separate stdout/stderr, quiet/process
witnesses, up to sixty ordered rows, per-cell paired deltas/medians and the two
separate improvement decisions. Setup, checkpoint, warmup and Close retain their
original untimed boundaries. The hosted result above is confined to its original
source binding. Pure source controls run with:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_cow_successor_workspace_diagnostic.py
PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_mvcc_primary_cost_gate.py
PYTHONDONTWRITEBYTECODE=1 python3 .github/scripts/test_treedb_ci_contract.py
```

## Canonical checkpoint integration

The combined delivery in PR #5209 freezes product
`682de67faf0812a88667fe3f8721b7bba92b7462`: external recovery #5208,
leaf maintenance #5216, raw dependency closure #5218 and this workspace change,
including the corrected DATA/PRIMARY/ordered producer distinctions and the
reviewed power-loss model. The final product also preserves admitted local COW
workspace under shared admission pressure, serializes physical retirement with
manager detachment, safely releases stale shared-manager observations, reuses
governed PRIMARY encoding scratch and removes Stats boxing. Focused causal
normal/race proofs protect those changes; the failed 5d98 performance rows and
contaminated COW diagnostic remain bound to that old source. The baseline remains
`8c392a8e27b8f23e795d07edfb3782ca32d46976`. The runner's five permitted
tooling paths, fixture hash, ten cells, 60 rows, deadlines, order and numerical
gates are unchanged. Its narrow branch selector now names the canonical
integration branch. The tooling-only descendant must qualify this complete
product; the original 030 and 88 evidence remains bound to those earlier
sources. No combined-head result exists at this tooling commit. Earlier strict
PRIMARY byte failures remain failed and are not replaced by the COW result.

### Maintenance and iterator follow-up

The next complete product is
`ed996b2fb4375f4f8e6f19d841af05e3d254206c`. It retains the preceding product
and adds two reviewed changes. Ordinary snapshot iterators borrow their existing
tree iterator for direct forwarding while retaining snapshot invalidation and
the sole close owner. Leaf-generation GC skips segment reachability scans only
when a fresh reconciled manifest contains exclusively writable generations or
sealed generations intersecting current producers. Those states cannot retire
a segment. All other states retain the complete scan; immutable manifest
revision GC always performs its independent fresh root protection.

At tooling head `d20594a712184c565646916c64723c4e2549a6e8`, the unchanged
public raw, PRIMARY and Stats gates passed. All 60 COW rows had quiet-host
witnesses, but pointer-prefix bytes failed (64300 to 64334 B/op). The separate
snapshot seek gate failed at +5.40%, and vacuum foreground-p99 stability failed
at CV 0.9525. Those failures remain bound to that source.

The follow-up's owning normal/race and exact retained-root GC/reopen controls
pass. One six-row public prefix diagnostic activates the no-candidate branch
and reduces median bytes from 64332 to 64277 B/op, with 25 allocations unchanged.
The ordinary iterator diagnostic improves Next about 5%; its 16384-key seek
median is effectively unchanged. These are development observations, not
replacements for the failed hosted gates. The rebound runner must qualify the
complete new product using the unchanged baseline, fixture, cells, row count,
order, resource limits and numerical gates. No result for the new tooling head
exists at this commit.
