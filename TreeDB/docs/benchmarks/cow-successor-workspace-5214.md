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
already supports admitted traversal reuse for direct raw points. The initial successor change
tries that owner's mutex; a busy owner uses the original per-cut path. The later
parallel extension is described below.
Copy-out finishes before unlocking. Snappy/LZ4 stay on the maintained bounded
block-decoder path, while generic codecs and dictionary authority retain their
existing policies. The exact basis owns the lazy scratch and any block decoder
until final release; multiple retained bases remain separately budget charged.

Batch arenas have the wrong last-consumer boundary. The ordinary tree iterator
pool does not carry COW admission or successor state. Reusing exported snapshot
identities would let stale closed aliases refer to a new owner. None is used.
The initial change adds no arena, pool, publisher, on-disk format or durability
boundary. The later extension adds a fixed set of admitted basis scratch owners.

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

### Parallel physical-pointer reuse and unused iterator leases

W3 tooling `bb966ad3bb942fe9ac0bd4dc9cc1ae95e9e0cd09` returned PRIMARY,
Stats and snapshot-iterator PASS. Its raw repeated-iterator paired time failed
+7.15%, and the COW pointer/depth1/concurrency8 cell failed +7.078%. All 60 COW
rows remain retained. The pointer-prefix bytes control passes at 64300 to 64279
B/op, with 25 allocations unchanged. These are results for W3, not the next
candidate. The separate M0 workflow failed all three original strict CV limits.

The next reuse change keeps four independently admitted scratch owners per
physical basis: the original owner and three fixed opportunistic slots. It never
waits for a busy slot, never expands the slot array, and preserves an already
admitted cut-local traversal before trying shared storage. All workspace, codec
and buffer storage is budget charged; the additional 48-byte amd64 basis header
is charged once per retained physical basis. Every slot closes before that
basis releases its original snapshot custody. Busy or refused admissions retain
the complete cut-local fallback.

An uncompressed physical pointer winner now reuses the selected basis owner's
record buffers and copies its final value before unlocking. Queued pointers keep
the complete cut resolver. Compressed pointers and refused buffer growth carry
only a caller-local inspected file/shape into that original resolver, avoiding
duplicate metadata reads while preserving dictionary and shared-codec authority.
The retained cut pins the file during this handoff. A fresh owner that refuses
value growth detaches and refunds before local fallback, including when the
local reader already owns input but no traversal scratch. Corruption, inspection
limits and pointer binding still use the maintained record reader.

The ordinary empty-queue forward/reverse iterator also omits a tracked memtable
view retain that was immediately released before backend/root construction.
Published roots remain immutable; nonempty queues keep the original tracked
view lease. Old-runtime controls are red, 47 owning normal/race controls pass,
and three local diagnostic pairs improve time 37.78% at unchanged 112B/1alloc.
Those diagnostic results do not replace the W3 failed gate.

The unchanged public COW fixture, all ten cells, 1024 total calls, three alternating
pairs, original admission/quiet rules and numerical guards remain authoritative.
Workflow scope now includes the physical read workspace and point-successor
representation files so later changes to these seams cannot miss this check.
The COW repair runtime was frozen at
`d4a2745ec52b2eae88ff398603fbde511b344955`. All COW caching controls pass
normally, and focused caching/public MVCC controls have normal/race coverage.
The final portable focused race controls pass (74 cases), all COW normal controls
pass (380 cases), and Windows test cross-compilation passes. The caller-local
inspection descriptor does not escape. Windows execution and the rebound tooling
qualification remain required. No final-source timing result is asserted here.

The final review repair additionally records the existing no-op cooldown after
a successful below-floor value estimate when leaf GC is unavailable. A genuine
old-product control fails; five affected scheduler controls pass normally
(0.524s) and under race detection (1.608s), including actual checkpoint avoidance,
cooldown expiry, leaf progress and errors. The ownership specification now
distinguishes application DATA mutations from checkpoint installation lifecycle
operations. Neither repair changes the COW read implementation. The complete
runtime is frozen at `d250f098c286ba639822d61d8c15ed860dc2764c`; tooling is
rebound to that source with every original fixture and numerical guard unchanged.
Earlier local and hosted results retain their original source bindings; final
current-head CI and review remain required.


### Completed-parent DATA barrier reuse

The next frozen runtime is `a0e35df9b10a0ede99c4ff06291f582afbeaa09b`.
For PRIMARY V6 publication with no inherited DATA writes, it can reuse a
completed parent DATA-sync receipt bound to the exact physical identity, pager
incarnation, selected parent and unchanged DATA/allocator projection. Open
starts without that receipt; materialization, mismatch and ambiguity retain
the ordinary DATA barrier. PRIMARY dependency/namespace/body/fence syncs and
original resource custody remain. The durability specification documents the
proof; no format, public API, default or ACK boundary changes.

The original public-path regression fails against the preceding runtime with
one unnecessary DATA barrier. The new physical-cut, materialization, reopen,
inherited-debt/retry, identity, failure and selective-writeback controls pass
normally and under race. The maintained tagged public generation control also
passes. Independent review accepts the exact source batch.

One trace of the unchanged 128-iteration public PRIMARY fixture records DATA
syncs falling from 133 to 3 and unchanged 266 PRIMARY syncs, including
setup/calibration/Close. A separate single matched allocation-only discriminator
(GOMAXPROCS=1, ordinary GC) records baseline 103,041 B/op and 374 allocs/op
versus candidate 98,818 B/op and 372 allocs/op. Both are developmental
controls, with no quiet-host or acceptance claim. The earlier GOMAXPROCS2
candidate observation of 105,045 B/op is retained separately.

The published W5 gate failure (+8.100% paired PRIMARY time), COW quiet-host NULL
and historical rewrite cleanup failure remain retained. New exact-head original
CI and review are required; these controls do not replace them. Only the
frozen-product binding and documentation/inventory are refreshed here; fixture,
workload, numeric/noise gates and original raw/PRIMARY/M0 checks are unchanged.

### Cold compressed-value handoff and Stats formatting

The complete runtime is now frozen at `24830c7eac5cab1a7b0595278c0eafabea74528c`. A cold primary physical reader now refunds freshly admitted traversal scratch before handing a compressed-value inspection to the cut-local decoder. The retained cut still pins the inspected file; warm readers, codec authority and original capacity checks remain unchanged. The existing constrained-budget regression now covers both idle primary and busy-primary parallel handoffs. Its idle-primary case fails on the old runtime; focused normal and race controls pass.

Public Stats formats its nine residency integer fields directly with strconv rather than the generic formatter. Signed decimal values, field names, samplers and synchronization are unchanged. Owning normal and race controls pass. One matched old/current public Stats diagnostic records 882,765 to 882,733 B/op and 2,852 to 2,844 allocs/op. It has no quiet-host or acceptance claim.

At the preceding W6 tooling head `029eeb9d4e07cfaa37dcf5552c73a790d2e94ad4`, the original raw controls, PRIMARY (+2.798% paired time) and all 60 COW rows pass with their original gates; PRIMARY and COW have quiet-host witnesses. Stats fails its strict byte guard by one median byte (882,719 to 882,720 B/op), despite removing 38 allocations. The new formatting diagnostic does not replace that failure.

W6 also fails the RabitQ exact-allocation assertion and all three legacy vacuum timing-CV limits. The RabitQ source is unchanged from the baseline; one isolated diagnostic passes, and compiler escape evidence rules out the measured stats literal escaping on the success path. The original allocation failure remains unexplained. Vacuum operations complete without unexpected errors, but stability remains failed. No fixture, timing/allocation/noise limit or failure classification is relaxed, and no selective original gate is rerun. The new reviewed runtime requires its own original current-head CI and review; neither these diagnostics nor an isolated passing test waive earlier failures or complete final graph qualification.

### Complete-cut capacity retry with warm workspace reuse

Runtime `f495aa04e9daf1f8cc7ccdb1adb46faf54129b9c` resolves fresh
dictionary, queued-pointer and direct-point handoffs without discarding useful
scratch after every read. An identity token borrows the fresh owner under the
original retained cut. If the original cut-local admission or materializer
returns capacity refusal, it can refund only the matching owner and retry that
same phase once. A stale token cannot close a replacement or authorize a retry.
Direct-point admission preserves an exact spare cut reference because Snapshot
admission consumes its input even on failure. Successful handoffs and logical
misses preserve reusable traversal. Existing warm owners, dictionary/codec
authority, inspection integrity and complete-cut file custody remain unchanged.

The old published runtime genuinely fails the constrained public controls.
The final repair passes owning normal (5.608s) and race (7.120s) controls,
including idle/busy dictionary and queue handoffs, all four direct-point APIs,
view-refusal reference/accounting cleanup, warm reuse and stale identity. A
source-reviewed intermediate retry discarded scratch on logical misses; its
specific regression is retained and fails causally before the final exemption.

The first local repair unconditionally refunded fresh scratch and passed its
correctness controls, but increased both direct-pointer and dictionary reads by
11,072 B/op and four allocations. That version was rejected before publication.
One fixed-count old/final public comparison of the selected replacement records:

| Public shape | Old B/op | Final B/op | Old allocs/op | Final allocs/op |
| --- | ---: | ---: | ---: | ---: |
| GetAppend physical pointer | 33,376 | 33,376 | 9 | 9 |
| GetAppend dictionary leaf | 87,049 | 87,049 | 53 | 53 |
| Fresh-cut successor queued pointer | 189,032 | 189,032 | 11 | 11 |
| Fresh-cut successor dictionary leaf | 87,169 | 87,169 | 55 | 55 |
| Fresh-cut successor compressed value | 62,889 | 51,817 | 14 | 10 |
| GetAppend missing key | 0 | 0 | 0 | 0 |

Each shape uses 16 warmup calls and 1,024 timed calls, ordinary GC,
GOMAXPROCS=1 and matching fixtures, toolchain and inherited affinity. Binaries
are prebuilt outside timing; byte/key result checks remain in the loops.
Compiler diagnostics confirm the fresh-owner parameters do not escape. This
single pair is an allocation/reuse discriminator without a quiet-host or timing
acceptance claim; it does not replace original CI or final Dgraph qualification.

The preceding W7 head `7b25ed187fa4dfb5c3ed6711b31624fe7d82245c` passes
all 54 reported original checks: raw controls, quiet PRIMARY (-1.678% median
paired time), all 60 quiet COW rows, quiet Stats, original M0 and the owning
RabitQ shard. Its later dictionary-capacity review finding required this final
repair, so those checks do not certify the new head. The macOS dependency
bootstrap TLS failure occurred before tests and remains retained alongside its
successful bounded infrastructure retry. Earlier W5/W6 failures and nulls are
unchanged. Only runtime binding, source-discovery inventory and evidence are
refreshed here; no workload, fixture, numeric/noise gate or original execution
policy is changed. New exact-head original CI and mature review remain required.

### Disposable performance-runner qualification

At W8 `29a2fdf153ce6e214abf9502d7970de727a38967`, the original COW
capture completes all 60 rows with every numeric guard passing, including the
intended four/eight-allocation improvements. Its qualification remains NULL:
the continuous quiet-host witness records external `provjobd6206737` PID 2168 at
9.91% CPU over 14.83 seconds. That original capture remains evidence; neither
its numeric results nor the other passing component gates waive its noise
requirement. An unchanged rerun hoping for a passing witness is not the remedy.

Infrastructure issue [#5221](https://github.com/snissn/gomap/issues/5221)
tracks this environment repair. The existing combined `mvcc-raw-path-gate`
job now supports an optional
administrator-controlled repository variable, `TREEDB_PERF_RUNNER_LABEL`.
Only a `pull_request` whose head repository equals this repository, with that
variable nonempty, uses its runner label. Fork PRs, other event types and an
unset/empty variable retain `ubuntu-24.04`. This routing change leaves every
job step, fixture, duration, workload, binding, numeric threshold and continuous
noise check unchanged; it does not establish that the selected runner is quiet.

The administrator must set a unique label for a disposable Ubuntu 24.04 VM
runner registered for one job, then delete the VM and remove the variable.
This opt-in must never select a persistent runner or run contributor fork code
on a persistent host. Registration, source selection and runner disposal remain
explicit operational responsibilities; the variable alone cannot enforce the
VM's lifecycle. Qualification still requires the unchanged capture and its
original witness to pass on the selected environment.

This is an infrastructure remedy for an observed external-process disturbance,
not a new product optimization or a waiver of W8's NULL. Runtime P8
`f495aa04e9daf1f8cc7ccdb1adb46faf54129b9c` remains unchanged. The existing
Dgraph W8 dependency stays a valid runtime ancestor if all Go sources and
module files remain byte-identical; it does not acquire new qualification from
a later tooling-only head without its own source-bound evidence.
