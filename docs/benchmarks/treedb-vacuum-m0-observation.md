# One M0 observation diagnostic

This manual harness is intended to land independently before collection. It
observes the fixed product `2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49` and does
not change the maintained M0 workflow, capture script, summarizer, product, or
10% stability limits. The original current-head M0 failure remains a failure.
Independent harness review, an exact landed harness SHA, and a separate grant
for one diagnostic execution are required before dispatch. No retry until PASS.

The workflow `.github/workflows/treedb-vacuum-m0-observation.yml` accepts no
inputs. It runs only by manual dispatch on ephemeral GitHub `ubuntu-24.04`,
with `contents: read`, pinned actions, and no persistent runner or secrets.
Dispatch the reviewed landed harness commit. A separate clean checkout pins
the product above; the harness commit and product commit are distinct inputs.

## Fixed plan and timers

The official Linux/amd64 Go1.26.9 archive is verified against
`42d158b4d8f7b61ac0a830567c940a86098fb7aac52e467a5ebec03ef5cc2f8d`.
The driver also checks the actual Go executable, retains the entire archive,
hashes every SDK file including compiler/linker, and records `go env` and both
pristine/overlay compile-only binaries with build information. Modules are
downloaded and verified before the source freeze; subsequent commands use
`GOPROXY=off`, `GOSUMDB=off`, `GOTOOLCHAIN=local`, `GOWORK=off`, `GOENV=off`.

After preparation, exactly one 10-second, 2%-external-process-CPU quiet
preflight may refuse. Refusal starts zero fixture/benchmark calls. There is no
quiet retry, cooldown, warmup, or sleep between scheduled calls. Accepted
preflight leads to the original two fixture calls followed by
L1,P1,...,L10,P10: fresh `go test ./TreeDB/db` processes, original selectors,
`1x`, `count=1`, `benchmem`, CPU affinity `0,1`, `GOMAXPROCS=2`, and `8GiB`.
The original default per-Go-test timeout remains 10 minutes. The workflow cap
remains 20 minutes including setup; the driver stops at 19 minutes from the
first step, reserving a minute for finalization/upload. This may truncate a
slow diagnostic; it never enlarges the original cap. Timeouts/interruption
kill the complete process group and reap the leader and adopted descendants.

The original capture uses fresh Go driver processes. This harness preserves
that behavior using an explicit `-overlay` and a Python `-exec` seam. The seam
copies/hashes each actual temporary ELF before benchmark setup, then `exec`s
that exact original path and arguments, preserving exit status and signals.
The extra copy/hash creates diagnostic I/O and may alter cache/writeback.
No prebuilt executable replaces any of the 22 calls. All actual binaries,
commands, environments, process IDs/groups and outputs are retained.

The checked test-only overlay changes exactly two call sites in
`benchmarkVacuumIndexOnlineCollectionForegroundChurn`: capture GC counters and
a wall timestamp before `ResetTimer`, then export after `StopTimer`. The
complete timed body and `vacuumCollectionForegroundWriter` remain byte-for-byte
unchanged. The export contains all already-collected durations, zero-based
ordinal and parity-derived point/range type, wall span, and before/after
`runtime.MemStats` including `NumGC`, `PauseTotalNs`, `PauseNs`/`PauseEnd` ring
entries and allocation counters. No timed logging, per-operation timestamp,
extra DB operation, forced GC, changed GOGC, percentile, or new gate is added.
`ReadMemStats` itself can affect later runtime scheduling despite being outside
timing. The foreground goroutine is created inside the loop after ResetTimer,
so it has not started when the initial ReadMemStats executes. Existing setup
and runtime goroutines can still be perturbed. The runtime ring can wrap; available pause timestamps are not a complete
GC-assist or scheduler trace. Ordinals/durations do not give exact individual
wall start times because unmeasured gaps exist between operations.

The public PL06 benchmark and both fixtures have no overlay call-site change.
The shared helper has no initialization side effects. The existing benchmark
still calculates its original p95/p99 and checks exposure after timing. The
exact candidate summarizer and its unit tests run without modification; its
nonzero outcome remains the workflow outcome. An observation PASS is never a
replacement for an uninstrumented M0 gate.

## Retained observations and limits

The driver imports the exact candidate `scripts/mvcc_primary_cost_gate.py`
observer (SHA256 `58c58cbd8ec10a44681e9d3bd6f2dc8049869b16a45388530983438c2a9f44fb`)
under a checked preimage. It is not assumed to exist on the main branch.
Its existing process identity/tick accounting and 2% quiet rule are retained.
The extension records nominal 250 ms samples with actual monotonic/wall times,
raw `/proc/stat` (including steal/context switches), `vmstat`, diskstats,
CPU/I/O/memory pressure, process stat/status/io (faults and context switches),
and unified-cgroup quota/throttle/I/O/pressure at the visible leaf and ancestors.
Unavailable files/permissions/cgroup-v1 data carry explicit availability flags
and reasons. Sampling work can overrun its nominal period. Exited processes
between samples, hidden cgroup ancestors, hypervisor scheduling, scheduler
waits and GC assists cannot all be resolved from these counters. Zero observed
steal is not proof of no host interference. Hosted runners are not guaranteed
quiet; post-preflight interference remains recorded and cannot waive a failure.

The packet includes pristine source archive and before/after tracked-byte
manifests, explicit overlay and helper hashes, harness identity, SDK archive and
manifest, binaries, raw attempts, quiet refusal, errors, group termination/join
receipts, original summary, and artifact hashes. Workflow bootstrap failures
retain bootstrap/terminal artifacts plus normal GitHub step logs. Hard runner
loss or job timeout can prevent even an `always()` upload; missing artifacts
remain unavailable, not successful collection.

Infrastructure is deliberately limited: no dedicated persistent runner and no
persistent build cache; fresh task caches live under `RUNNER_TEMP`. GitHub
artifact retention is 30 days, not permanent external archival. The complete
packet must be copied to retained project evidence before expiration.

## Review and pure validation

Run `python3 .github/scripts/test_mvcc_m0_observation.py` in a checkout containing
the fixed candidate object, or set `M0_TEST_PRODUCT` to its pristine checkout.
These harness-only tests cover preimage refusal, timer/writer equivalence,
schedule, missing counters, parent quota, actual toy process-group termination
and descendant join, actual `-exec` retention/exit propagation, and workflow
trust boundaries. They run no Go fixture or benchmark. Compile-only checks may
use the pinned SDK and explicit overlay without executing the resulting ELF.
