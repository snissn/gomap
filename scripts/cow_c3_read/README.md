# Matched MVCC read-admission evidence

This package captures matched public-path evidence for C3-read (#5076).
It changes no production API or read/write path and does not qualify native
pruning, sustained C4 retention, M7, parent #5044 or COW promotion. Expensive
collection starts only after this harness/schema is reviewed and landed, then
the measured product and tooling inputs are frozen.

`BenchmarkC3PublicReadAdmission` uses ordinary `mvcc.Store` calls, two keys,
timestamps 10/20 and equal 256-byte values. Writes overwrite the same versions.
All-version scans must return both versions of the exact key and exclude its
prefix neighbor. It covers three durability profiles, inline/forced-pointer
values, point/group/concurrent workloads and cow_btree/append_only/btree:
54 leaves. Concurrent cases run one grouped writer, one point reader and one
version scanner, each N calls. Seed, stats, sorting and Close are outside Go's
timer; actual calls, output inspection, validation and clocks are inside.

Fixed logical history does not make physical source cost stationary: legacy
snapshot rotations, queued sources and ordinary backpressure can change.
Every row retains rotation, shard and enqueue counters. All COW routes report
capture/preparation/publication and end/peak ownership charges; these are not
RSS or proof of a retained-history plateau. `close_ok` is an actual successful
Close receipt, not a post-Close resource census. Existing COW retained-cache
drain tests separately establish zero charges after owner release.

Combined writer_ops/s and reader_ops/s use elapsed time until all concurrent
callers join. Point/scan phase rates use time from the start barrier until that
reader completes, so durable-writer time does not hide reader admission.
Individual p50/p95/p99 latencies retain call boundaries. The overlap counter
may be zero and cannot prove an internal publication phase. The deterministic
prepared-cut functional test owns that invariant; paused cases have no
throughput claim.

Run the 1x parser-input check. `parser_smoke.py` requires a one-iteration row:

```sh
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/mvcc/cowbench -run '^$' \
  -bench '^BenchmarkC3PublicReadAdmission$' -benchtime=1x -benchmem -count=1 \
  > /tmp/cow-c3-fixture.stdout 2> /tmp/cow-c3-fixture.stderr
python3 -B scripts/cow_c3_read/parser_smoke.py \
  --stdout /tmp/cow-c3-fixture.stdout --out /tmp/cow-c3-parser-smoke
python3 -B scripts/cow_c3_read/config_storage_smoke.py --out /tmp/cow-c3-config-storage-smoke
python3 -B scripts/cow_c3_read/git_source_smoke.py --out /tmp/cow-c3-git-source-smoke
python3 -B scripts/cow_c3_read/watchdog_smoke.py --out /tmp/cow-c3-watchdog-smoke
python3 -B scripts/cow_c3_read/host_isolation_test.py
python3 -B scripts/cow_c3_read/load_readiness_test.py
python3 -B scripts/cow_c3_read/analyzer_refusal_smoke.py --out /tmp/cow-c3-analyzer-refusal
python3 -B scripts/cow_c3_read/build_module_smoke.py \
  --compiled-packages <real-compiled-dependencies.stdout> --out /tmp/cow-c3-module-smoke
```

Run a separate 54-leaf 128x correctness/schema smoke before preparing retained
collection. Keep its complete stdout/stderr separate from the parser input:

```sh
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/mvcc/cowbench -run '^$' \
  -bench '^BenchmarkC3PublicReadAdmission$' -benchtime=128x -benchmem -count=1 \
  > /tmp/cow-c3-fixture-128.stdout 2> /tmp/cow-c3-fixture-128.stderr
```

Parser framing and intentionally damaged copies are synthetic tool checks,
never performance samples. Retain the original complete stdout/stderr.
The watchdog smoke covers successful, failed and stuck children, including
SIGQUIT refusal followed by SIGKILL and complete reaping. The analyzer smoke
rejects incomplete or changed provenance; it does not fabricate successful
benchmark packets. Output directories must be new.
The module smoke also poisons ambient build/runtime settings and a persisted
GOENV file, then verifies the shared environment helper and an actual env
child receive only the fixed policy. This construction check runs no Go;
ordinary Go build/runtime smoke must verify the actual toolchain settings.

`prepare_config.py --out <draft.json>` produces a deliberately non-runnable
draft. Each exact leaf and case ID is derived from its profile/mode/layout/workload;
mislabeled, duplicate, missing or extra cells refuse. Freeze counters, workload,
timeout, environment, toolchain, host admission and spread/regression/effect
thresholds before seeing matched timings.
Canonical rules bind every operational counter and effect direction, including
COW capture/publication/ownership, legacy rotation, latency and concurrent phase
metrics. Changed, missing or extra rules refuse even when their schema is valid;
numeric and boolean values remain distinct. The producer and validators use the
same literal rule definitions.
Default scheduling is separate 128x warmups and three
ABBA cycles at 1024x: 108 warmups and 648 measurements, 756 fresh processes.
No post-hoc exclusions are allowed. Every noisy or adverse result needs a
source/workload-aware disposition; the analyzer cannot accept a change.

Build ordinary test binaries from immutable source directories with the same
fixture on both variants. `build.py` requires explicit GOROOT, GOCACHE,
GOMODCACHE, GOWORK=off, GOMAXPROCS=4, GOGC, GOMEMLIMIT, empty GOFLAGS and
TMPDIR controls. TMPDIR must be an existing owned real directory outside source
and build/packet output. Freeze its absolute path and actual filesystem device
in host.tmpdir/tmpdir_device; all Go builds and benchmark databases use it.
Free-space admission checks this database-temp filesystem; source filesystem
path/device/free-space are retained separately.
All four variant custody paths (source, binary, manifest and build receipt) must
be canonical absolute paths. Live collection refuses leaf or ancestor symlinks
instead of silently resolving a different invocation. Offline analysis validates
the spelling without requiring original paths to exist. The frozen harness covers every selected input in `TreeDB/mvcc/cowbench/`
and `TreeDB/internal/cowbench/`, including TestMain, admission tests and the local
counter. SHA256 and mode identities match both full Git source manifests and
actual pre/post compiler inputs. The ordinary mvcc dependency closure remains
product code; all other repository compiler inputs must belong to these harness
namespaces. Added, omitted or relocated test-only helpers fail admission.
Every case requires exactly 128 warmup and 1024 measured iterations; changing
either count fails configuration admission.
C3 matched admission requires distinct production commits, Git trees, exported
source digests and binary digests. The two variants must have disjoint source,
binary, manifest and build-receipt paths, with separate build directories.
This matched-product rule does not apply to candidate-only construction smokes.
Build and benchmark processes inherit no ambient environment variables.
One shared derivation passes exactly those nine controls plus PATH=os.defpath,
GOENV=off, GOTOOLCHAIN=local, LC_ALL=C and GOPATH equal to GOMODCACHE's
parent's parent, fixed CGO_ENABLED=0, GOAMD64=v1 and GOEXPERIMENT empty. These seventeen fields are identical for
build and collection. Thus persisted Go settings, ambient compiler/build flags and
runtime GODEBUG settings cannot enter either variant. The full effective
process environment is retained in both build receipts and the collection
packet and checked during collection and offline analysis. Actual full
`go env -json` remains retained, including the toolchain's default settings.
Go reports disabled `GOENV=off` as an empty configuration-file name; validation
checks that actual empty value while retaining `off` in the process environment.
The builder hashes the Go launcher and every regular executable below
`GOROOT/pkg/tool` before and after all build/provenance commands, refusing drift,
symlinks and an incomplete compiler/linker/assembler inventory. It retains one
`toolchain.json` artifact with relative paths, file sizes, executable modes and
SHA256 hashes. The same inventory includes every regular file below
`GOROOT/pkg/include`, including assembler textflag/funcdata headers and their
permission modes. It also binds the canonical regular `GOROOT/go.env` file's
bytes, hash and mode: `GOENV=off` disables the user file but Go still reads this
toolchain file. Header or go.env changes and missing/extra/symlink inputs refuse.
Freeze its canonical `toolchain_identity` digest alongside
`go_version` and `go_binary_sha256`; both build receipts, live collection and
offline analysis require the same identity. Collection checks the actual
inventory before and after capture. Offline analysis needs only retained bytes.
Build receipts must report integer exit zero and exactly
`<GOROOT>/bin/go test -c -trimpath -o <declared-binary> ./TreeDB/mvcc/cowbench`, plus the exact
`go list -compiled -deps -test -json ./TreeDB/mvcc/cowbench` provenance command. Custom
flags, package/output/launcher changes and failed builds refuse even if receipt
hashes are rebound. Both variants use `-trimpath` so their separate checkout paths
do not enter the compiled binaries. C4 uses the same canonical build flags.
Earlier builds without `-trimpath` retain their original evidence scope; matched
collection requires fresh builds made with the canonical command.
Draft generation and validation share one complete literal
workload contract; all fields and JSON types, exact logical work counts, latency
groups and ACK/timed-scope descriptions must match the public fixture.
Actual Goenv must report CGO_ENABLED=0, and build, collection and offline
analysis refuse any compiled CgoFiles. The public MVCC fixture uses no CGO or
network-specific behavior. Earlier CGO-enabled construction evidence is retained
under its original profile; both matched binaries need fresh disabled-CGO
builds and ordinary fixture smokes. The inventory binds Go tool executables;
the selected persistent input closure separately binds actual Go/assembly/header,
syso, embed and selected external module-metadata bytes and permission modes.
The builder derives this finite closure with `go list -compiled -deps -test`
before compilation and observes it again afterward, refusing drift or disappearance.
Its 12 artifact bindings include `compiled_inputs_before` and the existing
post-build `compiled_input_closure`. Every selected repository input, including
product Go/assembly/embed files and root go.mod/go.sum, must match its own
Git-authoritative manifest SHA256, byte size and exact Git-derived mode (0644
or 0755). Missing or duplicate authority paths refuse. The shared check runs
before compilation, after compilation, at collection and during offline analysis
for both C3 and C4. Repository inputs retain full Git authority;
GOROOT and effective external-module inputs use relative normalized identities,
so repository product changes and source/cache relocation remain admissible.
Each selected `REPO/` record retains canonical base64 `git_blob_bytes` in both
input inventories. Offline checks recompute the receipt-proven Git blob ID and
derive SHA256/size from these bytes before comparing manifest and closure fields.
Missing, invalid or noncanonical proofs refuse; older proofless inventories
require fresh builds. External records keep their existing fields and identities.
Freeze the actual `external_input_identity` in configuration. Both builds,
collection and offline analysis require identical external input identities,
beyond module version/checksum labels. Offline checks reconstruct the exact
selected paths from retained package records and compare pre/post inventory,
without reading the original host. Only Go's recognized generated test main
under GOCACHE is excluded from persistent inputs and recorded as a derivation;
an ordinary missing source/header is never classified as generated.
It does not claim an immutable copy of every GOROOT file or external system
libraries that this disabled-CGO measurement does not compile against.
Each profile also requires exact ordinary WAL counts in both variant rules
and comparable metrics: durable append/sync 1/1, relaxed 1/0, NoWAL 0/0.
The fixture observes the command-WAL/external-durability/redo-log routing at both
counter boundaries, parses actual append/sync counters for every profile, and
requires absolute zero at both NoWAL boundaries. Offline parsing binds each
counter delta to the reported per-operation count; disabled WAL does not imply
zero persistent value-log I/O or zero global fsync activity.

Physical layout is observed through public `AcquireSnapshot`, `GetEntryExact`
and `Close`, using the encoded keys for all four seeded versions before timing
and after timing. The fixture checks exact key identity, pointer flag, valid
persistent vlog FileID and zero inline ValuePtr; omitted pointer length hints
remain valid. Both phases report actual inline/pointer counts and expected count
four, and both variant rules require the requested representation. The pre-probe
finishes before the baseline stats; the post-probe starts after endpoint stats,
with no extra checkpoint. COW view/lease/cut census must return to its pre-probe
state. Non-COW acquisition may rotate pending memtables; COW acquisition may
raise lifetime residency high-water marks. Those diagnostics are outside timed
and delta counters, while lifetime peak metrics include setup/diagnostic residency.
The builder requires a Git repository containing the declared commit/tree.
Configuration accepts complete 40-character SHA1 or 64-character SHA256 Git
object IDs of matching width; the object proof enforces repository format.
It reads actual commit/tree objects with replacement objects disabled and verifies
the complete exported source against every Git blob, path and Git executable mode.
Wrong revisions, dirty exports, missing/extra paths, symlinks and mode changes
refuse before Go. A retained object proof binds commit to tree to the complete
manifest; collection rechecks actual source bytes, while offline analysis verifies
that object proof without requiring the original repository. The proof does not
replace the retained source-byte and compiled-input audit.
Offline analysis also verifies retained Python script bytes against their
receipt-proven Git blob IDs and manifest SHA256/size using the same byte verifier.
Invoke Python tools with `-B`, as shown, so importing them does not create
bytecode files in immutable source exports. Its output stays outside source
and retains full source manifests, actual
go env/module graph/compiled dependencies/build streams/buildinfo, complete
selected persistent input hashes/modes and explicitly recognized derived test-main inputs.
Actual compiled-package/test dependency Module records, selected versions/checksums
and in-tree local replacement bytes are canonicalized without hiding dependency
changes. Non-standard compiled packages without valid module identity, missing
checksums and inconsistent duplicate module records refuse. The complete declared
go.mod/go.sum remain source-bound. This is the effective compiled module graph,
not a claim that unused declarations or `go list -m all` are usable. A broader
construction inventory probe failed on an unused module invalid revision while
compilation succeeded; retain its original failed command/streams separately. External local replacements
require separate frozen-source support and currently refuse.

```sh
python3 -B scripts/cow_c3_read/build.py --source <immutable-source> \
  --git-repository <repository-containing-objects> \
  --git-head <commit> --git-tree <tree> --controls <controls.json> --out <new-build-dir>
python3 -B scripts/cow_c3_read/collect.py --config <frozen-approved.json> --out <new-packet>
python3 -B <new-packet>/analyze.py <new-packet>
```

Root must review actual source/build closures and canonical module provenance,
run source-bound ordinary fixture smoke for both binaries, and explicitly
freeze the configuration. `collect.py` never builds or SSHs. It checks both
sources/binaries before each fresh child, retains stdout/stderr, source
pre/post checks, tool/build hashes and Linux host/load/storage/process-name
snapshots, and stops at the first failure. Its independent wall-clock deadline
requests Go SIGQUIT stacks, then kills/reaps the owned process group after a
bounded grace; Go's benchmark timeout alone is insufficient. Child elapsed,
CPU and maximum RSS exclude collector postchecks and hashing. RSS is Linux
wait4 ru_maxrss in KiB and covers setup/Close, separately from Go timed B/op.

The `gomap-c3-read-matched-v3` configuration requires the exact fixed
`host_isolation` contract emitted by `prepare_config.py`. Load bounds alone
cannot admit performance collection. Endpoint process censuses and a census
inside the existing owned-child wait4 loop reject any non-zombie `go`, Go tool
(`compile`, `link`, `asm`, `cgo`, `vet`, `cover`, `test2json`, `preprofile`), or
`comm` ending in `.test`. At Linux's 15-byte boundary, names ending in `.t`,
`.te` or `.tes` are conservatively treated as potentially truncated test binaries
and refused too, even if absent from the configuration. Other 15-byte names are
not refused merely for their length. Configured benchmark basenames, truncated to Linux's 15-byte
`comm` limit, are also classified as foreign regardless of filename suffix.
Executable basenames must be ASCII and control-free; unsupported names refuse
configuration admission. Sleeping/stopped Go work also refuses admission. Census rows
retain PID, PPID, elapsed age, CPU, RSS, state, process-group ID and `comm` only;
arguments and unrelated secrets are never captured. Linux `comm` is name based
and can truncate long names. Unconfigured test names truncated before any of
those suffixes is visible, arbitrary renamed/unknown long tools, and jobs shorter
than the sampling interval can escape detection. Configured names still match
their truncated basename even without a visible suffix. This is bounded observation, not proof
of universal host exclusivity. The coordinator should reserve or coordinate a
quiet window. If a dedicated runner is unavailable, retain an explicit
`INFRASTRUCTURE_UNAVAILABLE` infrastructure receipt and describe the actual
quiet-host census/load admission fallback and its observational limits. This
fallback does not exempt foreign work or require killing unrelated jobs.

Sampling requests one census every 100 ms. A census lasting over 500 ms or a
gap over 500 ms refuses the packet; these fixed observation limits are not new
benchmark/runtime controls. The only active-process exemption during the child
is its actual PID, matching collector PPID, owned session/process-group ID and
the configured command's truncated executable basename. The full command remains
bound by the existing run-receipt check; names alone never exempt a process.
Same-name siblings and children are foreign. That PID cannot be reused while
unreaped; sampling never uses an owned exemption after wait4. Endpoint checks
have no exemption. Zombie rows are ignored. The collector records the actual
returned wait4 PID/status, each observation's monotonic interval, raw census
filename/hash and final monitor hash. All raw observations remain in the packet.
The offline analyzer audits every observation, endpoint and join, rejecting
missing, malformed, extra, changed, contaminated or unbound monitor evidence.
Scoped SIGTERM/SIGINT/SIGQUIT handlers record cancellation before spawn and remain
sticky through monitor initialization and kill/wait4 cleanup. They restore only
after joining the owned child, so repeated signals cannot abandon custody.
Initialization failures retain a separate actual PID/wait-status join receipt.
On contamination, the collector stops/reaps only its owned child process group,
retains the failure and stops without automatic retry. Cancellation/KeyboardInterrupt
also stop and reap the owned child; the existing timeout SIGQUIT/grace/SIGKILL
contract remains in force. Go version probes run before measurement, outside
the monitored benchmark lifetime; collection never exempts Go/tool probes.

Historical v1 packets remain readable by their retained v1 descriptive analyzer;
they cannot establish v2 quiet-host performance admission. In particular the
original M4's 756 structurally valid rows, 22 unresolved nonnoisy flags and 209
noisy metrics remain retained after the independent audit found foreign Go work.
No exclusions, thresholds or benchmark shapes are changed. A fresh namespace
and explicit coordinator grant are required after host-isolation repair; this
tooling never authorizes a rerun or accepts performance automatically.

V3 also requires the fixed `load_readiness` policy: five-second polling, a
600-second **total campaign** waiting budget and a conservative 4.5 ceiling for
both one-minute and five-minute load before spawning. The original host outcome
bounds remain unchanged (5.0 in the retained campaign); a snapshot can pass the
original 5.0 gate and still wait for the stricter readiness ceiling. Probes
distinguish `predeclared-host-load-bound` from `readiness-headroom` waits. This
margin is an admission policy, not proven four-thread headroom or a success
guarantee. High-load probes capture host snapshots and censuses without
repeating expensive source hashes. Once an observation is ready, the collector
refreshes the original full source/binary preflight for **both** products, then
captures a fresh final host/census admission. If load has risen again, it returns
to the same bounded wait. Source or binary drift during waiting refuses before
spawn; no pre-wait integrity result authorizes a child. Host identity, affinity, disk/free-space,
malformed evidence and foreign-Go failures remain immediate, even when load is
also high. Every rejected probe launches no child and produces no benchmark row.
The same rule applies to every product, warmup and measured leaf. Admission
still requires the original load limits; waiting never subtracts owned load,
normalizes by CPU count or exempts non-Go/kernel/storage work. After each child,
the original load/census and source checks remain immediate fatal gates.

The collector incrementally retains `readiness.json` and uniquely numbered
raw host/census probes, including rejected probes. It records each probe's
monotonic interval, source/binary preflight, decision and snapshot hash, plus
each actual sleep interval and the complete blocked intervals. Five seconds avoids polling faster than Linux's
coarse load updates and limits observer activity; ten minutes caps added waiting
for the whole campaign, without promising that any load spike will clear.
All monotonic elapsed time from the first rejected load probe through final
admission consumes the global budget, including retry probes, refreshed hashes,
ledger IO and scheduling delay. Polling sleeps alone are not the budget. Ordinary
ready observations and child timing are outside blocked intervals. Budget exhaustion or cancellation preserves the incomplete ledger
and failure without spawning a new child. It never renews the budget per cell,
automatically resumes a packet or changes the 54-cell/756-process ABBA matrix.
Runtime window planning must allow this extra 600 seconds and preparation/closure.

Each run binds its final admission index/digest and monotonic spawn time;
completion or failure binds the final ledger hash. The ordinary analyzer audits
all probes, raw bytes, decisions, ordering, source/binary bindings, actual waits,
global budget and final admission before parsing the complete result. It still
refuses `failure.json`. `protocol.validate_readiness` can separately audit a
failed ledger's exhausted/cancelled pre-spawn evidence; doing so grants no
performance acceptance. Missing, extra, changed or reordered raw probes/waits
refuse. Historical v2 packets retain their original analyzer and cannot establish
v3 readiness admission. The failed 113-leaf packet remains unaccepted; a new
complete campaign needs fresh coordinated host custody and an explicit grant.
Observed independent runner activity must be reconciled with the host owner;
readiness waiting cannot prove host exclusivity or guarantee a successful run.

`load_readiness_test.py` uses synthetic host/census evidence and a fake monotonic
clock, with no Go or child execution. It covers load-only waiting, non-load and
foreign/malformed refusals, source/binary changes during waiting, a shared finite
budget, cancellation, retained failed traces, admission ordering, raw/probe/wait
tampering, cancellation during final admission persistence, delay between a
persisted wait and the next probe, and unchanged post-leaf refusal. A cancelled
final admission can remain unused in a failed ledger; it launches no child and
its blocked interval ends at its recorded admission completion. These are
infrastructure controls.

`host_isolation_test.py` uses synthetic censuses and owned Python children only.
It covers foreign tooling/tests, zombies, PID custody/reuse, malformed census,
between-endpoint contamination, missing/tampered proof and join, positive quiet
records, spawn/monitor transition cancellation, repeated signals during cleanup
and child reaping. These tests are not Go measurements.

The offline analyzer binds accepted build receipts, all twelve provenance
artifacts (including pre-build selected inputs, Git-object source authority and Go tool inventory), source manifests, scripts,
raw streams, exact schedule and equal
declared logical work. It retains all six measured samples per variant and
three cycle means and makes a descriptive comparison, with no statistical
significance claim. Observed source/host receipts remain observations, not an
independent attestation. Final acceptance also requires review of provenance,
production source, correctness, allocation and current-head CI/review gates.

These standalone artifacts are not benchprof inputs. They do not change the
profile-dir filenames or existing native-prune validator contracts.

The build producer defaults to the standalone C3 target. The closed `--suite c4`
selector selects the dedicated `./TreeDB/mvcc/cowsustained` C4 target; receipts bind the suite
and exact matching compiler/provenance arguments. Both suites require the complete selected-minus-product harness closure and
bind `harness_input_identity`. C4 freezes its complete `TreeDB/mvcc/cowsustained/`
and shared `TreeDB/internal/cowbench/` inventories through its own protocol. The C3 phase work contract is unchanged between warmup and
measurement; iteration counts remain separately fixed at 128 and 1024.

The frozen Linux host includes `cpu_count` and `cpu_affinity`. The latter is the
sorted actual `os.sched_getaffinity(0)` mask: at least four distinct nonnegative
integer CPU IDs. Missing/unsupported affinity observation refuses admission.
Coordinator freeze records this actual mask; every collector pre/post snapshot
and both offline analyzers require its exact equality to the frozen mask, even
when copied host records and receipts are rehashed consistently. A large system
CPU count does not substitute for available CPUs. No CPU quota policy is inferred.

### Optional standard GitHub-hosted route (#5114)

The manual `cow-c3-hosted-qualification.yml` workflow defaults to **capacity-only**.
It records the first job step's aware UTC and monotonic observation before
checkout (not an API-observed job start), exports the namespace through
`GITHUB_ENV` from the physical `RUNNER_TEMP` and actual run IDs, then measures
the actual owned TMPDIR
filesystem, runnable CPU mask and platform. Full mode refuses before Go unless
Linux amd64, four runnable CPUs and 20 GiB free are actually present. It neither
cleans the hosted image nor buys a larger runner. Public compute availability
does not guarantee disk capacity or artifact storage quota. Failed admission or
upload cannot establish qualification.

Full dispatch is restricted to root account numeric ID `1981537`, landed `main`,
and a distinct run ID, run attempt and root attempt label. This optional route
uses baseline `2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c` and candidate
`8d8806b495422e44f7802a96f5737f536a3fb2f0`. It exports complete Git-object-bound
source bytes/modes using explicit `git -c tar.umask=0022 archive` (checked
against raw Git blob/mode authority), downloads the exact official Go 1.26.8 linux-amd64 archive,
retains all 15,036 toolchain file identities, and observes three real GCC-driver,
assembler and linker files before/after. Construction and matched before/final
refreshes retain the actual full Go inventory and three compiler snapshots
before comparing them with the frozen identities; actual regular-file modes
(including refused mode drift) are observed. Missing, symlink or special compiler
paths refuse without reading them and retain requested-path/type/error records.
Bootstrap still requires exactly three physical mode-0755 executables. That declaration is **not** a complete
C frontend, implicit library or libc closure. The actual race Go environment
and compiler version remain raw evidence.

A new VM executes all twelve supervised construction commands: compiler version,
race Go environment, fresh sixteen-test normal and race characterization,
baseline canonical build and full 54-cell 128x warm characterization, candidate
build and full warm characterization, 22 isolation controls, 25 readiness
controls, actual Linux census proof, and strict config freeze. Both canonical
builds retain all twelve provenance artifacts and complete selected compiler
inputs. No imported Linux185 commands, previous-failure release, historical
functional pass or old binary qualify this VM. Canonical seven C3 script bytes
remain unchanged; `hosted_collect.py` explicitly adapts only pre-child admission.

Construction is uploaded separately. With no Go active, the VM waits at most one
hour for **one immutable root-authored JSON comment on #5076**, posted only after
independent actual construction review and release of its readers. The comment
must contain exactly the following fields; every identity comes from that run's
sealed `hosted-state.json` and `construction-result.json`, not this example:

```json
{
  "repository": "snissn/gomap",
  "workflow_sha": "ACTUAL_LANDED_WORKFLOW_COMMIT",
  "run_id": "ACTUAL_INTEGER",
  "run_attempt": "ACTUAL_INTEGER",
  "dispatch_actor_id": 1981537,
  "attempt": "ACTUAL_ROOT_ATTEMPT",
  "construction_manifest_sha256": "ACTUAL_SEAL_SHA",
  "config_sha256": "ACTUAL_CONFIG_SHA",
  "policy_sha256": "ACTUAL_POLICY_SHA",
  "baseline": "2d6b07f58902537cfe8d2b3e3a8c3c9ba0f3c07c",
  "candidate": "8d8806b495422e44f7802a96f5737f536a3fb2f0",
  "verdict": "ACCEPT_ACTUAL_HOSTED_C3_CONSTRUCTION",
  "findings": [],
  "reader_release": "RELEASED",
  "all_source_and_artifact_readers_released": true,
  "accepted_utc": "ACTUAL_AWARE_UTC",
  "end_utc": "ACTUAL_AWARE_UTC"
}
```

This is a non-runnable shape illustration: run IDs/attempts must be integers;
SHA fields and UTC fields must be actual values. Receipt JSON is data only.
Duplicate keys, boolean IDs, edited or ambiguous comments, predating closure,
wrong source/config/attempt and less than 9,600 seconds remaining refuse. The
API creation timestamp must also follow actual construction closure. Read-only
API credentials exist only in the acceptance step; they are never captured in
state, raw evidence, Go environments or child receipts. Every original sealed
construction payload byte/size/mode and archive SHA is rechecked before launch.

The original 54-cell/756-child matrix, 108 warmups plus 648 measurements,
three ABBA cycles, GOMAXPROCS=4, 128/1024 iteration counts, 300-second leaf timeout,
600-second **total** readiness budget, 5-second polling, 4.5 readiness margin,
5.0 original host bound and .30/.05/.10 noise/adverse/benefit rules remain fixed.
All noisy/adverse flags remain visible; exclusions are empty. Construction
requires 10,200 seconds remaining; matched launch requires 9,600 seconds.

The 330-minute deadline measured from the first-step origin is a cooperative
**admission cutoff**. The source-bound adapter also enforces any earlier root
receipt end before every child, without renewing a 9,600-second minimum per
leaf. Expiry never signals an active child: it naturally joins under the original
300-second timeout, then stops before another child. Readiness probes and the
source refresh are unchanged. A separate 345-minute orchestration hang ceiling,
finite offline analysis and final archive fit within the 360-minute job limit;
the final thirty minutes are reserved for joins/export. This hang safeguard,
actual cancellation and canonical leaf timeout have separate raw custody
receipts. Any escalation with unproven detached-child custody, hard job timeout,
any remembered cancellation (including during final source refresh), partial
matrix, drift or contamination
stays unqualified. No automatic retry, resume, partial-row combination or expiry
kill is provided.

Before/during/after process censuses remain comm-only observations, not universal
host exclusivity or future quiet-host promises. Configured benchmark names,
known Go tools and the original conservative test-name taxonomy are unchanged.
Root must review actual CPU/affinity, memory/storage, raw custody, provenance,
functional evidence, all 756 rows and every noise/adverse flag. Uploaded sealed
success/failure archives preserve original streams and authority. A separate raw
fallback runs only if the closer failed; incomplete custody never becomes a
pass. Hosted tooling acceptance alone does not accept #5076 performance, native
#5111/#4878, C4 or parent #5044.

`python3 -B scripts/cow_c3_read/hosted_test.py` exercises capacity, authority,
sealed-payload/archive drift, compiler identity, joins, foreign censuses,
Go JSON grammar and cooperative deadline seams, plus tiny local Git archive
modes, the actual canonical generator and first-step environment setup, without
Go, network or an
operational helper main.
