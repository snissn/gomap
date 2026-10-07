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
C4 uses the same nonempty frozen Go-version contract as C3. The version is
selected and pinned by the evidence policy, rather than a second hardcoded
patch version in the C4 schema. Both builds and the retained live-toolchain
record must match that exact frozen version, launcher hash and inventory
identity. A newly approved official toolchain requires fresh binaries and
receipts; older packets keep their original toolchain identities.
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

## C4 sustained public lifecycle

The closed `--suite c4-sustained` dispatch shares the C3 builder, immutable Git
source authority, compiled module validation, fixed child environment, host and
TMPDIR admission, and process-group watchdog. C3 remains the default suite.
The builder's closed `--suite c4` selection compiles `./TreeDB/mvcc/cowsustained`, while
its default C3 selection compiles the isolated public C3 harness package.
The full maintained fixture contract and allocation
scope are in [cow-c4-sustained-evidence.md](../../TreeDB/docs/benchmarks/cow-c4-sustained-evidence.md).

`prepare_c4_config.py` creates a non-runnable draft covering all 36 leaves.
Freeze source/binary/fixture identities, the same explicit environment and host
controls as C3, finite epochs (1..8), noise policy and coordinator acceptance.
Set `result_class` to `construction` for one fresh candidate process per leaf,
or `matched-supported-evidence` for separate baseline/candidate warmups and
three ABBA cycles (504 processes). Both classes require both frozen build
closures, the 15-key CGO-disabled environment, all 12 build artifacts and the
retained live Go version and executable inventory. Construction may reuse one
product for both labels; matched evidence requires distinct products and
independent non-nested variant paths. Native requirements are typed `PENDING`, with whole public maintenance
caps of 32 records and 1 MiB. Every packet retains the literal
`pending_native_observations`; no successful packet promotes a product/native
qualification.

```sh
python3 -B scripts/cow_c3_read/prepare_c4_config.py --out <draft.json>
python3 -B scripts/cow_c3_read/collect.py --suite c4-sustained \
  --config <frozen-approved.json> --out <new-packet>
python3 -B <new-packet>/c4_analyze.py <new-packet>
python3 -B scripts/cow_c3_read/c4_packet_smoke.py \
  --positive <actual-successful-packet> --out <new-refusal-smoke>
```

The collector creates one explicit raw directory per child and passes it via
`-cow-c4-public-output-dir`. Each actual invocation emits an immutable JSON
receipt, including Go's initial 1x calibration and the requested finite count.
The raw schema binds exact leaf, b.N, options, resolved profile/ACK, call input
and output counts, call boundaries, all required engine counters, overlap,
per-epoch public checkpoint/Close/reopen and complete oracle receipts. COW
requires unchanged backend sequence within each epoch and advancement at its
checkpoint; legacy snapshot-driven progress is observed with nonregression.
Both retain the original seed pins across all checkpoints. Failed or partial
lifecycles, extra files, missing/duplicate work, nonfinite values, counter
regression and changed native claims refuse. File hashes and recomputed raw
validation are bound to the ordinary benchmark row and full process receipt.
Each process receipt hashes the work contract for its actual phase: a 1-epoch
warmup describes its 1-epoch history and layout cardinality even when measured
processes execute 8 epochs. The command, row and raw lifecycle count must agree.
At released, preclose and reopened boundaries, COW owner counters must match the
single published database cut, including generations, roots and external leases.

`c4_analyze.py` calls the shared provenance validator using an explicit closed
protocol and dependency selection. Its raw summary retains foreground call
quantiles and the complete calibration/requested receipt list. Raw duration
sums may overlap and are not elapsed wall time. Matched ns/op, B/op and allocs/op
comparisons retain all ABBA samples and frozen noise/effect flags. They do not
establish a retained-history plateau or tail acceptance. Construction packets
have no matched performance conclusion.

`c4_packet_smoke.py` requires an actual complete successful packet, validates it,
then copies and deliberately damages separate packets. It never synthesizes a
successful producer packet. Mutations replace copied inodes, preserving the
original even when copies use hard links. Copied stream, raw lifecycle,
configuration, process, source/object/mode, module, binary, tooling and host
corruption must all refuse. This refusal smoke and actual Linux normal/race
fixture execution remain distinct gates. Standalone JSON/Go logs are not
benchprof profile-dir inputs and introduce no benchprof filename changes.
The completion record has exactly the declared fields and the literal pending
native/product/C4 claim; copied packets cannot promote that claim. Use `--case`
to select named refusal checks when only an affected validator needs rechecking.
`python3 -B scripts/cow_c3_read/c4_contract_test.py` checks the phase contract.

The C4 schema v2 finite construction disables generational maintenance and all
three background checkpoint triggers plus background index vacuum using actual
requested options. Every real Stats boundary must resolve to that policy with
zero background work. Manual checkpoint deltas are exact, including zero during
pin release and one at preclose. The separate untimed
`TestCOWSustainedPublicMVCCManualMaintenancePins` holds real persistent-pointer
seed owners for 3.2 seconds, checks every observed tick and reopens the payloads;
`TestCOWSustainedPublicMVCCMaintenanceAdmissionRefusal` rejects each changed
requested control and an actual Open with generational HotWarmCold. Retain
`-cow-c4-maintenance-output-dir` receipts separately from benchmark lifecycles.
A controller must reparse every normal, race and maximum-epoch lifecycle before
reporting PASS. No retained worker window or extra checkpoint is tolerated.

Ordinary unit construction accepts a typed zero-overlap refusal only after the
entire finite lifecycle completes, including old-owner release, both Close
calls and the reopen oracle. Its emitted lifecycle is `refused`, never evidence
of overlap. Benchmark collection still fails on that refusal, and packet
validation still requires successful actual overlapping public-call intervals.
No schedule retry, artificial call extension or packet exclusion is used.

Canonical C4 workload, metric rules and pending native declarations use typed
JSON identity. Copied and rehashed packet mutations must reject boolean or
float substitutions for their literal integer fields. Raw COW limits also require
exact fields and integer values. Every required boundary counter, layout-owner
observation and persistent single-value byte counter must fit the producer's
uint64 domain; raw call times must fit nonnegative int64. The serialized receipt
check below uses retained actual raw lifecycles and tests the upper-bound values
as well as copied overflow and float substitutions:

```sh
python3 -B scripts/cow_c3_read/c4_integer_domain_smoke.py \
  --positive /retained/positive-c4-packet --out /new/integer-domain-check
```

The frozen Linux host includes `cpu_count` and `cpu_affinity`. The latter is the
sorted actual `os.sched_getaffinity(0)` mask: at least four distinct nonnegative
integer CPU IDs. Missing/unsupported affinity observation refuses admission.
Coordinator freeze records this actual mask; every collector pre/post snapshot
and both offline analyzers require its exact equality to the frozen mask, even
when copied host records and receipts are rehashed consistently. A large system
CPU count does not substitute for available CPUs. No CPU quota policy is inferred.
