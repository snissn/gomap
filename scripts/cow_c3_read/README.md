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

Run the inexpensive fixture smoke before preparing retained collection:

```sh
GOWORK=off GOMAXPROCS=4 go test ./TreeDB/mvcc -run '^$' \
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

Parser framing and intentionally damaged copies are synthetic tool checks,
never performance samples. Retain the original complete stdout/stderr.
The watchdog smoke covers successful, failed and stuck children, including
SIGQUIT refusal followed by SIGKILL and complete reaping. The analyzer smoke
rejects incomplete or changed provenance; it does not fabricate successful
benchmark packets. Output directories must be new.
The module smoke also poisons ambient build/runtime settings and a persisted
GOENV file, then verifies the shared environment helper and an actual env
child receive only the fixed policy. This construction check runs no Go;
ordinary Go build/runtime smoke must verify the actual toolchain defaults.

`prepare_config.py --out <draft.json>` produces a deliberately non-runnable
draft. Each exact leaf and case ID is derived from its profile/mode/layout/workload;
mislabeled, duplicate, missing or extra cells refuse. Freeze counters, workload, timeout, environment,
toolchain, host admission and spread/regression/effect thresholds before
seeing matched timings. Default scheduling is separate 128x warmups and three
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
the spelling without requiring original paths to exist. The frozen fixture must
name `TreeDB/mvcc/cow_c3_public_bench_test.go` with a SHA256 digest matching both
retained source manifests and actual inputs; unrelated files cannot replace it.
C3 matched admission requires distinct production commits, Git trees, exported
source digests and binary digests. The two variants must have disjoint source,
binary, manifest and build-receipt paths, with separate build directories.
This matched-product rule does not apply to candidate-only construction smokes.
Build and benchmark processes inherit no ambient environment variables.
One shared derivation passes exactly those nine controls plus PATH=os.defpath,
GOENV=off, GOTOOLCHAIN=local, LC_ALL=C and GOPATH equal to GOMODCACHE's
parent's parent, and fixed CGO_ENABLED=0. These fifteen fields are identical for
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
permission modes; header changes or missing/extra/symlink inputs refuse. Freeze its canonical `toolchain_identity` digest alongside
`go_version` and `go_binary_sha256`; both build receipts, live collection and
offline analysis require the same identity. Collection checks the actual
inventory before and after capture. Offline analysis needs only retained bytes.
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
post-build `compiled_input_closure`. Repository inputs retain full Git authority;
GOROOT and effective external-module inputs use relative normalized identities,
so repository product changes and source/cache relocation remain admissible.
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

The offline analyzer binds accepted build receipts, all eleven provenance
artifacts (including Git-object source authority and Go tool inventory), source manifests, scripts,
raw streams, exact schedule and equal
declared logical work. It retains all six measured samples per variant and
three cycle means and makes a descriptive comparison, with no statistical
significance claim. Observed source/host receipts remain observations, not an
independent attestation. Final acceptance also requires review of provenance,
production source, correctness, allocation and current-head CI/review gates.

These standalone artifacts are not benchprof inputs. They do not change the
profile-dir filenames or existing native-prune validator contracts.

## C4 sustained public lifecycle

The closed `--suite c4-sustained` dispatch shares the C3 builder, immutable Git
source authority, compiled module validation, fixed child environment, host and
TMPDIR admission, and process-group watchdog. C3 remains the default suite.
`build.py` is unchanged. The full maintained fixture contract and allocation
scope are in [cow-c4-sustained-evidence.md](../../TreeDB/docs/benchmarks/cow-c4-sustained-evidence.md).

`prepare_c4_config.py` creates a non-runnable draft covering all 36 leaves.
Freeze source/binary/fixture identities, the same explicit environment and host
controls as C3, finite epochs (1..8), noise policy and coordinator acceptance.
Set `result_class` to `construction` for one fresh candidate process per leaf,
or `matched-supported-evidence` for separate baseline/candidate warmups and
three ABBA cycles (504 processes). Both classes require both frozen build
closures, the 15-key CGO-disabled environment, all 11 build artifacts and the
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
