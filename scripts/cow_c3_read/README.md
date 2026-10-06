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
python3 scripts/cow_c3_read/parser_smoke.py \
  --stdout /tmp/cow-c3-fixture.stdout --out /tmp/cow-c3-parser-smoke
python3 scripts/cow_c3_read/watchdog_smoke.py --out /tmp/cow-c3-watchdog-smoke
python3 scripts/cow_c3_read/analyzer_refusal_smoke.py --out /tmp/cow-c3-analyzer-refusal
python3 scripts/cow_c3_read/build_module_smoke.py \
  --compiled-packages <real-compiled-dependencies.stdout> --out /tmp/cow-c3-module-smoke
```

Parser framing and intentionally damaged copies are synthetic tool checks,
never performance samples. Retain the original complete stdout/stderr.
The watchdog smoke covers successful, failed and stuck children, including
SIGQUIT refusal followed by SIGKILL and complete reaping. The analyzer smoke
rejects incomplete or changed provenance; it does not fabricate successful
benchmark packets. Output directories must be new.

`prepare_config.py --out <draft.json>` produces a deliberately non-runnable
draft. Freeze exact leaf names, counters, workload, timeout, environment,
toolchain, host admission and spread/regression/effect thresholds before
seeing matched timings. Default scheduling is separate 128x warmups and three
ABBA cycles at 1024x: 108 warmups and 648 measurements, 756 fresh processes.
No post-hoc exclusions are allowed. Every noisy or adverse result needs a
source/workload-aware disposition; the analyzer cannot accept a change.

Build ordinary test binaries from immutable source directories with the same
fixture on both variants. `build.py` requires explicit GOROOT, GOCACHE,
GOMODCACHE, GOWORK=off, GOMAXPROCS=4, GOGC, GOMEMLIMIT and empty GOFLAGS controls.
Its output stays outside source and retains full source manifests, actual
go env/module graph/compiled dependencies/build streams/buildinfo, complete
compiled input hashes and explicitly missing transient generated inputs.
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
python3 scripts/cow_c3_read/build.py --source <immutable-source> \
  --git-head <commit> --git-tree <tree> --controls <controls.json> --out <new-build-dir>
python3 scripts/cow_c3_read/collect.py --config <frozen-approved.json> --out <new-packet>
python3 <new-packet>/analyze.py <new-packet>
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

The offline analyzer binds accepted build receipts, all nine provenance
artifacts, source manifests, scripts, raw streams, exact schedule and equal
declared logical work. It retains all six measured samples per variant and
three cycle means and makes a descriptive comparison, with no statistical
significance claim. Observed source/host receipts remain observations, not an
independent attestation. Final acceptance also requires review of provenance,
production source, correctness, allocation and current-head CI/review gates.

These standalone artifacts are not benchprof inputs. They do not change the
profile-dir filenames or existing native-prune validator contracts.
