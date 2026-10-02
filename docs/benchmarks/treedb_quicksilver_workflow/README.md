# Canonical public TreeDB workflow

For the original-product and maintenance-control comparison, use the
[reviewed baseline-only construction](baseline/README.md) and its separate
absent-filter schema. Its Mac preparation is construction evidence only.

`BenchmarkQuicksilverWorkflow` is the dependency-free TreeDB workflow for the
Quicksilver investigation. It qualifies local engine behavior; it does not
reproduce Cloudflare's replication protocol or demonstrate a replacement for
its service. Full retained collection waits for this harness/schema to be
independently reviewed and landed. Standalone package artifacts use their own
schema and are not unified-bench profile-dir artifacts or benchprof inputs.

The workflow reuses the memory-budget fixture, storage inventory, owner stats
and existing main-cache overlay. Full cells contain 250,000 32-byte keys with a
24-byte shared prefix, 256-byte compressible or 4096-byte deterministic random
values, 1000-key synchronous CommandWAL batches, 40,000 permutation updates and
four update checkpoints. An additional present-empty sentinel is counted
separately. Even keys hit; odd keys miss inside the same key range. The bounded
pilot uses 8192 keys, 8000 updates and 6400 read keys instead of 256,000.

Owned reads use public Get or GetMany64, uniform or Zipf selection, and exact
0/50/90/99% miss schedules. Every hit is consumed with CRC32. Full byte checks of
all values and all interleaved misses occur separately before reads, after
updates, and after checked close/reopen; the sentinel must remain present-empty.
Default checksums remain enabled, and acknowledged command LSNs must be covered
by the final checkpoint. Background maintenance is disabled explicitly.

The 14 phases separate load acknowledgement, initial checkpoint, initial proof,
warm owned reads, four update intervals with their checkpoints, final proof and
reopen proof. Reopen time and post-reopen GC observations are separate fields.
Read throughput includes CRC consumption and uses read-key count/phase time;
latency samples cover only each owned API request and occur every 17 requests,
avoiding alignment with the deterministic miss schedule.
Batch latency is per 64-key request, not per key. Whole-workflow Go `ns/op` is
not a read-throughput result. No phase is a cold-device measurement: placement
scans/full proof precede the explicitly warm read phase.

The query table stores only key IDs and CRCs (8 bytes/read), not the payload
corpus. Its explicit allocation is separate from engine retention. Update
values are generated one batch at a time. Stats contain configured limits,
decoded-leaf valid bytes and entries, frame allocation and payload, backing-pool
owners, active mmap and observed process heap/RSS/HWM. Cache splits 0/64,16/48,
32/32,48/16,64/0 have equal **configured main-cache** bytes, not equal physical
RAM. Side-store and cache-layer reader limits remain their existing settings.
Optional filtering includes the sentinel and allocates approximately ten bits/key
(pilot10248/full312504 word-rounded bytes), in addition to those main-cache budgets; defaults remain
off. Saturation and bootstrap limits are documented in the negative-filter
qualification. Storage maps retain actual relative filenames and logical
sizes; redo WAL, persistent value/leaf logs, main index and side stores must be
reported separately. File length is not filesystem allocated-block size.

## Prepare once, run fresh processes

The memory-budget preparation compiles the same TreeDB package test executable,
including this harness, and retains the actual source/dependency/toolchain/
overlay/binary freeze. The canonical driver requires its harness to be an
actual compiled input. It preserves the build environment, records a separate
explicit run environment for its four workload controls, and hashes both raw
stdout and stderr. No second compiler or generic benchmark framework is added.

For a bounded smoke, use an explicit compatible Go binary and new output dirs:

```sh
GOMAXPROCS=2 GOMEMLIMIT=1GiB python3 scripts/treedb_memory_budget_capture.py prepare \
  --source-root "$PWD" --output /tmp/quicksilver-prepared \
  --go /absolute/path/to/go1.26/bin/go \
  --leaf-mib 32 --value-bytes 4096 --threshold 1024 --pilot
python3 scripts/treedb_quicksilver_capture.py run \
  --prepared /tmp/quicksilver-prepared --output /tmp/quicksilver-pilot \
  --miss-percent 90 --read-batch 64 --distribution zipf --filter on
python3 scripts/treedb_quicksilver_capture.py validate \
  --prepared /tmp/quicksilver-prepared --output /tmp/quicksilver-pilot --negative-checks
```

The runnable negative check removes a phase, value proof, binary identity and
latency count from the valid packet and requires rejection. The fixture test is
`GOWORK=off go test ./TreeDB -run '^TestQuicksilverWorkflowFixture$' -count=1 -timeout=2m`.
The reused capture self-check covers counter/owner/storage/header completeness.

For retained cells, omit `--pilot` at preparation and provide the independently
recorded landed `--runtime-head` and **memory harness** `--harness-sha256` there.
Record the emitted `freeze.json` SHA externally. At canonical run and validation,
provide that `--freeze-sha256`, the same runtime HEAD, and the **canonical
harness** SHA with `--harness-sha256`. Run also requires the coordinator's
exclusive `--grant`; validate uses `--retained`. The frozen source inventory and
actual compiled-input map bind both harnesses. Only clean landed checkouts may
prepare full cells. Source, compile inputs, overlays and binary must remain
identical before and after collection; altered logs, stale identities, missing
observations, incomplete proofs/counts and non-Linux retained RSS are rejected.
Incomplete process logs/run manifests remain failure evidence, not partial
accepted cells. `run.json` records each environment, argv, PID, timestamps and
exit status; `run.stdout` and `run.stderr` preserve raw process output.

## Repeated collection and profiles

The coordinator serializes timing on the allocated Linux runner and records
shared services, quietness, exact Go/toolchain/host and temp/storage placement.
Use at least five fresh-process interleaved pairs for a selected comparison,
alternating order and holding all unrelated fixture/settings constant. Report
medians, spread and allocations alongside phase owner/path counters. No
universal throughput threshold follows from a pilot. Correctness/count/source
invariants are hard gates; a material unexplained performance regression blocks
acceptance. Final evidence preserves runtime/harness identities and never
relabels a packet as measurement of a newer unmeasured source.

For diagnostic profiles, run the same frozen package binary with the recorded
canonical environment and benchmark argv, additionally passing
`-test.cpuprofile=/absolute/path/cpu_quicksilver.pprof` or
`-test.memprofile=/absolute/path/allocs_quicksilver.pprof`. Retain these in a
separate untimed diagnostic process with full provenance. They are standalone
Go profiles; inspect them with `go tool pprof`, not the unified-bench/benchprof
artifact parser. Profile collection does not replace unprofiled timing pairs.
