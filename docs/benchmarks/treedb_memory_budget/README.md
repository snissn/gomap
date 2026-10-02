# TreeDB main-cache memory/placement instrumentation (#4892)

This is the first instrumentation stage of #4892, not a retained-memory result
or a trim decision. Review and land this harness after the #4891/#342
predecessors, then freeze that landed source before full collection. No runtime
controller, public knob, default, on-disk format or storage lifecycle changes.

## Checkpoint free-bin candidate (#4914; qualification pending)

The candidate trims only free global append-only entry buffers at the end of
checkpoint maintenance, using the existing 32 MiB checkpoint entry target.
Largest size classes are drained first, newest buffers first within each class,
until charged backing is at most the target. Whole-buffer removal can overshoot
the required reduction: retain the exact before/after/drop bytes, rather than
assuming an exact 32 MiB result. Empty bin metadata is abandoned too; nonempty
bin metadata is separate from the entry-backing charge. The trim allocates no
replacement buffers or result storage.

Live tables, held iterator bytes, reset DB memtable leases, busy direct/batch
leases and scratch remain separately owned. Non-checkpoint flushes, pressure
control and the 256 MiB free-bin admission cap keep their current behavior.
Concurrent producers may return backing after the checkpoint trim boundary.
The bound is not a whole-process budget or proof of heap/RSS reclamation.

Freeze reviewed baseline and candidate preparations before paired runs. Start
with three balanced repeats of inline256/threshold1024 at leaf0/32/64 MiB and
pointer256/threshold1 plus random4096 controls at leaf16/64 MiB (seven cells).
Use identical compiler, host, filesystem, environment, harness and durability
controls. Compare phase total allocated bytes/objects and normalized per-op
costs; pool get/put/drop bytes; reserve growth; mutable/idle DB lease owners;
direct pre-Stats post-GC heap; separately sampled RSS/HWM; load/update/checkpoint
and read times; persisted WAL/value-log/index lengths; and the full byte/miss,
checked close, acknowledged-LSN and reopen oracles. Preserve failed/noisy runs.

Targeted acceptance requires repeatable direct post-GC heap reduction of at
least 16 MiB in the inline cells where at least 18 MiB of free backing is
removed, without material unexplained warm-write allocation/time, checkpoint,
read, peak-memory or durability regression. Removing free buffers can cost
warm write reuse; the paired update phase must measure that tradeoff. RSS is
observational, and timing dispersion cannot establish a fine speed threshold.
These are qualification gates, not results. The coordinator must accept the
paired evidence before promotion; full twenty-cohort qualification is required
before broader claims. A nominal capacity bound alone does not close #4914.

`BenchmarkMemoryBudgetWorkflow` runs exactly once in each fresh process. Full
mode loads 250,000 even hit keys (32 bytes, 24-byte shared prefix), in 1,000-key
public `Batch.WriteSync` batches. Values are either compressible 256-byte data
or deterministic xorshift 4,096-byte data, with ID/generation in the first 16
bytes. The harness regenerates one batch at a time rather than retaining a
second 1 GB dataset. The two static pointer thresholds are 1 and 1,024 bytes:
256-byte placement differs; 4 KB values must remain pointers in both cohorts.
A sealed checkpoint and backend snapshot `GetEntry.Flags` prove every persisted
placement before reading. That full placement scan warms index/leaf selection;
load/checkpoint, codec/dictionary training and OS-cache history also affect the
first sweep. These are fresh DBs, not an OS-cache-cold fixture comparison.

The next phases are a full public owned `Get` sweep, a full reused-destination
`GetAppend` sweep, GC1 and GC2 owner samples, 40,000 updates at stride 7,919,
`WriteSync`, checkpoint, all-value/interleaved-odd-miss verification, checked
close, reopen, the same verification, and checked final close. Checkpoint stats
must cover acknowledged command-WAL LSNs. Checksums stay at the default;
negative filtering, automatic checkpoint, background vacuum and pruning are
disabled. Snapshot handles are closed before reading. No unsafe view escapes.
Forced runtime GC is outside throughput phases and is a retention diagnostic,
not evidence of a production memory improvement or value-log segment GC.

## Budgets and measurement boundaries

| Main leaf MiB | Main grouped-frame MiB |
| --- | --- |
| 0 | 64 |
| 16 | 48 |
| 32 | 32 |
| 48 | 16 |
| 64 | 0 |

Existing `LeafPageReadCacheEntries` sets the decoded-leaf capacity to
`leaf_bytes / page.PageSize` (4,096). The overlay calls existing manager setters
only at backend main-DB construction; it does not modify dictdb, templatedb or
the cache-layer value-log reader manager. A zero frame-byte limit alone means
unbounded bytes, so the zero cohort also calls `SetGroupedFrameCacheEntries(0)`.
Each literal overlay is compiled into its own executable. All cells have 64 MiB
of **combined configured backend main-cache budgets**, not equal physical RAM
or a whole-process 64 MiB limit. Admission/raw-frame/entry limits can prevent
full use of a configured budget. Leaf slot buffers and per-file frame slots
allocate lazily; valid payload-byte counters exclude spare backing and metadata.

Each timed phase records elapsed ns, actual operation count, process allocated
bytes/objects, heap after the phase, and complete before/after public Stats.
Normalize those deltas by the phase's operations, not Go's whole-workflow op.
Read time includes the consumer's full-byte validation; load/update time includes
fixture generation. Stats collection, file inventory and forced GC are outside
phase timers/allocation deltas. Go ns/op/B/op/allocs/op cover the entire workflow
and should not be called engine-only throughput or allocations.

Use GC1/GC2 heap together with batch arenas, append-only entry/value backing,
decoded leaves, grouped frames and decode scratch owners in Stats. Owner counters
and aliases overlap; do not sum the `process`, `cache` and backend aliases.
Heap and engine peak counters are sampled, not continuous phase maxima.
Linux RSS/HWM is whole-process physical residency; mmap byte counts describe
mapped address space and must not be added to RSS. Mac RSS is unavailable,
never measured zero. A retained cell requires observed Linux RSS/HWM.

`logical_file_bytes` records every filename at each phase and `closed_files`
after final close: `maindb/wal/` is the redo/command journal;
`maindb/value_vlog/` and `maindb/leaf_vlog/` are persistent reachable storage;
`maindb/index.db` is the index; `dictdb/` and `templatedb/` are separate owners.
File lengths include padding/preallocation; they are neither allocated blocks
nor physical RAM. No old value-log segment is deleted by age.

## Fail-closed capture

Use the dedicated stdlib Python capture script, not bare Go test output, for
external evidence. Preparation inventories original Git-tracked/untracked
source bytes, the original harness SHA, actual `go list -deps -test` inputs
(including dependencies, embedded files, overlay replacements and toolchain
tools), runtime HEAD/TreeDB tree, overlay identity and binary SHA. It verifies
unchanged input hashes after compilation; HEAD alone cannot prove compiled
source. Full preparation requires a clean checkout plus externally supplied
reviewed runtime HEAD and harness SHA. The coordinator must independently
establish that these are landed; a clean checkout alone does not prove review
or merge. Automatic toolchain switching is disabled; select the actual Go
1.26+ executable explicitly if the PATH launcher is older.

Record the emitted freeze SHA externally before running. The run and validator
require it in full mode and compare all prepared input/overlay/binary identities
before and after the process. The freeze binds exact build argv/environment,
exit codes and separate stdout/stderr hashes. The run binds exact executable
argv, explicit child environment, PID, exit code and separate log hashes; the
Go packet independently reports its argv, PID, environment hash and binary
hash. Only a credential-free allowlist of ambient variables reaches the child;
the exact forwarded environment is retained. Every new cohort normalizes
GOMAXPROCS=2, GOMEMLIMIT=2GiB, GOGC=100, empty GODEBUG/GORACE,
GOTRACEBACK=single and the fixed Go build controls; full freezes missing or
changing those controls fail before execution. Historical pilots keep their
original identity.

A retained campaign must also compare complete cohort environments after
removing only the four TREEDB_MEMORY fixture controls. Require identical
resolved TMPDIR/GOTMPDIR and path/compiler settings, and independently record
the actual database filesystem/device before and after collection. A single
cell freeze proves that cell's launch environment; it does not establish
cross-cohort equality or stable mounts. Failed logs stay on disk.

The validator rejects changed identities/logs, missing/duplicate packets,
wrong counts/phases/budgets/placement, uncovered acknowledged LSN, unchecked
closure, disabled integrity, absent owner/heap/storage observations and pilots
presented as retained cells. Preserve the prepared checkout, dependencies,
toolchain, overlay, freeze, executable and raw run directory; validation checks
actual frozen files, not just self-reported artifact fields.

```sh
GOWORK=off go test ./TreeDB -run '^TestMemoryBudgetFixture$' -count=1
python3 scripts/treedb_memory_budget_overlay.py --self-check
python3 scripts/treedb_memory_budget_capture.py self-check

# Bounded constructor check; each output directory must be new.
python3 scripts/treedb_memory_budget_capture.py prepare \
  --source-root "$PWD" --output /tmp/memory-pilot-prepared \
  --go /path/to/go1.26/bin/go --leaf-mib 32 --value-bytes 4096 \
  --threshold 1024 --pilot
python3 scripts/treedb_memory_budget_capture.py run \
  --prepared /tmp/memory-pilot-prepared --output /tmp/memory-pilot-run

# Only after reviewed harness landing and exclusive Linux runner grant.
python3 scripts/treedb_memory_budget_capture.py prepare \
  --source-root "$PWD" --output /path/outside/checkout/prepared-cell \
  --go /path/to/go1.26/bin/go --leaf-mib 32 --value-bytes 4096 \
  --threshold 1024 --runtime-head FROZEN_LANDED_HEAD \
  --harness-sha256 FROZEN_REVIEWED_HARNESS_SHA256
# Independently preserve the emitted SHA before running.
python3 scripts/treedb_memory_budget_capture.py run \
  --prepared /path/outside/checkout/prepared-cell \
  --output /path/outside/checkout/run-cell \
  --freeze-sha256 EXTERNALLY_RECORDED_FREEZE_SHA256 --retained
python3 scripts/treedb_memory_budget_capture.py validate \
  --prepared /path/outside/checkout/prepared-cell \
  --output /path/outside/checkout/run-cell \
  --freeze-sha256 EXTERNALLY_RECORDED_FREEZE_SHA256 --retained
```

Pilot mode is explicitly unretained (8,192 keys, 8,000 updates) and cannot
satisfy full validation. A validated retained cell alone is not the issue's
owner/budget decision: collect every budget/value/threshold cohort repeatedly,
serially, in alternating fresh-process order, then publish dispersion and the
actual warm state. Start with three repeats; use five if noise requires it.
Do not change defaults or trim required live ownership without measured evidence,
a red owner/capacity regression and narrow existing-helper repair/guardrails.
These standalone Go package artifacts are not unified-bench profile-dir output
or benchprof inputs. No profile parser or schema change is involved.
