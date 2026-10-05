# Same initial-checkpoint read replay (diagnostic only)

This packet overlays only `cmd/unified_bench/suite_quicksilver.go`. Baseline H
`a6b383b6c0d6269a598390032ecce9fd619a4eac` (tree equals public frozen fb42) and M
`90219d64ced3539accff5d0e50d9b5fa6424ed20` have the identical original suite.
Production source, CLI, factories, profile resolution, compression and ownership
remain unchanged. It does not resolve the public end-to-end p99 gate.

Only explicit environment `GOMAP_QS_REPLAY_MODE=create|read`,
`GOMAP_QS_REPLAY_DB=/absolute/exclusively-owned-dir`, and
`GOMAP_QS_REPLAY_JSONL=/absolute/packet/live.jsonl` are added to the overlay.
The capture file must be outside the DB. `-keep` is mandatory. Create requires an
existing empty directory; read requires a populated clone. The original public
factory/profile selection remains in `main.go`. The overlay rejects parameters
outside the frozen durable-primary cell below. No test CLI flag is introduced.

Create uses the existing initial ordinary batches, deleted preparation, initial
checkpoint, close/reopen and full initial value/miss oracle, then returns before
warmup and mutations. Read skips load/deletion/checkpoint, retains initial stats,
close/reopen, full initial oracle, identical deterministic 50,000-read warmup,
newQuicksilverFixture and the exact mode 0/1/2 loops, then returns before writer.
Each mode has four workers and 6M reads, snapshot batches of 64, seed 24. Timed
per-read identity/length/generation checks are unchanged. Full initial bytes and
miss classes are verified on every process before timed phases. No explicit
write or checkpoint occurs in replay. Ordinary writable Open/Close can affect
physical metadata; background packing stays at the same public default off.

JSONL captures existing `quicksilverFiles` and `quicksilverStats` after reopen and
full initial oracle (before warmup), and after the three timed phases (before
Close). These calls are outside phase timers. Public result JSON still includes
initial files/stats, reopen time and per-phase before/after counters. JSONL is a
size/counter census, not an exact byte proof; external hashing below supplies it.

## Root-only command plan (nothing executed by this worker)

1. Copy this packet to the owned native graph. Verify SHA256SUMS. Build against
   root's exact frozen baseline and candidate extracted sources, using its existing
   toolchain/native loader/linkage setup. Bind manifests separately:

   ```sh
   python3 "$PACKET/generate.py" --build-root "$BASE_SOURCE" --label baseline
   python3 "$PACKET/generate.py" --build-root "$M_SOURCE" --label candidate
   ```

   This verifies the frozen suite hash and emits absolute `go -overlay` manifests.
   Build only when the native collection slot is released, using the same existing
   `go build` tags/options as public binaries and adding the appropriate overlay.
   Label binaries diagnostic and record source/overlay/compile-input/binary hashes.
   No build, Go parser or benchmark was run to validate this packet here.

2. `command-plan.json` retains the exact durable-primary public collector argument
   vector and environment, with only the diagnostic binary pathname and `-keep`
   changed. Reuse its loader/environment/budgets (GOMAXPROCS=12, GOMEMLIMIT=2GiB,
   GOGC=100, mapped sealed vlog cap=1GiB, public Bloom/cache defaults). Sanitize
   unrelated TREEDB environment exactly as the public collector does. Do not enable
   background packing, forced GC, profiling or config overrides for initial pairs.
   Create one NEW empty `$MASTER` under graph-owned working-dbs. Run the baseline
   diagnostic binary with the plan arguments and create env, capture stdout/stderr,
   and require zero exit/full initial oracle. No existing final mutated DB is used.

3. After create exits and Close succeeds, hash/seal the master outside its DB:

   ```sh
   python3 "$PACKET/physical.py" census "$MASTER" --output "$CREATE_PACKET/master-sealed.json"
   ```

   Never open the master again. Preserve it and all failed clones. Each of six
   fresh-process reads gets a fresh exclusively owned copy, order **AB / BA / AB**.
   `cp -a --reflink=auto "$MASTER" "$CLONE"` on Linux is acceptable; **no hardlinks**.
   Before any Open or rebind, prove exact bytes:

   ```sh
   python3 "$PACKET/physical.py" exact "$CLONE" --sealed "$CREATE_PACKET/master-sealed.json" --output "$CELL/pre-rebind.json"
   ```

4. Copying changes physical parent authority. Use the already frozen production-
   backed R2 helper `bin/rebind-owned-copy-restore-r2`, with its exact hash checked
   against `provisional-manifest-restore-r2.json` -> build receipt (helper source
   `maintenance-diagnostic-overlay/rebind-copy.go`; R2 source eaa0019...). It rebinds
   dictdb/templatedb first, then maindb. Run only on the owned clone and retain its
   stdout/stderr. Do not replace it with old R or automatic Open rebinding. Do not
   reopen a failed original. R2 restoration code is separate from the M read seam.

   ```sh
   "$FROZEN_R2_HELPER" "$CLONE"
   python3 "$PACKET/physical.py" rebind-diff "$CLONE" --master "$MASTER" --output "$CELL/post-rebind.json"
   ```

   This asserts identical file membership, all non-index bytes, and every index
   extent; records exact changed index page IDs and both page digests. Attribute
   each changed page to the helper's root manifests/records/metas or dependency
   directory before accepting the physical binding. A dependency directory can
   itself use ordinary B-tree page types, so blanket “all index differences are
   fine” or a broad leaf-page exemption is invalid. All other index pages must be
   byte equal. Record the helper's source/binary provenance with this page delta
   proof. If this qualification fails, stop and preserve the copy.

5. Launch the plan's baseline/candidate diagnostic binary in a **new process** on
   its qualified clone with read env. No other benchmark runs simultaneously.
   Preserve per-cell command/environment/hash/order/timing/JSONL/stdout/stderr.
   Require no errors, identical initial oracle counts versus create, exactly
   modes 0/1/2, 6M operations each and equal deterministic requested/miss-kind/
   distinct counters. Check per-phase mutation/checkpoint/background counters;
   inspect Open effects from prelaunch binding, initial stats/files and JSONL.
   Do not claim a cold device or cold frame cache: full oracle and warmup are shared.

6. After process exits, census/hash the actual closed clone again. Compare against
   post-rebind census and report every changed/new/removed file explicitly. A
   changed durable value/leaf payload or logical initial oracle is a rejection;
   Open/Close metadata differences require attribution, not normalization. Rehash
   sealed master at the end and require unchanged bytes. Compare paired modes,
   p50/p95/p99, throughput and route/copy/allocation counters. Persistent candidate
   slowdown on this identical checkpoint supports direct cache-path attribution;
   its absence narrows causes to end-to-end layout/scheduling interactions but
   does not waive the existing public tail gate. Holdout needs its own newly
   created/sealed checkpoint and explicitly extended guard, only if primary is
   insufficient; do not reuse primary physical state for holdout generation.

## Mechanical validation

`python3 generate.py` checks both exact source versions, unique patch anchors,
rejects repeat patching, and proves literal warmup/read-phase region and full
initial oracle retained. `physical.py` is stdlib only; it hashes regular files and
rejects symlinks and non-index rebind changes. No native execution, Go build/test,
profiling, production edit, PR change, or deletion occurred.
