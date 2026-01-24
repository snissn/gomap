> **Legacy note:** This runbook predates the WAL on/off simplification and may reference removed options.
> Use the current merge-gate runbook + docs for up-to-date guidance.

# PR14 Runbook — Deferred ValueLog (WAL off)
Branch: `sprint/slabopt-pr14-deferred-valuelog` (based on the PR13 branch in git history)

## Context / Decision
We are treating the current **eager ValueLog appends during live cached writes** as **not desirable for this project**.

Goal for PR14: implement **WAL off = deferred ValueLog**:
- Keep the **ValueLog on-disk format / plumbing** (same family as WAL on).
- Restore **legacy value-log-off semantics** for overwrite-heavy workloads: coalesce repeated updates before any value-store bytes hit disk.
- Reduce write amplification (bytes written + write syscall count) for workloads like `iavl-bench` where keys are frequently rewritten during a version.

This PR is the first step toward the larger architectural end-state: “ValueLog is the primary append-only record store; slab becomes the frozen/rotated ValueLog (or disappears).”

## WAL on/off Definitions (canonical for this sprint)
These names refer to cached-mode behaviors; exact env/flags vary per harness.

- **WAL on (durable path)**: `value_store=value_log`, redo log on
  - Journal (redo/commit-intent) enabled + value store is ValueLog.

- **WAL off (desired unsafe path)**: `value_store=value_log (DEFERRED)`, redo log off
  - Journal disabled (may lose since last checkpoint).
  - Value store is ValueLog, but **ValueLog writes are deferred** to commit/flush-time to coalesce overwrites.

Non-goal: keep/optimize **eager value-log appends**. It may remain as a debugging/experimental knob but must not be the default or the recommended fast path for our scope.

## Primary Hypothesis (what we’re fixing)
In overwrite-heavy workloads, eager ValueLog appends can write multiple payloads for the same logical key/value across a single version/commit (only the last one is reachable), causing:
- extra bytes written to disk,
- extra `write()` syscalls / flushes,
- worse wall time vs legacy value-log-off despite improved durability properties elsewhere.

PR13 already demonstrated a real syscall-level win by increasing the ValueLog writer buffer from 4MiB → 8MiB; PR14 is about eliminating the **semantic write amplification**.

## Design Sketch (minimal viable WAL off)
Implement an explicit write policy for the cached-mode value store:

1) **New option** (public, plumbed through wrappers + bench harness):
   - Example shape (final naming up to implementer):
     - `ValueLogWritePolicy: Eager|Deferred`, or
     - `DeferValueLogWrites bool`

2) **Deferred semantics**:
   - During a version/commit, do **not** append record bytes to ValueLog on every `Set`.
   - Instead, keep writes in the memtable (coalescing overwrites naturally).
   - At flush/commit boundary, build the final record stream and append once to ValueLog.

3) **Disk format**:
   - Keep the existing ValueLog framing/record format (do not invent a new format in PR14).
   - The change is **when** we emit, not what we emit.

4) **Durability**:
  - With `-treedb-disable-wal`, WAL off is still “unsafe” (lose since last checkpoint).
   - WAL off must not silently claim WAL on durability.

## Implementation Work Items (sequenced)
### Milestone A — Instrumentation / Proof
- Add minimal counters and/or trace hooks to quantify write amplification:
  - “logical records committed” vs “value-store bytes appended”
  - “unique keys in commit” vs “value-store appends”
- Provide a tiny reproduction test: repeatedly overwrite the same key N times in one commit/version with a “large” value and show:
  - Eager value-log writes append ~N payloads (baseline; for diagnosis only)
  - WAL off writes ~1 payload (target)

### Milestone B — Implement WAL off write policy
- Introduce the write policy option and thread it into the cached write-path.
- Make the eager path unreachable unless explicitly enabled (for our scope).
- Ensure “value_store=value_log” can operate with deferred emission.

### Milestone C — Correctness tests
- Add a regression test that fails if WAL off performs multiple ValueLog appends for the same key within one commit/version.
- Add a crash-safety note (WAL off is unsafe; may lose since last checkpoint) but ensure no panics/OOM on malformed data.

### Milestone D — Bench + syscall verification gates
Run (and include outputs in PR comment/body as appropriate):
- `iavl-bench` on Celestia Linux server comparing legacy value-log-off vs WAL on vs WAL off (RUNS=5 KEEP=3 SLEEP_S=5).
- `unified-bench` suite(s) that previously showed eager value-log wins, ensuring WAL off does not regress ingest-style workloads unacceptably.
- `strace` (small `target-version`) to confirm:
  - WAL off write syscall count and avg write size are closer to legacy value-log-off/WAL on expected patterns.
  - Total value-store bytes in WAL off is reduced relative to WAL on on overwrite-heavy workloads.

## Bench/Verification Commands (templates)
### Celestia Linux server (`mikers@192.168.0.185`)
General env hygiene:
- `export PATH=/home/mikers/.gvm/gos/go1.25/bin:$PATH`
- `export GOWORK=off`

#### Strace template (small run)
- Use `strace -yy -tt -T -f -e trace=openat,close,write,pwrite64,fsync,fdatasync,rename,unlink` with `--target-version 2`
- Capture per-mode to `/tmp/inspect_wal_{off,on}/strace.txt` (plus legacy value-log-off if still available in the harness).

#### iavl-bench template (full run)
- Use `~/dev/snissn/iavl-bench` and the mode scripts (update/add `2_run_wal_off.sh` in iavl-bench repo if needed).
- RUNS=5 KEEP=3 SLEEP_S=5; prioritize median-3.

### Local correctness gate
- `go test ./... -count=1`
- Focused package tests around the touched caching/value-store paths.

## PR Process (MUST)
- Commit early/often; push early/often.
- Open PR via GH CLI only:
  - `gh pr create --base <pr13-branch> --head sprint/slabopt-pr14-deferred-valuelog ...`
- Post benchmark + strace summaries as a PR comment (and/or PR body) with:
  - commands,
  - logs/paths,
  - median-3 result table,
  - key deltas.

## Success Criteria
- WAL off beats WAL on on `iavl-bench` wall time on the Celestia server (overwrite-heavy workload).
- WAL off does not materially regress the ingest-oriented `unified-bench` workloads that motivated WAL on (if it does, document tradeoff and decide).
- Strace/size evidence shows reduced write amplification vs WAL on on overwrite-heavy runs.
