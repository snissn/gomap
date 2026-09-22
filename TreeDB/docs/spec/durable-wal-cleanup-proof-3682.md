# Durable Command-WAL Cleanup Proof

Status: current backend contract for issues #3682 and #4376 under parent #1595.

TreeDB is pre-alpha. This contract deliberately fails closed rather than
preserving a cleanup path that can infer deletion authority from visible state,
a recently flushed meta page, segment rotation, or a caller-supplied LSN.

## Scope and ownership

This document owns ordinary removal of complete command-WAL segments that are
no longer needed by either recovery-selectable durable root or by command
replay.

It does not own terminal incomplete-tail classification or repair. Those are
the #3676 command-WAL V2 recovery rules in
`TreeDB/db/command_wal_v2_recovery.go`. It also does not mint durable dependency
frontiers or rotated-name stability; those remain the #3718 publication and
dependency-debt boundary.

## Destructive surface inventory

The production command-WAL mutation inventory is intentionally small:

| Mutation | Location | Authority | Disposition |
| --- | --- | --- | --- |
| unlink complete ordinary segment | `removeCoveredCommandWALSegmentsWithRegistry` in `TreeDB/db/command_wal_publish.go` | backend-private `durableWALCleanupProofV1`, revalidated durable-root cut, journal snapshot, exact segment identity, and identity-pin delete lease | #3682 ordinary cleanup |
| truncate active journal terminal tail during open | `truncateCommandJournalTail` in `TreeDB/internal/commitlog/journal_owner.go` | open-time terminal-tail validation while holding journal ownership | #3676 repair, not cleanup |
| truncate/remove V2 incomplete suffix and retain the physical repair anchor | `truncateAndSyncCommandWALV2` and `repairCommandWALV2Suffix` in `TreeDB/db/command_wal_v2_recovery.go` | V2 physical classification with retained/disposable path modes and recovery-required failures | #3676 repair, not cleanup |

Production ordinary cleanup enters only through
`DB.CleanupCommandWALCoveredSegments`. The raw-LSN convenience that composes a
scan and unlink exists only in tests. Production scanning may accept the proof
frontier as input because it is non-destructive; the unlink phase cannot be
reached from a public or user-constructible coverage value.

Current ordinary-cleanup callers are:

- checkpoint after command-journal rotation;
- read-write recovery after V2 repair/replay has completed and journal
  ownership has established the active namespace generation;
- close after writers and durable-root publication have drained, while journal
  ownership and recovery pins are still live;
- the explicit backend cleanup API.

Read-only open never performs ordinary cleanup.

## Proof contents

`durableWALCleanupProofV1` is backend-private. At deletion time it contains:

- the selected durable-root slot and every non-empty independently selectable
  `DurableRootRecordV1` plus its matching durable meta identity;
- `cleanupThrough`, the minimum `AppliedCommandLSN` across those roots;
- the dependency-closed `durableWALLSN`, which must cover every root's applied
  LSN;
- the command-journal cleanup epoch, namespace generation, lane, and active
  segment generation;
- the exact active segment path, physical identity, accepted byte frontier,
  and active maximum LSN;
- pending stable-rotation and pending-successor state, both of which must be
  empty before a proof is available and immediately before deletion;
- the complete scanned segment decisions, including captured lane/sequence,
  open handles, physical identities, sizes, scanned bytes, frame counts, and
  min/max LSN ranges.

The proof does not copy visible `DBState.AppliedCommandLSN`. Visible state may be
ahead of the last durable root. It also does not treat
`commandWALDurableLSN` as backend coverage: a durable WAL prefix can exist while
both durable roots still require replay from it.

## Eligibility rule

A segment is eligible only when all of the following hold:

1. its file name parses as a command-WAL segment; the captured lane/sequence is
   used only to conservatively retain active/post-capture generations, never to
   authorize deletion;
2. the exact opened file identity remains stable through scan and immediate
   pre-unlink revalidation;
3. a full frame scan validates the segment header and derives a complete
   min/max LSN range from contents rather than from the filename;
4. complete LSNs in the retained replay interval
   `(cleanupThrough, durableWALLSN]` form one duplicate-free contiguous
   lineage; frames at or below the cleanup frontier may be sparse after
   earlier multi-lane cleanup batches;
5. the segment's maximum complete LSN is at or below `cleanupThrough`;
6. the exact physical segment is neither the captured nor current journal
   active append target, and its captured lane sequence is below the captured
   active segment generation so rotations after the scan cannot make it a
   candidate;
7. no stable-resource capture, retry, replay, or repair pin blocks its physical
   identity or pathname namespace;
8. current durable-root cleanup coverage and durable WAL progress are not below
   the captured frontiers, the same journal owner is live, journal counters and
   segment generation have not regressed, same-segment path/identity/bytes/LSN
   state has only advanced, and pending ownership remains empty immediately
   before the destructive batch.

Any ambiguity retains data. Rotation alone authorizes no deletion.

## Ordered cleanup algorithm

Ordinary cleanup is serialized per DB and follows this order:

1. reject read-only and poisoned handles; during open, finish V2 repair/replay
   and establish journal ownership before entering ordinary cleanup;
2. capture both durable roots and the dependency-closed WAL frontier under the
   durable-publication lock;
3. capture the journal namespace snapshot and reject pending rotation or
   successor ownership;
4. scan every typed command segment completely and bind the exact decisions to
   the proof;
5. reacquire the durable-publication lock, freshly validate current root
   runtime, and require its minimum cleanup frontier and durable WAL LSN not to
   have regressed below the captured authority;
6. retain the journal owner lock while monotonically revalidating append and
   rotation progress, marking the captured and current active identities plus
   every captured-lane generation at or above the captured active sequence,
   acquiring all remaining candidate identity/namespace delete leases, and
   revalidating every pathname against its captured physical identity;
7. unlink only the eligible exact identities;
8. advance the in-process journal namespace generation and cleanup epoch, and
   record command-WAL namespace-sync debt, as soon as any unlink succeeds;
9. sync the WAL directory, emit the before/after durability boundaries, and
   clear the debt only after the full boundary succeeds;
10. report success only after the deletion namespace is durable. Cleanup never
    mints or advances the dependency-closed WAL frontier; checkpoint, recovery,
    and explicit durability barriers establish that authority before cleanup.

Recovery preserves the physical frontier produced by V2 classification. If a
relaxed frame is replayed into a newer durable root above that frontier, the
same handle's checkpoint cannot promote the gap by syncing only the empty or
new successor segment opened after replay. Ordinary cleanup remains unavailable
and retains the recovered WAL. A later reopen may use the now-durable applied
root as its recovery baseline; an actual durable append/barrier may also advance
the frontier through its separate dependency-closure contract.

`DB.Close` runs this algorithm after closing write admission and draining the
durable-root publisher, but before releasing journal or recovery ownership. It
retains the namespace without failing close when proof construction is
unavailable or stale, including an ambiguous retained replay lineage. Once the
destructive batch is entered, identity/delete-lease, unlink, observer, and
directory-sync failures are returned to the caller. Resource teardown continues
in either case so a cleanup failure cannot strand the journal owner.

Acquiring every delete lease before the first unlink prevents a later pinned
candidate from producing an avoidable partial batch. If an observer, unlink,
or directory-sync boundary fails after one unlink, the API returns
`ErrRecoveryRequired` and retains namespace-sync debt. A same-process retry
re-syncs the directory even when the removed pathname is already absent.

An actual restart remains conservative: if an unsynced unlink did not persist,
the segment is rediscovered and revalidated; if it did persist, recovery sees
the stable directory state that survived the crash.

## Metrics

The `treedb.command_wal.cleanup.*` stats expose:

- proof count/time, cleanup frontier, durable WAL LSN, selected/older root
  commit sequences, capture epoch, and namespace generation;
- scan count/time, scanned frames, and scanned bytes;
- covered, retained, and removed segments/bytes;
- retained reasons: active, uncovered, pinned, and error;
- cleanup retries and total cleanup latency;
- namespace-sync calls, errors, and pending debt;
- unlinked-but-not-yet-directory-synced segments/bytes, while successful
  removed counters remain unchanged until the deletion directory barrier
  completes;
- oldest LSN retained by an active or pinned segment.

Counters are cumulative. Performance reports must delta them over the measured
window.

## Required evidence

The focused test matrix proves:

- visible applied LSN and durable WAL prefix ahead of durable-root coverage do
  not advance cleanup;
- the older selectable root retains its replay source;
- root-frontier, durable-WAL, journal-counter, segment-generation, and
  pending-ownership regressions delete nothing;
- appends and one or more rotations after a scan preserve captured/current and
  post-capture active generations while covered old segments still converge;
- filename rebinding, duplicate lineage, gaps, incomplete non-active tails,
  and post-append poison fail closed;
- active identities and identity pins retain exact segments, and a pin-release
  retry rescans and converges;
- pending stable rotations and pending successor retries make the proof
  unavailable;
- failures before/after unlink and before/after deletion-directory sync remain
  recoverable and retryable;
- relaxed recovery followed by a same-session checkpoint does not manufacture
  a physical WAL frontier from the recovered root or an empty successor sync;
- stats account for full scanned bytes, proof frontiers, retention reasons,
  namespace work, and reclaimed bytes.

The benchmark `BenchmarkCommandWALCoveredSegmentCleanupProof` reports cleanup
latency and allocations for a bounded two-segment fixture. Append-path
benchmarks remain the hard regression gate: this cleanup proof is off the append
hot path and must add no ordinary command-append allocation or stable namespace
sync.

## Operational rule

Operators must not delete `wal/commit-l<lane>-<seq>.log` files manually. A WAL
segment that appears old or whose frames are visible in current state can still
be required by the older durable root, a recovery retry, or a physical repair
anchor. Use checkpoint/recovery/explicit cleanup and inspect the cleanup stats
instead.
