# Command-WAL Durable Write Contract

Status: current behavior and measurement contract for issue #3657 (M3
candidate). The candidate preserves this contract but does not meet the issue's
latency exit gate.

This document defines the durability boundary used by cached public raw-KV
operations in `ProfileCommandWALDurable`. It distinguishes a logical public
operation, a logical writer flush or sync, an actual file-sync hook, and the
Linux syscall reached by that hook. It does not authorize an optimization or a
weaker durability mode.

## Durable return boundary

For `SetSync`, `DeleteSync`, and a dirty `Batch.WriteSync`, a nil return means:

1. any value-log records referenced by the command frame have passed a file
   sync boundary;
2. the complete typed command-WAL frame has been written and the command-WAL
   file has passed a later file sync boundary; and
3. the mutation has been published to the cached memtable before the call
   returns.

The durable bytes are the external value-log records, including their RIDs and
record integrity fields, followed by the complete command envelope and payload
needed to replay the logical mutation. The boundary does not require a backend
root publication or `Checkpoint()`. Until a later checkpoint publishes roots
and `AppliedCommandLSN`, open-time recovery replays complete frames above the
durable applied-LSN prefix through the normal command executor.

An error after the command-WAL file sync is commit-ambiguous. The open handle is
poisoned and must be reopened; recovery may apply the durable frame. An error
while ordering external value-log references occurs before the command frame
sync and must not advance that command durability boundary.

`DurabilityWALOnRelaxed` retains the same ordering but replaces file-sync
guarantees with flush-to-kernel boundaries. Nothing in this document upgrades a
relaxed operation to power-loss durability.

## Current ordered paths

### Inline dirty `WriteSync`

```text
public Batch.WriteSync
  -> cached preflight / inline entry preparation
  -> append one RawKVBatch command frame
  -> flush command writer buffers
  -> command-WAL file Sync
  -> publish entries to cached memtables and reset the batch
  -> return nil
```

There is one logical command-WAL sync and one actual command-WAL file-sync hook
for this operation when no segment rotation occurs. Directory sync is a segment
creation/rotation concern, not a per-operation durability barrier.

### Pointer-backed dirty `WriteSync`

```text
public Batch.WriteSync
  -> append external records to the selected persistent value-log lane
  -> value-log materialization Sync
  -> retain that lane's durable-write reservation
  -> acquire the command publish/barrier ordering lock
  -> validate/read each pointer RID
  -> verify the active lane still has the certified file, sequence, and size
  -> reuse the materialization Sync for that unchanged active segment
  -> encode RID references and append one RawKVBatch command frame
  -> flush command writer buffers
  -> command-WAL file Sync
  -> publish pointers to cached memtables and reset the batch
  -> return nil
```

For an unchanged active segment, current code therefore performs one actual
value-log file-sync hook for a forced-pointer dirty `WriteSync`, attributed to
`materialization`; the logical `external_ref` sync and its duplicate physical
file sync are coalesced. The successful materialization sync records a
certificate containing the active file ID, lane sequence, and writer size while
holding `vlogMu`. The durable batch retains `lane.syncing` until
external-reference ordering. Reuse requires that reservation plus an exact
file/sequence/size match under the same lock. The value log is append-only, so
the certificate covers every referenced byte in that unchanged file at or
below the recorded boundary. Pointer validation and RID lookup complete before
external-reference ordering can append or sync a command frame. The later
command-WAL sync remains mandatory.

Every uncertainty falls back to the original external-reference sync: writer
growth, rotation, reservation release, missing active writer, a failed
materialization sync, or a referenced file different from the certified active
file. Malformed or unreadable pointers fail before command-WAL durability.

When a command frame references a rotated, non-current value-log segment, the
current implementation first flushes and, unless its own active reference is
certified, syncs the referenced lane's active writer, then opens and syncs each
distinct referenced old segment directly.
Each operation is a logical `external_ref` observation. The active-writer call
records its lane-lock wait; the direct old-segment call has zero lock wait and
records the direct request time. Failure to open an old segment is a logical error
without a physical file-sync attempt. A reached direct sync hook is both a
logical observation and a physical rotated-segment attempt. This is measured
ordering, not a required sync count, and it makes segment rotation an explicit
exclusion in any M3 coalescing proof.

### `Write` followed by `WriteSync`

```text
unsynced Write                         following WriteSync
---------------                       -------------------
append/flush external bytes, if any   materialize its external bytes, if any
append/flush command frame      --->  order/sync required value-log lanes
publish to memtable                    append its command frame
return                                 command-WAL file Sync
                                       publish to memtable
                                       return nil
```

File sync applies to the file contents preceding the boundary, not only the
latest logical frame. Consequently, the later command-WAL sync also covers
earlier flushed command frames in that segment. For pointer values, the later
operation covers earlier external bytes only when its value-log sync covers the
same pending lane/file. Callers that need a database-wide barrier independent of
the next dirty batch use an empty `WriteSync`.

### Empty `WriteSync` after unsynced writes

An empty public `Batch.WriteSync` takes the command publish barrier. When a
relaxed command prefix is waiting to be promoted, it syncs the exact pending
dependencies, appends and syncs one durable-prefix barrier, and leaves that
barrier covered by the same pending public checkpoint range as the preceding
mutations. When the current command prefix is already durable (including a
fresh database), it performs the physical sync without manufacturing another
LSN. It therefore cannot leave an unapplied no-op LSN between consecutive
public checkpoint ranges.

## Logical observations versus physical calls

The stats below are cumulative. Reports must delta them over the same active
window.

| Layer | Counters | Meaning |
| --- | --- | --- |
| public operation | `treedb.public.batch.write{,_sync}.*`, `treedb.public.point.*` | caller-visible operations and wall time |
| frame append | `treedb.command_wal.append.{point,payload,entry_scan,intent}.*` | one typed frame accepted by an append route; not a syscall count |
| writer boundary | `treedb.command_wal.flush.<path>.*`, `treedb.command_wal.sync.<path>.*` | logical command-writer Flush or durable Sync calls; `barrier` has no associated frame |
| command writes | `treedb.command_wal.write.{syscalls,bytes,ns,errors}_total` | actual underlying `write`/`writev` calls made by the command writer |
| command file sync | `treedb.command_wal.file_sync.{calls,ns,errors}_total` | calls at the injected production hook, which is `os.File.Sync` |
| directory sync | `treedb.command_wal.directory_sync.{calls,ns,errors}_total` | parent-directory sync on segment creation/rotation |
| value-log logical sync | `treedb.cache.value_log.sync.<path>.{calls,ns,wait_ns,errors}_total` | `materialization`, `external_ref`, `pending_barrier`, or `checkpoint` sync, including lane-lock wait separately; rotated-segment `external_ref` observations report zero wait |
| value-log file sync | `treedb.cache.value_log.file_sync.{calls,ns,errors}_total` | actual active-writer file-sync hooks plus direct rotated-segment hooks, each counted once |
| rotated value-log file sync | `treedb.cache.value_log.file_sync.rotated_segment.{calls,ns,errors}_total` | direct non-current segment sync-hook attempts; this is a subset of aggregate value-log file syncs |
| request partition | `treedb.public.batch.write_sync.phase.*` | exclusive top-level wall partition plus nested command subphases; enabled only by the diagnostic option |

`Flush` means buffered bytes were passed to the kernel and is not an fsync
promise. `Sync` is a logical durable-mode request. `file_sync.calls_total` is
the physical hook count. On Linux M2 and M3 validate that production `os.File.Sync`
calls appear as `fsync(2)` with `strace`; consumers must not assume a specific
syscall name on other Go versions or operating systems.

The request phase top-level partition is exclusive:
`checkpoint_gate + preflight_materialization + command_callback +
memtable_publication_reset + residual = wall` (subject only to integer
nanosecond rounding). Command preparation, external-reference ordering, append,
flush/sync, and pending-LSN bookkeeping are nested inside `command_callback` and
must not be added to the top-level fields a second time.

## Explaining the accepted state-shaped interval

The accepted M0 `state.db` interval is a point/point-sync/batch-sync mix per
block, plus checkpoint barriers:

```text
3,595 appends
  = 1,199 entry_scan batch WriteSync frames
  + 1,198 point SetSync/WriteSync frames
  + 1,198 point Set/Write frames

2,408 logical command-WAL sync observations
  = 1,199 entry_scan batch WriteSync syncs
  + 1,198 point syncs
  + 11 checkpoint/publish barrier syncs
```

The 1,198 unsynced point writes account for logical command-WAL flush
observations, not sync observations. There is not a duplicate command-WAL sync
inside each public batch `WriteSync`: the state workload contains two distinct
durable public operations per block. The accepted `application.db` window uses
the same implementation primitive but a different operation mix; its 1,212
sync observations do not show the extra per-block point-sync family present in
`state.db`.

## Recovery and failure proofs

The verification matrix requires all of the following:

- external-reference ordering failure leaves the command-WAL file-sync count
  unchanged;
- a malformed value-log pointer is rejected before any external-reference or
  command-WAL sync;
- a frame synced before a publication failure is replayed after reopen;
- inline and forced-pointer public writes survive reopen;
- an empty barrier after a prior unsynced pointer write syncs the pending
  value-log bytes before the command WAL;
- deterministic hooks agree with Linux syscall tracing for write and file-sync
  counts; and
- observer state remains race-safe.

## M3 result and remaining ownership

M3 removes the only redundancy proven by M2: the second value-log sync in the
unchanged active-segment pointer path. Deterministic counters and Linux syscall
tracing show one value-log sync rather than two, while rotation, writer growth,
missing-writer, released-reservation, and error fallbacks retain conservative
ordering. Crash/reopen, race, and full repository tests pass.

The identical focused benchmark does not demonstrate the required latency
improvement: the canonical alternating comparison is statistically
inconclusive and has a +3.0% geomean. The remaining power-loss-durable floor is
one value-log materialization sync followed by one command-WAL sync. Therefore
this candidate is a partial mechanism result and a no-go for #3657's latency
gate, not completion of that issue. Command-WAL sync removal, async
acknowledgement, relaxed durability, and per-write backend checkpointing remain
outside the optimization boundary.
