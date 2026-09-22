# Command-WAL Durable Write Contract

Status: current behavior and measurement contract, including the bounded
RID-plus-bytes materialization path from issue #3731 and the conservative
`SetRID` fallback.

This document defines the durability boundary used by cached public raw-KV
operations in `ProfileCommandWALDurable`. It distinguishes a logical public
operation, a logical writer flush or sync, an actual file-sync hook, and the
Linux syscall reached by that hook. It does not authorize an optimization or a
weaker durability mode.

## Durable return boundary

For `SetSync`, `DeleteSync`, and a dirty `Batch.WriteSync`, a nil return means:

1. any external value-log records referenced by the command frame have passed a
   file sync boundary, while any materialized RID operation carries the exact
   RID and logical bytes needed to recreate that record;
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

### Bounded materialized pointer-backed dirty `WriteSync`

When a batch has at most 256 total operations, every eligible value
is at most 64 KiB, the conservative full command-frame estimate is at most
1 MiB and below the configured command segment cap, and a capped active
value-log segment has enough conservatively reserved space to avoid rotation:

```text
public Batch.WriteSync
  -> append records to the selected persistent value-log lane without fsync
  -> acquire the command publish/barrier ordering lock
  -> validate/read each pointer RID
  -> encode each RID plus its exact logical bytes in RawKVBatchV2
  -> append one RawKVBatch command frame
  -> flush command writer buffers
  -> command-WAL file Sync
  -> publish pointers to cached memtables and reset the batch
  -> return nil
```

This path has one durable barrier: the command-WAL file sync. Recovery scans the
value log by RID. A missing RID is appended under the encoded exact RID; a
matching RID is reused; a present RID with different bytes fails closed. Replay
syncs any newly appended value-log bytes before publishing roots and
`AppliedCommandLSN`, so a replay crash can safely retry without allocating a
different RID or duplicating a logical record.

The backend encoder independently enforces the same 64 KiB value, 1 MiB frame,
and 256 total-operation bounds. Retaining `entry.Value` beside a pointer is not
by itself permission to emit `SetMaterializedRID`. One-shot raw entry APIs must
declare one of three append modes: relaxed, directly durable, or a grouped
durable-prefix participant. Relaxed mode always emits the V1 `SetRID` fallback.
A prefix participant may emit bounded V2 while appending an individually
relaxed frame, but its caller must not acknowledge the mutation until a later
durable-prefix barrier covers that frame. Reusable entry intents, whose eventual
boundary is unknown, conservatively remain V1.

### `SetRID` fallback

If any materialized value or the estimated frame exceeds the bounds above, or
the append could cross a configured value-log segment boundary, the whole batch
retains the external-reference path:

```text
append value-log records -> sync exact producer closure -> encode SetRID
-> sync command WAL -> publish pointers
```

The fallback keeps `ExternalRefFenceV1`, exact stable dependency handles,
rotation/old-segment handling, pending-debt barriers, and GC/rewrite protection.
A mixed V2 payload may contain both operations: only `SetRID` contributes to the
external RID fence and producer closure. Malformed or unreadable pointers fail
before command-WAL durability.

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

## Historical M3 result and #3731 boundary

M3 removes the only redundancy proven by M2: the second value-log sync in the
unchanged active-segment pointer path. Deterministic counters and Linux syscall
tracing show one value-log sync rather than two, while rotation, writer growth,
missing-writer, released-reservation, and error fallbacks retain conservative
ordering. Crash/reopen, race, and full repository tests pass.

That candidate left one value-log materialization sync followed by one
command-WAL sync. #3731 removes the first sync only for the bounded,
format-explicit materialized-RID path above. Oversized or uncertain batches
continue to use `SetRID`; command-WAL sync removal, async acknowledgement,
relaxed durability, and per-write backend checkpointing remain outside the
optimization boundary.
