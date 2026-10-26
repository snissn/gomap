# Durability Contracts

Durability answers: after an operation returns success, what must survive sudden
power loss? Atomicity and process-crash recovery are weaker claims unless the
required file and directory sync frontiers are stable.

## TreeDB

TreeDB uses immutable resolved profiles. Public servers and adapters choose one
of three production profiles; `bench_unsafe` is an explicit benchmark/test-only
surface with no durability promise.

| Profile | Ordinary acknowledgement | Explicit `*Sync` | `Checkpoint` / clean `Close` | Read integrity |
| --- | --- | --- | --- | --- |
| `command_wal_durable` | durable dependency-closed command-WAL prefix | durable dependency-closed command-WAL prefix | sealed durable root | verify |
| `command_wal_relaxed` | relaxed | durable dependency-closed command-WAL prefix | sealed durable root | verify |
| `no_wal_fast` | relaxed | sealed durable root covering the call | sealed durable root | verify |
| `bench_unsafe` | no promise | no promise | no production promise | benchmark-selected |

Ordinary new stores seal through the existing DATA B-tree root record and two
durable-meta slots in `index.db`. Explicit/persisted PRIMARY temporarily keeps
its original capsule constructor and recovery. A COW cache cut or an ordinary
volatile acknowledgement is not a physical seal in either representation.
Final PRIMARY removal does not change these durability boundaries.

`command_wal_durable` ordinary acknowledgements survive power loss through the
stable typed command frame and every required value-log, outer-leaf, or
auxiliary dependency. Backend-root publication may happen later.

`command_wal_relaxed` ordinary acknowledgements may lose a complete recent
suffix. A successful explicit `*Sync`, including an empty batch sync, persists
the dependency-complete command-WAL prefix captured by the call.

`no_wal_fast` ordinary acknowledgements are volatile. The profile supports
authoritative data with durable explicit database boundaries.
A crash may lose recent volatile writes; it must never expose a torn batch,
mixed root, or missing persistent reference. Independently buffered collection
domains do not promise a global ordinary-ACK-order prefix: an autonomous seal
can retain a later published operation while an earlier local write is still
volatile. Explicit database boundaries cover previously completed writes from
registered collection managers. Explicit `*Sync` operations wait for a sealed
complete root covering the call;
the persistent value log remains enabled and all referenced assets must be
stable before the root is selectable.

`Flush` and `FlushAll` are visibility/drain operations. They are not file-sync,
directory-sync, durable command-WAL, or sealed-root boundaries.

### Raw key/value API

- `Set`, `Delete`, `DeleteRange`, and `Batch.Write` use the selected ordinary
  acknowledgement class.
- `SetSync`, `DeleteSync`, and `Batch.WriteSync` use the selected production
  sync frontier.
- `Batch.Write` and `Batch.WriteSync` are atomic typed `RawKVBatch` commands.
  Recovery applies the entire command or none of it.
- A command-WAL `*Sync` does not require a full backend `Checkpoint()` per
  write. A no-WAL production `*Sync` does require a sealed backend root.

Cached command-WAL durable writes share one captured-prefix coordinator. An
idle singleton captures immediately; queued writes share the next collected
epoch and cannot acknowledge against an earlier capture. COW installs one
complete cut before its physical ACK wait, so readers may see an unacknowledged
complete mutation. The captured file synchronization runs outside append and
publication gates. Existing V2 dependency-before-marker ordering still requires
earlier external-reference and rotation synchronization under append ordering.
Only successful physical completion advances the existing stable-WAL frontier.
Checkpoint and Close join the same accepted-prefix and retained-resource owners.

Command-WAL logical rotation targets and initialized physical reserves are
separate. Fresh reserves grow only under physical pressure and reset to a small
size at forced/checkpoint boundaries. Capacity planning never resizes occupied
files or changes the complete initialization, namespace or ACK barriers; see
[the container contract](../../TreeDB/docs/spec/storage-format.md#initialized-command-wal-container-v3).

An ordinary prepared-payload append may keep its backend leases through capture,
while retaining the final complete publisher check. Pending publishers use the
original unlocked drain and capture loop for that same assigned mutation.
Capture still fixes the actual dependency closure and current journal file;
postassignment uncertainty poisons writes rather than granting a durable ACK.
Terminal failure publishes poison before retained append leases release, or
immediately when an unlocked drain returns its failure. Preappend refusals retain
their existing behavior.

For `no_wal_fast`, cached `Batch.WriteSync` covers the batch and earlier queued
writes even when it bypasses the cache or stores USER leaves in the index. A
successful backend write followed by a failed seal returns an error; the
already-visible mutation remains readable, without a durable acknowledgement.

### Collections and auxiliary assets

Supported collection/catalog mutations and their dictionary/template,
typed-column, vector, text, value-log, and outer-leaf dependencies use the same
resolved profile. A durable acknowledgement is not allowed until the exact
transitive dependency closure for its command/root is stable. Unsupported
command kinds or incomplete dependency evidence fail closed.

Exact reference counts recovered by a durable-root fallback scan remain private
until their matching candidate activates. Aborted candidates do not advance the
live tracker; reusing these counts changes neither acknowledgement durability
nor GC reachability rules.

### Checkpoint and close

`Checkpoint()` and clean `Close()` capture the current frontier, drain pending
work, publish a complete root/dependency manifest, sync required files and
namespace mutations, and return only after the sealed root is recoverable.

### Recovery

Command-WAL recovery validates V2 frames and external-resource fences, selects a
contiguous durable prefix, discards only a permitted relaxed suffix, replays
commands above `AppliedLSN`, and cleans segments only after durable-root coverage
is proven. Read-only open reports recovery required instead of mutating state.

Current command-WAL directories fail closed on replayable legacy redo by
default. Compatibility tests and forensic recovery must explicitly select
`AllowLegacyCachedRedoJournalReplay`.

For journal-free `no_wal_fast`, recovery selects the newest complete sealed
root, or the older complete sealed root when the newest candidate is incomplete.
It never assembles a root from mixed generations.

### Profile selection and pre-alpha formats

The default resolved profile is `command_wal_durable`. Production parsers accept
only the exact canonical strings `command_wal_durable`,
`command_wal_relaxed`, and `no_wal_fast`. The `bench_unsafe` token requires the
benchmark parser and `OptionsForBenchmark`/`ApplyBenchmarkProfile` boundary.

TreeDB is pre-alpha. If an old directory's required format features conflict
with the selected canonical profile or current binary, open fails closed and the
directory should be rebuilt; no complex migration scaffold is required yet.

### Resource cloning and content certificates

Freezing or cloning a resource set does not certify content durability. Clones
preserve the source token's exact certified frontier (or its absence), independently
of namespace stability, and retain producer-specific flush and sync callbacks. A
coalesced frontier beyond that certificate still requires its physical barrier; a
failed barrier remains retryable and cannot manufacture a stronger certificate.

## HashDB

HashDB is a high-performance mmap-backed hashmap engine with slab value-log
storage.

Durability contract:

- `Put` / `Delete` are not guaranteed durable; writes may be lost on power loss.
- `PutSync` / `DeleteSync` sync the slab value log, and the DB directory on slab
  segment rotation where supported.
- `ApplyBatch` is atomic in-process but not guaranteed durable.
- `ApplyBatchSync` is atomic and durable; recovery applies the entire batch or
  none of it.
- The mmap index is a derived cache. After an unclean shutdown HashDB rebuilds
  it by scanning the slab log and truncates torn tail records.

Operational notes:

- Sharded `Open`/`OpenWithShards` uses a write-back cache. Without optional
  cache WAL, pending writes are volatile until flushed; sync methods flush the
  cache and then perform the durable backend write.
- Optional per-shard cache WAL is configured through `HashDBOptions.CacheWAL`.
- `ApplyBatchSync` on sharded `*hashdb.HashDB` is atomic per shard, not across
  shards.
- Use TreeDB canonical production profiles when integrated command-WAL recovery,
  persistent value-log pointers, and stronger integrity checks are required.

Qualified public raw COW transactions use the existing canonical raw command
frame and external-reference prerequisites. A conflict, history expiration or
admission refusal appends no frame. An assigned-command failure is ambiguous
and poisons later acceptance before the publication lease is released. Durable
and relaxed WAL acknowledgements retain their existing exact request-prefix
requirements; NoWAL explicit sync seals the complete prior logical cut, including
an empty transaction's explicit sync boundary.

## Externally owned recovery

`treedb.OpenExternalRecovery` is an explicit journal-disabled `cow_ordered` integration for
an application that durably logs commands and decisions elsewhere. `Apply`
materializes state; it is not a local durable acknowledgement. The application
must retain its log until a matching successful certificate covers the complete
semantic cut. Periodic certificates retain exact DATA roots and their persistent
value dependencies. Ordinary `Open` and `OpenBackend` refuse owner-marked
directories. Portable install additionally requires the caller to drain readers
and writes, authorize the exact source cut, persist cross-store acceptance when
applicable, and rebuild adapters before resuming admission. See the complete
[external-recovery contract](../../TreeDB/docs/spec/external-recovery.md).
