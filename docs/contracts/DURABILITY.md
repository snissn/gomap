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

`command_wal_durable` ordinary acknowledgements survive power loss through the
stable typed command frame and every required value-log, outer-leaf, or
auxiliary dependency. Backend-root publication may happen later.

`command_wal_relaxed` ordinary acknowledgements may lose a complete recent
suffix. A successful explicit `*Sync`, including an empty batch sync, persists
the dependency-complete command-WAL prefix captured by the call.

`no_wal_fast` ordinary acknowledgements may lead sealed-root publication. Its
explicit `*Sync` operations wait for a sealed complete root covering the call;
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

### Collections and auxiliary assets

Supported collection/catalog mutations and their dictionary/template,
typed-column, vector, text, value-log, and outer-leaf dependencies use the same
resolved profile. A durable acknowledgement is not allowed until the exact
transitive dependency closure for its command/root is stable. Unsupported
command kinds or incomplete dependency evidence fail closed.

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
