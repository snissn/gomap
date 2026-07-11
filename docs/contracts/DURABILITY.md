# Durability

## TL;DR

- TreeDB provides explicit durability calls (`SetSync`, `DeleteSync`,
  `Batch.WriteSync`) for current command-WAL profiles.
- In the durable command-WAL profile, `*Sync` means command-WAL/value-log sync;
  it does not require a full backend `Checkpoint()` per write.
- `Checkpoint()` and `Close()` are backend publication/drain boundaries, not the
  normal per-transaction durability barrier for command-WAL writes.
- HashDB provides explicit durability calls (`PutSync`, `DeleteSync`) backed by the slab value log and crash recovery; non-sync writes may be lost on power loss.

## Who Is This For?

- Anyone who needs to understand what survives a crash / power loss.
- Anyone building a replication/consensus layer where “committed” must be durable.

## TreeDB

TreeDB exposes a single cached-mode engine with selectable durability semantics
via public profiles and `Options.Durability`. Public server and adapter code
should use `command_wal_durable` or `command_wal_relaxed`; `bench` is an
explicit no-WAL benchmark ceiling.

### Command-WAL durable (`ProfileCommandWALDurable`, `Durability = DurabilityDurable`)

Cached mode writes command frames to the command WAL and writes large values to
the persistent value log, then eventually flushes to the backend.

- `Set`, `Delete`, `DeleteRange`, `Batch.Write`: append recoverable
  command-WAL input, but do not establish a power-loss fsync boundary.
- `SetSync`, `DeleteSync`: append the point command-WAL frame and sync the
  command-WAL/value-log boundary; durable.
- `Batch.WriteSync`: appends the entire batch as a typed `RawKVBatch` command
  frame and syncs the command-WAL/value-log boundary; **atomic and durable**.
  On recovery, either the entire batch is applied or none of it is.
- None of these `*Sync` calls need a backend `Checkpoint()` or `treedb.commit_seq`
  advance per write. The backend root is published later by flush/checkpoint.
- The exact inline, pointer-backed, `Write`-then-`WriteSync`, and empty-barrier
  ordering is normative in
  `TreeDB/docs/spec/command-wal-durable-write-contract.md`. In particular,
  external value-log bytes are synced before the command frame, and cached
  publication occurs only after the command-WAL sync succeeds.

Crash recovery:
- On open, command-WAL segments in `Dir/maindb/wal/` are replayed through the
  normal command executor, covered by the backend/applied-LSN state, then
  cleaned only after coverage is proven.

### Command-WAL relaxed (`ProfileCommandWALRelaxed`, `Durability = DurabilityWALOnRelaxed`)

Command-WAL frames remain the local recovery source, but `*Sync` operations do
not `fsync`. This is crash-consistent for process-failure style recovery, but
not guaranteed durable on power loss.

- `Set` / `Batch.Write`: not guaranteed durable on power loss.
- `SetSync` / `Batch.WriteSync`: crash-consistent only (no `fsync`).

### Benchmark-only no WAL (`ProfileBench`, `Durability = DurabilityWALOffRelaxed`)

Benchmark-only no-WAL mode disables command-WAL/redo recovery while keeping the
persistent value log enabled. This improves write throughput but sacrifices
durability for the most recent writes since the last checkpoint/close boundary.

- `Set` / `Batch.Write`: not guaranteed durable (no redo log).
- `SetSync` / `Batch.WriteSync`: crash-consistent only (no redo log + no `fsync`).
- Use `Checkpoint()` to establish a durable boundary.

### Legacy compatibility replay

Old cached redo-journal segments are not the current public durability path.
Current command-WAL directories fail closed on replayable legacy redo by
default. Compatibility tests or forensic recovery flows must opt in explicitly
with `AllowLegacyCachedRedoJournalReplay`.

### Operational notes

- Always `Close()` iterators and DB handles to release pinned resources and directory locks.
- Multiple processes must not open the same directory concurrently (exclusive lock).

## HashDB

HashDB is designed as a high-performance mmap-backed hashmap engine with a slab value log.

Durability contract:
- `Put` / `Delete` are not guaranteed durable (no fsync); writes go through the OS page cache and may be lost on power loss.
- `PutSync` / `DeleteSync` fsync the slab value log (and the DB directory on slab segment rotation where supported) so the operation survives a crash/power loss.
- `ApplyBatch`: atomic in-process, but not guaranteed durable (no fsync).
- `ApplyBatchSync`: atomic and durable; on recovery, either the entire batch is applied or none of it is.
- The mmap index (`hashctl-*`, `hashkeys-*`) is treated as a derived cache; on open after an unclean shutdown HashDB rebuilds the index by scanning the slab log and truncates torn tail records.

Notes:
- HashDB’s sharded `Open/OpenWithShards` entrypoint uses a write-back cache. By default there is no cache WAL, so pending cache writes are volatile until flushed. `PutSync`/`DeleteSync` flush the cache and then perform a durable backend write.
- Optional: a per-shard cache WAL can be enabled via `hashdb.OpenWithOptions` / `hashdb.OpenWithShardsAndOptions` (`HashDBOptions.CacheWAL`). Depending on the fsync policy, non-`*Sync` cache writes may be recoverable after a crash.
- For the sharded `*hashdb.HashDB` entrypoint, `ApplyBatchSync` is atomic per shard, but not atomic across shards.
- If you need fully integrated journal-based durability with stronger corruption detection, use TreeDB `*Sync` operations today.
