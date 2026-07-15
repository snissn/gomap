# TreeDB Write Paths (Command-WAL and Legacy Compatibility)

This is a supporting write-path explainer.

For the canonical TreeDB spec, see:
- `TreeDB/docs/spec/README.md`
- `TreeDB/docs/spec/write-path-and-durability.md`

## Terminology (canonical)

- **Value log (vlog)**: append-only log for large values used by cached mode.
- **Command WAL**: current durable write log for public TreeDB write handles.
- **Legacy cached redo journal**: old cached-mode redo/commit log terminology.
  Current public docs should avoid generic WAL on/off guidance except when
  explicitly discussing legacy compatibility or benchmark-only unsafe modes.

## Write-path modes (cached mode)

This supporting document does not own the durability matrix. Use
`TreeDB/docs/spec/write-path-and-durability.md` for the canonical three-mode
matrix and `TreeDB/docs/spec/collections-write-domain.md` for current
collection flush-boundary behavior. The target collection/Mongo durability path
is the user-command WAL in `TreeDB/docs/spec/user-command-wal.md`.

Legacy modes (value-log off / backend-only) have been removed.

## Practical knobs (public API)

Use profiles rather than raw flags when possible:

```go
opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, "./db") // command WAL + durable sync
opts := treedb.OptionsFor(treedb.ProfileCommandWALRelaxed, "./db") // command WAL + relaxed sync
opts := treedb.OptionsFor(treedb.ProfileBench, "./db") // no WAL; benchmark-only unsafe ceiling
```

Equivalent option-level knobs:

- **Command WAL + durable sync**: `CommandWAL = true`, `Durability = DurabilityDurable`.
- **Command WAL + relaxed sync**: `CommandWAL = true`, `Durability = DurabilityWALOnRelaxed`.
- **No WAL benchmark ceiling**: `Durability = DurabilityWALOffRelaxed`
  (benchmark-only unsafe compatibility path).

Deprecated raw cached-journal option bundles still exist for compatibility and
low-level tests, but current server, collection, Mongo gateway, and YCSB
guidance should use `command_wal_durable`, `command_wal_relaxed`, or
benchmark-only `bench`.

Raw TreeDB command-WAL support currently covers point, batch, and range
key/value writes: `Set`, `SetSync`, `Delete`, `DeleteSync`, `DeleteRange`,
`Batch.Write`, and `Batch.WriteSync` are represented as typed `RawKVBatch`
command frames. Raw public operations that cannot be replayed as typed commands
yet fail closed under command WAL; today that includes callback-based `Update`
and `UpdateSync`.

## Legacy Compatibility Migration (old -> new)

TreeDB’s public `Options` API was simplified to make “intent” explicit:
durability/integrity are selected via `Options.Durability` and
`Options.ValueLog.*` rather than a loose set of booleans.

Common legacy compatibility mappings:

- `Options.DisableWAL=true` -> `Options.Durability = DurabilityWALOffRelaxed`
  (legacy compatibility / benchmark-only unsafe)
- `Options.RelaxedSync=true` (with legacy WAL on) ->
  `Options.Durability = DurabilityWALOnRelaxed` (legacy compatibility)
- `Options.DisableReadChecksum=true` → `Options.ValueLog.ReadIntegrity = IntegritySkipChecksums`
- `Options.AllowUnsafe=true` → removed from public API (unsafe modes are now explicit via the fields above)

Value-log configuration moved under `Options.ValueLog`:

- `Options.ValueLogPointerThreshold` → `Options.ValueLog.PointerThreshold`
- `Options.MaxValueLogRetainedBytes` → `Options.ValueLog.MaxRetainedBytes`
- `Options.MaxValueLogRetainedBytesHard` → `Options.ValueLog.MaxRetainedBytesHard`
- `Options.ValueLogCompressionAutotune` → `Options.ValueLog.CompressionAutotune`

Note: internal tools (like `cmd/unified_bench`) may still require an explicit
`-treedb-allow-unsafe` flag to reduce accidental use of relaxed durability
settings.

## File layout (cached mode)

TreeDB `Options.Dir` is a *root* directory. `treedb.Open` manages:

- `Dir/maindb/`: main DB (index + journal + value log)
  - `Dir/maindb/index.db`: B+Tree pages + metadata.
  - `Dir/maindb/wal/`: current command-WAL segments; legacy cached redo-journal
    segments may exist only in compatibility/recovery fixtures and fail closed
    by default unless explicitly opted in.
  - `Dir/maindb/value_vlog/`: value-log segments for large values.
  - `Dir/maindb/leaf_vlog/`: optional split leaf-log segments.
  - `Dir/maindb/LOCK`: cross-process exclusive-open lock.
- `Dir/dictdb/`: dictionary store (for value-log compression)
  - `Dir/dictdb/index.db`: dictionary metadata (internal).
  - `Dir/dictdb/LOCK`: cross-process lock for the dictionary store.

The lower-level `caching.Open(cacheDir, backend, ...)` API may use a cache
directory that differs from a TreeDB backend's `Dir()`. In that configuration,
only the cached command WAL lives under `cacheDir/wal/`; persistent
`value_vlog/` and `leaf_vlog/` segments live under the backend directory so its
durable roots remain independently reopenable.

Note: value-log dictionary compression applies to value-log records and does
not require any split-log option.

## Debug logging

`treedb.Open` is bench-friendly by default. To emit write-path diagnostics on
open, set:

```
TREEDB_WRITE_PATH_LOG=1
```

## Related docs

- `docs/TREEDB_PROFILES.md`
- `docs/TREEDB_DOWNSTREAM_VALIDATION.md`
- `docs/TREEDB_VALUELOG_AUTOTUNE.md`
- `docs/TREEDB_RECOVERY.md`
- `docs/contracts/DURABILITY.md`
