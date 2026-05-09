# TreeDB Write Paths (WAL on/off)

This is a supporting write-path explainer.

For the canonical TreeDB spec, see:
- `TreeDB/docs/spec/README.md`
- `TreeDB/docs/spec/write-path-and-durability.md`

## Terminology (canonical)

- **Value log (vlog)**: append-only log for large values used by cached mode.
- **Journal / WAL**: redo/commit log for cached mode (metadata durability). We
  use *WAL* as a shorthand for the journal in public docs.

## Write-path modes (cached mode)

This supporting document does not own the durability matrix. Use
`TreeDB/docs/spec/write-path-and-durability.md` for the canonical three-mode
matrix and `TreeDB/docs/spec/collections-write-domain.md` for current
collection flush-boundary behavior. The target collection WAL overlay is in
`TreeDB/docs/spec/collection-wal-durability-plan.md`.

Legacy modes (value-log off / backend-only) have been removed.

## Practical knobs (public API)

Use profiles rather than raw flags when possible:

```go
opts := treedb.OptionsFor(treedb.ProfileDurable, "./db") // WAL on
opts := treedb.OptionsFor(treedb.ProfileFast, "./db") // WAL off (unsafe)
opts := treedb.OptionsFor(treedb.ProfileWALOnFast, "./db") // WAL on (relaxed durability)
```

Equivalent option-level knobs:

- **WAL on (default)**: `Durability = DurabilityDurable`.
- **WAL on (relaxed)**: `Durability = DurabilityWALOnRelaxed`.
- **WAL off (unsafe)**: `Durability = DurabilityWALOffRelaxed`.

## Migration (old → new)

TreeDB’s public `Options` API was simplified to make “intent” explicit:
durability/integrity are selected via `Options.Durability` and
`Options.ValueLog.*` rather than a loose set of booleans.

Common mappings:

- `Options.DisableWAL=true` → `Options.Durability = DurabilityWALOffRelaxed`
- `Options.RelaxedSync=true` (with WAL on) → `Options.Durability = DurabilityWALOnRelaxed`
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
  - `Dir/maindb/wal/`: commit journal segments and future collection WAL segments.
  - `Dir/maindb/value_vlog/`: value-log segments for large values.
  - `Dir/maindb/leaf_vlog/`: optional split leaf-log segments.
  - `Dir/maindb/LOCK`: cross-process exclusive-open lock.
- `Dir/dictdb/`: dictionary store (for value-log compression)
  - `Dir/dictdb/index.db`: dictionary metadata (internal).
  - `Dir/dictdb/LOCK`: cross-process lock for the dictionary store.

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
- `docs/TREEDB_VALUELOG_AUTOTUNE.md`
- `docs/TREEDB_RECOVERY.md`
- `docs/contracts/DURABILITY.md`
