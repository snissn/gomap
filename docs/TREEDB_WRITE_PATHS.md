# TreeDB Write Paths (WAL on/off)

This document defines the canonical cached-mode write paths and terminology.
It is the recommended reference for WAL on/off semantics.

## Terminology (canonical)

- **Value log (vlog)**: append-only log for large values used by cached mode.
- **Journal / WAL**: redo/commit log for cached mode (metadata durability). We
  use *WAL* as a shorthand for the journal in public docs.

## Write-path modes (cached mode)

| Mode | Journal (WAL) | Value log | Durability | Status |
| --- | --- | --- | --- | --- |
| **WAL on** | ON | ON | Durable after `*Sync` or `Checkpoint()` | **Default** |
| **WAL off** | OFF | ON | Unsafe (recent writes may be lost) | **Opt-in + AllowUnsafe** |

Legacy modes (value-log off / backend-only slabs) have been removed.

## Practical knobs (public API)

Use profiles rather than raw flags when possible:

```go
opts := treedb.OptionsFor(treedb.ProfileDurable, "./db") // WAL on
opts := treedb.OptionsFor(treedb.ProfileFastIngest, "./db") // WAL off (unsafe)
opts.AllowUnsafe = true
```

Equivalent option-level knobs:

- **WAL on (default)**: `DisableWAL=false`.
- **WAL off (unsafe)**: `DisableWAL=true`, `AllowUnsafe=true`.

## File layout (cached mode)

- `Dir/index.db`: B+Tree pages + metadata.
- `Dir/wal/`: journal + value-log segments (internal file naming may change).

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
