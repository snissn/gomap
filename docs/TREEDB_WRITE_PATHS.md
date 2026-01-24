# TreeDB Write Paths (Mode3 / Mode4)

This document defines the **canonical** cached-mode write paths and the
preferred terminology. It is the single recommended reference for Mode3/Mode4
semantics. All other docs should link here.

## Terminology (canonical)

- **Backend slab**: append-only `data-*.slab` files managed by the backend engine.
- **Value log (vlog)**: append-only log for large values used by cached mode.
- **Journal**: redo/commit log for cached mode (metadata durability).
- **WAL (legacy term)**: use only as a synonym for *journal*; avoid introducing
  new synonyms in public docs.

## Write-path modes (cached mode)

| Mode | Journal | Value log | Durability | Status |
| --- | --- | --- | --- | --- |
| **Mode3** | ON | ON | Durable after `*Sync` or `Checkpoint()` | **Preferred default** |
| **Mode4** | OFF | ON | Unsafe (recent writes may be lost) | **Opt-in + AllowUnsafe** |

**Deprecated paths** (legacy):

- **Mode1**: value log OFF (large values written via backend slabs only).
- **Mode2**: historical/unsupported combinations.

Do not recommend Mode1/Mode2 in user-facing docs; keep them only for legacy
compatibility or migration testing.

## Practical knobs (public API)

Use profiles rather than raw flags when possible:

```go
opts := treedb.OptionsFor(treedb.ProfileDurable, "./db") // Mode3
opts := treedb.OptionsFor(treedb.ProfileFastIngest, "./db") // Mode4 (unsafe)
opts.AllowUnsafe = true
```

Equivalent option-level knobs:

- **Mode3 (default)**: `DisableJournal=false`, `DisableValueLog=false`.
- **Mode4 (unsafe)**: `DisableJournal=true`, `DisableValueLog=false`,
  `AllowUnsafe=true`.
- **Deprecated mode1**: `DisableValueLog=true` (or `DisableWAL=true`).

## File layout (cached mode)

- `Dir/index.db`: B+Tree pages + metadata.
- `Dir/data-*.slab`: **backend slabs** (permanent value storage for backend mode).
- `Dir/wal/commit-*.log`: **journal** segments (redo/commit records).
- `Dir/wal/value-*.log`: **value log** segments (large values).

Note: value-log dictionary compression requires `SplitValueLog=true` so the
value log is stored in separate segments from the journal.

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
