# TreeDB agent notes (current)

TreeDB stores large values in a **persistent value log** under `Options.Dir/maindb/wal/`.
There is **no slab storage path** in TreeDB.

## Canonical docs (trust these)

- `docs/TREEDB_STORAGE_FORMAT.md` (on-disk layout + `ValuePtr` + value-log lifecycle)
- `docs/TREEDB_WRITE_PATHS.md` (WAL on/off semantics)
- `docs/TREEDB_RECOVERY.md` + `docs/contracts/DURABILITY.md` (crash/power-loss behavior)
- `docs/contracts/*` (behavioral guarantees)

## Invariants (do not break)

- The **value log is persistent storage**; `ValuePtr` pointers are valid long-term.
- **Never truncate/delete value-log segments by age**. Segments are removed only when unreachable (GC) or after rewrite/compaction.
- WAL/journal and value log are decoupled. WAL can be disabled via `Options.Durability = DurabilityWALOffRelaxed` while value-log pointers remain enabled.

## When changing storage behavior

- If you change on-disk formats, pointer encoding, GC rules, or durability boundaries:
  - update `docs/TREEDB_STORAGE_FORMAT.md`
  - add/adjust a reopen or crash-recovery test

## Existing tests to lean on

- Pointer persistence after reopen: `TreeDB/reopen_verify_test.go`
- Value-log GC reachability: `TreeDB/db/vlog_gc_test.go`
- Crash recovery tiers: `TreeDB/recovery_spec_test.go`
- Leaf key compression density: `TreeDB/node/leaf_density_test.go` (`BenchmarkLeafPageDensity`)
