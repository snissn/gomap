# TreeDB agent notes (current)

TreeDB stores large values in a **persistent value log** under
`Options.Dir/maindb/value_vlog/`. The redo journal/WAL lives under
`Options.Dir/maindb/wal/`. There is **no legacy alternate value-store path** in
TreeDB.

## Project status (pre-alpha)

- TreeDB is **pre-alpha**. On-disk formats and public APIs may change without backward-compatibility guarantees.
- It is acceptable for new binaries to fail to open old DB directories (and vice versa).
- When changing on-disk formats, update `TreeDB/docs/spec/storage-format.md` and add/adjust tests; avoid building migration infrastructure until we commit to stability.

## Canonical docs (trust these)

- `TreeDB/docs/spec/README.md` (spec index and scope)
- `TreeDB/docs/spec/storage-format.md` (on-disk layout + `ValuePtr` + local frame formats)
- `TreeDB/docs/spec/write-path-and-durability.md` (WAL on/off and sync semantics)
- `TreeDB/docs/spec/recovery.md` (startup recovery and replay rules)
- `TreeDB/docs/spec/value-log-lifecycle.md` (GC/rewrite/retention lifecycle)
- `TreeDB/docs/spec/contracts.md` (API behavior contracts)
- `TreeDB/docs/spec/verification.md` (test matrix for invariants)

## Invariants (do not break)

- The **value log is persistent storage**; `ValuePtr` pointers are valid long-term.
- **Never truncate/delete value-log segments by age**. Segments are removed only when unreachable (GC) or after rewrite/compaction.
- WAL/journal and value log are decoupled. WAL can be disabled via `Options.Durability = DurabilityWALOffRelaxed` while value-log pointers remain enabled.

## When changing storage behavior

- If you change on-disk formats, pointer encoding, GC rules, or durability boundaries:
  - update `TreeDB/docs/spec/storage-format.md` and any affected spec sections
  - add/adjust a reopen or crash-recovery test

## Existing tests to lean on

- Pointer persistence after reopen: `TreeDB/reopen_verify_test.go`
- Value-log GC reachability: `TreeDB/db/vlog_gc_test.go`
- Crash recovery tiers: `TreeDB/recovery_spec_test.go`
- Leaf key compression density: `TreeDB/node/leaf_density_test.go` (`BenchmarkLeafPageDensity`)

## Benchmark and profiling guidance

Use the shared [profiling workflow](../CONTRIBUTING.md#benchmark-profiling-workflow)
for capture commands, artifact/parser contracts and collection insert profiling.
Keep producer, analyzer, tests and tool READMEs in sync when that contract changes.
