# TreeDB agent notes

Apply the repository-wide persistent value-log and pre-alpha guidance in
[AGENTS.md](../AGENTS.md). The value log resides under
`Options.Dir/maindb/value_vlog/`; the decoupled redo journal resides under
`Options.Dir/maindb/wal/`. `Options.Durability = DurabilityWALOffRelaxed` can
disable the WAL while persistent value-log pointers remain enabled.

The [TreeDB spec index](docs/spec/README.md) links the canonical storage format,
write and durability, recovery, value-log lifecycle, API contract and verification
documents. Changes to disk formats, pointer encodings, GC rules, or durability
boundaries must update `docs/spec/storage-format.md` and any affected specs, and add or
adjust reopen or crash-recovery tests. Relevant
coverage includes `reopen_verify_test.go`, `db/vlog_gc_test.go`,
`recovery_spec_test.go`, and `node/leaf_density_test.go`.

For benchmark and profiling contracts, see the root `AGENTS.md` and the
[unified-bench README](../cmd/unified_bench/README.md).
