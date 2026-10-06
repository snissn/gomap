# Native indexed typed rows

Run from the repository root with Go 1.26 or later:

```sh
GOWORK=off go run ./examples/typed_rows
```

The program creates and retains a new temporary DB and prints its directory. You
can instead pass `-dir /path/to/new-empty-directory`; an existing nonempty
directory is rejected. The example verifies complete rows and secondary indexes
through typed insert, generic indexed update, atomic mixed existing/missing
upsert, delete, captured-reader reuse, ordinary bounded range, checkpoint and
durable reopen.

The selected layout has four required string columns owned by `typed_row_asset`.
Residual JSON owns numeric, boolean and explicit-null fields. Choose a retained
document for more flexible schemas; a typed-column part is intended for scans and
aggregates. This native carrier accepts required strings and FP32 vectors, with
every declared column supplied for each row. A metadata declaration alone does
not establish a numeric, boolean, nullable or missing-field typed carrier.

`command_wal_durable` covers acknowledged local crash recovery. The persistent
value log holds long-lived values alongside the index and typed assets. Prepared
views drain pending writes when opened and pin captured state until closed. Use
one view per worker or external synchronization. Fetch output owns its bytes;
visitor IDs are borrowed until callback return. `GetInto` can reuse the caller's
buffer, so clone output before reusing that buffer when you need to retain it.

`UpdateBatch` uses complete-document callbacks for top-level field changes;
`UpdateTypedMetadataByID` is scoped to `meta.*`. Native replacement
requires all declared columns, while upsert atomically inserts missing IDs and
replaces existing ones. The example stops on all errors and never blindly retries
`ErrCommitAmbiguous`: reopen and reconcile the whole command's IDs and indexes.
Duplicate or unique errors after an uncertain result do not prove exactly-once
delivery. Power-loss guarantees also depend on the filesystem and storage device.

See the [selected R1 contract](../../TreeDB/docs/spec/r1-indexed-row-contract.md),
[complete-row reads](../../TreeDB/docs/spec/r1-row-reads.md), and
[indexed mutations](../../TreeDB/docs/spec/r1-indexed-mutations.md).
This is a pre-alpha API example, with no claim of performance superiority.
