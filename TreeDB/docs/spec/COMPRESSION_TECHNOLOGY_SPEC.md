# TreeDB Compression Technology Spec

Status: canonical placeholder for compression technology decisions. Current
value-log compression behavior is implemented in code and supporting docs; this
file owns future cross-storage compression compatibility questions once they are
promoted to canonical spec status.

Compression codecs, dictionary formats, and template side stores must preserve
the durability and recovery boundaries defined by `storage-format.md`,
`value-log-lifecycle.md`, and `recovery.md`. Any compression feature that creates
external side bytes for collection roots is also blocked by the collection WAL
side-ref and restore-validation gates in
`collection-wal-durability-plan.md`.

Production persistent column-store collection writes are blocked until
`TreeDB/docs/spec/collection-wal-durability-plan.md` M7 sign-off links to green
M1-M6 collection WAL evidence. Before that gate, column-store work is limited
to docs, benchmarks, pure codecs, filters/search packages, and isolated
encode/decode tests that do not publish persistent collection roots. Persistent
column-store APIs, column part descriptor roots, secondary indexes pointing at
column-store rows, column-file side refs in published roots, and crash/reopen
safety claims for column-store writes are blocked.
