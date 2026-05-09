# TreeDB Column Store RFC

Status: canonical placeholder for column-store persistence scope. Detailed
format and execution design remains future work.

Production persistent column-store collection writes are blocked until
`TreeDB/docs/spec/collection-wal-durability-plan.md` M7 sign-off links to green
M1-M6 collection WAL evidence. Before that gate, column-store work is limited
to docs, benchmarks, pure codecs, filters/search packages, and isolated
encode/decode tests that do not publish persistent collection roots. Persistent
column-store APIs, column part descriptor roots, secondary indexes pointing at
column-store rows, column-file side refs in published roots, and crash/reopen
safety claims for column-store writes are blocked.

This file exists so the spec index and docs-lint manifest have a stable owner
for column-store persistence questions. When a full RFC lands, it must keep the
blocker paragraph above unchanged or explicitly update the WAL gate, verification
matrix, and spec README in the same change.
