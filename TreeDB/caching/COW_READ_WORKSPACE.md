# COW physical read workspace

Fresh COW snapshots keep their original complete logical cut. Cached sources,
revisions, tombstones and pointer resolution use that cut. Only physical leaf
traversal and dictionary-free decoding share the existing immutable backend
basis workspace. The basis does not retain a caller snapshot.

The caller holds its read lifecycle and `cowReadMu`, then takes `basis.readMu`.
Inspection uses the caller's original limits and the basis Snapshot's pinned
file. Inline point values are copied into the caller's admitted output before
unlocking. Successor keys and inline values are copied into owned results there.
Winning pointer scalars resolve their file and record shape through the original
caller cut. Compressed dictionary-free values then reuse the same basis decoder
as physical leaves: the file, pointer and shape are borrowed only for that read,
and borrowed values are copied into the original reader-admitted output before
unlocking. Uncompressed and dictionary values keep their original input/raw
workspace and definition ownership.

Synchronous DB `Get`, `GetAppend` and versioned variants instead copy the fully
validated value directly into caller-owned output before releasing the existing physical-read workspace
lock and exact cut. They do not materialize a shared value-output buffer or a
second cut-local output buffer. The same shape, checksum, decode and value
selection code serves both routes; input/raw/decoder storage remains admitted.
Exported Snapshot, iterator, callback View and leaf readers retain their original
borrowed-output or separately admitted callback lifetimes.
Dictionary leaves retry the physical lookup through that original workspace,
preserving its definition/resource ownership. Native root reads keep
their existing narrower codec policy independently of raw reads.

The existing original budget admits workspace, traversal scratch and decoder
backing. The last original basis edge detaches and closes the workspace before
Snapshot cleanup; failed Snapshot cleanup keeps its existing custody. No new
pool, owner, refcount or decoded-page cache is introduced. Shared physical reads
serialize on one basis mutex; concurrent throughput remains a qualification
requirement. Seek, decompression and checksum work still occur on each read.

This changes transient ownership/reuse only, with no on-disk format, durability,
GC or public API change.
