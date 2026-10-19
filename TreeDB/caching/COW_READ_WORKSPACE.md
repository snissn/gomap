# COW physical read workspace

Fresh COW snapshots keep their original complete logical cut. Cached sources,
revisions, tombstones and pointer resolution use that cut. Only physical leaf
traversal and dictionary-free decoding share the existing immutable backend
basis workspace. The basis does not retain a caller snapshot.

The caller holds its read lifecycle and `cowReadMu`, then takes `basis.readMu`.
Inspection uses the caller's original limits and the basis Snapshot's pinned
file. Inline point values are copied into the caller's admitted output before
unlocking. Successor keys and inline values are copied into owned results there.
Winning pointer scalars are decoded after unlocking through the original caller
workspace. Dictionary shapes retry the physical lookup through that original
workspace, preserving its definition/resource ownership. Native root reads keep
their existing narrower codec policy independently of raw reads.

The existing original budget admits workspace, traversal scratch and decoder
backing. The last original basis edge detaches and closes the workspace before
Snapshot cleanup; failed Snapshot cleanup keeps its existing custody. No new
pool, owner, refcount or decoded-page cache is introduced. Shared physical reads
serialize on one basis mutex; concurrent throughput remains a qualification
requirement. Seek, decompression and checksum work still occur on each read.

This changes transient ownership/reuse only, with no on-disk format, durability,
GC or public API change.
