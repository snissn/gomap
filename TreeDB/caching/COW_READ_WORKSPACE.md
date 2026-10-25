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

Owned version-successor reads resolve dictionary leaves and winning values
through their exact retained cut. Decode into caller leaf storage or copy the
winning value before releasing the cache physical mutex, using an independently
owned dictionary definition. An already-admitted matching cut-local decoder can
serve the same synchronous copy-out under the caller read mutex. Borrowed
Snapshot and iterator results keep their original decoder lifetimes.

The shared workspace retains one generic producer decoder. Same-producer reads
reuse it; incompatible producer or capacity transitions retire idle backing
before replacement admission. Alternating producer IDs can therefore rebuild a
decoder; the approximately 19 MiB envelope is a conservative reservation, not a
measurement of allocation per transition. Multiple definitions cannot simply be
registered together: distinct content-addressed producer IDs can reuse the same
inner zstd dictionary ID. Genuine oversized or concurrently retained borrowed
scratch still receives the original explicit capacity refusal.

If borrowed generic admission is refused, a single nonblocking physical-mutex
attempt may refund an idle shared generic decoder and retry that same admission
once. Busy shared scratch and other borrowed owners are never evicted or waited
on by this fallback. Reference removal and decoder/definition closure precede
receipt refund. The original limits, record inspection, full CRC, and definition
provenance apply on every route.
