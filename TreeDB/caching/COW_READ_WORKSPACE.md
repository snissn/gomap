# COW physical read workspace

Fresh COW snapshots keep their original complete logical cut. Cached sources,
revisions, tombstones and pointer resolution use that cut. Only physical leaf
traversal shares the existing immutable backend basis workspace. Generic
decompression shares the original cache workspace. Neither retains a caller
snapshot.

The caller holds its read lifecycle and `cowReadMu`, then takes `basis.readMu`.
Inspection uses the caller's original limits and the basis Snapshot's pinned
file. Inline point values are copied into the caller's admitted output before
unlocking. Successor keys and inline values are copied into owned results there.
Winning pointer scalars resolve their file and record shape through the original
caller cut. Compressed values reuse the original cache decoder where independent
dictionary definition ownership is available: the file, pointer and shape are borrowed only for that read,
and borrowed values are copied into the original reader-admitted output before
unlocking. Uncompressed values retain their original input/output workspace.
Providers without independent dictionary admission retain the complete-cut
local decoder route.

Synchronous DB `Get`, `GetAppend` and versioned variants instead copy the fully
validated value directly into caller-owned output before releasing the existing physical-read workspace
lock and exact cut. They do not materialize a shared value-output buffer or a
second cut-local output buffer. The same shape, checksum, decode and value
selection code serves both routes; input/raw/decoder storage remains admitted.
Exported Snapshot, iterator, callback View and leaf readers retain their original
borrowed-output or separately admitted callback lifetimes.
Capable dictionary leaves finish the original retained-basis traversal using a
per-call independent-definition capability. The basis never retains the caller
or provider. Captured-only providers retain the complete-cut retry and original
Snapshot admission/refund route. Dictionary admission capacity can also hand a
synchronous point back to that exact cut after releasing the basis lock. Native
root reads keep their narrower codec policy independently of raw reads.

The existing original budget admits workspace, traversal scratch and decoder
backing. The last original basis edge detaches and closes the workspace before
Snapshot cleanup; failed Snapshot cleanup keeps its existing custody. No new
pool, owner, refcount or decoded-page cache is introduced. Shared physical reads
serialize on one basis mutex; concurrent throughput remains a qualification
requirement. Every physical read still inspects the exact retained file and
applies shape, codec and caller-limit policy before a verified leaf-cache hit.
Capable dictionary leaves use the existing bounded decoded-page cache too: a
miss fully validates definition identity, record CRC and the decoded leaf before
admission. Under the cache's fixed COW limits, a verified immutable page hit
needs neither a live decoder nor another definition capture. It avoids decode
and full-record CRC work, while the tree still validates the copied page.
Different private limits bypass dictionary cache hits and follow decode/refusal.
No new cache registry or decoder allocation is needed solely for a hit.

This changes transient ownership/reuse only, with no on-disk format, durability,
GC or public API change.

Owned version-successor reads resolve dictionary leaves and winning values
through their exact retained cut. Decode into caller leaf storage or copy the
winning value before releasing the cache physical mutex, using an independently
owned dictionary definition. An already-admitted matching cut-local decoder can
serve the same synchronous copy-out under the caller read mutex. Snapshot and
iterator dictionary reads use the same shared decoder kernel: leaves decode
into the cursor's original destination, and pointer values are copied into the
reader's admitted output before unlocking. Returned bytes never alias shared
decoder storage. Their original validity and movement/Close boundaries remain
unchanged; simultaneous iterators need separate result storage, not separate
generic decoder envelopes.

The shared workspace retains one generic producer decoder. Same-producer reads
reuse it; incompatible producer or capacity transitions retire idle backing
before replacement admission. Alternating producer IDs can therefore rebuild a
decoder; the approximately 19 MiB envelope is a conservative reservation, not a
measurement of allocation per transition. Multiple definitions cannot simply be
registered together: distinct content-addressed producer IDs can reuse the same
inner zstd dictionary ID. Genuine oversized records, retained output or legacy
local decoder pressure still receive the original explicit capacity refusal.

If borrowed generic admission is refused, a single nonblocking physical-mutex
attempt may refund an idle shared generic decoder and retry that same admission
once. Busy shared scratch and other borrowed owners are never evicted or waited
on by this fallback. Reference removal and decoder/definition closure precede
receipt refund. The original limits, record inspection, full CRC, and definition
provenance apply on every route.
