# Prepared insert memory gate (#4819)

This document records the current behavior of the prepared insert lane and
the remaining acceptance gates for its loader integration.

The byte gate covers all memory owned by an admitted request: source backing,
the prepared token, ordered-commit materialization, command WAL and frame
scratch, value-log output, root rewriting, pager and zipper output, freelist
copy-on-write state, Manager scans and sets, and visible installation. A
handed-off backing is charged once. `ReservedBytes` is admission credit rather
than heap or process-RSS telemetry.

The engine gate is closed for the supported no-index JSON semantic-stream
lane. End-to-end JSONBench acceptance remains open until the exact-head 1M and
10M runs report throughput, query hashes, admission telemetry, and peak RSS.
The current 2 GiB per-token ceiling in focused tests and the public benchmark
is a candidate, not a loader default. The loader may use a larger
source-justified ceiling when the full shared-lane ledger and real 16K/RSS
witness require it.

## Source and preparation envelope

`PrepareInsertBatchOwned` admits at most 16,384 rows, 1,024 bytes per ID, and
128 KiB per document. The lane supports at most five Int64/String paths of at
most 1,024 bytes, a 512-byte asset namespace, and three validated aggregate
metadata specs. It charges outer ID/document slices, element capacities,
prepared runs, declared rows, semantic blocks, typed backing, and a 32 KiB
stable-schema copy reserve. Unused outer-slice slots must be nil. Each element
capacity must describe its complete non-aliased allocation because Go cannot
discover backing hidden by a full-slice expression.

Preparation partitions scratch across at most four concurrent 4,096-row
blocks. Each block checks its fixed reserve before cursor, raw, and zstd
allocation, then debits quota before path, trie, map, interned-string, and
entry-header growth. The process-wide raw pool retains at most four 8 MiB
buffers; JSONBench must hold the corresponding 32 MiB shared-lane credit even
when they are idle. The pinned, single-worker zstd encoder has a separate 8
MiB workspace allowance. Changing that encoder or its options requires
rederiving the allowance.

Preparation validates source semantics before existing-ID conflicts. A
prepared malformed document therefore returns its source-validation error;
an otherwise valid prepared token checks the current root and conflicts at
Commit. Ordinary `InsertBatch` retains its existing conflict-first behavior.

## Ordered commit and publisher tranche

Before Prepare returns, the token holds separate credit for initial iterator
materialization, result IDs, command WAL/frame/run/pointerization buffers,
typed images, row assets, sidecars, catalog/manifest copies, both system-delta
materializations, and the ordered root publisher. The publisher tranche is
fixed for the admitted engine envelope:

```text
P = 8,192 existing pager pages
F = 32 registered value-log files
V = 64 visible resources/members
Q = 237,942 total output pages across the root group

publisher bytes = Q*4096 + P*4096
                + (P+Q)*(512+256)
                + 3*64 MiB + 64 MiB + 128 MiB
                = 1,599,848,960 bytes
```

The page images cover pager/zipper output. The per-page terms cover root-scan
maps and COW collections. The record terms cover encoded, decoded, and
selected value-log records plus the grouped-frame cache. The fixed term covers
Manager/resource sets, historical leaf-generation clones and maps,
descriptors, builders, bounded seal/debt slices, and other slice growth. This
tranche is part of `PreparedInsertBatch.ReservedBytes()` before a successor
may be admitted and remains held through Commit or Abandon.

Under the serialized publication lock, the prepared route profiles the exact
captured primary, stream, manifest, locator, and system roots. It rejects
non-point shapes, excessive depth/base traversal, total output above `Q`, more
than 4,096 manifest or descriptor records, more than 1 MiB of their key/value
bytes, pointer-backed descriptors, unsupported aliases, and manifest deletes.
The system-delta source is captured before WAL and checked again before each
post-WAL materialization. Materialized batches use a fresh exact-capacity
entry array so pooled backing cannot exceed the charged source bound.

The same pre-WAL profile covers every full-set or clone source: pager pages,
all Manager files including zombies, every historical leaf generation and
file-ID slice, pending leaf file IDs, value-log read record/cache
configuration, visible resources and members, allocator debt, seals, seal
prefixes, and every collection in the live freelist COW transaction and
reservation ledger. Warm zipper applies share one output counter across pager
allocations and value-log leaf appends and check it before the allocation.
Allocation sites recheck mutable counts under their own locks before
allocating. An installed value-log dictionary callback is admissible. Before
each prepared zipper leaf read, the publisher inspects only the fixed record
and frame prefixes and rejects a dictionary-coded source before callback or
codec-cache allocation. No-dictionary zstd decode caps its destination at the
admitted raw length, including streaming and multipart frames; ordinary
dictionary and multipart decoding retains its existing behavior. Any bound
violation after command-WAL append is a broken admission invariant;
publication fails through the existing poison/recovery path rather than
retrying the batch.

Ordinary command-WAL publication may return after coordinator admission. The
prepared route instead waits through its exact commit sequence before Commit
returns. The resource/manifest clone, seal COW candidate, and allocator-debt
prefix therefore finish while the request still holds the fixed publisher
tranche; the caller cannot retire the token credit while that seal work is
still pending.

## Current bounded evidence

The warm 16K five-root focused witness profiled 33,170 primary pages, 74 stream
pages, 8,294 manifest pages, 32,952 locator pages, and 92 system pages: 74,582
total, below `Q`. Its base profile had 273 pager pages, two Manager files, four
visible resources, one visible member, and a valid COW profile with high-water
291, 41 retired pages, one changed chunk, and 225 ledger owners. A near-limit
high-entropy 16K batch reserved 2,014,371,990 bytes and completed inside the
candidate 2 GiB ceiling.

Five one-iteration public-path 4x16K runs observed median wall times of 86.6
ms for ordinary insert, 158.3 ms for prepared serial, and 146.2 ms for the
prepared pipeline. The prepared modes now include the exact-sequence
durability wait described above and held a maximum token of 1,971,047,420
bytes. In an already-built test binary, macOS `/usr/bin/time -l` reported
240,893,952 bytes maximum resident set size and 215,368,496 bytes peak memory
footprint for those repeated runs. The committed near-limit 16K high-entropy
witness reserved 2,014,371,990 bytes and reported 141,115,392 bytes maximum
RSS. These figures exercise the public path and guard against obvious
reservation/RSS regressions; they do not replace the required exact-head
1M/10M JSONBench evidence.

JSONBench must reserve exact-capacity source backing before cloning, retain
one source slot at depth zero, and keep the token's full `ReservedBytes()` in
the shared producer/committer ledger until Commit or Abandon. It must not
shrink the token credit after Prepare. `ErrPreparedInsertResourceLimit` fails
closed and must not fall back to ordinary `InsertBatch`; only an unsupported
configuration may do so.
