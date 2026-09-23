# Prepared insert memory gate (#4819)

The byte gate applies to **incremental pipeline-owned memory**, independently
of the number of rows already stored. It covers the next source batch, the
prepared token and its transient builders, and collection-owned commit inputs
that coexist with that token. A handed-off input backing is charged once.
Existing pager, zipper, and value-log Manager publication scratch used by an
ordinary ordered commit is baseline DB work; whole-process RSS is a separate
measured guardrail. `ReservedBytes` is admission credit, not measured heap.

`PrepareInsertBatchOwned` accepts at most 16,384 rows, 1,024 bytes per ID,
and 128 KiB per document on the no-index JSON semantic-stream lane. It
supports at most five Int64/String paths of at most 1,024 bytes, a 512-byte
asset namespace, and three validated aggregate-metadata specs with bounded
predicates. It charges outer ID/document slices, element capacities, prepared
runs, declared rows, semantic blocks, typed backing, and a 32 KiB stable-schema
copy reserve. Unused outer-slice slots must be nil. The public owned-buffer
contract requires each ID/document capacity to describe its full non-aliased
allocation; Go cannot discover a larger backing hidden by a full-sliced view.

The stream path partitions preparation credit across at most four concurrent
4,096-row blocks. Each block checks a source-derived fixed reserve before
cursor/raw/zstd allocation, then debits quota before path, trie, map,
interned-string, and entry-header growth. Limits include 1,024 cursor
descriptors, 1,024 paths, 131,072 entries, and 16 MiB of entry headers per
block. Typed scalar preparation borrows the already charged declared strings;
it checks a temporary dictionary/map allowance before building vectors and
final dictionaries, then charges retained typed backing. Prepared raw scratch
is not returned to the uncharged process-wide raw-buffer pool.
The prepared zstd encoder appends into a nil destination. Its fixed block
reserve includes the old and replacement destination backings at slice growth
and the exact stored wrapper; this avoids an otherwise unused maximum-size
output allocation. The internal encoder workspace remains an open proof term.

Before creating the token, the engine reserves separate ordered-commit credit
for initial primary/stream iterator materialization, result IDs and command
WAL/frame/run/pointerization buffers, typed part/image, row asset,
dictionary/int64/aggregate sidecars, and catalog/manifest copies. The two
input iterators share one materialization budget. Pre-WAL catalog checks
reject a manifest above 4,096 inline records or 1 MiB of key/value bytes,
pointer-backed root descriptors, and unsupported root aliases. The retained
10M diagnostic catalog had 3,127 entries and 753,928 key/value bytes; the
final exact-head run must revalidate that whole-collection ceiling.

| Owner | Current admission method | Remaining proof work |
| --- | --- | --- |
| Source and queued successor | JSONBench reserves exact-capacity source backing before clone, including scanner scratch and outer headers; one source slot stays available to depth zero. | Verify final 1M/10M counters and report RSS beside credit. |
| Stream cursor and retained block | Source-derived fixed reserve plus per-growth block quota; final retained capacity charge. | Confirm pinned zstd workspace and small dynamic buffers fit the 8 MiB term for every admitted input. |
| Typed preparation | Prebuild reserve of 4,224 bytes per row-column cell plus 8 MiB; final vector/dictionary capacity charge. | Confirm transient map growth and dictionary-mode restrictions cover every eligible config. |
| Ordered commit | Separate source-derived allowances for materialization, WAL/runs, typed image, row asset, sidecars, and catalog. | Complete the allocation-site audit for typed section compression/workspace and aggregate maps, including old/new backing during growth. |

The ordered-commit formula is conservative admission work in progress. Its
WAL/run term separately allows three published-input copies for command
payload and old/new V2 frame scratch, three for result IDs and pointerized
run keys/values, 2,048 bytes per row for run/header/ref backing, and 8 MiB
fixed overhead. A near-16K high-entropy test and a 512-row maximum-length-ID
test exercise acceptance and clean pre-WAL rejection. The typed/image and
aggregate terms still need a complete source-backed proof before the strict
byte gate can be marked passed. A resource rejection after command-WAL append
is an ambiguous accepted-write/recovery case, not normal admission.

The ordered publisher may still perform a first-publication candidate scan,
read the Manager's registered file set, and rewrite outer-leaf pages. These
are existing synchronous publication costs rather than memory newly retained
by the one-ahead queue. Prepared commits preserve their GC pins, recovery,
and post-WAL ambiguity semantics. Any collection-owned WAL, typed, run,
manifest, or waiting-result backing coexisting with the successor remains
inside incremental credit even when the DB publisher consumes it.

The JSONBench lane holds producer and committer credit in one ledger. Depth
zero retains input producer/consumer overlap; depth one may prepare the next
token while the sole ordered committer publishes its predecessor. Both use
the same source policy. `ErrPreparedInsertResourceLimit` must fail closed and
must not be retried through ordinary `InsertBatch`; only unsupported config
may fall back. Final acceptance needs exact-head adversarial, durability and
no-LSN rejection tests, paired 1M/10M throughput and query hashes, allocated
bytes per row, live owned backing, and peak RSS. Diagnostic runs are not that
final evidence.
