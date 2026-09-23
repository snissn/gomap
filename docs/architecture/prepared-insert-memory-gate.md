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
Before allocating the ordered entry and row-header arrays, preparation checks
128 bytes per row plus 1 MiB of batch overhead against the remaining credit.

The stream path partitions preparation credit across at most four concurrent
4,096-row blocks. Each block checks a source-derived fixed reserve before
cursor/raw/zstd allocation, then debits quota before path, trie, map,
interned-string, and entry-header growth. Limits include 1,024 cursor
descriptors, 1,024 paths, 131,072 entries, and 16 MiB of entry headers per
block. Typed scalar preparation borrows the already charged declared strings;
it checks a temporary dictionary/map allowance before building vectors and
final dictionaries, then charges retained typed backing. Prepared compressed
blocks return unowned raw scratch to the process-wide pool, which retains at
most four 8 MiB buffers. JSONBench permanently reserves that 32 MiB idle
capacity in its shared lane ledger; in-use scratch remains charged to its
prepared block. Raw fallback blocks take ownership of their backing and do
not return it to the pool.
The prepared zstd encoder appends into a nil destination. Its fixed block
reserve includes the old and replacement destination backings at slice growth
and the exact stored wrapper; this avoids an otherwise unused maximum-size
output allocation. The separate 8 MiB encoder-workspace charge follows from
the pinned `github.com/snissn/compress` fork (see `zstd/encoder_options.go`,
`enc_fast.go`, `enc_base.go`, `blockenc.go`, `fse_encoder.go`, and
`huff0/compress.go`). The nil-writer, concurrency-one, dictionary-free,
CRC-free, low-memory `SpeedFastest` encoder uses one 64 KiB block and a 1 MiB
window. Its history is at most the window plus one 128 KiB maximum block;
the fast hash table is 256 KiB. A block has at most 64 KiB of literals and
at most `ceil(64 KiB/3)` 16-byte sequences. Charging three times each
maximum for old and replacement Go slice backing, plus 32 bytes per sequence
and two block lengths for encoded block output, leaves under 6 MiB for these
buffers even with allocator rounding. The remaining 2 MiB covers the bounded
Huffman/FSE tables and one-block compression scratch; the fork's parallel
four-stream Huffman branch is disabled. The raw input, append-grown final
zstd destination, and stored wrapper are charged separately by the block
output reserve. A multi-block incompressible/compressible test exercises the
destination-growth and wrapper paths. A change to the pinned encoder options
or fork requires rederiving this charge before prepared admission is enabled.

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
| Stream cursor and retained block | Source-derived fixed reserve plus per-growth block quota; final retained capacity charge. Pinned single-encoder workspace fits its separate 8 MiB charge as derived above. | Recheck the source-derived charge whenever the pinned encoder or its options change. |
| Typed preparation | Prebuild reserve of 4,224 bytes per row-column cell plus 8 MiB; final vector/dictionary capacity charge. | Confirm transient map growth and dictionary-mode restrictions cover every eligible config. |
| Ordered commit | Separate source-derived allowances for materialization, WAL/runs, typed image, row asset, sidecars, and catalog. | Complete the allocation-site audit for typed section compression/workspace and aggregate maps, including old/new backing during growth. |

The ordered-commit formula is conservative admission work in progress. Its
WAL/run term separately allows three published-input copies for command
payload and old/new V2 frame scratch, three for result IDs and pointerized
run keys/values, 2,048 bytes per row for run/header/ref backing, and 8 MiB
fixed overhead. A near-16K high-entropy test and a 512-row maximum-length-ID
test exercise acceptance and clean pre-WAL rejection. The row-asset allowance
uses the sum of string lengths across all source rows, rather than distinct
interned backing: its binary length-prefix writer serializes each row value
and `bytes.Buffer` can hold old and replacement backing while growing. The
aggregate allowance likewise sums each group string across source rows and
specs, covering repeated groups emitted by separate entry sets. A repeated
8 KiB group test rejects insufficient credit before WAL and commits with the
derived adequate credit. The typed/image and aggregate terms still need a
complete source-backed proof before the strict byte gate can be marked passed.
A resource rejection after command-WAL append is an ambiguous accepted-write/
recovery case, not normal admission.

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
