# Native captured hybrid reads

Ordinary JSON collections use the maintained hybrid executor for lexical search,
ordinary cosine float32 ColumnGraph and native-runtime HNSW, scalar equality/range/conjunction
filters, rank fusion and final JSON projection.
This is a read adapter over existing text and scalar domains. It does not add a
publication authority or select a server default.

The executor owns one `CollectionReadView` and its `NativeJSONRead` for the whole
request. Scalar iterators, text analysis/search, no-document HNSW traversal and final document fetch borrow
that capture and its immutable schema. A native snapshot explicitly reports
`Native`, `NativeLogicalSequence`, `CollectionIncarnation` and
`DocumentGeneration`. Existing physical snapshot fields describe the captured
physical basis; `CollectionGeneration` retains its legacy meaning and is not a
logical-sequence alias. A cold reopen can legitimately report a nil semantic
state and logical sequence zero while the read privately imports the captured
physical catalog with its persisted incarnation and generation.

At the existing current-snapshot phase boundaries, a short-lived native capture
compares the current **global** logical sequence to the original sequence. Every
probe closes on success and failure. An intervening logical publication, including
one in an unrelated collection, fails closed and clears results and debug
candidates. A physical-only checkpoint does not change this identity. Independent
held public read views retain their old data. The legacy `validateOpen` fence and
legacy physical snapshot comparison remain unchanged.

Hybrid temporary arrays, copied scalar IDs, dedupe maps, candidate conversion,
fusion and final fetch storage grow the private view's original native receipt
before allocation. Charges remain cumulative until the complete request returns:
serially abandoned Go backing can coexist until collection. Text analysis and
record reads reuse that callback for this private hybrid view; ordinary standalone
text search continues owning its original operation lease. Map allowances include
small groups, load-factor rounding and growth cohorts. Native seek inspection
counts tombstoned and shadowed physical sources rather than only emitted live IDs.
Final fetch admits its result headers, ID copies and full ordinary native input before allocation. Its independent cap-limited document output transfers directly to the final response; no redundant final clone is needed. The maintained JSON projection constructor retains its existing public-output scratch contract: path maps, RawMessage map/key arrays, encoding/json decoder and encoding buffers are not claimed to be individually governed by the native receipt. Feature allocation/footprint qualification must include that inherited construction cost. Plain Get/fetch keep their existing nil-admission output contract and typed reconstruction is not expanded.

The native read's existing admitted point scratch may reuse its last decoded
immutable leaf only for the complete same leaf pointer and exact scratch backing
under that same retained capture. Other destination buffers, misses and failed
reads invalidate this identity; Close detaches the backing before refund. Each
tree traversal still validates page checksum/type and PRIMARY operands. A held
decoded page does not promise a fresh disk-corruption inspection on every use;
fresh captures and physical misses retain the original file/record checks.

The existing per-leaf, aggregate, postings, cancellation and no-primary-scan
contracts still apply. Empty scalar sets still validate the captured text analyzer.

Maintained V2 query caches may retain only strict scalar-allowed ordinals in
decoded norm/docmap blocks, on both legacy and native reads. Every excluded entry
is still structurally parsed and validated. Capacity follows selected ordinals
within each block; an all-ordinals shortcut and unfiltered/rewrite caches retain
complete blocks. Borrowed docmap IDs keep their entire original raw record live,
and native admission remains conservative for that complete source. This changes
retained working storage without changing scoring, scan counters or codecs.

Scalar-filtered V2 ordinal lookup can borrow one named-domain point reader for
multiple requested IDs. Each point resolves the original newest logical sources
first; physical successors are absence and are never decoded as requested rows.
The reader reuses the existing owned projection and ordinary `Seek`. Its held,
authenticated leaf can position a later key within that leaf; other requests use
the original root positioning, without walking intervening records. The original
capture, PRIMARY choice, value-copy/codec admission and status checks still apply.
Reader storage, its method closure and lazy physical projection are admitted on
the enclosing cumulative receipt and detached before that receipt is released.

Ordinary `native_runtime` cosine float32 vector candidates borrow the same cut.
The existing graph-only traversal and scalar-column matcher supply IDs and
scores without document fetch or exact scan fallback. Cold load decodes that
captured graph image; short installer captures validate identity only. Native
scalar plans bypass the legacy retained cache because their ID sets belong to
the request view. Parallel text/vector branches serialize receipt growth through
a private mutex, while traversal remains independent.

Ordinary unquantized `column_graph` candidates use the existing prepared-pack
cache with the same captured native read and manifest identity. For default
mixed-source execution, cold pack preparation finishes before text/vector
candidates run concurrently. Its original temporary validation leases end before
text scratch grows; the independently charged retained pack remains owned by the
cache. Traversal receives the one actual acquisition result and statistics, and
still checks the prepared owner's lock and closed state. No warm-up request or
second capture is required after reopen. Explicit source-order strategies retain
their existing short-circuit order and finite admission checks.

A native `VectorIndexSearchBuffer` retains its separate query receipt until
`Reset` or reuse. Reset drops all native scratch/result backing before refund,
so returning buffers to an existing pool cannot retain uncharged arrays. Result
IDs remain valid after the read view closes, until buffer reset; document fetch
must finish before then. Legacy buffers retain their existing reusable capacity.
The public ordinary `idx.Search` retains its existing ownership and callback
postvalidation; its query charge calculation is shared without changing ranking.

Specialized typed, normalized and quantized vector dispatch remain explicit and
are not reinterpreted by this adapter. New ordinary service indexes retain the
original omitted-strategy policy: cosine selects `column_graph`; other supported
metrics select `native_runtime`. An existing index with omitted strategy binds its
persisted algorithm. Explicit native-runtime HNSW remains supported within this
adapter's capability. Ordinary native ColumnGraph supports its captured graph,
text, scalar and final-document reads without algorithm substitution or legacy
fallback. Broader default adoption and actual server storage defaults remain
separate parent obligations and require their public performance gates.
The original eight-cell service fixture and partial failure stay in the evidence;
these dispatch rules do not establish performance acceptance.

The selected public controls cover V1/V2 text, scalar equality/range/AND, projection, held
views, logical stale checks, checkpoint and cold reopen, empty-set analyzer
validation, finite refusals, cancellation and release. The incarnation-change
control uses the existing internal catalog replacement plus real native commit
because no public collection-drop API exists. Small scalar-limit controls use the
private existing plan limits; these controls do not change production scalar-plan
limits or public query options.

`TestOrdinaryColumnGraphNativeColdHybrid4096` covers the maintained durable
4096-document, fixed-128 request geometry and the first mixed request after clean
reopen, without a preceding ANN warm-up. It also checks retained pack reuse and
stable cold/warm ranks on the same unchanged cut. This correctness control does
not qualify concurrent ingestion, performance or power-loss recovery.
