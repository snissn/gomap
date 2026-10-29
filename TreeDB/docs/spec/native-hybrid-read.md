# Native captured hybrid reads

Ordinary JSON collections use the maintained hybrid executor for lexical search,
ordinary cosine float32 ColumnGraph, scalar equality/range/conjunction
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

Hybrid temporary arrays, copied scalar IDs, candidate conversion, fusion and
final fetch storage grow the private view's original native receipt before
allocation. Native scalar-ID sets, scanner deduplication and fusion/adaptive
proof indexes use a concrete local ID index with checked power-of-two cell
capacity and full string equality. Dynamic growth keeps live entries plus
tombstones at most half of cell capacity after insertion, so at least half the
cells remain empty; constructor hints retain their conservative spare slot.
Header, replacement cells, typed value lanes
and independent key copies are admitted before allocation or mutation; displaced
backing remains cumulatively charged until the complete request ends. These
indexes add no identity authority or owner. The shared carrier keeps legacy map
lookup and distinguishes an absent filter from a present empty set. Text analysis
and record reads reuse the private view's callback; ordinary standalone text
search continues owning its original operation lease. This representation does
not certify unrelated query or DDL maps. Native seek inspection counts tombstoned
and shadowed physical sources rather than only emitted live IDs.
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

Ordinary unquantized `column_graph` candidates use the existing prepared-pack
cache with the same captured native read and manifest identity. For default
mixed-source execution, cold pack preparation finishes before text/vector
candidates run concurrently. Its original temporary validation leases end before
text scratch grows; the independently charged retained pack remains owned by the
cache. Traversal receives the one actual acquisition result and statistics, and
still checks the prepared owner's lock and closed state. No warm-up request or
second capture is required after reopen. Explicit source-order strategies retain
their existing short-circuit order and finite admission checks.

Cold pack opening still reserves its complete construction envelope before file
mapping, parsing and validation. On capacity refusal it may ask the original live
capture to [release idle shared ZSTD decoder scratch](cow-cache-publication.md), then
retry the same reservation once only after an actual release. After the complete
opener succeeds, a supported flat single-file view may shrink that same receipt
to its checked retained backing, preserving the original state/graph and control
floors. The retained census includes the actual mapped extent or heap copy,
view/chunk capacities, manager/handle/pin controls, retained strings and file
ownership. Unknown shapes, overflow or a nonbeneficial census keep the full
reservation. Failure and cache Close retain the original handle-before-receipt
cleanup order. The independent process namespace-path cache is outside this
per-pack receipt; this is not a whole-process RSS bound or zero-global-retention
claim. No limit, graph identity, corruption check or query contract changes.

A native `VectorIndexSearchBuffer` retains its separate query receipt until
`Reset` or reuse. Reset drops all native scratch/result backing before refund,
so returning buffers to an existing pool cannot retain uncharged arrays. Result
IDs remain valid after the read view closes, until buffer reset; document fetch
must finish before then.

Specialized typed, normalized and quantized vector dispatch remain explicit and
are not reinterpreted by this adapter. New ordinary service indexes select cosine
`column_graph`. Ordinary filtered ANN and vector hybrid requests fail closed;
typed filtered graph requests and text-only filters retain their own contracts.
Ordinary native ColumnGraph supports its captured graph,
text, scalar and final-document reads without algorithm substitution or legacy
fallback. Broader default adoption and actual server storage defaults remain
separate parent obligations and require their public performance gates.
The two document commands compose that public native owner and its one manager,
service, diagnostics and maintenance lifetime. `-legacy-storage` explicitly
selects the maintained owner for specialized typed/normalized/quantized routes;
ordinary ColumnGraph does not require it. Ordinary ColumnGraph remains
`command_wal_durable` only under either owner. Deferred and raw wire vector
mutations preserve immediate document/text/scalar visibility, but mixed Hybrid
and unfiltered ANN refuse unresolved or stale graph sources until explicit
Optimize. Prefix materialization and Flush/Checkpoint do not rebuild the graph.
The HTTP command exposes Optimize; the standalone wire command requires orderly
shutdown, public owner/API Optimize against the same directory/profile, and
restart. Plain indexed wire CRUD remains supported across all three profiles.
Unsupported declarations never retry another owner. Partition/replication availability still requires actual endpoint
and runtime registration; the flag does not add them. Command 64/v5 transports
the ordinary public Dense route, including intentional filtered exact scans,
without claiming HNSW or typed attribution for ordinary ColumnGraph. See the
[command owner contract](../document-service-native-constructor.md#command-storage-owner).
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

Ordinary native V2 Keyword requests with a finite primitive scalar prefilter and
no vector source may reuse a composite derived entry on the existing immutable
`nativeCollectionState`. It contains the complete conjunction's immutable IDs
and the already validated forward ordinal identities/sorted ordinal vector.
Mixed Hybrid, V1, compound/multikey or unsupported clauses, absent filters and
empty selections keep the existing path. Query analysis, root formats/status,
posting/norm validation, candidate limits, final capture checks and projection
still run for each request. Misses perform the original scalar and forward work
before optional publication at final successful query completion; malformed forward identities are never published.

The exact captured owner supplies schema, domain roots and source interpretation;
ordered typed clauses (including absent bounds and inclusivity), text index and
scalar lookup/aggregate limits supply the query key. A forward borrower also
checks its owning capture and freshly decoded complete text status. No read,
callback, context or scratch is retained. Published IDs are read-only and never
enter destructive mixed-Hybrid refinement. An operation-local construction is
attached only after the full conjunction is finished.

Native read-view foreground scopes use the backend already borrowed by the live
`NativeJSONRead`, including warm scalar-plan reuse. The native collection facade
keeps its legacy handle empty. Closed views do not start a foreground scope.

The existing original parent admits a separate independent retention lease,
bounded by the same 64 slots and 8 MiB used by native scalar plans. Complete
independent table, map, vector, clause/string and metadata backing is admitted
before cloning or publication. Optional lookup keys use a separate short-lived
independent lease, with every key alias cleared and the lease released before
original miss work. Empty owners skip key compilation. The existing operation
scratch holds the unencoded construction inline; successful completion alone
attempts retention after all original result/projection/fusion admission.
Concurrent fills recheck under the owner mutex;
there is no eviction of active borrowers. Optional refusal or saturation leaves
the successful uncached construction available. New logical, DDL and physical
replacement states start empty, while held old captures retain their entries.
Last-reference cleanup detaches all entry and lease aliases before transferring
the independent receipt to the existing completion chain. Maintenance can make
subsequent requests cold; it remains enabled during public measurement.

This is a provisional Keyword optimization. Its activation/lifetime controls and
original public workload comparison are required before adoption; helper results
or profile ceilings do not establish public performance acceptance.
