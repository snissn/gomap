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
stable cold/warm ranks on the same unchanged cut. Every acknowledged document,
including its vector and metadata, is compared with the retained public input
after Optimize, an explicit checkpoint and fresh reopen. These full-content
checks preserve the original first Optimize and first cold Hybrid request order.
This correctness control does not qualify concurrent ingestion, performance or
power-loss recovery.

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

### Materialization failure observations

The maintained `TestOrdinaryColumnGraphNativeColdHybrid4096` logs the original
Optimize error and its service cause on failure. Native physical construction
errors retain their existing sentinel through wrapping and identify catalog,
ordinary-domain, typed-collection, root-descriptor or workspace stages. Typed
construction additionally identifies delta ordering, source validation, row/span,
graph, compaction and resource-freeze stages. Ordinary graph refusals report
bounded numeric recipe/build roots and original LSN plus invariant booleans;
these observations do not select a replacement basis or expose document rows.
The first terminal failure remains on its original work ledger. These labels do
not make partial materialization retryable, clear accepted-prefix poison or
change publication/recovery authority. A stage report distinguishes future
failures; it does not attribute the retained #5332 race7 failure, whose Optimize
cause and precise refusing guard were not recorded.

### Pending graphs across private leaf packing

Private COW leaf packing can relocate a persisted column manifest while an
already accepted ordinary graph command remains pending. Its immutable recipe
and row-asset identities remain unchanged; its private typed header must follow
the equivalent manifest root before physical construction. The materialization
root guard remains mandatory.

Only the original private leaf-pack producer can authorize this transition. Its
fixed-size proof binds the original backend, transaction owner, index and exact
state tokens. The preparation step may register empty successor generations:
that producer must authenticate the exact state before its own registration,
its exact generation-only endpoint and the retained copy snapshot. The accepted
endpoint comes from the actual visible-state installation, including an
accepted error; a later current-state observation supplies no authority. An
intervening publication or an unexplained generation change fails closed.

Cache adoption validates this proof against its original paired snapshots under
the existing writer gate. It preserves all logical command identities, raw and
semantic sources, and current/frozen cuts. Collections additionally authenticates
the unchanged catalog identity and schema. It admits and clones only the typed
header prefix through the oldest affected ordinary graph marker, reattaches the
complete older tail, and shares immutable rows, recipes, asset references and
the original construction promise. A manifest-only move also touches the
primary header. Partial preparation uses the existing owned cleanup and refund
path; an adoption failure retains recovery-required custody.

This proof authorizes only the demonstrated leaf-copy operation. General value
rewrites, column compaction, GC and captured-prefix consumption retain their
separate guards. On-disk encodings, WAL assignment, durability and recovery
ordering do not change. See the private leaf-pack controls in
[verification.md](verification.md); authored coverage and a controlled causal
failure do not establish the historical CI interleaving.

### Scalar-forward pressure assertion and delivered controls (2026-10-31)

Exact `7fab8bacaae3142584ebdea8919297251465edcb`, TreeDB run
`41033504030`, macOS job `123988627373`, failed
`TestNativeScalarForwardOptionalRefusalAndBound` at the uncached-constructor
refusal assertion. The original failure remains causally unresolved in #5339.
The retained log establishes neither a production admission bypass nor a cleanup
timing cause.

The correction merged in [PR #5332](https://github.com/snissn/gomap/pull/5332)
separates shared-parent observations from mandatory capacity denial. The public
`TestNativeScalarForwardOptionalRefusalAndBound` keeps the real pressure setup,
independent copies, fallback, retained borrower and cache bounds. Its test-local
observer forwards the original allocation and admission unchanged. Optional
misses check their own receipt's refusal or completed release; uncached
construction must request the original nonzero charge. Actual refusals propagate
the original error, and accepted charges must match operation scratch growth.
Successful construction must produce independent validated identities matching
the retained plan. Global parent counters are not an owned-receipt ledger.

Full reserved bytes is a snapshot, not a continuing refusal guarantee: admission
may reclaim idle read scratch, and the original flush worker may refund semantic
cleanup even with periodic checkpointing disabled. Preexisting idle scratch
alone does not explain the retained assertion because the preceding optional-key
refusal also preserved parent counters. A later refund or transfer would need
actual evidence. No speculative cleanup barrier, wait, retry, cleanup suppression
or production resource-policy change was added.

`TestNativeScalarForwardHeldParentRefusal` supplies the mandatory real-budget
control. Its finite parent has held original receipts, no concurrent owner,
queued cleanup or idle scratch. With the parent exactly full, optional retention
and uncached construction must refuse while valid owned provenance borrows the
retained object without admission. Releasing only the same blocker must allow
the same original admission again. This is a direct-constructor/budget control,
not a reproduction of the original public DB schedule.

`TestNativeScalarForwardOriginalCleanupAdmission` adds a test for a public-owner
counterexample to the old unconditional refusal oracle. It holds a capture of
the exact engine superseded by the original insert, installs a test-local
forwarding observer before that engine can enqueue cleanup, then closes the
extra capture before saturation. The original flush worker is held before the
real destructor. Both original pressure setups, the unchanged optional-miss
parent counters, and zero-admission validated borrow precede release of this
selected owner. The observer forwards the real destructor and its independent
refund once, records both phases, and holds later worker advancement while the
unchanged uncached constructor follows original admission. The test requires
positive owner-attributed headroom, exact accepted scratch charges, independent
identity parity and checked original manager/DB Close with final budget zero.
There is no arbitrary lease refund, alternate admission, cap change or retry.
This constructed supported schedule can establish the fixture-contract defect;
it cannot recover which event occurred in the retained macOS failure.

`TestNativeScalarForwardOriginalAdmissionRefusal` separately uses a recording
refusal callback: validated reuse avoids admission; missing, unpublished,
foreign-owner, wrong-index and wrong-status provenance preserves the exact
original nonzero admission refusal before storage lookup. The owned-slot cases
isolate publication/index/status guards from the foreign-owner guard. This
callback control supplements the public and real-budget fixtures.

The actual PR [TreeDB run 41051075545](https://github.com/snissn/gomap/actions/runs/41051075545)
at head `c8c44856c24374180416ff1dc8c7e237b8d61450` completed all 38 jobs
successfully. Retained native PASS events for the public fixture, held-parent
refusal and cleanup-refund mechanism control cover Linux, Windows, macOS and
Linux race. These results qualify those controls on that candidate; they do not
explain the historical failure or qualify later production changes. The original
RED and the historical incident's causal closure obligation remain retained.

### Controlled owner cleanup for #5339

The retained exact7fab scalar-forward CI failure remains unattributed. The
following control tests one mechanism; it does not relax the original gate.

For #5339, `TestCOWSemanticRefundChangesAdmissionAfterFullSnapshot` uses the
maintained caching fixture, the actual budget, intrusive semantic cleanup queue,
`drainSemanticCleanup` owner routine, completion refund, and frontier join. A
channel holds one original Finish before its original external receipt is
returned. After exact parent saturation, the fixed request must refuse without
changing any budget counters. Releasing only that original cleanup must return
exactly its recorded charge and one external receipt, after which the same fixed
request must admit exactly its requested growth under the unchanged cap. There
is no sleep/retry or alternate destructor/refund. Timeouts fail an invalid
witness; they never establish success. This is a deterministic owner-level
mechanism control, not the public scalar fixture and not a reproduction of its
unobserved cleanup timing. The controlled worker invokes the production drain
routine in the existing unit fixture; it is not the public DB's flushLoop.

### Accepted ordinary graph across a captured prefix (#5338)

An ordinary graph may be accepted while an older captured typed prefix is still
being materialized. Its recipe describes the complete logical input, including
that pending prefix, and initially binds the captured `column/manifest` root.
The original physical handoff advances that manifest and removes only its exact
captured prefix. The graph remains in the newer typed suffix. Preserving only the
recipe's original physical root across this handoff makes the later graph build
reject a logically unchanged source. The accepted command must not be rebuilt or
assigned again to hide that failure.

Each typed graph delta now has a private `graphSourceRoot` header, initially equal
to the immutable recipe's original `sourceRoot`. The ordinary-prefix handoff may
advance this header only on the already-owned cloned suffix. It authenticates
the primary and manifest results from the same original build, the exact
collection incarnation, manifest interpretation/catalog/base roots, recipe
schema and source options, and the header's old basis. General physical
compaction that rewrites previously physical sources is outside this proof and
does not gain permission to advance the graph header.

The original typed producer records a fixed-size census of the ordinal and
vector codec policies selected for its prepared images. The same original build
carries this census to handoff. These are the actual prepared producer policies,
not a claim that every stored section uses compression: a producer can emit an
uncompressed fallback. A graph with an exact pending-source codec promise accepts
only policies within that promise; mixed policies cannot collapse into a falsely
compatible single value. A conservative original promise retains its existing
decoder capacity. No document rows or assets are inspected again at handoff, and
no replacement admission receipt, graph constructor or source backing is created.
The recipe, typed row backing, assigned LSN/member ordering and build-capacity
receipt stay unchanged. Held captures retain their original header and recipe.

`materializeOrdinaryVectorGraph` still checks the authenticated effective source
root against the build's input manifest root before graph output. Diagnostics
report both original recipe and effective header roots. Successors must continue
to preserve the original constructor promise until its graph finishes; advancing
a header does not end the pending-source allowance when those sources become
physical.

A physical handoff can also happen during recipe preparation, before command
assignment. Final publication preparation checks the original document
generation and physical basis. A real logical change remains a conflict. An
exclusive physical-basis stale result may refresh preparation once, after the
first transaction has fully released its input, only when no LSN was assigned,
no publication was accepted or installed, and the result is not uncertain or
ambiguous. Joined cleanup failures remain visible and prohibit this refresh.
Accepted-command and partially materialized failures retain the original
terminal error and recovery-required behavior.

`TestNativeOrdinaryGraphPreassignmentSourceChangesHaveNoEffect` stages through
the original graph census, admission and canonical command payload, then uses a
real public checkpoint or row insertion to change the captured source before
commit. It checks the original refusal has no command assignment, uncertainty,
logical installation, accepted marker or WAL-sequence advance, and preserves the
original recipe and all rows. A fresh public rebuild follows complete release
of the refused transaction. This control does not directly schedule the
automatic wrapper's second attempt; that bounded retry policy is source-reviewed.

`TestNativeOrdinaryGraphAcceptedAcrossCapturedManifestBuild` crosses these real
public boundaries. The maintained asset-after-prepare hook holds the original
older build; a nonblocking, root-filtered test observer records the singleton's
existing accepted-publication event and real assigned LSN. The observer cannot
replace publication, pause the engine or direct cleanup. The witness checks the
exact schema/incarnation, captured boundary, original recipe basis and actual
newer suffix before releasing the build. Missing prerequisites or timeouts fail
as `INVALID_WITNESS`; sleeps, polling, fabricated roots and direct recipe
mutation do not establish acceptance.

The changed-manifest case requires an actual old-to-new base transition and
checks unchanged held cuts, original resource identities, every acknowledged
row, successful original graph completion, checked Close and fresh reopen. Its
sequential and unchanged-manifest controls keep the same public data and APIs.
The process-exit variant stops after the acknowledged graph seal, before an
extra checkpoint or owner Close, then verifies the exact acknowledged manifest
identity and all documents in a fresh owner. It covers process recovery, not
power loss. The original 4096/128 service and cold-Hybrid controls remain
independent requirements. Source-bound execution records must distinguish this
captured-prefix mechanism from the retained exact7fab/#5332 incident, whose
original refusing guard was not recorded and remains unattributed.

The [#5338 evidence record](../evidence/native-columngraph-handoff-5338/README.md)
binds the controlled RED/GREEN, independent reviews, lifecycle measurements and
original validation failures to the tested source. It distinguishes the repaired
handoff from the historical incident and retains incomplete or failing gates.
