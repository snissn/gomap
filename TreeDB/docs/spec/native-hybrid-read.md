# Native lexical hybrid reads

Ordinary JSON collections use the maintained hybrid executor for lexical search,
scalar equality/range/conjunction filters, rank fusion and final JSON projection.
This is a read adapter over existing text and scalar domains. It does not add a
publication authority or select a server default.

The executor owns one `CollectionReadView` and its `NativeJSONRead` for the whole
request. Scalar iterators, text analysis/search and final document fetch borrow
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

The existing per-leaf, aggregate, postings, cancellation and no-primary-scan
contracts still apply. Empty scalar sets still validate the captured text analyzer.

Native vector candidate requests are explicitly refused before opening a read.
Ordinary HNSW hybrid/buffer adaptation and default ColumnGraph service eligibility
remain separate required capabilities. The unchanged eight-cell service fixture
must keep those later failures visible; passing filtered lexical search does not
certify a complete document service or any performance claim.

The selected public controls cover V1/V2 text, scalar equality/range/AND, projection, held
views, logical stale checks, checkpoint and cold reopen, empty-set analyzer
validation, finite refusals, cancellation and release. The incarnation-change
control uses the existing internal catalog replacement plus real native commit
because no public collection-drop API exists. Small scalar-limit controls use the
private existing plan limits; production defaults and public options are unchanged.
