# Collection Text Index v2 Contract (M0 / #2623)

Status: M0 design gate plus M1-M6 landed behavior for `#2622` and the bounded
`#2733` phrase/proximity/analyzer extension. This document fixes production
contract, rollout vocabulary, storage boundaries, counters, and benchmark gates
that later text-v2 implementation PRs must satisfy before they can claim
readiness.

TreeDB remains pre-alpha, so on-disk formats may change, but unsupported or
corrupt text-v2 formats must fail closed. Later milestones may refine binary
encodings, but they must not weaken the storage and evidence requirements below.

## Non-negotiable storage contract

Text index v2 is B-tree-native TreeDB storage:

- no LSM, external IR engine, Lucene/Tantivy/Bleve dependency, standalone
  posting-block files, legacy large-value storage path, or private text-block GC
  domain;
- every text-v2 root is a normal collection root descriptor published through
  the system tree with the same ordered-root publication discipline as other
  collection roots;
- inline-sized docmap, term, posting, norm, position, and generation payloads
  live in B-tree leaf values;
- oversized payloads use the existing persistent `value_vlog` pointer path
  selected by root storage policy, and live pointers must remain visible to the
  normal maintenance-root scanner so `ValueLogGC` and value-log rewrite preserve
  reachable blocks;
- physical reclamation composes with ordered root publication, value-log
  GC/rewrite, leaf-generation GC/pack, index vacuum, and `CompactStorage`;
- logical text maintenance owns generations, tombstones, small-block coalescing,
  high-document-frequency term rewrite/merge, and deleted-doc cleanup.

## Public version and rollout vocabulary

`TextIndexDefinition` now carries the contract fields:

```go
type TextIndexDefinition struct {
    Name    string
    Fields  []TextIndexField
    Analyzer TextAnalyzer
    Version TextIndexVersion     // "v2" default for new indexes; explicit "v1" is compatibility/opt-out
    Rollout TextIndexRolloutMode // "primary" default today
    // existing position/offset/storage/schema fields...
}
```

`TextIndexVersion` values:

- `""` / `"v2"`: default B-tree-native v2 physical contract for newly-created text indexes. Root-state creation, writable posting/norm/docmap maintenance, exact BM25F serving over posting blocks, scalar-pruned hybrid candidates, block-max skipping, lazy compact/detailed field-term summaries, rewrite/merge, and normal TreeDB physical maintenance are supported.
- `"v1"`: explicit compatibility/opt-out path for the legacy per-`(term, documentID)` text index and existing v1 on-disk indexes. V1 remains readable/writable, but new default-sensitive callers must request it explicitly.

`TextIndexRolloutMode` values:

- `""` / `"primary"`: active production read/write path. V2 is the default readable/searchable path for newly-created text indexes. Explicit v1 indexes remain readable/writable for compatibility. V2 serves BM25F from posting/norm/docmap roots, supports scalar-pruned hybrid candidate generation, and builds lazy compact/detailed field-term summaries for final results with an optional positions/offset payload lane for `StorePositions` indexes.
- `"shadow"`: future v2 build/validate without serving reads.
- `"dual_write"`: future v1+v2 mutation maintenance with explicit read choice.
- `"disabled"`: future metadata-present but non-serving/non-writing state.

Non-primary rollout modes are reserved and fail closed until the corresponding
implementation lands. Later PRs that enable them must add reopen, mutation,
status, benchmark, and downgrade/fail-closed tests.

`Collection.TextIndexStatus(indexName)` reports the active contract:

- version and rollout mode;
- ready/readable/writable booleans;
- fail-closed reason when unavailable;
- active root names (v2 by default, v1 for explicit compatibility indexes) and reserved v2 roots;
- required counter names;
- lightweight rewrite/merge readiness (`ready` for v2) and TreeDB
  physical reclamation path. Full storage validation and detailed
  `rewrite_merge_pending`/`compacted` state are reported by
  `TextIndexStorageStats` so health/status calls do not scan large roots.

## Reserved v2 root families

For collection `docs` and index `lexical`, v2 root descriptors are reserved as:

| Family | Root descriptor |
| --- | --- |
| document ordinal map | `docs/text-v2-docid/lexical` |
| ordinal-to-documentID blocks | `docs/text-v2-docmap/lexical` |
| term metadata/statistics | `docs/text-v2-terms/lexical` |
| compressed posting blocks | `docs/text-v2-posting-blocks/lexical` |
| packed norm/field-length blocks | `docs/text-v2-norm-blocks/lexical` |
| optional positions/offsets | `docs/text-v2-positions/lexical` |
| generations/tombstones/rewrite state | `docs/text-v2-generations/lexical` |

M1/M2 may define the exact key/value encodings, but these roots remain normal
TreeDB collection roots. Pointer-backed values must use `value_vlog`, not a new
text asset store.

## M1 root/key/value format (#2624)

M1 assigns the first concrete v2 root format. Every v2 root contains a versioned
format record. Default `TextIndexVersionV2` creation through `CreateTextIndex`
publishes all seven v2 root descriptors through the normal collection-root
system records. `CreateCollection` with default-v2 text indexes bootstraps empty
v2 root descriptors, format records, corpus/field-stat shells, and the status
record atomically with collection metadata, so a newly-declared text index is not
left as metadata without roots. V2 indexes maintain durable v2 roots and posting
blocks for create/backfill/insert/update/delete. `TextIndexStatus` reports
`readable=true` and `writable=true` for v2 indexes: query routing serves BM25F
from v2 posting blocks, packed norm blocks, and docmap blocks. Score-only result
mode carries only document ID, rank, score, and score kind; compact/detailed
modes build field-term summaries only for returned final results. There is no
fallback to v1 for requested/default v2.

Stable M1 ordinal semantics:

- document ordinals are `uint64`, start at `1`, and are assigned in deterministic
  documentID/primary-key order for backfill and mutation batches;
- `0` is invalid and reserved;
- ordinals are never reused. Reinsert after delete receives a fresh ordinal;
- updates keep the ordinal and increment the document generation;
- deletes increment generation and leave tombstoned docID/docmap/norm records so
  later postings can reject stale `(ordinal,generation)` matches after reopen.

M1 root contents:

| Root | Key/value records |
| --- | --- |
| `text-v2-docid` | documentID key -> `{ordinal,generation,flags}`; tombstone flag marks deleted/currently non-ranking documentIDs. |
| `text-v2-docmap` | ordinal block key -> sorted ordinal-to-documentID entries with generation/tombstone flags for final ID materialization and stale-generation checks. |
| `text-v2-terms` | corpus stats, field stats for BM25F average length, and term stats shell (`df`, `ttf`, posting block count placeholder). |
| `text-v2-posting-blocks` | M2 posting-block keys are `(term, blockStartOrdinal, blockID)` under the normal collection root; values contain compressed scoring postings and block summaries. |
| `text-v2-norm-blocks` | ordinal block key -> packed per-field token lengths with generation/tombstone flags, sufficient for BM25F field-length/norm inputs without per-candidate text-state lookup. |
| `text-v2-positions` | optional M6 lazy payload entries keyed by `(ordinal, term)` for `StorePositions` indexes; format-only when positions are disabled. |
| `text-v2-generations` | index status record: root/stats/docmap/norm/term generations, next ordinal, live documents, deleted/tombstoned ordinals. |

All v2 decoders check the key/value version, root family, block bounds, strict
ordinal ordering, supported flags, field counts, status-generation bounds, and
trailing bytes. Unsupported versions or malformed records return
`ErrTextIndexStorageCorrupt`. Corpus and field stats are validated against the
status generation and live document count, while per-term stats carry a
non-zero generation bounded by the term/status generation so unchanged terms can
remain valid across root-state updates. BM25F average-length inputs are read
from one published root set.

## M2 posting block format (#2625)

M2 assigns the compressed scoring-block contract for the reserved
`text-v2-posting-blocks` root. Posting blocks are still ordinary B-tree records
inside that collection root; there are no external posting files, typed assets,
legacy large-value side stores, or private text-block GC records. The root uses
the text index storage policy: cache-local blocks are targeted at about 128
postings and <=16 KiB
encoded payloads, while unusually large values use the existing TreeDB
value-log/leaf-log pointer path when the root policy selects it. Those pointers
remain reachable through the collection root descriptor and normal maintenance
root scanning.

Posting block keys have a term prefix plus block identity:

```text
textV2KeyVersion | postingBlockKind | len(term) | term | big-endian blockStartOrdinal | big-endian blockID
```

`blockStartOrdinal` is the first document ordinal in the value and `blockID` is
non-zero. The ordering lets iterators range-scan one term with a prefix and see
blocks in increasing ordinal/block identity order. `blockID` gives later M3/M7
writers room for append-only delta/micro blocks and rewrite generations without
rewriting one giant high-df value. Expected lifecycle: sealed blocks are the
stable compact representation, micro/delta blocks absorb small writes for
high-df terms, and later merge/rewrite tooling replaces old block keys through
ordinary collection-root deltas so physical reclamation remains normal TreeDB
root/value-log/leaf maintenance.

Posting block values are versioned and fail closed. As of M5, newly written
values include a checksum over the encoded block payload so block-max search can
trust summary bytes before deciding to skip entry decoding; checksum mismatch is
text-v2 storage corruption. Each value stores:

- block kind: sealed, delta, or micro;
- block identity (`blockStartOrdinal`, `blockID`) repeated for key/value
  validation;
- exact summary: first/last ordinal, doc count, max total term frequency, and
  max per-field term-frequency lanes;
- `UpperBoundKind=BM25FLaneMax`, meaning the per-field maxima are admissible
  BM25F upper-bound inputs for non-negative BM25F weights. A later scorer may
  combine those maxima with term IDF and the most favorable valid norm/length
  assumptions to get a safe (possibly loose) block upper bound. If a future
  scorer cannot compute or validate such an admissible bound for a query, it
  must treat the block as unskippable and fall back to exhaustive scoring;
- entries encoded as strictly-increasing document-ordinal deltas, document
  generation, term frequency, and one term-frequency lane per indexed field.

Positions and offsets are deliberately absent from the scoring block. M6 stores
optional lazy position/offset payloads in `text-v2-positions` for indexes created
with `StorePositions`; the hot scoring value remains unchanged.

M2 provides codecs, builders, storage validation, and streaming/range iterators
that reuse entry scratch while scanning a block. It does not route
`SearchText`/`SearchHybrid` through v2 posting blocks.

## M3 streaming write path (#2626)

M3 makes explicit `TextIndexVersionV2` indexes writable. The write path uses a
streaming analyzer sink and compact per-field/per-document term accumulators,
avoiding the previous token
slice allocation in the hot analyzer path. Backfill emits sealed posting blocks;
maintained insert/update writes emit append-friendly micro blocks with
`blockID` in a mutation-generation namespace (currently the high-bit block ID
range ORed with the next text-v2 root generation, fixed for every micro chunk
in that generation because `blockStart` already distinguishes chunks) so
repeated updates of a high-document-frequency term do not rewrite or collide
with old term state.
Deletes do not emit posting tombstone blocks; instead docID/docmap/norm
generations and tombstone flags advance so later readers reject stale
`(ordinal,generation)` entries until
M7 rewrite/merge compacts old blocks.

M3 maintains term `PostingBlockCount` alongside live `df`/`ttf` stats. A term may
retain zero live `df`/`ttf` with non-zero historical posting blocks until normal
TreeDB root maintenance and later text rewrite/merge remove stale blocks. All
posting, norm, docmap, docID, term, and status records remain ordinary collection
root values and continue to use existing value-log/leaf maintenance paths.

## M4 score-only search executor (#2627)

M4 enabled v2 indexes for exhaustive score-only BM25F serving. The
executor analyzes query terms through the streaming analyzer sink, de-duplicates
and deterministically orders v2 terms, opens posting-block iterators for every
query term under one TreeDB snapshot, scans posting-block entries, and scores
current candidates using packed norm blocks and term/field stats from the same
published v2 root set. It performs no per-candidate v1 text-state root lookup in
the normal path.

Visibility is generation-based: posting entries whose `(ordinal,generation)` do
not match the current norm/docmap generation or that map to tombstoned ordinals
are ignored, so append-only historical blocks from updates/deletes are safe until
later rewrite/merge compacts them. Doc IDs for ranking tie-breaks and final IDs
come from v2 docmap blocks. Snapshot-local norm/docmap caches are bounded by the
number of ordinal blocks touched by the query and are discarded after the search;
no shared cross-snapshot cache is introduced.

M4 is deliberately exhaustive. It reports `posting_blocks_visited`, keeps
`posting_blocks_skipped=0`, and does not implement WAND/BMW or scalar pruning;
those are owned by #2628. V2 score-only candidate generation reports
`docs_fetched=0`, `match_details_built=0`, `state_lookups=0`, norm-block read
counts, postings decoded, blocks visited, and candidates scored. M6 later adds
lazy compact/detailed materialization for final results; score-only mode remains
empty by contract.

## M5 block-max skipping and scalar pruning (#2628)

M5 adds an exact block-max path for the common single-term v2 BM25F top-K shape.
The executor maintains an exact top-K threshold while scanning posting blocks in
term/ordinal order. A block is skipped only when all of the following hold:

- the posting-block value carries the checksum-protected v2 block payload;
- the block summary has supported `UpperBoundKind=BM25FLaneMax` metadata with one
  max term-frequency lane per indexed field;
- the computed block upper bound is strictly lower than the current Kth score.

Strict comparison is required for deterministic tie handling: equal upper bounds
remain unskipped because a later document with the same score but a smaller
DocumentID could still displace the current tail.

The admissible BM25F bound is intentionally conservative. Field weights are
validated finite and non-negative. For every field with term frequency `tf`,
BM25F normalization divides by `1-b + b*(fieldLength/avgLength)`. Since
`fieldLength >= 0`, `avgLength > 0`, and `b=0.75`, the denominator is at least
`1-b`. Therefore each posting's field contribution is bounded by the block's
`maxFieldTF/(1-b)` lane, the weighted sum is an upper bound on combined TF, and
the BM25 saturation function is monotone for non-negative combined TF. Applying
the term IDF to that saturated combined-TF bound gives an admissible per-block
score upper bound. Stale generations and tombstoned ordinals can only make this
bound looser.

Multi-term OR/AND queries remain exact by falling back to the M4 exhaustive
scorer until a fuller WAND/BMW doc-at-a-time algorithm lands. Fallbacks are
reported with `blockmax_fallbacks/search`. Threshold raises are reported with
`threshold_updates/search`. The fallback scorer still uses scalar ordinal
pruning where safe.

M5 also threads hybrid scalar prefilters into v2 text candidate generation. The
hybrid planner's bounded scalar-index document-ID allow-set is translated through
the `text-v2-docid` root into an ordinal allow-set under the same snapshot
checks. Empty filters return no text candidates without traversing postings;
rare filters can skip posting blocks whose ordinal ranges do not intersect the
allow-set; broad filters remain exact and may decode more blocks. This path
fetches zero full documents and treats missing/tombstoned scalar-filtered docID
mappings as text-v2 storage corruption.

## M6 lazy match details and positions (#2629)

M6 defines result modes for `SearchText`:

- `score_only`: document ID, rank, score, and score kind only;
- `compact`: `TextMatches` field/term summaries only;
- `detailed` / zero value: `TextMatches` plus legacy `MatchedTerms` and
  `MatchedFields` for API compatibility.

V2 match summaries are materialized after final top-K selection. The score-only
search and hybrid candidate hot paths do not build match summaries, do not read
v1 text-state, and do not fetch full documents. `SearchText` with
`IncludeDocuments=true` still fetches documents only after final ranking and
remains bounded by returned top-K.

For exhaustive multi-term v2 search, final-result summaries are reconstructed
from the already-scored final candidates' term/field-frequency lanes. For the
single-term block-max path, compact posting detail is retained only for the
bounded top-K heap so final summaries do not require a full posting rescan. The
`text-v2-posting-blocks` scoring format is unchanged.

If a v2 index is created with `StorePositions`, M6 writes optional
`text-v2-positions` entries keyed by `(ordinal, term)`. Current value format v2
keeps the current document generation and per-field term frequency, delta-codes
strictly increasing token positions, stores optional offsets, and intentionally
omits the term already present in the key. Legacy value format v1 (with the term
and absolute positions in the value) remains readable for pre-alpha test
fixtures, but newly written position entries use the smaller key-bound v2
payload. Detailed/compact materialization validates these entries for returned
final results and fails closed on missing, corrupt, mismatched, or unsupported
payloads; score-only mode does not read the lane. Update/delete maintenance
removes old position entries through ordinary root deltas before writing any
replacement entries, so physical reclamation remains normal TreeDB
root/value-log maintenance. There is no standalone text-block GC.

`SearchHybridTextCandidates` and the text leg of `SearchHybrid` use score-only
mode by default. `HybridTextQuery.IncludeTextMatches=true` opts into compact
field/term summaries for the bounded requested text candidate set; default hybrid
candidate generation emits no `TextMatches` and keeps `docs_fetched=0`,
`state_lookups=0`, and `match_details_built=0`.

## Bounded phrase/proximity and analyzer options (#2733)

Phrase/proximity search is exposed as structured `TextSearchOptions.Phrase`, not
as a Lucene-compatible query DSL. It is supported only for text-v2 indexes built
with `StorePositions=true`; v1 indexes, v2 indexes without positions, mixed
`Query`/`Phrase` requests, unsupported grouped/quoted DSL in `Query`, excessive
phrase term counts, and phrase slop above the bounded implementation maximum
fail closed instead of scanning documents.

Phrase semantics are ordered and position-lane based. The analyzer tokenizes the
phrase query with the same persisted analyzer configuration used at index build
time. Exact phrase uses `Slop=0`; positive slop permits that many total
intervening tokens across the ordered phrase within a single indexed field. The
serving path generates candidates from existing posting blocks, validates
`text-v2-positions` entries for candidate ordinals under the same TreeDB
snapshot, then scores matching candidates with the unchanged BM25F/BM25F term
statistics. Score-only phrase/proximity candidate generation still keeps
`docs_fetched=0`, `state_lookups=0`, and `match_details_built=0`; document fetch
is only the existing explicit final top-K fetch.

Analyzer options are persisted in `TextIndexDefinition.AnalyzerOptions`. The
simple analyzer supports normalized stopword filtering. Stopword filtering
preserves original token positions, so phrase/proximity slop can account for
removed-token gaps. Stemming and synonym configuration fields are reserved as
extension seams and currently fail closed during index-definition normalization;
they are not silently accepted because they would change indexed terms and
positions. Changing analyzer options changes the physical term/position contract
and requires rebuilding the text index; TreeDB is pre-alpha and does not provide
an in-place migration scaffold for analyzer-option changes.

## M7 rewrite/merge hardening and default switch (#2630/#2690)

M7 adds explicit logical rewrite/merge maintenance for text-v2 indexes through
`Collection.RewriteTextIndex(indexName, TextIndexRewriteOptions)`. The rewrite
runs entirely through ordinary TreeDB collection roots: it scans the
`text-v2-posting-blocks` root under one snapshot, keeps only postings whose
`(ordinal,generation)` still match current docmap/norm entries, rewrites retained
postings into sealed posting blocks, deletes obsolete micro/delta/stale block
keys, updates term posting-block counts, and publishes the new generation/status
record as an ordered-root delta. Deleted-document docID/docmap/norm tombstones
are purged after stale postings are removed, unless explicitly disabled by the
maintenance option. Optional positions/offset entries remain in the normal
`text-v2-positions` root; update/delete maintenance already removes stale
position payloads, and rewrite validates detailed serving by preserving current
scoring postings.

This is logical text maintenance, not a private physical GC subsystem. Obsolete
inline or pointer-backed block payloads become unreachable only by normal root
publication and snapshot release; physical bytes are reclaimed by existing
TreeDB maintenance (`ValueLogGC`, value-log rewrite, leaf-generation pack/GC,
index vacuum, and `CompactStorage`). Text-v2 roots still have no standalone
posting files/assets and no separate text-block GC domain.

Readiness/status exposes lightweight rewrite readiness without scanning large
roots:

- `ready` is the static explicit-v2 `TextIndexStatus` state;
- `rewrite_merge_pending` is reported by `TextIndexStorageStats` when
  deleted-document tombstones, micro blocks, or delta blocks remain;
- `compacted` is reported by `TextIndexStorageStats` after rewrite has published
  sealed blocks and removed tombstones.

`TextIndexStorageStats` performs full v2 storage inspection, reports
sealed/delta/micro posting-block counts, and fail-closes on malformed v2 storage
instead of declaring the inspected state ready.

## Default-rollout write and maintenance budget guidance (#2689)

The default-rollout maintenance contract is budgeted through the normal text-v2
counters rather than a private text GC. Operators and benchmark PRs SHOULD use
`TextIndexStorageStats` as the offline/maintenance inspection tool, not as a
per-request health check: it scans the durable v2 roots to validate counts,
`V2RewriteMergeState`, `V2SealedPostingBlocks`, `V2DeltaPostingBlocks`,
`V2MicroPostingBlocks`, `V2DeletedDocs`, and `EncodedBytes`. Lightweight serving
status remains `TextIndexStatus`.

`RewriteTextIndex` SHOULD be scheduled when `TextIndexStorageStats` reports
`rewrite_merge_pending`, especially when micro/delta posting blocks materially
exceed the sealed-block baseline for the workload, deleted/tombstoned ordinals
accumulate, or `posting_blocks/doc` / `write_amp_entries/doc` grows far beyond a
fresh backfill row. The production default `TargetPostingsPerBlock=0` uses the
text-v2 block target (currently 128 postings, cache-local encoded values);
smaller targets are primarily for tests, tiny fixtures, or intentionally more
frequent block splitting. A rewrite is logical maintenance only: it publishes new
ordinary collection roots, removes stale posting keys and tombstones from the
live root set, and reports `posting_blocks_read`, `posting_blocks_written`,
`posting_blocks_deleted`, `stale_postings_purged`, and tombstones purged so the
next physical maintenance pass has auditable work.

Physical cleanup MUST remain on TreeDB maintenance paths. After a rewrite (and
after old snapshots have released), callers SHOULD use the usual sequence for
their deployment: checkpoint, `ValueLogGC`, value-log rewrite when the value-log
rewrite plan shows stale source bytes, leaf-generation pack/GC or
`CompactStorage`, and index vacuum. Live v2 roots must remain reachable through
collection root descriptors throughout this sequence; stale roots become normal
unreferenced root/value-log/leaf payloads and must not require a standalone
text-block GC.

Default-selection decision for #2690: v2 is now the production default for newly-created TreeDB collection text indexes. The switch includes `normalizeTextIndexVersion`, `CreateCollection` empty-v2 root/status bootstrap, explicit v1 compatibility tests, v1/v2 coexistence tests, and the final clean-load matrix. The old per-`(term,documentID)` v1 path is retained as an explicit compatibility/opt-out path and existing v1 on-disk indexes remain readable/usable.

## Explicit v1 compatibility path inventory

Explicit v1 write/backfill hot path:

- metadata/backfill: `CreateTextIndex`, `buildCreateTextIndexBackfillPlan`,
  `analyzeTextIndexDocument`, `analyzeTextIndexField`, `addTextPostingsForDocument`,
  `addTextStatsEntries`;
- maintained writes: `appendTextIndexInsertPlanDeltas`,
  `appendTextIndexNoIndexInsertDeltas`, `appendTextIndexUpdateDeltas`,
  `appendTextIndexDeleteDeltas`, `appendTextIndexMutationDeltas`;
- storage/codecs: `encodeTextPostingKey`, `encodeTextPostingValue`,
  `encodeTextDocumentStateValue`, `encodeTextStats*`.

Explicit v1 search/hybrid hot path:

- `SearchHybridTextCandidates` adapts `SearchText` into hybrid candidates;
- `SearchText` / `executeTextSearchAtSnapshot` reads stats, range-scans postings,
  accumulates candidates, looks up text state/norm lengths, scores BM25F, sorts,
  and optionally fetches final top-K documents;
- `scanTextSearchPostingsTerm`, `collectionGetAppendAtCatalogRoot` /
  `Tree.GetAppend`, `decodeTextDocumentStateFieldLengths`,
  `textSearchCandidateMatchDetails`, `hybridTextMatchesFromSearchResult`, and
  query analysis are the current v1 search allocation/CPU owners.

Related issues that v2 should absorb or supersede with production evidence:

- #1764: collection-native text search baseline and v1 correctness contract;
- #2578: hybrid text-search hot-path optimization tracker;
- #2580: low-allocation text candidate path;
- #2581: reduce BM25F text-state lookup/decode in the hot path.

## Required counters

Every text-v2 performance PR must expose and benchmark these counters where the
row applies. Explicit v1 rows report compatible zero/legacy values so v1/v2 comparisons use one vocabulary.

| Counter | Required meaning |
| --- | --- |
| `postings_scanned` / `text_postings/search` | postings or posting entries decoded/scanned |
| `posting_blocks_visited` | v2 posting blocks decoded/scored (skipped blocks are counted separately) |
| `posting_blocks_skipped` | exact scalar-pruning or block-max/WAND/BMW skips; zero for v1 |
| `blockmax_fallbacks` | v2 query shapes or metadata states that used exact exhaustive scoring instead of block-max skipping |
| `threshold_updates` | top-K threshold raises observed by exact block-max/BMW serving |
| `candidates_scored` | candidates with BM25/BM25F score computed |
| `state_lookups` | v1 text-state lookups or v2 equivalent metadata lookups |
| `norm_lookups` | field-length/norm lookups or block reads |
| `docs_fetched` | full documents fetched after final top-K only |
| `match_details_built` | result/candidate match-detail materializations |
| `position_lookups` | position-lane root lookups used by phrase/proximity or detailed validation |
| `phrase_candidates_checked` | candidates whose positions were tested for phrase/proximity semantics |
| `phrase_candidates_matched` | checked candidates that satisfied phrase/proximity semantics |
| `scalar_filter_selectivity` | matched/(matched+rejected), reported as pct or ppm |
| `fail_closed` | fail-closed count and reason |
| `write_amplification` | text-root entries or bytes emitted per document |
| `index_bytes_per_doc` | durable text-index bytes divided by indexed documents; storage stats also expose v2 lane byte totals for docid, docmap, posting blocks, norm blocks, positions, term stats, and status/format records |
| `rewrite_merge_state` | v2 generation/rewrite/merge lifecycle state |

Candidate generation must keep `docs_fetched=0` and `full_doc_fallbacks=0`.
Hybrid final fetch must keep `docs_fetched <= topK`.

## Benchmark matrix

The normative command/runbook is
`docs/benchmarks/treedb_text_v2_contract_benchmarks.md`. Later PRs must include
all applicable rows, exact commands, hardware/OS/Go, branch/SHA, dataset shape,
measured boundary, `ns/op`, derived `ops/sec`, `B/op`, `allocs/op`, before/after
delta, and the domain counters above.

Required fixture scales:

- 256 documents: local/CI-friendly small fixture and #2564/#2589 continuity;
- 10k documents: larger local row that exposes O(df) scan/state costs;
- >=100k documents: local artifact row unless hardware limits are explicitly
  justified in the PR/issue; optional 1M smoke/profile is encouraged.

Required query and serving shapes:

- common-term, rare-term, multi-term AND/OR, scalar-filtered, phrase/proximity,
  analyzer-option, and score-only rows; M6 detailed-match rows must prove
  `match_details_built` is bounded by returned top-K/requested final results
  while score-only rows keep it zero;
- isolated text write, backfill, update, and delete rows with `-benchmem`;
- current #2589/#2564 indexed insert/search guardrail matrix on the latest base, including v1, v2, vector, and hybrid rows;
- M6 lazy detail rows comparing score-only versus detailed top-K materialization;
- concurrent serving/load row with reader concurrency, p50/p95/p99 latency,
  steady-state memory, cache warm/cold boundary, and optional mixed write or
  snapshot churn.

## Target envelope for child PRs

These targets are gates, not claims for M0:

| Row | Current context | M0 target for v2 stack | Stretch |
| --- | ---: | ---: | ---: |
| text candidates no-doc, 256 docs | ~132-141 us, ~109 KB, 878 allocs | <=40 us, <=32 KB, <=200 allocs | <=20 us, <=16 KB, <=100 allocs |
| hybrid no-doc scalar filter, 256 docs | ~164 us, ~198 KB, 1,078 allocs | <=90 us, <=96 KB, <=350 allocs | <=60 us, <=64 KB, <=200 allocs |
| text-index write overhead | not isolated enough before M0 | isolate and reduce >=3x vs current text-index-enabled path | reduce >=5x |
| larger common-term topK | v1 scans O(df) | v2 must prove block skipping/sublinear behavior | WAND/BMW skip ratio is material |

Material regressions in vector guardrails, storage durability, candidate
zero-document generation, or final-fetch bounds block mergeability unless the
coordinator explicitly accepts them with profile-backed rationale.

## Rollout/fail-closed requirements for later milestones

- M1 enables v2 root creation only with version mismatch/corruption, reopen,
  root publication, and value-log reachability tests.
- M2/M3 must prove posting/norm/docmap payloads remain ordinary B-tree values or
  existing `value_vlog` pointers reachable from collection root descriptors.
- M4/M5 search must keep score-only candidate generation zero-doc and
  zero-match-detail. M6 public v2 `SearchText` detailed/compact modes must build
  match summaries only for returned final results, and optional bounded final
  document fetch remains a separate post-ranking phase.
- M5 block skipping must be exact: upper bounds are admissible and results match
  exhaustive scoring. Approximate ranking requires a separate mode/tracker.
- Any future old-path retirement must include v1/v2 coexistence,
  downgrade/fail-closed behavior, rewrite/merge state, vacuum/GC/rewrite, and
  `CompactStorage` evidence.
