# Collection Text Index v2 Contract (M0 / #2623)

Status: M0 design gate for #2622. This document fixes the production contract,
rollout vocabulary, storage boundaries, counters, and benchmark gates that later
text-v2 implementation PRs must satisfy before they can claim readiness.

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
    Version TextIndexVersion     // "v1" default today, "v2" reserved
    Rollout TextIndexRolloutMode // "primary" default today
    // existing position/offset/storage/schema fields...
}
```

`TextIndexVersion` values:

- `""` / `"v1"`: current per-`(term, documentID)` text index.
- `"v2"`: explicit v2 physical contract. M1 root-state creation is supported,
  but v2 search/rollout behavior remains fail-closed until later milestones; it
  must not silently fall back to v1.

`TextIndexRolloutMode` values:

- `""` / `"primary"`: active production read/write path. Today this is v1 only;
  explicit v2 indexes can maintain M1 root-state shells, but status remains
  fail-closed/non-readable/non-writable for the full v2 executor/write pipeline.
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
- active v1 roots and reserved v2 roots;
- required counter names;
- rewrite/merge state and TreeDB physical reclamation path.

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
format record. Explicit `TextIndexVersionV2` creation through `CreateTextIndex`
publishes all seven v2 root descriptors through the normal collection-root
system records. Declaring a v2 text index directly in `CreateCollection` is
rejected because it would create metadata without the required root status
records. V2 search is still fail-closed until the later executor milestones;
`TextIndexStatus` reports `readable=false` and `writable=false` for v2 so the M1
root-state maintenance shell is not advertised as the full M3+ write pipeline.
There is no fallback to v1 for requested v2.

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
| `text-v2-posting-blocks` | format-only in M1; #2625 owns posting block payloads. |
| `text-v2-norm-blocks` | ordinal block key -> packed per-field token lengths with generation/tombstone flags, sufficient for BM25F field-length/norm inputs without per-candidate text-state lookup. |
| `text-v2-positions` | format-only in M1; later lazy detail/position milestones own payloads. |
| `text-v2-generations` | index status record: root/stats/docmap/norm/term generations, next ordinal, live documents, deleted/tombstoned ordinals. |

All v2 decoders check the key/value version, root family, block bounds, strict
ordinal ordering, supported flags, field counts, status-generation bounds, and
trailing bytes. Unsupported versions or malformed records return
`ErrTextIndexStorageCorrupt`. Corpus and field stats are validated against the
status generation and live document count, while per-term stats carry a
non-zero generation bounded by the term/status generation so unchanged terms can
remain valid across root-state updates. BM25F average-length inputs are read
from one published root set.

## Current v1 path inventory

Current write/backfill hot path:

- metadata/backfill: `CreateTextIndex`, `buildCreateTextIndexBackfillPlan`,
  `analyzeTextIndexDocument`, `analyzeTextIndexField`, `addTextPostingsForDocument`,
  `addTextStatsEntries`;
- maintained writes: `appendTextIndexInsertPlanDeltas`,
  `appendTextIndexNoIndexInsertDeltas`, `appendTextIndexUpdateDeltas`,
  `appendTextIndexDeleteDeltas`, `appendTextIndexMutationDeltas`;
- storage/codecs: `encodeTextPostingKey`, `encodeTextPostingValue`,
  `encodeTextDocumentStateValue`, `encodeTextStats*`.

Current search/hybrid hot path:

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
row applies. Current v1 rows now report compatible zero/legacy values so later
PRs can compare one vocabulary.

| Counter | Required meaning |
| --- | --- |
| `postings_scanned` / `text_postings/search` | postings or posting entries decoded/scanned |
| `posting_blocks_visited` | v2 posting blocks decoded/considered |
| `posting_blocks_skipped` | exact block-max/WAND/BMW skips; zero for v1 |
| `candidates_scored` | candidates with BM25/BM25F score computed |
| `state_lookups` | v1 text-state lookups or v2 equivalent metadata lookups |
| `norm_lookups` | field-length/norm lookups or block reads |
| `docs_fetched` | full documents fetched after final top-K only |
| `match_details_built` | result/candidate match-detail materializations |
| `scalar_filter_selectivity` | matched/(matched+rejected), reported as pct or ppm |
| `fail_closed` | fail-closed count and reason |
| `write_amplification` | text-root entries or bytes emitted per document |
| `index_bytes_per_doc` | durable text-index bytes divided by indexed documents |
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

- common-term, rare-term, multi-term AND/OR, scalar-filtered, score-only, and
  detailed-match rows;
- isolated text write, backfill, update, and delete rows with `-benchmem`;
- current #2589 indexed insert/search guardrail matrix on the latest base;
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
- M4+ search must be score-only by default for candidate generation and must
  materialize match details only when requested/final.
- M5 block skipping must be exact: upper bounds are admissible and results match
  exhaustive scoring. Approximate ranking requires a separate mode/tracker.
- M7 default-switch or old-path retirement must include v1/v2 coexistence,
  downgrade/fail-closed behavior, rewrite/merge state, vacuum/GC/rewrite, and
  `CompactStorage` evidence.
