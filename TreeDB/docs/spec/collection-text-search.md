# Collection Text Search Contract (M0-M4)

Status: PR4 ranked SearchText slice for issue #1764. TreeDB collections are
pre-alpha; these text-search storage formats are versioned and may change before
stabilization, but malformed or unsupported versions must fail closed. The
text-v2 production contract, rollout vocabulary, reserved roots, and benchmark
matrix are defined in `TreeDB/docs/spec/collection-text-v2-contract.md`.

## Implemented scope

- `CollectionMeta.TextIndexes []TextIndexDefinition` metadata.
- `TextIndexDefinition` / `TextIndexField` validation and JSON persistence.
- Root naming and storage policy handling:
  - `<collection>/text-index/<indexName>` for term/document postings; uses
    `TextIndexDefinition.StoragePolicy`.
  - `<collection>/text-state/<indexName>` for document analyzed state; uses
    `CollectionOptions.IndexStateStoragePolicy`.
  - `<collection>/text-stats/<indexName>` for corpus and term/field statistics;
    uses `CollectionOptions.IndexStateStoragePolicy`.
- `simple` analyzer: Unicode lowercase tokenization over letters, digits, and
  `_` so code-ish identifiers such as `HTTP_500` remain searchable.
- `CreateTextIndex` backfills persistent postings/text-state/text-stats roots
  over existing documents and publishes roots plus metadata atomically.
- `DropTextIndex` removes metadata and clears the text root descriptors.
- `TextIndexStorageStats` validates storage versions and returns root accounting.
- Insert, delete, update, and batch write paths maintain postings, text-state,
  and corpus/term/field stats for declared text indexes.
- `SearchText(TextSearchOptions)` executes bounded postings range scans, applies
  simple term `AND`/`OR` semantics, scores candidates with BM25F-style field
  weighting from persisted stats/text-state, and fetches documents only for final
  top-K results when requested.

Not implemented yet:

- phrase/proximity/highlighting/stemming/trigram/fuzzy search;
- query-vector syntax, gateway integration, or hybrid text+vector executor integration.

## Metadata

```go
type TextIndexDefinition struct {
    Name             string
    Fields           []TextIndexField
    Analyzer          TextAnalyzer
    Version           TextIndexVersion
    Rollout           TextIndexRolloutMode
    StorePositions    bool
    StoreOffsets      bool
    StoragePolicy     RootStoragePolicy
    SchemaGeneration  uint64
}

type TextIndexField struct {
    Field  string
    Weight float64
}
```

Validation rules:

- `Name` is required and uses normal collection index-name validation.
- At least one field is required.
- Field paths use the same dotted-path validation as scalar indexes.
- Duplicate fields in one text index are rejected.
- `Weight == 0` normalizes to `1.0`; non-zero weights must be finite and
  non-negative.
- Analyzer `""` normalizes to `"simple"`; other analyzers fail closed until
  implemented.
- Version `""` normalizes to `"v1"`; `"v2"` is reserved by the #2623 contract
  and currently fails closed until text-v2 roots land.
- Rollout `""` normalizes to `"primary"`; `"shadow"`, `"dual_write"`, and
  `"disabled"` are reserved and currently fail closed until the matching v2
  rollout implementation lands.
- `StoreOffsets` requires `StorePositions` in this first format.
- Text index names share the collection-wide index namespace with scalar and
  vector indexes.

## M2 storage encoding

All text roots use versioned binary keys/values. Decoders return
`ErrTextIndexStorageCorrupt` on malformed data or unsupported versions.

### Postings root

Root: `<collection>/text-index/<indexName>`.

Key:

```text
u8 key_version=1 | uvarint term_len | term bytes | document_id bytes
```

Value:

```text
u8 value_version=1
uvarint document_term_frequency
uvarint field_count
repeated field_count:
  string field_name
  uvarint field_term_frequency
  uvarint position_count | repeated uvarint positions
  uvarint offset_count   | repeated (uvarint start, uvarint end)
```

The postings root has one entry per `(term, documentID)` and stores per-field
frequency plus optional positions/offsets according to the index definition.

### Text-state root

Root: `<collection>/text-state/<indexName>`.

Key:

```text
u8 key_version=1 | document_id bytes
```

Value:

```text
u8 value_version=1
uvarint field_count
repeated field_count:
  string field_name
  uvarint field_length_tokens
  uvarint term_count
  repeated term_count:
    string term
    uvarint frequency
    uvarint position_count | repeated uvarint positions
    uvarint offset_count   | repeated (uvarint start, uvarint end)
```

Backfill and insert maintenance write a text-state entry for every indexed
document so update/delete maintenance can diff or decrement document-level
accounting without re-reading old document payloads.

### Text-stats root

Root: `<collection>/text-stats/<indexName>`.

Keys:

```text
u8 key_version=1 | kind=1                       // corpus document count
u8 key_version=1 | kind=2 | string term          // term statistics
u8 key_version=1 | kind=3 | string field_name    // field length statistics
```

Values use `u8 value_version=1` followed by:

- corpus: `uvarint document_count`;
- term: `uvarint document_frequency | uvarint total_term_frequency`;
- field: `uvarint field_document_count | uvarint total_token_count`.

## Create/backfill/drop

`CreateTextIndex` is a schema barrier: pending writes from all registered
collection managers/write domains for the collection are drained before the
backfill snapshot is taken, new document writes wait behind the barrier, the
primary root is scanned, analyzed text is encoded into the three text roots,
and root IDs plus metadata are published in one commit. It rejects command-WAL
catalog mutation mode until text-index catalog commands are added.

Backfill extracts string fields and arrays of strings from JSON-materialized
documents. Non-string text fields are skipped. Collections with retained column
payloads that omit requested text fields are rejected for now rather than
backfilling incomplete text.

`DropTextIndex` removes the text index metadata and clears all three text root
descriptors. It does not delete historical root pages immediately; normal TreeDB
root reachability/GC handles unreachable pages.

## Query shape

```go
type TextSearchOptions struct {
    IndexName            string
    Query                string
    Operator             TextSearchOperator // "or" default, or "and"
    TopK                 int
    CandidateLimit       int // optional candidate budget before scoring
    MaxPostingsScanned   int // optional postings scan budget
    IncludeDocuments     bool
    DocumentFetchOptions DocumentFetchOptions
}
```

The parser accepts whitespace-separated terms and optional explicit `AND` or
`OR` separators. Mixed `AND`/`OR`, phrases, and grouped syntax fail closed. Query
terms are analyzed with the declared index analyzer.

`SearchText` normalizes duplicate analyzed terms, range-scans the postings root
by encoded term prefix, builds a bounded unique-document candidate set, and
scores candidates from postings + text-state + text-stats. It does not scan or
rank all collection documents. If candidate or postings budgets are exceeded, it
returns `ErrTextIndexUnavailable` with truncation/fail-closed counters rather
than returning silently incomplete rankings. Empty analyzed queries return an
empty result set.

Results expose response-owned document IDs, source index name, one-based lexical
rank, higher-is-better BM25F score, score kind `bm25f`, matched terms/fields, and
optional final documents. Documents are fetched only after top-K ranking, so
`DocumentsFetched <= TopK` for successful searches.

`TextSearchStats` includes text/hybrid-compatible counters:
`text_candidates_requested`, `text_candidates_returned`,
`text_postings_scanned`, `text_posting_blocks_visited`,
`text_posting_blocks_skipped`, `text_candidates_scored`, `text_state_lookups`,
`text_norm_lookups`, `text_match_details_built`, `documents_fetched`,
`documents_missing`, `full_document_scan_fallbacks`, `truncated`, `fail_closed`,
and `fail_closed_reason`, plus text-only aliases and scan/score/fetch timing
fields. v1 reports posting-block counters as zero; v2 PRs must populate them.
`TextCandidatesScored` counts the full bounded scored candidate set;
`TextCandidatesReturned` counts ranked results/candidates actually returned after
TopK truncation.

## Mutation maintenance

Text-indexed insert paths analyze the stored document, set one text-state entry,
set one posting per `(term, documentID)`, and increment corpus/term/field stats.
Delete paths load the text-state entry, delete the document's postings and
state, and decrement stats. Update paths load old text-state, analyze the new
stored document, replace postings/state, and apply a stats delta. Stats entries
whose counts drop to zero are deleted; the corpus stats entry remains with the
current document count.

M3 keeps text-indexed writes on immediate root-publish paths rather than staging
new text deltas in buffered write domains. Existing buffered writes are drained
by `CreateTextIndex` before backfill, and `Flush`/`Checkpoint`/reopen preserve
maintained text roots.

## Follow-up execution plan

The #2503 hybrid seam now exposes
`Collection.SearchHybridTextCandidates(HybridTextQuery)` as a candidate-only
adapter over `SearchText`; it requests no full documents and reuses the shared
hybrid candidate/stat vocabulary.

Later text-search milestones will:

- add gateway/query-language integration;
- add phrase/proximity/highlighting/stemming/trigram/fuzzy search only after
  explicit storage/query contracts land.
