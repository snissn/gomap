# Collection Text Search Contract (M0-M3)

Status: PR3 mutation-maintenance slice for issue #1764. TreeDB collections are
pre-alpha; these text-search storage formats are versioned and may change before
stabilization, but malformed or unsupported versions must fail closed.

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
- `SearchText(TextSearchOptions)` query-shape validation that fails closed before
  scanning because ranked text search execution is not implemented yet.

Not implemented yet:

- BM25/BM25F scoring and ranked SearchText execution;
- phrase/proximity/highlighting/stemming/trigram/fuzzy search;
- query-vector syntax, gateway integration, or hybrid text+vector executor integration.

## Metadata

```go
type TextIndexDefinition struct {
    Name             string
    Fields           []TextIndexField
    Analyzer          TextAnalyzer
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
    IncludeDocuments     bool
    DocumentFetchOptions DocumentFetchOptions
}
```

The parser accepts whitespace-separated terms and optional explicit `AND` or
`OR` separators. Mixed `AND`/`OR`, phrases, and grouped syntax fail closed. Query
terms are analyzed with the declared index analyzer.

`SearchText` currently returns `ErrTextIndexUnavailable` for non-empty analyzed
queries against an existing text index. This is intentional: normal text queries
MUST be indexed, bounded, and ranked from postings; M3 does not implement the
ranked executor and must not fall back to scan-and-rank. Empty analyzed queries
return an empty result set.

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

Later #1764 milestones will:

- range-scan postings by analyzed term;
- bound the candidate set;
- score candidates from persisted stats with BM25/BM25F-style field weighting;
- fetch full documents only for final top-K results.
