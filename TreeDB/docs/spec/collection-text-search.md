# Collection Text Search Contract (M0/M1)

Status: PR1 substrate for issue #1764. TreeDB collections are pre-alpha; this
contract freezes the first metadata/analyzer/root/query shape, while persistent
postings/text-state/stats storage and ranked execution remain follow-up work.

## Scope in this milestone

Implemented now:

- `CollectionMeta.TextIndexes []TextIndexDefinition` metadata.
- `TextIndexDefinition` / `TextIndexField` validation and JSON persistence.
- Root naming helpers for future durable text state:
  - `<collection>/text-index/<indexName>` for term/document postings.
  - `<collection>/text-state/<indexName>` for document analyzed state.
  - `<collection>/text-stats/<indexName>` for corpus and term statistics.
- `simple` analyzer: Unicode lowercase tokenization over letters, digits, and
  `_` so code-ish identifiers such as `HTTP_500` remain searchable.
- `SearchText(TextSearchOptions)` query-shape validation that fails closed before
  scanning because storage is not implemented yet.

Not implemented in this milestone:

- persistent postings, text-state, or text-stats values;
- create/backfill/drop text index commands;
- mutation maintenance for text indexes;
- BM25/BM25F scoring;
- phrase/proximity/highlighting/stemming/trigram/fuzzy search;
- gateway or hybrid text+vector executor integration.

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

The PR1 parser accepts whitespace-separated terms and optional explicit `AND` or
`OR` separators. Mixed `AND`/`OR`, phrases, and grouped syntax fail closed. Query
terms are analyzed with the declared index analyzer.

`SearchText` currently returns `ErrTextIndexUnavailable` for non-empty analyzed
queries against an existing text index. This is intentional: normal text queries
MUST be indexed and bounded, and PR1 must not implement a fake scan-and-rank
fallback. Empty analyzed queries return an empty result set.

## Fail-closed write behavior

Collections that declare text indexes reject insert/update/delete operations with
`ErrTextIndexUnavailable` until M2/M3 land durable postings/text-state/stats and
mutation maintenance. This prevents silent divergence between primary documents
and text metadata.

## Follow-up storage plan

Later #1764 milestones will publish the three text roots atomically with primary
and other collection roots:

- postings entries keyed by analyzed term and document ID, with term frequency
  and field information in values;
- text-state entries keyed by document ID for update/delete diffing;
- stats entries for corpus document count, term document frequency, and field
  length totals used by BM25/BM25F.

The first ranked execution milestone must range-scan postings by term, bound the
candidate set, score candidates from persisted stats, and fetch full documents
only for final top-K results.
