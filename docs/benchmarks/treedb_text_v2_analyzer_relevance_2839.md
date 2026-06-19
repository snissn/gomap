# TreeDB text-v2 analyzer/relevance expansion (#2839)

This note records the bounded analyzer/relevance surface added for phase 2 under
parent #2833. It is intentionally conservative: default BM25F behavior remains
`simple`, and optional analyzer features must be persisted in text-index metadata
and applied identically at write and query time.

## Implemented contract

- `TextAnalyzerOptions.StopWords` is persisted with `TextIndexDefinition` and
  normalized through the simple analyzer.
- Stopwords are removed during indexing and query analysis; phrase/proximity
  matching preserves analyzer token positions so gaps introduced by removed
  terms remain visible to slop semantics.
- Stopword-only text queries return an empty result instead of widening to a
  full scan.
- Reserved analyzer seams (`Stemmer`, `Synonyms`) fail closed with
  `ErrTextIndexUnavailable` until their bounded indexing/query semantics are
  implemented.
- Query explain reports analyzer-normalized terms, serving path, phrase/slop
  counters, and fail-closed reasons for unsupported analyzer/relevance shapes.

## Validation commands

```sh
GOWORK=off go test ./TreeDB/collections \
  -run 'TestTextV2PhraseAnalyzerStopwordsAndScalarAllowSet|TestTextV2PhraseInternalStopwordPositionGaps|TestTextV2PhraseUnsupportedShapesFailClosed|TestTextV2QueryExplainShapes2838'

GOWORK=off TREEDB_TEXT_V2_ANALYZER_DOCS=256 go test ./TreeDB/collections \
  -run '^$' -bench '^BenchmarkTextV2AnalyzerStopwordsWrite2733$' \
  -benchmem -benchtime=3x -count=1
```

## Evidence

Artifact root: `/tmp/gomap_issue_2839_analyzer_20260618_194629`.

The existing test matrix covers stopword normalization, phrase/proximity ranking
with internal stopword gaps, scalar allow-set interaction, unsupported
stemmer/synonym fail-closed behavior, and explain payload shape.

Benchmark `BenchmarkTextV2AnalyzerStopwordsWrite2733` reports write-path
allocation and index-byte counters for `simple_no_stopwords` versus
`simple_stopwords`. Use it as the overhead gate when changing analyzer metadata
or adding future bounded profiles.

| row | ns/op | B/op | allocs/op | index bytes/doc | posting blocks/doc |
| --- | ---: | ---: | ---: | ---: | ---: |
| simple_no_stopwords | 3,764,391 | 3,545,397 | 23,742 | 293.5 | 2.348 |
| simple_stopwords | 3,594,963 | 3,611,938 | 31,873 | 285.8 | 2.316 |

## Non-goals left for follow-up

- No stemming implementation yet.
- No synonym query expansion yet.
- No fuzzy/wildcard/prefix expansion.
- No approximate ranking or document-fetch validation in candidate generation.
