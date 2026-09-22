# TreeDB text-v2 scalar-aware WAND/block pruning (#2836)

This note records the phase-2 scalar-aware WAND/block-pruning contract for
parent #2833. It builds on the text-v2 block-max serving path and query explain
counters: scalar prefilters are converted to an ordinal allow-set, posting blocks
whose ordinal ranges cannot intersect the allow-set are skipped, and remaining
postings are rejected before scoring when their ordinals are disallowed.

## Contract

- Candidate generation remains exact: filtered block-max results must match the
  exhaustive filtered scorer for the same query, top-k, and allow-set.
- No full-document fetches are allowed during text candidate generation.
- Unsupported or over-budget shapes fail closed via existing text-v2 budget and
  storage-corruption reasons instead of silently scanning full documents.
- Counters expose scalar work:
  - `TextScalarPrefilterIDs`
  - `TextScalarPostingBlocksSkipped`
  - `TextScalarPostingsRejected`
  - plus WAND/posting/candidate/fail-closed counters surfaced by explain.

## Local validation

```sh
GOWORK=off go test ./TreeDB/collections \
  -run 'TestTextV2ScalarAwareWANDPruningContract2836|TestTextV2BlockMaxMultiTerm(AND|OR)ScalarPrefilter|TestTextV2HybridUnionFusionPreservesUnfilteredTextRanks'
```

The focused #2836 contract test covers:

| shape | assertion |
| --- | --- |
| high-selectivity OR | exact parity with exhaustive scorer, posting blocks skipped, scored candidates bounded by allow-set size |
| moderate-selectivity AND | exact parity with exhaustive scorer, disallowed postings rejected, scored candidates bounded by allow-set size |

## Benchmark smoke

Artifact root: `/tmp/gomap_issue_2836_scalar_wand_20260618_193529`

Command:

```sh
GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkTextV2ScalarAwareWANDPruning2836$' \
  -benchmem -benchtime=3x -count=1
```

Results on Linux / Go `go1.26.0` / i5-11400F:

| row | ns/op | B/op | allocs/op | allow-set ids | blocks skipped | postings rejected | candidates scored | postings scanned |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| high_selectivity_or | 144,789 | 93,504 | 366 | 16 | 62 | 112 | 16 | 256 |
| moderate_selectivity_and | 926,922 | 656,064 | 835 | 128 | 0 | 3,968 | 128 | 8,192 |

The high-selectivity row demonstrates block-range pruning. The moderate row
keeps exact ranking when the allow-set intersects every block and therefore uses
posting-level rejection rather than block skipping.
