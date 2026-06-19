# TreeDB text-v2 phase-2 scale/soak refresh (#2837)

This note records the phase-2 scale/soak status for parent #2833 after the
footprint (#2835), scalar-aware pruning (#2836), and explain/analyzer work in
the stacked branch.

## Status

A full 10M+ corpus soak was not run in this coordinator session because the
GitHub queue was saturated with latest-head CI for the phase-2 stack and a 10M
local run would exceed the interactive merge window. The checked-in scale
harness remains the intended path for the full artifact; this PR adds the
current reproduction commands, a 10k smoke artifact, and explicit risk notes so
#2837 does not silently imply production-scale parity.

## 10k smoke artifact

Artifact root: `/tmp/gomap_issue_2837_scale_smoke_20260618_200105`

Command:

```sh
GOWORK=off TREEDB_TEXT_V2_SCALE_DOCS=10000 TREEDB_TEXT_V2_SCALE_QUERIES=64 \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkTextV2ContractSearchScale2623/docs_10000/(score_only_common_no_docs|multi_term_or_no_docs|multi_term_and_no_docs)$' \
  -benchmem -benchtime=3x -count=1
```

| row | ns/op | B/op | allocs/op | docs fetched | fail closed | candidates scored | postings scanned |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| score_only_common_no_docs | 15,085,012 | 5,686,114 | 30,120 | 0 | 0 | 10,000 | 10,000 |
| multi_term_or_no_docs | 19,410,268 | 5,926,320 | 40,128 | 0 | 0 | 10,000 | 20,000 |
| multi_term_and_no_docs | 18,427,475 | 5,926,338 | 40,129 | 0 | 0 | 10,000 | 20,000 |

## Full-scale reproduction plan

Use a dedicated long-running shell/tmux session and capture artifacts outside
this interactive PR loop:

```sh
RUN_DIR=/tmp/gomap_issue_2837_scale_10m_$(date +%Y%m%d_%H%M%S) \
TREEDB_TEXT_V2_SCALE_DOCS=10000000 \
TREEDB_TEXT_V2_SCALE_QUERIES=10000 \
GOWORK=off go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkTextV2ContractSearchScale2623/docs_10000000/' \
  -benchmem -benchtime=1x -count=1 | tee "$RUN_DIR/search_scale_10m.txt"
```

Recommended companion captures:

- ingest/build benchmark with storage bytes/doc;
- reopen/search sanity after checkpoint;
- phrase/analyzer rows when `StorePositions=true`;
- scalar-selectivity sweep from #2836;
- heap profile and `/usr/bin/time -v` high-water RSS.

## Risk notes

- This PR does not claim 10M+ parity; it documents that the full soak remains a
  required production-readiness artifact.
- The 10k smoke confirms the no-document-fetch and fail-closed counters for the
  current harness shapes only.
- Any future claim about 10M/50M behavior should link the full artifact root and
  update this note with p50/p95/p99, storage growth, rewrite debt, and memory
  high-water data.
