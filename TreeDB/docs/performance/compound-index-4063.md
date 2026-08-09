# Compound BSON index component-count characterization (#4063)

This is a local characterization artifact for the direct collection API. It is
not a query-planner benchmark and does not imply planner selection, hints, or
sort semantics.

## Capture

- Base: `be80a3ff55ef2255afe21a77bf00fb1f61ba65c6`
- Capture source: `236bc9c666c576ddaa08d03bef7c53c49e292983` plus the
  benchmark-harness change in this branch
- Host: Apple M3, `darwin/arm64`
- Command:

  ```sh
  GOWORK=off GOCACHE=/tmp/gomap-4063-go-cache \
    go test ./TreeDB/collections -run '^$' \
    -bench '^BenchmarkCollectionBSONCompoundIndexComponents$' \
    -benchtime=10x -count=3
  ```

The harness fixes BSON shape, indexed-field cardinality, and storage policy.
It compares explicit ordered BSON v2 definitions with one through four
ascending components. `build` inserts distinct documents, `mutate` replaces a
stable `_id`, `scan` performs a direct equality-prefix scan over 64 seeded
documents, and `storage` batch-inserts 1,024 documents and reports durable
secondary key bytes per document. Each timed row reports Go's `ns/op`, `B/op`,
and `allocs/op`; the table gives the median of three short repetitions. The
short run is deliberately a smoke characterization, not a release throughput
claim.

| Components | Build ns/op, B/op, allocs/op | Mutate ns/op, B/op, allocs/op | Scan ns/op, B/op, allocs/op | Secondary key bytes/doc |
| --- | ---: | ---: | ---: | ---: |
| 1 baseline | 31,079; 5,120; 62 | 1,611,158; 338,315; 713 | 13,104; 8,868; 15 | 16 |
| 2 | 39,629; 5,488; 68 | 1,827,617; 327,124; 720 | 6,308; 8,884; 16 | 24 |
| 3 | 30,246; 5,648; 73 | 1,074,075; 366,403; 730 | 3,738; 8,900; 16 | 32 |
| 4 | 16,292; 6,118; 78 | 1,207,408; 361,595; 739 | 3,946; 8,900; 16 | 40 |

The stable storage counter rises by eight bytes for each added scalar component
in this fixed fixture (16, 24, 32, 40). Timing variance is expected at this
small repetition count; retain the raw command and rerun with a larger
`-benchtime` before making throughput comparisons across hosts or revisions.
