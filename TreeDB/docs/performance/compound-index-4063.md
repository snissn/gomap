# Compound BSON index component-count characterization (#4063)

This local characterization covers the direct collection API only. It does not
benchmark or imply query-planner selection, hints, or sort semantics.

## Reproducible capture

- Base: `be80a3ff55ef2255afe21a77bf00fb1f61ba65c6`
- Source: `3c371be890f397b83c874a1a185d3cf60978742e` plus the pending bounded
  iterator and operation-isolated checkpoint/reopen repairs in this branch.
- Host: Apple M3, darwin/arm64, Go test process reporting `-8` logical CPUs.
- Fixture: BSON documents, 64-value indexed-field cardinality, 64 seeded scan
  documents, and 1,024 storage documents. Components two through four have a
  descending second component; scans alternate forward/reverse.
- Repetitions: three, with 25 timed operations per throughput row. Values below
  are medians; storage/maintenance probes are deliberately outside the Go
  benchmark timer and report their own isolated elapsed counters.

```sh
TREEDB_COLLECTION_REPORT_VLOG_REWRITE=true \
TREEDB_COLLECTION_REPORT_POST_MAINTENANCE_INDEX_VACUUM=true \
GOWORK=off GOCACHE=/tmp/gomap-4063-go-cache \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionBSONCompoundIndexComponents$' \
  -benchtime=25x -count=3
```

`build` inserts distinct documents. `mutate` replaces a fixed document.
`scan` is an equality-prefix direct scan. `prefix_range_mixed_direction` is an
equality prefix plus one range suffix (or single-component exact prefix) with
alternating traversal direction. `checkpoint` and `reopen` contain only their
named operation after fixture setup; reopen additionally opens the collection
catalog and reads a seeded document. The one-component row is the equivalent
single-field baseline.

| Components | Build ns/op; B/op; allocs/op | Mutate ns/op; B/op; allocs/op | Exact prefix ns/op; B/op; allocs/op | Prefix range/mixed direction ns/op; B/op; allocs/op | Checkpoint ns/op | Reopen ns/op; B/op; allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 baseline | 26,945; 4,718; 59 | 745,100; 285,200; 656 | 6,928; 8,517; 18 | 5,675; 8,611; 20 | 265 | 12,934,442; 45,091,036; 2,175 |
| 2 | 21,312; 5,082; 65 | 683,845; 284,092; 663 | 3,793; 8,531; 19 | 5,668; 8,739; 28 | 260 | 14,015,195; 45,102,257; 2,178 |
| 3 | 15,492; 5,242; 70 | 531,870; 271,627; 668 | 3,247; 8,547; 19 | 4,163; 8,755; 28 | 273 | 13,535,928; 45,130,360; 2,185 |
| 4 | 14,120; 5,704; 75 | 614,183; 276,318; 676 | 5,985; 8,547; 19 | 3,343; 8,755; 28 | 145 | 12,126,477; 45,134,752; 2,188 |

| Components | Secondary bytes/doc | Total durable bytes | Total bytes/doc | Amplification vs 1-component | Rewrite ns | GC ns | Vacuum ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 baseline | 16 | 4,250,023 | 4,150 | 1.000x | 14,083 | 192,042 | 24,761,125 |
| 2 | 24 | 4,263,376 | 4,163 | 1.003x | 13,875 | 185,000 | 24,422,375 |
| 3 | 32 | 4,276,723 | 4,176 | 1.006x | 12,458 | 178,000 | 24,086,125 |
| 4 | 40 | 4,290,070 | 4,190 | 1.009x | 12,292 | 189,875 | 26,902,333 |

The maintenance probes issued `ValueLogRewriteOnline`, `ValueLogGC`,
`VacuumIndexOnline`, and a checkpoint after each durable maintenance boundary.
All three repetitions reported zero copied/deleted value-log records and zero
byte change on this inline-value fixture; those are valid no-op maintenance
counters, not a claim that persistent ValueLog maintenance was skipped. The
total-storage column, rather than only secondary-key bytes, is the on-disk
amplification comparison.

## Operation-focused profile

The following profile selects only the 4-component prefix-range/mixed-direction
row and runs 100,000 timed calls so fixture setup is not the dominant sample.

```sh
PROFILE_DIR=/tmp/gomap-4063-compound-profile-20260809
GOWORK=off GOCACHE=/tmp/gomap-4063-go-cache \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionBSONCompoundIndexComponents/components_4/prefix_range_mixed_direction$' \
  -benchtime=100000x -count=1 \
  -cpuprofile="$PROFILE_DIR/prefix-range.cpu.pprof" \
  -memprofile="$PROFILE_DIR/prefix-range.mem.pprof"
```

It reported 2,400 ns/op, 8,264 B/op, and 28 allocs/op. SHA-256:

- `prefix-range.cpu.pprof`: `b0d778d78920241039dd91524e9f9316fb43d015721397092a27fa52aec58135`
- `prefix-range.mem.pprof`: `11a75a75037b3a2068bee36dddec5ade6a107bfe2a0dacb7c1c4d87297a015e6`

`go tool pprof -top` sampled 380ms CPU. The highest application-level CPU
entries visible in the top sample were BSON component-length parsing and
collection index iterator key extraction; the allocation profile was dominated
by the buffered freeze-sort iterator copy path (65.14% flat allocation).
Profiles remain local `/tmp` artifacts identified by the hashes above. These
numbers are a reproducible local characterization, not a cross-host throughput
claim.
