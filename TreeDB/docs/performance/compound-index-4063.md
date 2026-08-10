# Compound BSON index component-count characterization (#4063)

This local characterization covers the direct collection API only. It does not
benchmark or imply query-planner selection, hints, or sort semantics.

## Reproducible capture

- Base: `be80a3ff55ef2255afe21a77bf00fb1f61ba65c6`
- Source: `aa63c8f57bd030e218bcf3f91f59fc36a491fe53` (the timed upper-bound,
  non-leading `$set`, and nontrivial range-fixture repairs included).
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
`scan` is a 64-result equality-prefix direct scan: its leading component is
constant across the fixture. `prefix_range_mixed_direction` is a 32-result
equality-prefix plus one range suffix (or a 64-result single-component exact
prefix) with alternating traversal direction, and the harness asserts those
cardinalities before timing. `checkpoint_after_write` times only the checkpoint
after a deliberately dirty direct indexed write outside the timer. `reopen`
contains only close/open/catalog recovery and a seeded-document read. The
one-component row is the equivalent single-field baseline.

| Components | Build ns/op; B/op; allocs/op | Mutate ns/op; B/op; allocs/op | Exact prefix ns/op; B/op; allocs/op | Prefix range/mixed direction ns/op; B/op; allocs/op | Checkpoint-after-write ns/op; B/op; allocs/op | Reopen ns/op; B/op; allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 baseline | 27,642; 4,716; 59 | 682,305; 278,439; 655 | 25,172; 13,482; 151 | 23,528; 14,067; 153 | 15,603,856; 190,095; 482 | 12,832,965; 45,098,826; 2,175 |
| 2 | 20,782; 5,082; 65 | 581,787; 283,619; 663 | 18,108; 14,008; 152 | 15,028; 11,296; 102 | 16,513,203; 190,521; 482 | 13,506,843; 45,095,102; 2,173 |
| 3 | 11,897; 5,242; 70 | 674,423; 279,054; 670 | 14,700; 15,032; 152 | 8,655; 11,808; 102 | 16,156,738; 190,410; 481 | 14,067,723; 45,135,909; 2,186 |
| 4 | 22,830; 5,704; 75 | 777,870; 266,020; 674 | 26,772; 15,032; 152 | 17,587; 11,808; 102 | 16,754,328; 192,340; 482 | 14,068,580; 45,142,511; 2,191 |

| Components | Secondary bytes/doc | Total durable bytes | Total bytes/doc | Amplification vs 1-component | Rewrite ns | GC ns | Vacuum ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 baseline | 16 | 4,250,023 | 4,150 | 1.000x | 8,750 | 116,500 | 21,804,750 |
| 2 | 24 | 4,263,376 | 4,163 | 1.003x | 10,584 | 153,500 | 21,472,000 |
| 3 | 32 | 4,276,723 | 4,176 | 1.006x | 11,917 | 186,916 | 25,844,709 |
| 4 | 40 | 4,290,070 | 4,190 | 1.009x | 10,917 | 181,042 | 23,696,083 |

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
PROFILE_DIR=/tmp/gomap-4063-compound-profile-20260809c
GOWORK=off GOCACHE=/tmp/gomap-4063-go-cache \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionBSONCompoundIndexComponents/components_4/prefix_range_mixed_direction$' \
  -benchtime=100000x -count=1 \
  -cpuprofile="$PROFILE_DIR/prefix-range.cpu.pprof" \
  -memprofile="$PROFILE_DIR/prefix-range.mem.pprof"
```

It reported 8,888 ns/op, 11,809 B/op, and 102 allocs/op. SHA-256:

- `prefix-range.cpu.pprof`: `03aa6f63b2e378aeff394bae99b495f0213327323a9d5a4b1f293edef395c74a`
- `prefix-range.mem.pprof`: `64ea90126e41964b37ad38c5562639bb4384a2fdc08c53afda808e16dea23bfa`

`go tool pprof -top` sampled 940ms CPU. The highest application-level CPU
entries visible in the top sample were BSON component/string-length parsing;
the profile also records ordinary runtime copy and memory-management costs.
Profiles remain local `/tmp` artifacts identified by the hashes above. These
numbers are a reproducible local characterization, not a cross-host throughput
claim.
