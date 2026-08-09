# Compound BSON index component-count characterization (#4063)

This local characterization covers the direct collection API only. It does not
benchmark or imply query-planner selection, hints, or sort semantics.

## Reproducible capture

- Base: `be80a3ff55ef2255afe21a77bf00fb1f61ba65c6`
- Source: `fc480cf6ff19bed5c3bf7d675580b764a6382757` plus the pending
  non-leading `$set` mask and nontrivial range-fixture repairs in this branch.
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
| 1 baseline | 22,997; 4,716; 59 | 1,046,440; 274,229; 656 | 23,635; 13,482; 151 | 23,613; 14,067; 153 | 18,496,437; 190,136; 482 | 20,179,548; 45,097,224; 2,175 |
| 2 | 31,677; 5,096; 65 | 617,162; 278,860; 663 | 28,208; 14,008; 152 | 23,252; 13,568; 127 | 16,797,060; 191,697; 483 | 20,406,373; 45,093,871; 2,175 |
| 3 | 28,073; 5,242; 70 | 735,158; 269,061; 667 | 16,998; 15,032; 152 | 24,248; 14,320; 127 | 16,599,390; 190,737; 482 | 17,389,578; 45,138,101; 2,188 |
| 4 | 27,728; 5,706; 75 | 696,212; 266,409; 674 | 21,690; 15,032; 152 | 13,778; 14,320; 127 | 16,218,357; 195,352; 495 | 11,257,077; 45,142,102; 2,189 |

| Components | Secondary bytes/doc | Total durable bytes | Total bytes/doc | Amplification vs 1-component | Rewrite ns | GC ns | Vacuum ns |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 baseline | 16 | 4,250,023 | 4,150 | 1.000x | 16,000 | 271,334 | 25,836,500 |
| 2 | 24 | 4,263,376 | 4,163 | 1.003x | 13,750 | 504,458 | 24,259,917 |
| 3 | 32 | 4,276,723 | 4,176 | 1.006x | 10,500 | 252,208 | 18,409,083 |
| 4 | 40 | 4,290,070 | 4,190 | 1.009x | 10,375 | 151,917 | 21,497,916 |

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
PROFILE_DIR=/tmp/gomap-4063-compound-profile-20260809b
GOWORK=off GOCACHE=/tmp/gomap-4063-go-cache \
  go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkCollectionBSONCompoundIndexComponents/components_4/prefix_range_mixed_direction$' \
  -benchtime=100000x -count=1 \
  -cpuprofile="$PROFILE_DIR/prefix-range.cpu.pprof" \
  -memprofile="$PROFILE_DIR/prefix-range.mem.pprof"
```

It reported 11,625 ns/op, 14,321 B/op, and 127 allocs/op. SHA-256:

- `prefix-range.cpu.pprof`: `4ab698038eeeb36ab98b75ac990ae36291a004ea0981cf4fdf49fb70b93387db`
- `prefix-range.mem.pprof`: `6afde0c91f5a57946317db1cc1613099ef72b553bf1b50181b32e629c7ce6165`

`go tool pprof -top` sampled 1.18s CPU. The highest application-level CPU
entries visible in the top sample were BSON string/component-length parsing;
the allocation profile was led by the buffered freeze-sort iterator copy path
(40.57% flat allocation), direct result cloning (22.90%), and `bytes.Clone`
(17.13%).
Profiles remain local `/tmp` artifacts identified by the hashes above. These
numbers are a reproducible local characterization, not a cross-host throughput
claim.
