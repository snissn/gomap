# #4379 O2a attribution overhead

Code head: `5a1e0b23523461bf08ed0bb21f2a5c9a930a17b9` plus this O2a change.

The changed production path publishes one owned `VacuumOnlineStats` value once
per vacuum attempt. It is outside the page, record, and mutation loops. The
following counterbalanced five-pair control/candidate measurement isolates the
snapshot seam; each sample used a fresh `go test` process on the same host.

```sh
go test ./TreeDB/db -run '^$' -bench '^BenchmarkVacuumOnlineStatsSnapshot$' -benchtime=200ms -count=1 -benchmem
```

| | median ns/op | median B/op | median allocs/op |
| --- | ---: | ---: | ---: |
| base `5a1e0b235` | 9.952 | 0 | 0 |
| O2a candidate | 9.259 | 0 | 0 |

Candidate median latency is -7.0% (within the <=3% regression gate); every
paired delta was negative and allocations were unchanged. Raw samples are
retained under `/mnt/fast4tb/codex-gomap-4378-exec-20260826/evidence/`.

`BenchmarkVacuumIndexOnlineCollection/bytes_1x` was also sampled as a whole
vacuum guardrail, but repeated multi-iteration runs did not terminate promptly
on this host and one-iteration samples were dominated by sync I/O variance.
It is therefore not used to attribute O2a overhead. The direct benchmark is
the gate evidence because the new work is O(1) snapshot publication, not an
inner-loop counter.
