# #4379 O2a attribution overhead

This partial O2a PR is based on `5a1e0b23523461bf08ed0bb21f2a5c9a930a17b9`.

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

Candidate median latency was -7.0%; every paired delta was negative and
allocations were unchanged. Raw samples are retained under
`/mnt/fast4tb/codex-gomap-4378-exec-20260826/evidence/`.

The maturity pass adds the required owned snapshot fields. A second
counterbalanced five-pair run compares the initial O2a commit `b6b087d` with
the deepened candidate using the same command:

| | median ns/op | median B/op | median allocs/op |
| --- | ---: | ---: | ---: |
| initial O2a `b6b087d` | 12.90 | 0 | 0 |
| deepened O2a candidate | 14.64 | 0 | 0 |

That is +1.74 ns/op (+13.5%) for copying the deliberately larger owned
diagnostics value, with no allocation regression. This is a scrape-only read
boundary and publication remains once per vacuum attempt, outside page, row,
and write loops. It is not the final #4379 <=3% product gate: that gate is
paired diagnostics-on load throughput and stage wall time after O2b/#4380 can
exercise the complete lifecycle. It remains pending, not waived.

The exact scanner currently exposes unique external segment identities, not
stable outer-leaf record identities. O2a reports `UniqueExternalSegments`
without calling it a leaf count; exact unique outer-leaf accounting requires a
shared scanner callback semantic expansion and is a truthful successor seam.

`BenchmarkVacuumIndexOnlineCollection/bytes_1x` was also sampled as a whole
vacuum guardrail, but repeated multi-iteration runs did not terminate promptly
on this host and one-iteration samples were dominated by sync I/O variance.
It is therefore not used to attribute O2a overhead. The direct benchmark is
the gate evidence because the new work is O(1) snapshot publication, not an
inner-loop counter.
