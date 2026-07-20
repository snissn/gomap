# Vector-partition M1 evidence ledger

This ledger binds the M1 measurements to candidate
`0aae57a7a3f6ddb9dea3b7a789559928f6428071` on base
`dc383529612a5a81be8d499bfcd7b09f1b7873e5`. It is a bounded local
microbenchmark (`production_evidence=false`), not ANN search throughput or a
production multi-group Raft result.

Correctness and scale use deliberately different fixtures. The correctness
tests exercise genuine stable-authority ready publication, `OpenActive`, and
Raft snapshot/install with the ready manifest, active pointer, router, and
partition assets intact. The measurements use
`synthetic_ready_manifest_scale_v1`: a test-only manifest derived from a valid
ready template and expanded to 10k, 100k, and 1M disjoint memberships. Fixture
and authority construction are outside the Go benchmark timer. No production
bypass API exists.

## Storage gate

The encoded 1M-membership VPM1 manifest is 12,000,642 bytes, or 12.000642
metadata bytes/vector. This is below the #3910 gate of 64 bytes/vector. The
12,573,184-byte 1M snapshot archive is a different metric: it is the complete
archive stream for the side-store namespace and referenced assets and must not
be divided by vector count as manifest metadata.

## Codec and warm-path measurements

| memberships | decode/validate ns/op | decode B/op | decode allocs/op | encode ns/op | encode B/op | encode allocs/op | process max RSS KiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 1,041,943 | 1,609,696 | 85 | 1,726,755 | 913,888 | 92 | 2,551,496 |
| 100,000 | 11,324,115 | 14,003,392 | 97 | 21,091,240 | 17,594,744 | 114 | 425,344 |
| 1,000,000 | 112,092,008 | 191,125,336 | 95 | 207,993,450 | 228,014,456 | 119 | 429,952 |

| warm operation | ns/op | B/op | allocs/op | process max RSS KiB |
| --- | ---: | ---: | ---: | ---: |
| open active | 94,539 | 14,440 | 63 | 418,816 |
| public status | 21,140,117 | 1,144,704 | 3,291 | 419,456 |

`VectorPartitionStatusV1` validates live TVIS/base identity; its result is not
a constant-time pointer lookup.

## Raft snapshot measurements

| memberships | archive ns/op | archive bytes | archive B/op | archive allocs/op | install ns/op | install B/op | install allocs/op | process max RSS KiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 58,529,576 | 693,248 | 1,710,056 | 1,196 | 645,308,362 | 59,158,592 | 9,469 | 1,272,424 |
| 100,000 | 56,899,998 | 1,773,568 | 4,610,672 | 1,207 | 632,118,035 | 74,189,584 | 9,463 | 323,968 |
| 1,000,000 | 58,345,919 | 12,573,184 | 25,786,608 | 1,210 | 743,943,969 | 273,417,104 | 9,529 | 326,016 |

Each timing is one benchmark iteration. `/usr/bin/time -v` maximum RSS covers
the complete `go test` process, not only the timed benchmark. In particular,
the 10k codec and snapshot captures were cold processes whose compile and cache
population dominate RSS; the per-operation `B/op` and allocation figures are
the benchmark-local allocation metrics.

The canonical machine-readable record is
`vector-partition-m1-evidence.json`. The complete command output is
`vector-partition-m1-0aae57a7a.raw.txt`, SHA-256
`bee8884f2a06ccf226a4f442dbc402606eb11c4aa05f450eb4d9cc3473764403`.
The JSON ledger records all eight exact benchmark commands, the timed boundary,
fixture attribution, toolchain and hardware, every allocation/RSS value, and
the exact candidate/base pair.
