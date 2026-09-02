# Vector-partition M1 evidence ledger

This ledger binds the M1 measurements to candidate
`b16bcbf989d9fb9333d95fe8de08fcc6c1122bcc` on base
`35e8a45d9d50e5f48e70b998d8f2ac19cafc199b`. It is a bounded local
microbenchmark (`production_evidence=false`), not ANN search throughput or a
production multi-group Raft result.

Correctness and scale use deliberately different fixtures. The correctness
tests exercise genuine stable-authority-ready publication, `OpenActive`, and
Raft snapshot/install with the checkpoint-reduced active state, router, and
partition assets intact. Snapshot measurements use
`synthetic_checkpoint_ready_manifest_scale_v1`: an explicitly
`treedb_benchmark`-tagged fixture derived from a genuinely authorized one-row
ready template and expanded to 10k, 100k, and 1M disjoint memberships. The
tagged helper publishes the expanded VPM1 through the VCP1/VLC1 lifecycle but
does not revalidate the synthetic row count against live TVIS state. Fixture
and authority construction are outside the Go benchmark timer. Default and
production builds expose no bypass API.

## Storage gate

The encoded 1M-membership VPM1 manifest is 12,000,642 bytes, or 12.000642
metadata bytes/vector. This is below the #3910 gate of 64 bytes/vector. The
12,575,232-byte 1M snapshot archive is a different metric: it is the complete
archive stream for the side-store namespace and referenced assets and must not
be divided by vector count as manifest metadata.

## Codec and warm-path measurements

| memberships | decode/validate ns/op | decode B/op | decode allocs/op | encode ns/op | encode B/op | encode allocs/op | process max RSS KiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 981,828 | 1,609,696 | 85 | 1,908,414 | 1,962,592 | 109 | 436,480 |
| 100,000 | 9,336,004 | 14,003,392 | 97 | 17,765,410 | 17,594,744 | 114 | 439,168 |
| 1,000,000 | 102,101,968 | 191,125,240 | 95 | 184,429,532 | 228,014,440 | 118 | 440,192 |

| warm operation | ns/op | B/op | allocs/op | process max RSS KiB |
| --- | ---: | ---: | ---: | ---: |
| open active | 196,040 | 69,528 | 373 | 435,968 |
| public status | 13,157,171 | 670,080 | 3,256 | 439,296 |

`VectorPartitionStatusV1` validates live TVIS/base identity; its result is not
a constant-time pointer lookup.

## Raft snapshot measurements

| memberships | archive ns/op | archive bytes | archive B/op | archive allocs/op | install ns/op | install B/op | install allocs/op | process max RSS KiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 46,190,914 | 695,296 | 13,443,136 | 2,058 | 282,760,342 | 83,787,088 | 10,572 | 326,144 |
| 100,000 | 229,331,503 | 1,775,616 | 130,632,208 | 2,114 | 662,289,049 | 337,747,928 | 10,670 | 331,008 |
| 1,000,000 | 2,094,246,445 | 12,575,232 | 1,614,262,104 | 2,198 | 4,542,216,670 | 3,628,172,544 | 10,855 | 617,296 |

Each timing is one benchmark iteration. `/usr/bin/time -v` maximum RSS covers
the complete `go test` process, not only the timed benchmark. In particular,
the 10k codec and snapshot captures were cold processes whose compile and cache
population dominate RSS; the per-operation `B/op` and allocation figures are
the benchmark-local allocation metrics.

The canonical machine-readable record is
`vector-partition-m1-evidence.json`. The complete command output is
`vector-partition-m1-b16bcbf98.raw.txt`, SHA-256
`e4be520d04d28730a1c92ddcd5eeca58632d950be1171f1bb5ea5552a8e8ad52`.
The JSON ledger records all eight exact benchmark commands, the timed boundary,
fixture attribution, toolchain and hardware, every allocation/RSS value, and
the exact candidate/base pair.
