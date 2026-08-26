# O1 diagnostics gate overhead

Exact-head command on `c3e5de7953ae7b04fa995dc72dab1edae5f82507`
(five off/on samples in one process shape, 11th Gen Intel Core i5-11400F):

```sh
go test ./TreeDB/documentservice -run '^$' -bench '^BenchmarkDiagnosticsOpenIndex$' -benchtime=100x -count=5 -benchmem
```

| listener | ns/op samples | median ns/op | median ops/s | median B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| off | 280680, 281133, 282980, 281218, 283324 | 281218 | 3556.0 | 6048 | 13 |
| on | 285072, 282070, 282311, 283505, 284575 | 283505 | 3527.3 | 7586 | 14 |

The configured diagnostics gate changed the median latency by +0.8%, within
the issue's 3% ceiling. The enabled path retains one immutable snapshot per
index name, accounting for its one allocation and roughly 1.5 KiB per open;
the disabled path returns before this work. This is a small `OpenIndex`
microbenchmark, not the issue-wide Cohere load qualification, which remains
the successor harness's responsibility.
