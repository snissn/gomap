# O1 diagnostics gate overhead

Exact-head command on `9336a27ad46fc8ca10328ca97ffeda392c731d95`
(five off/on samples in one process shape, 11th Gen Intel Core i5-11400F):

```sh
go test ./TreeDB/documentservice -run '^$' -bench '^BenchmarkDiagnosticsOpenIndex$' -benchtime=100x -count=5 -benchmem
```

| listener | ns/op samples | median ns/op | median ops/s | median B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| off | 276801, 277890, 279404, 276252, 277593 | 277593 | 3602.4 | 6048 | 13 |
| on | 280734, 278491, 275828, 279055, 278394 | 278491 | 3590.8 | 7586 | 14 |

The configured diagnostics gate changed the median latency by +0.3%, within
the issue's 3% ceiling. The enabled path retains one immutable snapshot per
index name, accounting for its one allocation and roughly 1.5 KiB per open;
the disabled path returns before this work. This is a small `OpenIndex`
microbenchmark, not the issue-wide Cohere load qualification, which remains
the successor harness's responsibility.
