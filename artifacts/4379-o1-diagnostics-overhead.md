# O1 diagnostics gate overhead

Command (five off/on samples, same process shape):

```sh
go test ./TreeDB/documentservice -run '^$' -bench '^BenchmarkDiagnosticsOpenIndex$' -benchtime=100x -count=5
```

| listener | ns/op samples | median |
| --- | --- | --- |
| off | 276429, 275069, 276777, 279479, 275064 | 276429 |
| on | 272257, 276140, 279516, 278837, 276028 | 276140 |

The configured diagnostics gate changed the median by -0.1%. This is a small
`OpenIndex` microbenchmark of the only new hot-path branch; it is not the
issue-wide Cohere load qualification, which remains the successor harness's
responsibility.
