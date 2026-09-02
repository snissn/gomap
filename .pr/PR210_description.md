## Summary
- add zipper maintenance budget (ops-per-coalesce) plumbing + TreeDB option
- add unified_bench maintenance_budget suite to sweep K and recommend a default
- set default maintenance K to 400k based on sweep results

## Benchmark
GOWORK=off ./bin/unified-bench -suite maintenance_budget -profile fast -keys 2000000 -format markdown

```
# unified_bench suite: maintenance_budget

- keys: 2,000,000
- valsize: 128
- batchsize: 1000
- tests: batch_write, random_write, batch_delete
- checkpoint row: batch_delete (checkpoint after random_write)
- k sweep: 0,50000,100000,200000,400000,800000
- size slack: 10%

```text
K       checkpoint  index.db  size_ratio
0       3.56s       1.9 GiB   1.03x
50000   3.60s       1.9 GiB   1.03x
100000  3.62s       1.9 GiB   1.03x
200000  3.58s       1.9 GiB   1.03x
400000  3.49s       1.9 GiB   1.03x
800000  3.60s       1.8 GiB   1.00x
```

- recommended K: 400000 (checkpoint 3.49s, index.db 1.9 GiB)
```

## Testing
- not run (unit tests)
