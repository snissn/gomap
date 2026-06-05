# TreeDB-vs-SQLite Collection Concurrency Benchmark — June 4, 2026

This report adds collection-layer concurrency evidence alongside the canonical
single-client insert/read rows. It should not be mixed with the single-client
storage-density claims in the README tables.

## Run Context

- Commit: `4ee6a8891f976de22216fcfa56e061b3bb54ea73` plus issue #2327 benchmark harness changes.
- Host: Linux `mikers-B560-DS3H-AC-Y1`, Intel i5-11400F, Go `go1.25.7 linux/amd64`.
- Artifact: `/tmp/collections_concurrency_2327_current_20260604_151045`.
- Command:

```sh
OUT_DIR=/tmp/collections_concurrency_2327_current_20260604_151045 \
  COUNT=1 BENCHTIME=1s CPU_LIST=1,2,4,8,12 \
  scripts/bench_collections_concurrency.sh
```

TreeDB settings were the collection benchmark defaults used by the script:
`command_wal_relaxed`, template-v1 documents, batch size `16000`, and data/index
outer leaves in the value log. SQLite used WAL, `synchronous=NORMAL`,
`wal_autocheckpoint=0`, and rows report `sqlite_max_open_conns` when relevant.

## Interpretation

- Existing README collection insert/read/lookup rows remain single benchmark-driver
  rows unless they explicitly reference this concurrency report.
- Go's `-cpu` suffix records `GOMAXPROCS`; benchmark rows also emit a
  `gomaxprocs` metric.
- TreeDB mixed rows below use one writer and concurrent readers. SQLite mixed
  rows include the configured reader/writer counts and connection-pool size.
- These rows are benchmark harness evidence, not a complete concurrency contract.

## Selected Read/Lookup Rows

| workload | engine / format | GOMAXPROCS=1 ns/op | GOMAXPROCS=12 ns/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Primary read into caller buffer | TreeDB template-v1 | 725.0 | 332.3 | 1 |
| Primary read | SQLite native columns, pooled conns | 4,968 | 1,438 | 33 |
| Unique secondary lookup | TreeDB template-v1 | 2,482 | 2,207 | 20 |
| Unique secondary lookup | SQLite native columns, pooled conns | 3,627 | 1,278 | 20 |
| Nonunique secondary lookup | TreeDB template-v1 | 6,675 | 4,799 | 81 |
| Nonunique secondary lookup | SQLite native columns, pooled conns | 28,004 | 5,150 | 208 |

## Selected Mixed Read/Write Rows

| workload | engine / format | GOMAXPROCS=1 reader ops/sec | GOMAXPROCS=12 reader ops/sec | GOMAXPROCS=1 writer docs/sec | GOMAXPROCS=12 writer docs/sec |
| --- | --- | ---: | ---: | ---: | ---: |
| primary reads + 1 writer | TreeDB template-v1 | 509,944 | 2,964,434 | 26,299 | 44,298 |
| unique-index reads + 1 writer | TreeDB template-v1 | 145,286 | 88,160 | 21,039 | 49,211 |
| 4 readers + 1 writer | SQLite native columns | 271,237 | 731,421 | 32,320 | 30,835 |
| 8 readers + 2 writers | SQLite native columns | 290,805 | 778,468 | 21,096 | 107,872 |

## Full Artifact Pointers

- TreeDB JSON output: `/tmp/collections_concurrency_2327_current_20260604_151045/treedb/go_test.json`
- TreeDB bench text: `/tmp/collections_concurrency_2327_current_20260604_151045/treedb/bench.txt`
- SQLite JSON output: `/tmp/collections_concurrency_2327_current_20260604_151045/sqlite/go_test.json`
- SQLite bench text: `/tmp/collections_concurrency_2327_current_20260604_151045/sqlite/bench.txt`

## Reproduction Notes

Use the checked-in runner:

```sh
OUT_DIR=/tmp/collections_concurrency_$(date +%Y%m%d_%H%M%S) \
  COUNT=1 BENCHTIME=3s CPU_LIST=1,2,4,8,12 \
  scripts/bench_collections_concurrency.sh
```

The runner writes normalized `go test -json` artifacts and extracted benchmark
text for TreeDB and SQLite under the output directory.
