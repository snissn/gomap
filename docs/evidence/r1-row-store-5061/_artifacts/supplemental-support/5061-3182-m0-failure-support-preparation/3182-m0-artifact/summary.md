# TreeDB Vacuum M0 Capture

- SHA: `3182e15dfe1aa11120db9d309ad0d590283398d6` (clean)
- Go: `go version go1.26.8 linux/amd64`; `linux/amd64`
- Storage: `/dev/root` (`ext4`)
- Timing boundary: `one fixed-work vacuum operation; setup excluded`
- Repetitions: `10` interleaved legacy/public runs
- Status: `production-index-vacuum-available`

## Fixture

- Digest: `42a8a8ef9648dced3ae61f8caa35e921058c17d73265ffdf2db47b93a9d9b599`
- Index bytes: `1441792` -> `131072`
- Reclaimable pages: `208` (`59.26%`)
- Value-log bytes: `1207296` -> `1207296`
- Leaf-log bytes: `0` -> `0`
- Parameters: `{"chunk_size": 65536, "collection_documents": 128, "collection_generations": 3, "debt_compactions": 16, "pointer_threshold": 512, "user_generations": 3, "user_keys": 384}`

## Stability Gates

| Metric | Median | CV | Gate |
| --- | ---: | ---: | --- |
| `vacuum-total-ns/op` | 37198472571.500 | 0.1552 | fail |
| `max-writer-pause-ns` | 87651948.500 | 0.4712 | fail |
| `foreground-p99-ns/op` | 94169225.000 | 0.3853 | fail |

## Legacy Completion

- `concurrent-aborts/op`: median `0.000`

## Public Classification

- `vacuum-unsupported/op`: median `0.000`
- `vacuum-unexpected-errors/op`: median `0.000`
- Unavailable status requires one unsupported result and one exposure miss with zero retries, unexpected errors, and foreground overlap in every sample.
- Available status requires at least one successful vacuum, only typed transient retries, zero unexpected errors/exposure misses, and positive foreground overlap in every sample.

## Commands

```sh
GOWORK=off GOMAXPROCS=2 GOMEMLIMIT=8GiB taskset -c 0,1 go test ./TreeDB/db -run '^$' -bench '^BenchmarkVacuumIndexOnlineCollectionForegroundChurn/bytes_64x$' -benchtime=1x -count=1 -benchmem
GOWORK=off GOMAXPROCS=2 GOMEMLIMIT=8GiB taskset -c 0,1 go test ./TreeDB/db -run '^$' -bench '^BenchmarkPL06ExternalVacuumCollectionForegroundChurn/bytes_64x$' -benchtime=1x -count=1 -benchmem
```

Raw inputs are under `raw/`; the complete machine-readable summary is `results.json`.
