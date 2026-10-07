# TreeDB Vacuum M0 Capture

- SHA: `f2c93cfcdf5f7ef54f6d7bc4ff9e6946fb21a72c` (clean)
- Go: `go version go1.26.4 linux/amd64`; `linux/amd64`
- Storage: `/dev/nvme1n1p2` (`ext4`)
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
| `vacuum-total-ns/op` | 2936116704.500 | 0.1114 | fail |
| `max-writer-pause-ns` | 17795996.000 | 0.1775 | fail |
| `foreground-p99-ns/op` | 3341392.000 | 0.4273 | fail |

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
