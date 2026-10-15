# TreeDB Vacuum M0 Capture

- SHA: `2588cd26780fe9ffd8c33797fc6a2bfdf2c00d49` (clean)
- Go: `go version go1.26.9 linux/amd64`; `linux/amd64`
- Storage: `/dev/root` (`ext4`)
- Timing boundary: `one fixed-work vacuum operation; setup excluded`
- Repetitions: `10` interleaved legacy/public runs
- Status: `production-index-vacuum-available`

## Fixture

- Digest: `42a8a8ef9648dced3ae61f8caa35e921058c17d73265ffdf2db47b93a9d9b599`
- Index bytes: `917504` -> `131072`
- Reclaimable pages: `169` (`77.88%`)
- Value-log bytes: `1207296` -> `1207296`
- Leaf-log bytes: `0` -> `0`
- Parameters: `{"chunk_size": 65536, "collection_documents": 128, "collection_generations": 3, "debt_compactions": 16, "pointer_threshold": 512, "user_generations": 3, "user_keys": 384}`

## Stability Gates

| Metric | Median | CV | Gate |
| --- | ---: | ---: | --- |
| `vacuum-total-ns/op` | 55681501782.500 | 0.1315 | fail |
| `max-writer-pause-ns` | 102723350.500 | 0.5530 | fail |
| `foreground-p99-ns/op` | 153448543.000 | 0.2302 | fail |

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
