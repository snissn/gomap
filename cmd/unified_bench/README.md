# unified_bench

Side-by-side benchmarks for `HashDB`, `BTreeOnHashDB`, `TreeDB` (cached), `TreeDBBackend` (uncached), and LevelDB.

## Run

- Build: `make unified-bench` (writes `bin/unified-bench`)
- Run: `./bin/unified-bench`
- Or: `go run ./cmd/unified_bench`

## Reproducibility

- Randomized tests use a per-test PRNG derived from `-seed` so every DB sees the same random key/query sequence.
- The chosen seed is printed to stderr at startup.

## Tests

- `write_seq` — Sequential Write
- `write_rand` — Random Write
- `batch_write` — Batch Write
- `delete_rand` — Random Delete
- `read_rand` — Random Read
- `full_scan` — Full Scan (iterate the full keyspace)
- `prefix_scan` — Prefix Scan (range scans over `[start,end)`)
  - Aliases: `scan` → `full_scan`, `range_scan` → `prefix_scan`

## Common flags

- `-dbs` (`all` or CSV): `hashdb,btree,treedb,treedbbackend,leveldb`
- `-tests` (`all` or CSV): see list above
- `-keys` number of keys (default 100000)
- `-valsize` value size in bytes (default 128)
- `-batchsize` batch size (default 1000)
- `-range-queries` number of prefix/range queries (default 200)
- `-range-span` number of keys per range (default 100)
- `-seed` PRNG seed for randomized tests (default 1; `0` = time-based)
- `-keep` keep temp DB directories after run
- `-progress` live table updates to stderr (default true)

