# unified_bench

Side-by-side benchmarks for `HashDB`, `BTreeOnHashDB`, `TreeDB` (cached), `TreeDBBackend` (uncached), Badger, and LevelDB.

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

- `-dbs` (`all` or CSV): `hashdb,btree,treedb,treedbbackend,badger,leveldb`
- `-test` (`all` or CSV): see list above
- `-keys` number of keys (default 100000)
- `-keycounts` comma-separated key counts to sweep over (overrides `-keys`)
- `-keyscale` generate keycounts by scale: `log10` or `doubling` (uses `-keys-min` / `-keys-max`)
- `-valsize` value size in bytes (default 128)
- `-batchsize` batch size (default 1000)
- `-range-queries` number of prefix/range queries (default 200)
- `-range-span` number of keys per range (default 100)
- `-treedb-flush-threshold` TreeDB (cached) flush threshold in bytes (default 64MB)
- `-treedb-max-queued-memtables` TreeDB (cached) max queued immutable memtables before applying backpressure flush (`0`=default, `<0`=disable)
- `-treedb-slowdown-backlog-seconds` TreeDB (cached) start backpressure when queued backlog exceeds this many seconds of flush work
- `-treedb-stop-backlog-seconds` TreeDB (cached) block writers when queued backlog exceeds this many seconds of flush work
- `-treedb-max-backlog-bytes` TreeDB (cached) absolute cap on queued backlog bytes
- `-treedb-writer-flush-max-memtables` TreeDB (cached) max memtables a writer will help flush per op
- `-treedb-writer-flush-max-ms` TreeDB (cached) max time (ms) a writer will help flush per op
- `-treedb-iter-debug` print prefix scan iterator timing + debug stats
- `-treedb-iter-debug-limit` max per-query debug lines to print (default 20)
- `-treedb-bg-vacuum-interval` TreeDB: background index vacuum interval (0=disabled)
- `-treedb-bg-vacuum-span-ppm` TreeDB: background index vacuum span ratio threshold (ppm), `0`=default
- `-seed` PRNG seed for randomized tests (default 1; `0` = time-based)
- `-keep` keep temp DB directories after run
- `-settle-before-scans` close+reopen DBs before `full_scan`/`prefix_scan` to measure scan performance on a “settled” (fully flushed) state
- `-progress` live table updates to stderr (default true)
- `-format` output format: `table` or `markdown`
- `-cpuprofile`, `-blockprofile`, `-mutexprofile`, `-trace` write profiling artifacts to files
- `-max-wall` abort the run if wall time exceeds this duration (guardrail; `0` = disabled)
- `-max-rss-mb` abort the run if RSS exceeds this many MiB (guardrail; `0` = disabled; Linux-only)
- `-checkpoint-between-tests` force a best-effort durability checkpoint between tests (DBs that support `Checkpoint()`)
- `-checkpoint-every-ops` force a best-effort durability checkpoint every N ops during write-heavy tests (DBs that support `Checkpoint()`)
- `-checkpoint-every-bytes` force a best-effort durability checkpoint every N approx bytes during write-heavy tests (DBs that support `Checkpoint()`)
- `-suite` named suite:
  - `readme` — generates the README graphs + sweep tables
  - `churn` — churn + settled scans (`treedb,leveldb`)
  - `churnvacuum` — churn + settled scans, then VACUUM and scan again
  - `churnmaint` — churn + settled scans, then slab compaction + VACUUM and scan again
  - `flushthrash` — forces a small TreeDB flush threshold; catches flush thrash / runaway backlog regressions
  - `bigkeys_guard` — small TreeDB flush threshold + large keycount, with wall/RSS caps for CI guardrails
  - `longmix` — long-ish mixed workload + settle boundary with fragmentation reports
  - `sload_readheavy` — settled point reads with pointer values (exercises slab reads) + forkchoice-style batch commits
- `-outdir` output directory for suite artifacts (plots/images; used by `-suite readme`)
