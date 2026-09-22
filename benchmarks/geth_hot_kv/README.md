# geth/Nitro hot-KV soak benchmark

This directory contains a durable copy of the #2392-style hot-KV benchmark
harness. It is intentionally stored under `testdata/` because it must be run
from a go-ethereum/Nitro checkout, not compiled as part of gomap.

The harness exercises the integrated downstream path:

- `node.OpenDatabase(...)`
- `--db.engine pebble|leveldb|treedb` via `node.Config.DBEngine`
- geth `ethdb.Database` / `ethdb.Batch`

Workload shape:

1. Build deterministic geth-like binary/prefix keys.
2. Write the records through geth batches.
3. Run deterministic random point reads.
4. Full ordered iteration.
5. Delete all records with both DB-level `DeleteRange` and batch
   `DeleteRange`.
6. Close/reopen and verify all benchmark keys are gone.
7. Report the historical #2392 columns: write ops/sec, read ops/sec,
   iterate keys/sec, DeleteRange affected-keys/sec, and on-disk size bytes.
   `size bytes` is measured before the destructive DeleteRange phase; the JSON
   and matrix also include `post_delete_size_bytes` after close/reopen.
8. Label TreeDB value-log read integrity (`verify` or the explicitly unsafe
   `unsafe-skip-checksums` ceiling) and iteration mode (`value` or `key-only`).
9. For TreeDB runs, include per-phase stat deltas for value-log CRC checks,
   grouped-frame cache activity, mmap hits/ReadAt fallbacks, outer-leaf
   value-log-backed B-tree page loads, and focused write-path counters where
   the adapter exposes `Stat()`.

The exact original `/tmp/treedb_nitro_soak.go` was not recovered, so the matrix
script intentionally sweeps the uncertain knobs that may affect directionality:
key shape, value size/shape, and batch flush cadence. The default matrix keeps
realistic geth-like mixed prefixes and mixed value sizes as the primary shape,
then compares a single-prefix key variant to check whether prefix mix materially
changes results.

## One-shot run

```sh
cd /path/to/go-ethereum
go run /path/to/gomap/benchmarks/geth_hot_kv/testdata/treedb_nitro_soak.go \
  -n 30000 \
  -reads 12000 \
  -engines pebble,leveldb,treedb \
  -out /tmp/treedb_soak_results.json
```

TreeDB checksum verification is the default. The unsafe checksum-disabled mode
is benchmark-only and must be selected explicitly:

```sh
go run /path/to/gomap/benchmarks/geth_hot_kv/testdata/treedb_nitro_soak.go \
  -n 30000 \
  -reads 12000 \
  -engines treedb \
  -treedb-read-integrity unsafe-skip-checksums \
  -out /tmp/treedb_soak_skip_checksums.json
```

Use `-iteration-mode key-only` to measure ordered traversal without calling
`Iterator.Value()` or materializing value-log payloads during the iterate phase:

```sh
go run /path/to/gomap/benchmarks/geth_hot_kv/testdata/treedb_nitro_soak.go \
  -n 30000 \
  -reads 12000 \
  -engines treedb \
  -iteration-mode key-only \
  -out /tmp/treedb_soak_key_only.json
```

## Matrix run

```sh
GETH_REPO=/path/to/go-ethereum \
  scripts/treedb_geth_hot_kv_matrix.sh
```

Useful quick smoke:

```sh
GETH_REPO=/path/to/go-ethereum \
KEYS=1000 READS=300 \
KEY_SHAPES=geth-mixed \
VALUE_SIZES=128 \
BATCH_TARGET_BYTES=102400 \
  scripts/treedb_geth_hot_kv_matrix.sh
```

Checksum verify vs unsafe ceiling and value vs key-only split:

```sh
GETH_REPO=/path/to/go-ethereum \
ENGINES=treedb \
KEYS=30000 READS=12000 \
KEY_SHAPES=geth-mixed \
VALUE_SIZES=128 \
BATCH_TARGET_BYTES=102400 \
TREEDB_READ_INTEGRITIES=verify,unsafe-skip-checksums \
ITERATION_MODES=value,key-only \
  scripts/treedb_geth_hot_kv_matrix.sh
```

TreeDB-only profiling:

```sh
GETH_REPO=/path/to/go-ethereum \
ENGINES=treedb \
KEY_SHAPES=geth-mixed \
VALUE_SIZES=128 \
BATCH_TARGET_BYTES=102400 \
PROFILE_DIR=/tmp/geth_hotkv_profiles \
  scripts/treedb_geth_hot_kv_matrix.sh
```

Profile artifacts are written per phase, for example
`cpu_write_treedb.pprof`, `memstats_read_treedb.json`,
`allocs_cumulative_read_treedb.pprof`, `block.pprof`, and `mutex.pprof`.
Matrix output also includes `phase_counters.tsv` for the headline read counters
and `phase_stat_deltas.tsv` for all nonzero parseable TreeDB stat deltas,
including focused write-path counters. Allocation pprof files are explicitly
labeled cumulative; use the `memstats_*` JSON files for phase-local allocation
deltas.
