# application.db Offline Density Comparison: TreeDB vs goleveldb vs PebbleDB

Date: 2026-04-13

## What this note is actually measuring

The primary question in this document is:

**After each engine has been given its appropriate offline cleanup path, what is
its compacted on-disk size?**

That is the comparison that should be cited from this work.

This point matters especially for TreeDB. A fresh TreeDB rebuild can create
substantial temporary physical slack, and TreeDB's own offline
`vlog_rewrite` is the mechanism that recovers it. Because of that, the right
TreeDB comparison metric in this note is **rebuild + offline `vlog_rewrite`**,
not rebuild-only and not the intermediate `2.604 GiB` result from the
LevelDB-source matrix.

## Primary comparison to cite

If a reader wants one table from this note, it should be this one.

| engine | primary compacted size GiB | workflow used for the comparison |
|---|---:|---|
| PebbleDB | 1.773 | aggressive rebuild from frozen LevelDB `application.db`, then full compact |
| TreeDB | 2.092 | compact TreeDB source -> fresh TreeDB rebuild -> offline `vlog_rewrite` |
| goleveldb | 2.316 | aggressive rebuild from frozen LevelDB `application.db`, then full compact |

Primary ranking from this work:

1. PebbleDB: `1.773 GiB`
2. TreeDB: `2.092 GiB`
3. goleveldb: `2.316 GiB`

That ordering is what this note is intended to communicate at a high level.

## Why TreeDB's number is `2.092 GiB`, not `2.604 GiB`

The earlier `2.604 GiB` TreeDB number came from the aggressive cross-engine
matrix built from a frozen LevelDB `application.db` source. That is useful
supporting evidence, but it is **not** the main TreeDB outcome this note should
privilege.

The key TreeDB behavior is the self-roundtrip result on the same logical TreeDB
contents:

- compact TreeDB source: `2.031 GiB`
- fresh KV rebuild into a new TreeDB store: `2.626 GiB`
- rebuild + offline `vlog_rewrite`: `2.092 GiB`

That is the important TreeDB signal because it shows:

1. rebuild alone can create about `0.594 GiB` of additional physical slack
2. offline `vlog_rewrite` recovers about `0.534 GiB` of that slack
3. the rebuilt-and-rewritten result lands only about `0.060 GiB` above the
   original compact TreeDB source

So for TreeDB, the comparison point that reflects its actual offline compacted
state is `2.092 GiB`.

## TreeDB self-roundtrip details

This is the experiment that establishes the TreeDB comparison metric.

Source TreeDB home used for the roundtrip:

- compact source: `/home/mikers/.celestia-app-mainnet-treedb-20260412141324-offline-vlogrewrite/data/application.db`
- compact source size: `2.031 GiB`

Roundtrip result:

| TreeDB-only step | disk GiB | wall s | max RSS GiB | note |
|---|---:|---:|---:|---|
| compact source | 2.031 | n/a | n/a | already offline-rewritten TreeDB state |
| fresh KV rebuild into new TreeDB | 2.626 | 63.28 | 5.642 | same logical TreeDB contents, new physical layout |
| rebuild + offline `vlog_rewrite` | 2.092 | 93.63 | 0.178 | rebuild-created slack largely recovered |

Roundtrip artifact paths:

- original compact source:
  `/home/mikers/.celestia-app-mainnet-treedb-20260412141324-offline-vlogrewrite/data/application.db`
- rerun capture:
  `/tmp/treedb_roundtrip_capture_20260413/application.db`
- rerun logs:
  `/tmp/treedb_roundtrip_capture_20260413/rebuild.json`
  `/tmp/treedb_roundtrip_capture_20260413/rewrite.json`

## Supporting cross-engine rebuild matrix

The cross-engine matrix is still useful. It shows how all three engines behave
when rebuilt from the same frozen LevelDB `application.db` corpus. But it is a
supporting section, not the section that defines the TreeDB headline number.

Frozen LevelDB source used for the matrix:

- source DB: `/home/mikers/.application-db-matrix-source-20260413/application.db`
- source size: about `2.703 GiB`
- observed corpus:
  - `48,149,288` keys
  - `1,504,200,810` key bytes
  - `4,207,935,641` value bytes

### Default LevelDB-source rebuild matrix

| engine | rebuild GiB | post-cleanup GiB | cleanup delta GiB | rebuild s | cleanup s | rebuild RSS GiB | cleanup RSS GiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| goleveldb | 2.676 | 2.676 | +0.000 | 88.77 | 71.50 | 0.319 | 0.063 |
| PebbleDB | 2.690 | 2.628 | -0.062 | 80.59 | 10.47 | 0.472 | 0.029 |
| TreeDB | 2.612 | 2.604 | -0.008 | 42.04 | 22.64 | 3.273 | 0.110 |

Observed ordering in this supporting default matrix:

1. TreeDB: `2.604 GiB`
2. PebbleDB: `2.628 GiB`
3. goleveldb: `2.676 GiB`

### Aggressive LevelDB-source rebuild matrix

Configuration:

- goleveldb: `snappy`, block size `65536`, restart interval `256`
- PebbleDB: `zstd`, block size `65536`, target file size `67108864`
- TreeDB: `wal_on_fast`

| engine | rebuild GiB | post-cleanup GiB | cleanup delta GiB | rebuild s | cleanup s | rebuild RSS GiB | cleanup RSS GiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| goleveldb | 2.316 | 2.316 | -0.000 | 85.92 | 52.88 | 0.324 | 0.053 |
| PebbleDB | 1.835 | 1.773 | -0.063 | 73.67 | 9.50 | 0.510 | 0.023 |
| TreeDB | 2.612 | 2.604 | -0.008 | 42.13 | 21.35 | 3.266 | 0.111 |

Observed ordering in this supporting aggressive matrix:

1. PebbleDB: `1.773 GiB`
2. goleveldb: `2.316 GiB`
3. TreeDB: `2.604 GiB`

Important: this `2.604 GiB` TreeDB result is **not** the TreeDB headline number
for this note. It is the supporting result from the LevelDB-source matrix. The
TreeDB headline number remains the self-roundtrip `2.092 GiB` result above.

## Interpretation

There are two separate conclusions here.

First, for the public-facing offline compacted-size comparison, the numbers to
remember are:

- PebbleDB: `1.773 GiB`
- TreeDB: `2.092 GiB`
- goleveldb: `2.316 GiB`

Second, the TreeDB-specific behavior is itself meaningful:

- TreeDB rebuild can transiently inflate physical layout substantially
- TreeDB offline `vlog_rewrite` recovers most of that inflation
- therefore TreeDB should be evaluated at rebuild + rewrite when discussing its
  offline compacted state

A shorter way to say it is:

- PebbleDB won the strongest offline density result observed here
- TreeDB landed ahead of goleveldb once TreeDB is measured at rebuild + rewrite
- goleveldb remained behind TreeDB at `2.316 GiB`
- TreeDB's meaningful offline compacted number is `2.092 GiB`, not `2.604 GiB`

## Caveats

- The primary comparison table mixes two source paths:
  - LevelDB-source rebuilds for PebbleDB and goleveldb
  - TreeDB self-roundtrip for TreeDB
- That mixed provenance is intentional, because the goal of the primary table is
  to compare each engine's meaningful offline compacted outcome, not to force
  TreeDB to be represented by an intermediate measurement that understates its
  final compacted state.
- The supporting cross-engine matrix remains in the document for readers who
  want the strict same-source LevelDB rebuild comparison.
- This is not a live Celestia sync benchmark.
- This is not a 15-minute dwell benchmark.
- This does not measure read latency, write latency, or steady-state runtime
  cost.
- Directory size includes engine metadata, manifests, and other on-disk files,
  not just user payload.

## Reproduction

### Cross-engine default matrix

```bash
OUT_DIR=/home/mikers/.application-db-engine-matrix-mainnet-default-20260413 \
GOLEVELDB_BLOCK_SIZE=4096 \
GOLEVELDB_BLOCK_RESTART_INTERVAL=16 \
PEBBLE_COMPRESSION=snappy \
PEBBLE_BLOCK_SIZE=4096 \
PEBBLE_TARGET_FILE_SIZE=0 \
scripts/run_application_db_engine_matrix.sh \
  /home/mikers/.application-db-matrix-source-20260413/application.db
```

### Cross-engine aggressive matrix

```bash
OUT_DIR=/home/mikers/.application-db-engine-matrix-mainnet-aggressive-20260413 \
GOLEVELDB_BLOCK_SIZE=65536 \
GOLEVELDB_BLOCK_RESTART_INTERVAL=256 \
PEBBLE_COMPRESSION=zstd \
PEBBLE_BLOCK_SIZE=65536 \
PEBBLE_TARGET_FILE_SIZE=67108864 \
scripts/run_application_db_engine_matrix.sh \
  /home/mikers/.application-db-matrix-source-20260413/application.db
```

### TreeDB self-roundtrip capture

The latest exact rerun was captured with:

- source parent:
  `/home/mikers/.celestia-app-mainnet-treedb-20260412141324-offline-vlogrewrite/data`
- helper binary:
  `/home/mikers/.treedb-roundtrip-from-203g-20260413/bin/treedb-roundtrip`
- output capture:
  `/tmp/treedb_roundtrip_capture_20260413`

## Artifacts on this machine

- cross-engine default JSON:
  `/home/mikers/.application-db-engine-matrix-mainnet-default-20260413/results.json`
- cross-engine aggressive JSON:
  `/home/mikers/.application-db-engine-matrix-mainnet-aggressive-20260413/results.json`
- TreeDB roundtrip helper:
  `/home/mikers/.treedb-roundtrip-from-203g-20260413/bin/treedb-roundtrip`
- matrix runner:
  `/home/mikers/dev/snissn/gomap/scripts/run_application_db_engine_matrix.sh`

## Bottom line

The main comparison this note should communicate is not the aggressive matrix's
`2.604 GiB` TreeDB row. The main comparison is the best offline compacted
outcome per engine after the engine-appropriate cleanup path:

- PebbleDB: `1.773 GiB`
- TreeDB: `2.092 GiB`
- goleveldb: `2.316 GiB`

That is the framing this note is intended to preserve.
