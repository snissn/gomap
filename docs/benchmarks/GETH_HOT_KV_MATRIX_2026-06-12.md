# geth/Nitro hot-KV matrix — 2026-06-12

Directional benchmark for the #2392 hot-KV workload using the integrated
geth/Nitro path (`node.OpenDatabase` / `ethdb`), not the gomap raw `kvstore`
adapters.

## Command

```sh
GETH_REPO=/path/to/go-ethereum \
RUN_DIR=/tmp/geth_hotkv_matrix_30k_20260612T192600Z \
KEYS=30000 \
READS=12000 \
ENGINES=pebble,leveldb,treedb \
KEY_SHAPES=geth-mixed,single-prefix \
VALUE_SHAPES=geth-mixed \
VALUE_SIZES=128,512 \
BATCH_TARGET_BYTES=102400,1048576 \
  scripts/treedb_geth_hot_kv_matrix.sh
```

Context:

- go-ethereum checkout: `/path/to/go-ethereum`
- go-ethereum head used for this run: `6371ea5ca750`
- Matrix artifact dir used for this run: `/tmp/geth_hotkv_matrix_30k_20260612T192600Z`
- `size bytes` is loaded DB size after write/read/iterate and before the
  destructive DeleteRange phase.
- `post-delete bytes` is measured after DeleteRange close/reopen verification.
- DeleteRange throughput reports affected keys/sec, not range calls/sec.

## Results

| key shape | value shape | value size | batch target bytes | engine | write ops/sec | read ops/sec | iterate keys/sec | DeleteRange keys/sec | size bytes | post-delete bytes |
|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| geth-mixed | geth-mixed | 128 | 102,400 | pebble | 538,244 | 901,518 | 23,529,412 | 9,192,114 | 11,294,970 | 3,439 |
| geth-mixed | geth-mixed | 128 | 102,400 | leveldb | 2,127,157 | 950,025 | 25,584,543 | 2,342,202 | 11,290,958 | 11,886,131 |
| geth-mixed | geth-mixed | 128 | 102,400 | treedb | 166,815 | 533,855 | 2,333,019 | 967,875 | 27,125,469 | 15,794,662 |
| geth-mixed | geth-mixed | 128 | 1,048,576 | pebble | 466,875 | 1,002,569 | 24,673,589 | 17,491,436 | 11,292,738 | 3,478 |
| geth-mixed | geth-mixed | 128 | 1,048,576 | leveldb | 2,297,251 | 1,109,596 | 26,657,781 | 2,659,123 | 11,289,097 | 11,886,132 |
| geth-mixed | geth-mixed | 128 | 1,048,576 | treedb | 172,603 | 425,539 | 1,118,441 | 972,859 | 27,116,234 | 15,793,859 |
| geth-mixed | geth-mixed | 512 | 102,400 | pebble | 224,252 | 565,931 | 14,357,213 | 8,737,440 | 41,554,870 | 4,757 |
| geth-mixed | geth-mixed | 512 | 102,400 | leveldb | 926,202 | 633,482 | 17,928,290 | 2,330,542 | 41,545,986 | 42,554,647 |
| geth-mixed | geth-mixed | 512 | 102,400 | treedb | 124,585 | 279,113 | 1,763,388 | 675,645 | 88,107,350 | 46,551,816 |
| geth-mixed | geth-mixed | 512 | 1,048,576 | pebble | 222,439 | 598,302 | 15,304,818 | 8,223,590 | 41,546,558 | 4,429 |
| geth-mixed | geth-mixed | 512 | 1,048,576 | leveldb | 977,185 | 678,313 | 17,742,734 | 2,345,437 | 41,539,108 | 42,554,643 |
| geth-mixed | geth-mixed | 512 | 1,048,576 | treedb | 141,780 | 259,190 | 727,385 | 772,772 | 88,054,549 | 46,530,147 |
| single-prefix | geth-mixed | 128 | 102,400 | pebble | 926,729 | 1,249,024 | 33,449,477 | 8,532,525 | 4,955,433 | 3,228 |
| single-prefix | geth-mixed | 128 | 102,400 | leveldb | 3,376,905 | 1,059,525 | 29,295,702 | 2,697,084 | 4,952,448 | 5,405,600 |
| single-prefix | geth-mixed | 128 | 102,400 | treedb | 337,847 | 1,036,363 | 17,165,751 | 928,656 | 14,360,855 | 9,365,386 |
| single-prefix | geth-mixed | 128 | 1,048,576 | pebble | 708,104 | 1,274,940 | 33,133,904 | 9,396,168 | 4,954,466 | 3,018 |
| single-prefix | geth-mixed | 128 | 1,048,576 | leveldb | 2,981,428 | 1,142,544 | 17,387,101 | 2,606,976 | 4,951,630 | 5,405,600 |
| single-prefix | geth-mixed | 128 | 1,048,576 | treedb | 327,423 | 1,089,547 | 18,724,652 | 951,933 | 14,357,157 | 9,359,955 |
| single-prefix | geth-mixed | 512 | 102,400 | pebble | 452,179 | 802,814 | 16,829,511 | 15,988,275 | 16,481,881 | 3,868 |
| single-prefix | geth-mixed | 512 | 102,400 | leveldb | 1,912,356 | 942,893 | 23,653,094 | 2,239,084 | 16,477,032 | 17,131,332 |
| single-prefix | geth-mixed | 512 | 102,400 | treedb | 263,583 | 361,562 | 5,794,302 | 506,840 | 37,650,746 | 21,346,727 |
| single-prefix | geth-mixed | 512 | 1,048,576 | pebble | 307,968 | 589,781 | 14,394,820 | 8,235,349 | 16,478,591 | 3,549 |
| single-prefix | geth-mixed | 512 | 1,048,576 | leveldb | 1,446,196 | 593,037 | 24,391,910 | 2,002,843 | 16,474,298 | 17,131,335 |
| single-prefix | geth-mixed | 512 | 1,048,576 | treedb | 245,811 | 496,459 | 8,856,635 | 770,086 | 37,638,362 | 21,346,727 |

## TreeDB ratios versus Pebble

| key shape | value shape | value size | batch target bytes | write ratio | read ratio | iterate ratio | DeleteRange ratio | size ratio |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| geth-mixed | geth-mixed | 128 | 102,400 | 0.310x | 0.592x | 0.099x | 0.105x | 2.402x |
| geth-mixed | geth-mixed | 128 | 1,048,576 | 0.370x | 0.424x | 0.045x | 0.056x | 2.401x |
| geth-mixed | geth-mixed | 512 | 102,400 | 0.556x | 0.493x | 0.123x | 0.077x | 2.120x |
| geth-mixed | geth-mixed | 512 | 1,048,576 | 0.637x | 0.433x | 0.048x | 0.094x | 2.119x |
| single-prefix | geth-mixed | 128 | 102,400 | 0.365x | 0.830x | 0.513x | 0.109x | 2.898x |
| single-prefix | geth-mixed | 128 | 1,048,576 | 0.462x | 0.855x | 0.565x | 0.101x | 2.898x |
| single-prefix | geth-mixed | 512 | 102,400 | 0.583x | 0.450x | 0.344x | 0.032x | 2.284x |
| single-prefix | geth-mixed | 512 | 1,048,576 | 0.798x | 0.842x | 0.615x | 0.094x | 2.284x |

## Directional takeaways

- The key prefix shape matters materially for TreeDB iteration/read throughput:
  the single-prefix shape is much closer to Pebble than the mixed geth-prefix
  shape. Tuning should focus on mixed-prefix traversal/iterator behavior.
- Larger values improve TreeDB write ratio versus Pebble, but TreeDB loaded size
  remains about `2.1x` to `2.9x` Pebble in this un-compacted hot-KV fixture.
- The 100 KiB vs 1 MiB batch target changes write throughput, but does not
  erase the main gaps. It is still worth keeping in the benchmark matrix because
  some phases move noticeably.
- DeleteRange is consistently the largest throughput gap in this matrix.
