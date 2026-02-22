# Outer-Leaf Modes

This document defines behavioral expectations for `IndexOuterLeafMode`.

If `IndexOuterLeafMode` is unset (`""`), TreeDB defaults to `v2_fenceptr`.

## Mode Semantics

| Mode | Index lookup model | Pointer payload model | Intended use |
| --- | --- | --- | --- |
| `v1` | exact-key leaf entry per user key | raw value-log record payload | baseline exact-key mode |
| `v1_leaflog` | exact-key leaf entry per user key | outer-leaf envelope payload (`TOL2`) through mature value-log block codec path | exact-key semantics with outer-leaf payload encoding |
| `v2_blockptr` | exact-key leaf entry per user key | grouped outer-leaf block payload | better pointer payload batching with exact-key index semantics |
| `v2_fenceptr` | fence-key routing + predecessor probe (steady state) | grouped outer-leaf block payload; WAL-on `rid_join` oversized writes may temporarily surface direct exact-key pointers before flush collapse | smallest index footprint with fence-key routing |

## Correctness Requirements

`v1_leaflog` MUST satisfy:

1. Point-read parity with `v1` for hit/miss semantics.
2. Iterator parity with `v1` for forward/reverse/range/prefix bounds.
3. Reopen parity for overwrite/delete/update sequences.
4. Value-log rewrite parity: rewritten pointers remain readable after close/reopen.
5. Value-log GC parity: unreachable segments are eligible/deleted while live keys remain readable after reopen.
6. Crash-recovery parity under WAL replay for delete-range and large-value durability tiers.

## Validation Coverage

Primary coverage for `v1_leaflog` mode parity:

- `TreeDB/reopen_verify_test.go`
  - read/write/reopen parity
  - iterator parity
  - rewrite + GC reopen parity
- `TreeDB/recovery_spec_test.go`
  - crash replay matrices including `v1_leaflog`

## Benchmark Workflow (Unified Bench)

Use unified bench for mode sanity checks:

```bash
GOWORK=off make unified-bench
./bin/unified-bench -dbs treedb -profile fast -keys 500000 -progress=false -format markdown \
  -checkpoint-between-tests -test all \
  -treedb-index-outer-leaf-mode v1 -treedb-force-value-pointers=true
./bin/unified-bench -dbs treedb -profile fast -keys 500000 -progress=false -format markdown \
  -checkpoint-between-tests -test all \
  -treedb-index-outer-leaf-mode v1_leaflog -treedb-force-value-pointers=true
```

For small-value workloads (where inline-vs-pointer behavior matters), run with:

```bash
./bin/unified-bench -dbs treedb -profile fast -keys 500000 -valsize 1 -progress=false -format markdown \
  -checkpoint-between-tests -test batch_write,batch_write_steady,batch_random,random_read_parallel,full_scan,prefix_scan \
  -treedb-index-optimizations=false -treedb-force-value-pointers=false \
  -treedb-index-outer-leaf-mode <v1|v1_leaflog>
```

Pre-alpha note: benchmark numbers are workload and option sensitive; use them as regression guards, not compatibility contracts.
