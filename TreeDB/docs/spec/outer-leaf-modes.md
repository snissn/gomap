# Outer-Leaf Modes

This document defines behavioral expectations for `IndexOuterLeafMode`.

If `IndexOuterLeafMode` is unset (`""`), TreeDB defaults to `v1_leaflog_route`.

## Mode Semantics (Step 2)

| Mode | Internal nodes / leaf entries | Value-log payloads | Intended use |
| --- | --- | --- | --- |
| `v1` | internal nodes + exact-key leaf entry per user key | raw value-log record payload | baseline exact-key mode |
| `v1_leaflog` | internal nodes + leaf anchors (one anchor per outer-leaf payload block), with predecessor probing scoped to one candidate | outer-leaf envelope payload (`TOL2`) | routing-style index contract with API parity to `v1` |
| `v1_leaflog_legacy` | internal nodes + exact-key leaf entry per user key (legacy `v1_leaflog` behavior) | outer-leaf envelope payload (`TOL2`) | compatibility/bisect mode while `v1_leaflog` evolves |
| `v2_blockptr` | internal nodes + exact-key leaf entry per user key | grouped outer-leaf block payload | payload batching with exact-key index semantics |
| `v2_fenceptr` | internal nodes + fence-key leaf anchors with predecessor probing (steady state) | grouped outer-leaf block payload; WAL-on `rid_join` oversized writes may temporarily surface direct exact-key pointers before flush collapse | smallest index footprint with fence-key routing |

## Current Guarantees vs Future Work (#610)

Current Step 2 guarantees:

1. `v1_leaflog` and `v1_leaflog_legacy` are distinct public mode strings and MUST NOT be auto-aliased.
2. Known mode names are case-insensitive for parsing/normalization; canonical runtime mode strings are lowercase constants.
3. `ValueLog.WALFenceMode=simple_inline` is valid only for `v2_fenceptr`; `v1_leaflog` and `v1_leaflog_legacy` continue to reject it.
4. This step does not rewrite read/write algorithms; existing mode behavior remains in place.

Future work tracked in #610 (not guaranteed by this step):

- additional routing-path hardening and instrumentation for `v1_leaflog`.
- any migration/deprecation plan for `v1_leaflog_legacy`.
- any behavior changes will update this spec and the verification matrix in the same change.

## Correctness Requirements

`v1_leaflog` and `v1_leaflog_legacy` MUST satisfy:

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
- `TreeDB/db/value_reader_test.go`
  - `v1_leaflog` vs `v1_leaflog_legacy` lookup-mode contract
- `TreeDB/db/outer_leaf_mode_options_test.go` and `TreeDB/public_outer_leaf_mode_test.go`
  - mode parsing/normalization and WAL-fence option guardrails

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
./bin/unified-bench -dbs treedb -profile fast -keys 500000 -progress=false -format markdown \
  -checkpoint-between-tests -test all \
  -treedb-index-outer-leaf-mode v1_leaflog_legacy -treedb-force-value-pointers=true
```

For small-value workloads (where inline-vs-pointer behavior matters), run with:

```bash
./bin/unified-bench -dbs treedb -profile fast -keys 500000 -valsize 1 -progress=false -format markdown \
  -checkpoint-between-tests -test batch_write,batch_write_steady,batch_random,random_read_parallel,full_scan,prefix_scan \
  -treedb-index-optimizations=false -treedb-force-value-pointers=false \
  -treedb-index-outer-leaf-mode <v1|v1_leaflog|v1_leaflog_legacy>
```

Pre-alpha note: benchmark numbers are workload and option sensitive; use them as regression guards, not compatibility contracts.
