# TreeDB Freelist Buckets (Design + Locality Notes)

## Goals

- Reduce on-disk span inflation (`treedb.user.pages.span_ratio_ppm`) under churn.
- Preserve locality while keeping alloc/free O(1) in the common case.
- Keep metadata bounded and checksummed; avoid per-op global scans.

## Non-Goals

- Changing page size or the B+Tree format.
- Eliminating the need for vacuum/compaction.
- Perfect locality under adversarial workloads.

## Current State

- Single freelist chain (`PageTypeFreelist`) with a region-biased selection inside
  the head page when `FreelistRegionPages`/`FreelistRegionRadius` are enabled.
- `PreferAppendAlloc` bypasses reuse entirely, appending new pages to preserve
  sequential allocation at the cost of file growth.

## Proposed Bucketed Freelist Layout (Format v2)

### Summary

Partition the freelist into per-region buckets. Each bucket is a freelist chain
holding pages from a contiguous region of the file. Allocation uses the hint to
select a region bucket first, then fans out within a small radius, with a global
fallback.

### On-Disk Changes

**Meta page additions**
- `FreelistFormatVersion` (uint32): `0` = legacy single list, `1` = bucketed.
- `FreelistBucketRegionPages` (uint64): region size in pages (e.g., 8192).
- `FreelistBucketDirPageID` (uint64): head of freelist directory pages.

**New page type**
- `PageTypeFreelistIndex`: directory pages that map region ranges to bucket heads.

**Directory page layout**

```
Header (PageHeaderSize)
NextPageID (8 bytes)
BaseRegion (8 bytes)  // region index of the first bucket in this page
BucketCount (2 bytes)
BucketHeads[BucketCount] (BucketCount * 8 bytes) // freelist head page IDs
```

Each directory page covers a contiguous range of region indices. Pages are
linked by `NextPageID`.

**Bucket pages**
- Existing `PageTypeFreelist` pages remain unchanged.
- Each bucket head points to a freelist chain that only stores page IDs from
  that bucket’s region.

### Algorithms

**Free(id)**
- Compute `region = id / FreelistBucketRegionPages`.
- Find the directory page that covers `region`.
- Push `id` onto that bucket’s freelist head (allocate a new freelist page
  if the bucket is empty).

**Alloc(hint)**
- Compute target region from `hint` (or `lastAlloc` if hint is 0).
- Try bucket for target region.
- If empty, probe neighbor regions within `FreelistRegionRadius`.
- Fallback: global bucket (region 0) or append allocation if all empty.

### Migration / Compatibility

- If `FreelistFormatVersion == 0`, use legacy freelist chain.
- To enable bucketed freelist:
  - Create directory pages and set meta fields on checkpoint/vacuum.
  - Migrate entries by walking the legacy list and inserting each ID into its
    region bucket.
- Downgrade path: optional offline tool to collapse buckets back to a single
  list (not automatic).

### Invariants & Validation

- Directory pages must verify checksum; bucket heads must reference
  `PageTypeFreelist`.
- A page ID appears in exactly one bucket.
- `readFreelistStats` should be extended to walk bucketed format for reporting.

## Locality Profiling Notes (2025-12-26, local)

**Workload**
- `./bin/unified-bench`
- `-exclude-dbs '' -dbs treedb`
- `-test sequential_write,fragmentation_report_pre,random_write,random_delete,fragmentation_report_post`
- `-keys 30000 -valsize 64 -batchsize 500 -range-queries 0 -range-span 0`
- `-checkpoint-between-tests`
- Seed: 1 (default)

**Default (region-biased freelist)**
- Pre: `treedb.user.pages.span_ratio_ppm=19612448`, `treedb.pages.total=23878`
- Post: `treedb.user.pages.span_ratio_ppm=77234578`, `treedb.pages.total=110014`

**PreferAppendAlloc**
- Pre: `treedb.user.pages.span_ratio_ppm=52700414`, `treedb.pages.total=63563`
- Post: `treedb.user.pages.span_ratio_ppm=183609035`, `treedb.pages.total=414767`

**Interpretation**
- Region-biased reuse produced lower span ratios than append-only allocation
  under the same churn (lower is better).
- PreferAppendAlloc increased span ratio significantly due to file growth.
- For this workload, current region bias helps; bucketed freelist should only
  be pursued if higher-churn or larger-key workloads show unacceptable span
  ratios with region bias.


## Locality Profiling Notes (2025-12-26, local, 300k)

**Workload**
- `./bin/unified-bench`
- `-exclude-dbs '' -dbs treedb`
- `-profile fast`
- `-test sequential_write,fragmentation_report_pre,batch_delete,batch_random,fragmentation_report_post`
- `-keys 300000 -valsize 64 -batchsize 1000 -range-queries 0 -range-span 0`
- Seed: 1 (default)
- Note: `batch_delete`/`batch_random` used to keep runtime reasonable at 300k; `batch_random` uses a 10x keyspace spread.

**Region bias enabled (default)**
- Pre: `treedb.user.pages.span_ratio_ppm=4300688`, `treedb.pages.total=51922`
- Post: `treedb.user.pages.span_ratio_ppm=369769263`, `treedb.pages.total=4343765`, `treedb.user.pages.span=4342940`

**Region bias disabled (`treedb-freelist-region-radius=-1`)**
- Pre: `treedb.user.pages.span_ratio_ppm=4052657`, `treedb.pages.total=48931`
- Post: `treedb.user.pages.span_ratio_ppm=367952149`, `treedb.pages.total=4340975`, `treedb.user.pages.span=4321598`

**Interpretation**
- At 300k, span ratios after churn are massive in both modes; bias on/off differs by <0.5%.
- The allocator choice is not the dominant driver here; churn plus wide keyspace spread dominates file growth.
- Bucketed freelist is unlikely to materially reduce span ratios unless allocation/compaction strategy changes.

## Next Steps

- Done: add unified-bench flags for `FreelistRegionPages`/`FreelistRegionRadius`.
- Done: re-run locality profile at 300k with region bias enabled and disabled (see above).
- Open: revisit bucketed freelist if allocator/compaction changes still leave span ratios high.
