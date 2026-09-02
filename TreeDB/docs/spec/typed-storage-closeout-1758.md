# Typed-Storage Closeout Evidence (#1758)

Status: closeout note for parent tracker #1744 on `origin/main` at
`a7c82af8fd2028987f4706ea581abdbc8dfe8d7b`.

This document summarizes the typed-storage work that has landed through
#1757/#1781 and records the explicit handoff facts for the remaining #1736
row+column copy-on-write maintenance pass. It is evidence and vocabulary only;
it does not introduce code, a public API removal, or a native vector graph path
switch.

## Vocabulary and ownership model

`typed storage` is the umbrella subsystem for non-document physical storage of
schema-declared typed fields. The current vocabulary is:

| Term | Current meaning |
| --- | --- |
| `retained_document` / `document_payload` | Flexible JSON/document owner or retained residual payload used for reconstruction. |
| `typed-row storage` / `typed_row_asset` | Compatibility row-record physical asset path, encoded as `TCPA`/`tcs1_part_image`; also remains the row-ID/tombstone locator for generations with typed-column fields. |
| `typed-column storage` / `typed_column_part` | Opt-in sectioned, column-major `tcs1_typed_column_part` assets built through `TreeDB/internal/typedcolumn`. |
| `derived_accelerator` | Non-authoritative duplicated/index/cache/metadata bytes derived from an authoritative owner/generation. |

Authoritative field ownership is exclusive per logical field and generation:

```text
one logical field -> one authoritative owner
```

Allowed authoritative owners are retained document payload, typed-row asset, and
typed-column part. Existing dictionary-code assets, int64-values assets,
aggregate metadata, vector graph assets, read caches, and decoded metadata caches
remain `derived_accelerator`s. They may duplicate bytes for speed, but they do
not become a second source of truth for a declared field.

Existing `ColumnStore*` names are compatibility names unless they refer to a true
sectioned column-part data-plane term. New umbrella prose should say `typed
storage`; new durable column-major assets should say `typed-column` or
`typed_column_part`.

## Landed behavior and fallback boundaries

- Existing collection configs with declared columns and no explicit owner still
  resolve to `typed_row_asset`.
- `ColumnStoreColumn.Owner` is a compatibility-retained metadata field carrying
  canonical `TypedStorageFieldOwner` values.
- Explicit scalar `typed_column_part` owners can publish durable
  `tcs1_typed_column_part` refs for bool, int64, float32, double/float64, and
  string fields.
- Explicit fixed-dimension `float32_vector` `typed_column_part` owners can
  publish durable dense row-major little-endian `float32` sections.
- After #1783, explicit non-nullable fixed-degree `adjacency_list`
  `typed_column_part` owners with positive `adjacency_degree` can publish durable
  dense row-major little-endian `uint32` sections.
- At #1758 closeout, nullable/missing typed-column values remained fail-closed; #1784 documents scalar nullable typed-column representation using the nullable-int64 carrier while vector/adjacency nullable support stays staged/fail-closed.
- Retained-payload reconstruction composes retained document bytes, typed-row
  values, and typed-column values after checkpoint/reopen.
- Direct typed views require #1736 `mappedresource` validation for lifetime,
  range, endian/format, length, and alignment. Misaligned/truncated/corrupt
  assets fail closed or use the safe fallback path covered by tests.
- The typed-column int64 predicate scan is an explicit scoped API; automatic
  planner routing is deferred.

## Child-ticket status

| Ticket / PR | Status at closeout | Evidence / boundary |
| --- | --- | --- |
| #1750 / PR #1759 naming scaffold | Done | `typed-storage-naming.md` establishes umbrella vocabulary and compatibility-name classes. |
| #1751 / PR #1760 layout resolver | Done | `ResolveTypedStorageLayout` normalizes current configs to `typed_row_asset` and supports future/hybrid owners. |
| #1752 / PR #1762 umbrella cleanup | Done for safe scope | Existing public `ColumnStore*` names retained as compatibility; broad file/symbol churn deferred. |
| #1753 / PR #1763 | Done | Non-authoritative `TreeDB/internal/typedcolumn` transplant from `experiments/colgranule`; tests listed below. |
| #1754 / PR #1766 | Done | Adapter seam from collection metadata/resources to `typedcolumn`; no private lifecycle. |
| #1755 / PR #1769 | Done for scalar publication | Durable scalar `tcs1_typed_column_part` refs, reopen/reconstruction/reachability; vector/adjacency/query switch deferred. |
| #1756 / PR #1770 | Done for dense sections and vector publication | Fixed-dimension `float32_vector` publication and direct-view validation; native vector graph switch not landed. |
| #1781 / PR #1792 | Done | Perf guardrails for dense vector direct views and reconstruction generation cache. |
| #1757 / PR #1793 | Done for int64 predicate MVP | Explicit typed-column int64 equality/range scan with min/max pruning and fallback. |
| #1782 | Deferred | Native vector graph switch to typed-column parts is not landed. |
| Authoritative `adjacency_list` publication (#1783) | Done after closeout | Fixed-degree dense `uint32` schema via `adjacency_degree`; CSR/vector graph switch remains deferred. |
| Nullable/missing typed-column values (#1784) | In progress after closeout | Scalar nullable int64 representation and retained-payload semantics are documented first; vector/adjacency nullable support remains staged/fail-closed. |
| Dictionary/string predicate query integration (#1785) | Deferred | String data-plane/dictionary descriptors exist; production query MVP is not landed. |
| Aggregate metadata query integration (#1786) | Deferred | Aggregate metadata descriptors exist; production query MVP is not landed. |
| Multipart lifecycle/compaction (#1787) | Deferred to #1736/follow-ups | Current direct int64 MVP is insert-only for typed-column parts; mutation-bearing multipart fast path is deferred. |
| Full row+column COW maintenance (#1736/#1788) | Deferred | See handoff section below. |
| Post-foundation umbrella (#1791) | Open follow-up tracker | Owns typed-storage follow-ups that should not block #1744 closeout. |

## Evidence from merged PRs

### #1751 / PR #1760: typed-storage layout resolver

Scope: pure metadata ownership resolution; no IO, mmap, publication, query
planning, or durable typed-column format work.

Tests recorded in the PR:

```sh
go test -count=1 ./TreeDB/collections -run TestTypedStorage
go test -count=1 ./TreeDB/collections
```

Required coverage included document-only layout normalization, current
`ColumnStoreConfig` normalization to `typed_row_asset`, fail-closed placeholder
`typed_column_part` ownership before publication support, hybrid owners,
overlapping authoritative-owner rejection, `ColumnRetainedPayloadFull`
compatibility duplication semantics, derived accelerators as non-authoritative,
and resolver purity.

### #1753 / PR #1763: typed-column data-plane transplant

Scope: internal, non-authoritative data plane; no production publication or query
planner switch.

Tests recorded in the PR:

```sh
go test -count=1 ./TreeDB/internal/typedcolumn -run TestTypedColumnTransplant -v
go test -count=1 ./TreeDB/internal/typedcolumn ./TreeDB/collections
go test -count=1 ./TreeDB/...
rg -n "ColumnStore|column store|column-store" TreeDB docs experiments
```

Required coverage mapping included part image round trip, section directory round
trip, invalid magic/version rejection, truncated/out-of-bounds rejection, row
locator round trip, part-set latest-visible rows, missing locator rejection,
predicate metadata round trip, dictionary/aggregate descriptor round trip,
fixed-width section alignment, and no production publication.

### #1754 / PR #1766: typed-column adapter seam

Scope: non-authoritative adapter from TreeDB typed-storage metadata and #1736
mappedresource handles to the transplanted typed-column data plane.

Tests recorded in the PR:

```sh
go test -count=1 ./TreeDB/collections -run TestTypedColumnAdapter -v
go test -count=1 ./TreeDB/internal/typedcolumn -run TestTypedColumnTransplantNoProductionPublication -v
go test -count=1 ./TreeDB/internal/typedcolumn ./TreeDB/collections
go test -count=1 ./TreeDB/docs ./TreeDB/internal/typedcolumn ./TreeDB/collections
go test -count=1 ./TreeDB/...
```

Required coverage included TreeDB declared-type mapping, scalar round trips,
vector/adjacency fail-closed behavior at that stage, existing config fallback to
`typed_row_asset`, retained-payload split/restore, mappedresource mmap/heap
parity, fixed-width typed-view validation, reserved/duplicate/ambiguous field
fail-closed checks, schema/value-type metadata mismatch rejection, and no private
production lifecycle.

### #1755 / PR #1769: durable scalar typed-column publication

Scope: opt-in scalar publication/reconstruction only. Existing configs still
resolve to typed-row assets unless a column explicitly sets `Owner:
typed_column_part`.

Tests recorded in the PR:

```sh
go test -count=1 ./TreeDB/docs ./TreeDB/internal/typedcolumn ./TreeDB/collections
go test -count=1 ./TreeDB/...
```

Required tests included:
`TestTypedColumnPublicationCheckpointReopen`,
`TestTypedColumnReconstructionHybridOwners`,
`TestTypedColumnReconstructionScanHybridOwners`,
`TestTypedColumnPublicationRejectsOverlappingOwners`,
`TestTypedColumnPublicationMissingAssetFailsClosed`,
`TestTypedColumnPublicationCorruptAssetFailsClosed`,
`TestTypedColumnManifestRecoveryRefsSurviveReopen`,
`TestTypedColumnReachabilityRefsExposedForMaintenance`, and
`TestTypedColumnPublicationExistingTypedRowCompatibility`.

Benchmark smoke recorded for the touched reconstruction path:

```sh
go test -run '^$' -bench BenchmarkColumnStoreGetReconstructionM13C/rows_1024 -benchtime=100x -count=1 ./TreeDB/collections
```

Result on Apple M3: `462986 ns/op`, `166706 B/op`, `120 allocs/op`.

### #1756 / PR #1770: dense vector typed-column sections

Scope: dense fixed-dimension `float32_vector` sections, internal dense `uint32`
adjacency validation, and vector publication/reconstruction. No native vector
graph switch.

Tests recorded in the PR:

```sh
go test -count=1 ./TreeDB/docs ./TreeDB/internal/typedcolumn ./TreeDB/collections
go test -count=1 ./TreeDB/...
```

Required coverage included direct-view alignment and misalignment/fallback tests
for vector and adjacency sections, counter checks, mmap/heap parity,
`TestTypedColumnAdapterRoundTripFloat32Vector`,
`TestTypedColumnVectorDensePublicationCheckpointReopen1756`, vector layout
support, and adjacency fail-closed coverage.

Benchmark smoke:

```sh
go test -run '^$' -bench 'BenchmarkTypedColumnVectorDense(DirectView|Section)Scan' -benchmem -benchtime=100x -count=1 ./TreeDB/internal/typedcolumn
```

| Benchmark | ns/op | ops/sec | rows/s | elements/s | direct views/op | scratch decodes/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkTypedColumnVectorDenseDirectViewScan-8` | 21,870 | 45,725 | 46,821,253 | 749,140,048 | 1.000 | 0 | 0 | 0 |
| `BenchmarkTypedColumnVectorDenseSectionScan-8` | 66,167 | 15,113 | 15,476,070 | 247,617,116 | n/a | n/a | 131,170 | 6 |

### #1781 / PR #1792: typed-column/vector perf guardrails

Tests recorded in the PR:

```sh
go test -count=1 ./TreeDB/internal/typedcolumn ./TreeDB/collections
go test -count=1 ./TreeDB/...
```

Benchmark command:

```sh
go test -run '^$' -bench 'BenchmarkTypedColumnVectorDense|BenchmarkTypedColumnReconstruction' -benchmem -benchtime=100x -count=1 ./TreeDB/internal/typedcolumn ./TreeDB/collections
```

| Benchmark | Shape | ns/op | ops/sec | rows/s | elements/s | direct views/op | scratch decodes/op | mapped bytes | heap-copy bytes | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkTypedColumnVectorDenseDirectViewScan-8` | 1024 rows x 16 dims | 12,451 | 80,315 | 82,240,739 | 1,315,851,822 | 1.000 | 0 | 0 | 65,536 | 0 | 0 |
| `BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan/mapped-8` | 1024 rows x 16 dims | 12,192 | 82,021 | 83,991,775 | 1,343,868,395 | 1.000 | 0 | 65,536 | 0 | 0 | 0 |
| `BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan/heap-8` | 1024 rows x 16 dims | 12,117 | 82,529 | 84,511,669 | 1,352,186,698 | 1.000 | 0 | 0 | 65,536 | 0 | 0 |
| `BenchmarkTypedColumnVectorDenseSectionScan-8` | 1024 rows x 16 dims | 29,259 | 34,178 | 34,998,077 | 559,969,240 | n/a | n/a | n/a | n/a | 131,168 | 6 |

| Benchmark | Shape | ns/op | ops/sec | rows/s | rows/op | part loads/op | typed part decodes/op | cache hits/op | cache misses/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkTypedColumnReconstructionCache1781-8` | 128 rows, 1 generation | 156,789 | 6,378 | 816,385 | 128.0 | 1.000 | 1.000 | 127.0 | 1.000 | 364,742 | 1,142 |

### #1757 / PR #1793: int64 typed-column predicate MVP

Tests recorded in the PR:

```sh
go test -count=1 ./TreeDB/collections -run 'TestTypedColumnInt64Scan'
go test -count=1 ./TreeDB/collections ./TreeDB/internal/typedcolumn
go test -count=1 ./TreeDB/...
go test -count=1 ./...
```

Required coverage included equality, range, min/max pruning, all-pruned no-match,
fallback unsupported/not-selected, stale/fail-closed metadata/assets, reopen,
and direct row identity without reconstruction.

Benchmark command:

```sh
go test -run '^$' -bench 'BenchmarkTypedColumnInt64PredicateScan' -benchmem -benchtime=100x -count=1 ./TreeDB/collections
```

| Benchmark | ns/op | ops/sec | B/op | allocs/op | mapped bytes/op | heap-copy bytes/op | decoded bytes/op | rows scanned/op | parts pruned/op | blocks pruned/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkTypedColumnInt64PredicateScan/typed_column_part-8` | 512,787 | 1,965 | 1,101,902 | 297 | 529,722 | 0 | 36,864 | 4,096 | 1.000 | 1.000 |
| `BenchmarkTypedColumnInt64PredicateScan/document_full_scan_fallback-8` | 5,646,920 | 177.2 | 13,066,670 | 163,941 | 0 | 0 | 0 | 8,192 | 0 | 0 |

## Final latest-head evidence for #1758

Final local validation for this docs/evidence patch ran on Apple M3
(`darwin/arm64`, Go `go1.25.5`) from branch `snissn/1758-manager` after applying
this closeout patch.

Correctness commands:

```sh
go test -count=1 ./TreeDB/internal/typedcolumn ./TreeDB/collections \
  -run 'TestTypedStorageLayout|TestTypedColumnAdapter|TestTypedColumnTransplant|TestTypedColumnPublication|TestTypedColumnReconstruction|TestTypedColumnVector|TestTypedColumnAdjacency|TestTypedColumnDense|TestTypedColumnInt64Scan'
go test -count=1 ./TreeDB/...
go test -count=1 ./TreeDB/docs
```

Results: all passed. The full `./TreeDB/...` sweep included `TreeDB/docs`,
`TreeDB/internal/mappedresource`, `TreeDB/internal/typedcolumn`,
`TreeDB/collections`, `TreeDB/db`, and the remaining TreeDB packages.

Benchmark command:

```sh
go test -run '^$' \
  -bench 'BenchmarkTypedColumnVectorDense|BenchmarkTypedColumnReconstruction|BenchmarkTypedColumnInt64PredicateScan' \
  -benchmem -benchtime=100x -count=1 \
  ./TreeDB/internal/typedcolumn ./TreeDB/collections
```

| Benchmark | ns/op | ops/sec | rows/s | elements/s | mapped bytes/op | heap-copy bytes/op | decoded/derived bytes/op | direct views/op | scratch/fallback decodes/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `BenchmarkTypedColumnVectorDenseDirectViewScan-8` | 23,928 | 41,792 | 42,794,462 | 684,711,386 | 0 | 65,536 | 0 | 1.000 | 0 | 0 | 0 |
| `BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan/mapped-8` | 19,526 | 51,214 | 52,442,225 | 839,075,603 | 65,536 | 0 | 0 | 1.000 | 0 | 0 | 0 |
| `BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan/heap-8` | 20,328 | 49,193 | 50,373,051 | 805,968,813 | 0 | 65,536 | 0 | 1.000 | 0 | 0 | 0 |
| `BenchmarkTypedColumnVectorDenseSectionScan-8` | 47,131 | 21,217 | 21,726,750 | 347,628,007 | n/a | n/a | n/a | n/a | n/a | 131,220 | 6 |
| `BenchmarkTypedColumnInt64PredicateScan/typed_column_part-8` | 498,587 | 2,017 | 4,096 scanned/op | n/a | 529,722 | 0 | 36,864 decoded | n/a | 0 fallback path | 1,101,902 | 297 |
| `BenchmarkTypedColumnInt64PredicateScan/document_full_scan_fallback-8` | 5,553,188 | 180.2 | 8,192 scanned/op | n/a | 0 | 0 | 0 | n/a | document fallback | 13,066,669 | 163,941 |
| `BenchmarkTypedColumnReconstructionCache1781-8` | 123,632 | 8,088 | 1,035,326 | n/a | n/a | n/a | 364,726 B/op materialization | n/a | 1 part load/decode/op | 364,726 | 1,142 |

Memory/accounting summary:

| Path | mapped bytes | heap-copy bytes | decoded/derived bytes | active handle / view evidence | fallbacks / denied resources |
| --- | ---: | ---: | ---: | --- | --- |
| Dense direct-view heap-backed vector scan | 0 | 65,536 | 0 | `1.000 direct_views/op`; zero allocations in timed kernel | `0 scratch_decodes/op`; denied resources not exercised |
| Dense direct-view mmap-backed vector scan | 65,536 | 0 | 0 | `1.000 direct_views/op`; zero allocations in timed kernel | `0 scratch_decodes/op`; denied resources not exercised |
| Dense section decode scan | n/a | n/a | 131,220 B/op, 6 allocs/op | Direct-view counters not applicable | Safe decode path, not a denied-resource fallback |
| Int64 typed-column predicate scan | 529,722 | 0 | 36,864 decoded bytes/op | Reads durable `typed_column_part` refs through typed asset/mappedresource paths | No document fallback on typed-column sub-benchmark; pruned 1 part and 1 block/op |
| Document full-scan fallback | 0 | 0 | 0 typed-column decoded bytes | No typed-column handles | Explicit compatibility fallback; 13,066,669 B/op and 163,941 allocs/op |
| Reconstruction generation cache | n/a | n/a | 364,726 B/op materialization | `1.000 part_loads/op`, `1.000 typed_part_decodes/op`, `127 cache_hits/op`, `1 cache_misses/op` | Guards against per-row typed-part reload/decode |

## Final naming audit classification

Audit command:

```sh
rg -n --glob '!vendor/**' 'ColumnStore[A-Za-z0-9_]*|column store|column-store' \
  TreeDB docs experiments > /tmp/gomap-1758-evidence-final/name-audit-with-experiments.txt
wc -l /tmp/gomap-1758-evidence-final/name-audit-with-experiments.txt
```

The final audit found `2053` matching lines. Breakdown from the same worktree:
`TreeDB/collections=1766`, `TreeDB/internal/typedcolumn=0`, `TreeDB/docs=214`,
`docs=0`, `experiments=73`; by pattern, `ColumnStore*` symbol-ish matches were
`1816`, plain `column store` matches were `46`, and `column-store` matches were
`202`; those pattern-hit counts are non-exclusive because a single line can
match more than one pattern. The total count is expected because compatibility
APIs and historical docs remain. The major classes are:

| Occurrence group | Classification | Closeout action |
| --- | --- | --- |
| Public `ColumnStoreConfig`, `ColumnStoreColumn`, `ColumnStoreValueType`, retained-payload options | compatibility-retained | Keep; normalized by typed-storage layout resolver. No public API removal in #1758. |
| Collection implementation files with `column_store`/`ColumnStore*` control-plane names | compatibility-retained/deferred | Keep behavior; future wrapper/rename work needs a deprecation plan. |
| `column_physical_*` TCPA files and tests | compatibility-retained/deferred | Treat as `typed_row_asset`; broad file/symbol rename remains out of scope. |
| `column_asset_*` manager/reachability/GC/rewrite files | compatibility-retained | Directory/manifest compatibility names remain; semantics now cover typed-row and typed-column refs. |
| `column_dictionary_codes_asset.go`, `column_int64_values_asset.go`, aggregate/vector graph assets | derived accelerator | Non-authoritative sidecars tied to an owner/generation. |
| `TreeDB/internal/typedcolumn` | no legacy-name hits in final audit | Keep typed-column data-plane names (`ColumnPart*`) as true typed-column terminology. |
| `experiments/colgranule/**` | true typed-column terminology / historical experiment | Keep as reference unless explicitly retired. |
| `TreeDB/docs/spec/GOMAP_TREEDB_COLUMN_STORE_RFC.md` and vector reconstruction docs | historical/deferred | Do not treat as current umbrella terminology; update only in a dedicated docs rewrite. |
| `column_assets` directory and manifest/storage docs | compatibility-retained on-disk metadata | Keep directory name while surrounding prose uses typed-storage vocabulary. |

## #1736 handoff facts

#1736 Gate A is complete via PR #1747 and provides `mappedresource` resource
classes, asset keys, lifetime scopes, handles, stats/accounting, direct typed-view
validation, fake column-part seams, and maintenance-visible active pins. The
remaining #1736 work is the broad adoption and post-#1744 COW maintenance pass.

Actionable asset/ref facts now available for #1736:

- `tcs1_part_image` / `TCPA` refs are compatibility typed-row assets. They carry
  row IDs, tombstones, and any `typed_row_asset` field values.
- `tcs1_typed_column_part` refs are durable sectioned typed-column part images.
  Current publication pairs one typed-column part with the same generation as
  the typed-row locator asset for inserts/updates. Multipart lifecycle work adds
  manifest `base`/`delta`/`tombstone` roles so latest-visible readers resolve
  row identity through typed-row locator/tombstone assets before reading the
  matching typed-column row.
- `ColumnAssetRef` contains namespace, kind, generation, part id, segment file
  id, offset, length, and checksum. Maintenance must key reachability by these
  refs and fail closed on kind/generation/part/checksum mismatches.
- Typed-column refs are exposed through manifest scan/reopen, reachability, and
  rewrite/GC range accounting. Maintenance should enumerate manifest/control
  roots and snapshots, not scan row documents, to discover typed-storage assets.
- Active typed-column reads can use the typed asset read cache with
  `mappedresource.ClassTypedColumnAsset`; direct fixed-width views must remain
  protected by live #1736 handles.
- Resource/accounting fields to preserve in maintenance/debug output include
  mapped bytes, heap-copy bytes, decoded/derived bytes, active handles, direct
  views, scratch/fallback decodes, denied resources, and pinned bytes blocking
  deletion/rewrite.
- Derived accelerators that need COW accounting include dictionary-code assets,
  int64-values assets, aggregate metadata, vector graph assets, and decoded
  metadata caches. They are not authoritative field owners but must be tied to
  an owner/generation before rewrite or deletion.

Remaining COW maintenance work for #1736/#1788:

- Add full row+column COW reachability and rewrite/GC over typed-row assets,
  typed-column parts, prepared refs, quarantined refs, superseded generations,
  snapshots, and active handles.
- Distinguish directly deletable all-dead typed asset segments from mixed
  live/dead rewrite debt.
- Protect active mapped/heap/direct-view handles during destructive maintenance;
  delete only after handles/snapshots release or mark zombie/quarantine.
- Add namespace inventory reconciliation for `column_assets` segment files,
  prepared registries, quarantine, and orphan/suspect assets.
- Preserve fail-closed behavior for incomplete protection state, corrupt refs,
  stale checksums, unsupported versions, and mutation-bearing typed-column part
  fast paths until multipart latest-visible maintenance is implemented.
- Keep #1782 native vector graph switching, nullable vector/adjacency typed-column
  integration, dictionary/string query integration, aggregate query integration,
  multipart lifecycle/compaction, and full row+column COW cleanup as explicit
  follow-ups, not implied #1744 completions. Authoritative fixed-degree
  adjacency publication is tracked as done in the status table above.

## Post-closeout #1789 schema evolution policy

Issue #1789 adds the current typed-column schema evolution and migration policy
in `TreeDB/docs/spec/typed-column-schema-evolution.md`. The closeout facts above
remain the implementation baseline, but typed-column image, descriptor, manifest,
and schema formats are still pre-alpha: unsupported versions and schema/layout
mismatches must fail closed rather than trigger implicit migration, cleanup, or
rewrite. Benchmark and experiment directories should be rebuilt after typed-column
format/schema changes until explicitly scoped migration tooling exists. Future
hot-path format changes must report baseline-versus-final `B/op` and `allocs/op`
and preserve 0-alloc/near-0-alloc direct decode/scan paths or document a
benchmarked fallback.

## Post-closeout #1788 maintenance update

Issue #1788 adds the current row+column typed-asset maintenance contract in
`TreeDB/docs/spec/typed-asset-maintenance-1788.md`. The implementation keeps the
handoff boundaries above, but the destructive maintenance path now has shared
reachability for typed-row `tcs1_part_image`, typed-column
`tcs1_typed_column_part`, aggregate metadata, dictionary-code, int64-value, and
vector graph derived refs. `ColumnAssetGC` and `ColumnAssetRewrite` automatically
fold active process-local `mappedresource` pins into their plans, report mapped
bytes, heap-copy bytes, active handles, pinned bytes, fallbacks, and denied
resources, and fail closed on unconvertible active pins or incomplete segment/ref
classification.
