# Typed-Column Direct-View Closeout (#1899)

Status: final documentation/spec closeout for the narrowed #1886 stack. This
note closes the current fixed-width typed-column direct-view work by recording
merged child evidence, the final coverage matrix, safety-check placement,
counters, storage-overhead vocabulary, and explicitly safe deferrals. It does not
implement or claim physical row-asset direct views (#1897) or adjacency direct
views (#1901).

Canonical safety rules remain in `typed-column-direct-view-alignment.md`; this
closeout is the issue #1899 evidence map and final reviewer checklist.

## Consumed child issue and PR facts

| Child issue | PR | Merge commit | Closeout contribution |
| --- | --- | --- | --- |
| #1893 | #1902 | `29dd3cb3857baff8b7889c21b8be30957f9e9bc2` | Aligned fixed-width direct-view contract, conformance vocabulary, all-owner classification scaffold. |
| #1737 | #1905 | `7dc8eb7c895ff198280660348ee9cd6ed0287241` | Native little-endian scalar float payload bytes and fixed-width payload fixtures/helpers. |
| #1895 | #1907 | `37d3cb2b1e30a7c2ddb9bce59a05e323d2c69900` | Writer-side layout contracts, deterministic padding, and certification accounting for typed-column-part fixed-width candidates. |
| #1896 | #1908 | `c98424835a4c61ba5ffd4edc2fb72c922d626827` | Generic typed-column reader consumption with fail-closed certification and direct-source counters. |
| #1898 | #1910 | `8086044ae7df9178257dc08d09dc924c5fa161f8` | Column-graph typed-column vector-source consumption and physical-row/adjacency deferral boundaries. |

## Final all-type inventory

Each declared `ColumnStoreValueType` has one final #1886 closeout status:

| Declared value type | Final #1899 status |
| --- | --- |
| `bool` | Safe fallback only. Current bitpack/RLE representations are not dense fixed-width direct-view payloads. |
| `int64` | Covered as a typed-column-part fixed-width scalar direct-view candidate when raw, non-null, uncompressed, little-endian, writer-certified, absolutely aligned, and read-time validated. |
| `float32` | Covered as a native typed-column-part scalar direct-view candidate for raw little-endian IEEE-754 payloads. Raw-int64 compatibility carriers remain fallback-only and are not direct-view evidence. |
| `double` | Covered as a native typed-column-part scalar direct-view candidate for raw little-endian IEEE-754 payloads. Raw-int64 compatibility carriers remain fallback-only and are not direct-view evidence. |
| `string` | Safe fallback only for declared string values and dictionary string tables in this stack. Dictionary-code derived sidecar row payloads are a separate post-#1899 little-endian `uint32` direct-view sidecar format. |
| `float32_vector` | Covered as a typed-column-part dense fixed-dimension vector direct-view candidate for raw row-major little-endian payloads, including the column-graph typed-column vector source. |
| `adjacency_list` | Safe-deferred fallback only. Durable fixed-degree payload publication may exist, but certified direct views are deferred to #1901. |

## Final coverage matrix

The meaningful combinations below summarize `ColumnStoreValueType`, storage
owner/path, and consumer path. Rows marked active still require every safety
check in this document before exposing an unsafe typed view.

| ColumnStoreValueType | Storage owner/path | Consumer path | Final status | Evidence interpretation |
| --- | --- | --- | --- | --- |
| `bool` | `typed_column_part` | generic typed-column readers | Fallback-only. | Bitpack/RLE can use streaming or scratch paths, not mmap direct-view evidence. |
| `int64` | `typed_column_part` | generic typed-column readers | Active for certified raw little-endian scalar payloads. | `mmap_direct_view` is valid zero-copy evidence only when source is mapped and checks pass. |
| `float32` | `typed_column_part` | generic typed-column readers | Active for certified native raw little-endian scalar payloads. | Raw-int64 compatibility carriers are fallback-only and not direct-view evidence. |
| `double` | `typed_column_part` | generic typed-column readers | Active for certified native raw little-endian scalar payloads. | Raw-int64 compatibility carriers are fallback-only and not direct-view evidence. |
| `string` | `typed_column_part` | generic typed-column readers | Fallback-only. | Variable-width values and dictionary string tables are not direct-view payloads; dictionary-code derived sidecars use a separate fixed-width `uint32` sidecar contract. |
| `float32_vector` | `typed_column_part` | generic typed-column readers | Active for certified fixed-dimension row-major little-endian payloads. | Use direct/fallback counters and row/dimension checks to separate mmap evidence from heap or scratch fallback. |
| `float32_vector` | `typed_column_part` | `column_graph` typed-column vector source | Active for certified fixed-dimension row-major little-endian payloads. | Counts may demonstrate vector-source mmap direct coverage; adjacency and row assets are excluded from current-stack wins. |
| `adjacency_list` | `typed_column_part` | adjacency consumers | Deferred/fallback-only to #1901. | Existing durable payload and fixtures may verify byte format and corruption handling, not current mmap direct-view speedup. |
| `bool` | physical row asset | generic row consumer | Deferred/fallback-only to #1897. | Current safe behavior is decode/copy through existing row asset paths. |
| `int64` | physical row asset | generic row consumer | Deferred/fallback-only to #1897. | Current safe behavior is decode/copy through existing row asset paths. |
| `float32` | physical row asset | generic row consumer | Deferred/fallback-only to #1897. | Current safe behavior is decode/copy through existing row asset paths. |
| `double` | physical row asset | generic row consumer | Deferred/fallback-only to #1897. | Current safe behavior is decode/copy through existing row asset paths. |
| `string` | physical row asset | generic row consumer | Deferred/fallback-only to #1897. | Current safe behavior is decode/copy through existing row asset paths. |
| `float32_vector` | physical row asset | generic row consumer | Deferred/fallback-only to #1897. | Do not count physical row vector views as #1886 mmap evidence. |
| `float32_vector` | physical row asset | vector consumer | Deferred/fallback-only to #1897. | Do not count physical row vector views as #1886 mmap evidence. |
| `adjacency_list` | physical row asset | generic row consumer | Deferred/fallback-only to #1897, with adjacency direct-view semantics also owned by #1901. | Do not claim adjacency mmap wins in #1899. |
| `adjacency_list` | physical row asset | adjacency consumer | Deferred/fallback-only to #1897, with adjacency direct-view semantics also owned by #1901. | Do not claim adjacency mmap wins in #1899. |

## Safety-check placement

Direct-view eligibility is fail-closed. A reader may hoist checks into
certification only when the same evidence is persisted and the unsafe view still
performs the read-time checks that depend on the concrete handle.

| Check family | Placement | Required outcome |
| --- | --- | --- |
| Absolute-offset alignment | Certification/open-time validation against `asset_ref.offset + section.offset` and `asset_ref.offset + block.payload_offset`. | Relative image alignment is insufficient; unaligned absolute offsets reject mmap direct view or use fallback. |
| Actual pointer alignment | Immediately before unsafe slice construction. | The concrete Go byte-slice address must satisfy the element alignment. |
| Lifetime/stale handle | Immediately before and during view construction/use. | Nil, released, stale, or out-of-lifetime mappedresource handles fail closed. |
| Checksum/integrity | Certification/open-time validation when the read-integrity policy requires it. | Manifest, descriptor, section bounds, and checksums must identify the bytes being viewed. |
| Row/dimension/length/endian/corruption | Certification for persisted contract fields; read time for concrete byte length, element count, and host endian compatibility. | Mismatches reject direct views; wrong-endian or malformed data falls back or fails closed according to the caller contract. |
| Reopen/persistence | Reopen and reconstruction paths must validate manifest refs and layout contracts before serving prepared views. | Certified eligibility must survive close/reopen for durable typed-column-part assets; missing or old contracts are not silently trusted. |

## Direct-source counters and interpretation

Use the following vocabulary in tests, diagnostics, benchmark tables, and PR
evidence:

| Counter/reason vocabulary | Interpretation |
| --- | --- |
| `mmap_direct_view` | Zero-copy typed view from mapped storage after certification and read-time checks. This is the only current-stack mmap direct-view speedup evidence. |
| `heap_copy_typed_view` | Safe typed view over owned heap bytes. This is a fallback, not zero-copy evidence. |
| `scratch_decode` | Decode into caller/session scratch. Safe fallback and useful for correctness evidence. |
| `streaming_fallback` | Streaming codec or byte-loop fallback. Safe behavior for unsupported/variable-width/compressed layouts. |
| `certification_failure` | Layout, manifest, checksum, schema, or certification identity rejected direct-view eligibility. |
| Reason buckets | Stable reason strings include `absolute_offset_unaligned`, `actual_pointer_unaligned`, `stale_handle`, `wrong_endian`, `length_multiple_mismatch`, `row_count_mismatch`, `dimension_mismatch`, `nullable_default_wrapper`, `compressed`, and `direct_view_deferred`. |

Heap-copy typed views are intentionally permitted because they preserve the typed
reader API and safety checks for heap-backed handles, but they must be reported
separately from mmap direct views and must not be used as zero-copy performance
proof.

## Padding and storage-overhead fields

Evidence and diagnostics should separate these byte classes so reviewers can
verify alignment cost without conflating it with payload growth:

| Field | Meaning |
| --- | --- |
| payload bytes | Logical value bytes: element size multiplied by element count. |
| layout-contract bytes | Descriptor/manifest/layout-contract metadata needed to certify the view. |
| in-image padding | Deterministic zero padding inside the typed-column image to align sections/blocks relative to image start. |
| row-record padding | Physical row-record padding; current narrowed #1886 typed-column-part stack records this as `0` or not applicable. |
| segment/appender prefix padding | Deterministic zero bytes inserted before a typed-column-part asset so absolute segment offsets satisfy required alignment. |

## Safe deferrals

Physical row-asset direct views are deferred to #1897. Current physical row asset
consumers remain safe because they continue to use existing decode/copy paths and
must not be counted as current-stack mmap direct-view evidence.

Adjacency direct views are deferred to #1901. Current adjacency payload handling
remains safe because byte-format publication, validation, and corruption tests do
not authorize unsafe adjacency views in the narrowed #1886 closeout.

Therefore #1886 may close with these deferrals if the typed-column fixed-width
scalar/vector tests and evidence pass: the active closeout surface is
writer-certified `typed_column_part` scalar/vector coverage plus the
`column_graph` typed-column vector source, not physical row assets and not
adjacency mmap direct views.

## Benchmark and evidence section

Closeout evidence was captured on `darwin/arm64`, Apple M3, `go1.25.5`, with
`GOMAXPROCS=8`. Benchmark numbers below are representative local medians from
five runs with `-benchtime=500ms`; the PR body records the exact final commit and
artifact paths.

Correctness commands:

```sh
GOWORK=off go test ./TreeDB/internal/typeddecode ./TreeDB/internal/columnlayout ./TreeDB/internal/typedcolumn \
  -run 'Test(Int64DirectView|DirectView|ScalarFloat|DenseFloat32Vector|AdjacencyDirectView|ColumnPartLayoutContract|RawFixedWidth|FloatAndNonInt64|CapabilityValidation)' \
  -count=1

GOWORK=off go test ./TreeDB/collections \
  -run 'TestTypedColumnDirectView|TestColumnAssetTypedColumnPartDirectView|TestColumnAssetReachabilityDirectView|TestColumnVectorGraphTypedColumn|TestSearchVectorIndexTypedColumnVector|TestColumnVectorGraphBlockViewAdjacency|TestColumnVectorGraphAdjacency' \
  -count=1
```

Both commands passed during #1899 closeout. The first command covers native
scalar `float32`/`double` direct-view planning, raw-bit preservation, stale
handles, actual-pointer alignment, wrong endian/length/row/dim mismatches, and
adjacency direct-view deferral. The second command covers all-type inventory,
writer certification/storage accounting, absolute-offset alignment, multi-asset
segment/appender padding, reopen/persistence, corruption/fallback, column-graph
typed-column vector counters, and row-asset/adjacency guardrails.

Benchmark commands:

```sh
GOWORK=off go test ./TreeDB/internal/typeddecode \
  -run '^$' -bench 'BenchmarkFixedWidthScalarDirectView1899' \
  -benchmem -benchtime=500ms -count=5

GOWORK=off go test ./TreeDB/internal/typedcolumn \
  -run '^$' \
  -bench 'BenchmarkScalarFloatRawDecode1737|BenchmarkTypedColumnVectorDense(DirectViewScan|MmapHeapDirectViewScan|SectionScan)|BenchmarkTypedColumnDenseFloat32Dot1790' \
  -benchmem -benchtime=500ms -count=5

GOWORK=off go test ./TreeDB/collections \
  -run '^$' \
  -bench 'BenchmarkTypedColumn(Int64PredicateAggregate|Int64PredicateScan|FloatFallback)|BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3|BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReader(V4|WithDocumentsV4|WithDocumentsExcludeEmbedding1875)' \
  -benchmem -benchtime=500ms -count=5
```

Timer boundaries: scalar direct-view construction measures certification, fixed-width
option normalization, and handle-backed typed slice view construction over
already-acquired mapped bytes; each scalar subbenchmark explicitly revalidates
direct-view certification inside the timed loop. Vector dense scan measures
view-backed dense payload iteration; column-graph benchmarks time search and, for
the document variants, final document fetch/reconstruction separately through
reported `doc_*` counters.

| Scenario | Benchmark | ns/op | ops/sec | B/op | allocs/op | Direct-source counters |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| int64 mmap view construction | `BenchmarkFixedWidthScalarDirectView1899/int64_mmap` | 275 | 3,633,721 | 0 | 0 | `mmap_direct_view/op=1`, heap/scratch/certification failures `0`, `mapped_B=65536` |
| float32 mmap view construction | `BenchmarkFixedWidthScalarDirectView1899/float32_mmap` | 195 | 5,133,470 | 0 | 0 | `mmap_direct_view/op=1`, heap/scratch/certification failures `0`, `mapped_B=32768` |
| double mmap view construction | `BenchmarkFixedWidthScalarDirectView1899/float64_mmap` | 276 | 3,624,502 | 0 | 0 | `mmap_direct_view/op=1`, heap/scratch/certification failures `0`, `mapped_B=65536` |
| vector mmap direct scan | `BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan/mapped` | 12,568 | 79,567 | 0 | 0 | `direct_views/op=1`, `mapped_B=65536`, `heap_copy_B=0`, `scratch_decodes/op=0` |
| vector heap-copy typed-view scan | `BenchmarkTypedColumnVectorDenseMmapHeapDirectViewScan/heap` | 12,311 | 81,228 | 0 | 0 | `direct_views/op=1`, `mapped_B=0`, `heap_copy_B=65536`, `scratch_decodes/op=0`; safe fallback, not mmap evidence |
| vector section decode scan | `BenchmarkTypedColumnVectorDenseSectionScan` | 29,845 | 33,506 | 131,169 | 6 | scratch/section decode comparison path |
| column_graph typed vector search | `BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3` | 13,669 | 73,158 | 0 | 0 | `vector_mmap_direct/search=164`, vector heap/scratch/certification failures `0`, `typed_column_vector_fallbacks/search=0`, adjacency mmap/heap `0`, `adjacency_scratch_decodes/search=104` |
| open searcher no-doc | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4` | 13,020 | 76,805 | 784 | 2 | `vector_mmap_direct/search=164`, vector heap/scratch/certification failures `0`, adjacency mmap/heap `0`, `adjacency_scratch_decode/search=104` |
| open searcher full-doc | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithDocumentsV4` | 102,448 | 9,761 | 95,045 | 345 | same vector/adjacency counters as no-doc; includes final document fetch/reconstruction |
| open searcher projected-doc | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderWithDocumentsExcludeEmbedding1875` | 35,936 | 27,827 | 25,120 | 308 | same vector/adjacency counters as no-doc; excludes embedding from final document fetch |

Storage/padding evidence from `TestTypedColumnDirectViewWriterStorageAccounting1895`
and the segment/appender alignment tests:

| Fixture | Type | Rows | Dims | Image bytes | Layout-contract bytes | In-image padding | Row-record padding | Segment prefix padding | Total padding | Direct certified | Fallback/deferred |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `bool_bitpack_rle` | `bool` | 3 | 0 | 2372 | 691 | 20 | 0 | 0 | 20 | false | fallback=true, deferred=false |
| `int64_raw_fixed_width` | `int64` | 3 | 0 | 3472 | 693 | 32 | 0 | 7 | 39 | true | fallback=false, deferred=false |
| `float32_native_raw_fixed_width` | `float32` | 3 | 0 | 2404 | 697 | 26 | 0 | 7 | 33 | true | fallback=false, deferred=false |
| `double_native_raw_fixed_width` | `double` | 3 | 0 | 2408 | 696 | 20 | 0 | 7 | 27 | true | fallback=false, deferred=false |
| `string_dictionary_codes` | `string` | 3 | 0 | 2567 | 693 | 15 | 0 | 0 | 15 | false | fallback=true, deferred=false |
| `float32_vector_fixed_dim` | `float32_vector` | 3 | 3 | 2452 | 706 | 28 | 0 | 7 | 35 | true | fallback=false, deferred=false |
| `adjacency_deferred` | `adjacency_list` | 3 | 2 | 2440 | 706 | 28 | 0 | 0 | 28 | false | fallback=true, deferred=true |
