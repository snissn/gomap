# Typed-Column List Adjacency Benchmark Closeout (#1990)

Status: benchmark/profile evidence for the #1982 vector-index adjacency stack. This note compares the legacy graph-specific adjacency-source path against the corrected vector-index state `uint32_list` path over physical `raw_uint32_offsets_list`. It does not claim SIMD/vector-scoring wins and does not optimize legacy `adjacency_list` / `adjacency_layout` storage.

## Artifact roots

| Kind | Path |
| --- | --- |
| Old graph-specific small profile | `/tmp/treedb_search_profile_20260528_125524/` |
| Old graph-specific production profile | `/tmp/treedb_search_profile_prod_20260528_125618/` |
| Corrected typed-list full matrix/profile run | `/tmp/treedb_1990_after_20260529_013352/` |
| Local inventory/smoke files | `.orca/1990-inventory/` |

Current after-run metadata: Apple M3, Darwin arm64, Go `go1.25.5`, branch `snissn/1990-manager`, base/runtime commit `f794aab9fc979e940ab782f9f087426061918aca`. A later benchmark-only edit added state adjacency offsets/value reporting to `BenchmarkColumnGraphRebuildVectorIndexV2A`; the search/profile evidence is unaffected.

## Commands

```sh
go test ./TreeDB/collections ./TreeDB/internal/typeddecode ./TreeDB/internal/typedcolumn \
  -run 'Test.*(VectorIndex|Adjacency|Uint32List|OffsetsList|TypedColumn)' -count=1

RUN_DIR=/tmp/treedb_1990_after_20260529_013352
FINAL_RE='^(BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3|BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnProduction8192V3|BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnV3|BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnProduction8192V3|BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4|BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4|BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4|BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferParallelV4|BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderSetupV6|BenchmarkTypedColumnAdapterUint32ListPrimitive1985|BenchmarkTypedColumnAdapterVariableAdjacencyScan1917|BenchmarkTypedColumnAdjacencyDenseFallbackScan|BenchmarkTypedColumnAdapterUint32OffsetsListDirectReader1916|BenchmarkColumnGraphRebuildVectorIndexV2A|BenchmarkColumnGraphVectorIndexStatusLoadedV2A|BenchmarkVectorIndexStatusV2A|BenchmarkCollectionVectorIndexMetadataCreateDrop|BenchmarkCollectionOpenVectorIndexMetadata)$'

go test ./TreeDB/collections ./TreeDB/internal/typedcolumn \
  -run '^$' -bench "$FINAL_RE" -benchmem -benchtime=3s -count=5 \
  > "$RUN_DIR/bench_after.txt" 2>&1

benchstat "$RUN_DIR/bench_after.txt" > "$RUN_DIR/bench_after_benchstat.txt"

go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3$' \
  -benchmem -benchtime=8s \
  -cpuprofile "$RUN_DIR/search_cpu.pprof" \
  -memprofile "$RUN_DIR/search_mem.pprof" \
  > "$RUN_DIR/search_bench.txt" 2>&1

go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnProduction8192V3$' \
  -benchmem -benchtime=20s \
  -cpuprofile "$RUN_DIR/search_prod_cpu.pprof" \
  > "$RUN_DIR/search_prod_bench.txt" 2>&1

go tool pprof -top "$RUN_DIR/search_cpu.pprof" > "$RUN_DIR/search_cpu_top.txt"
go tool pprof -top "$RUN_DIR/search_prod_cpu.pprof" > "$RUN_DIR/search_prod_cpu_top.txt"
```

Benchmark-only storage-accounting edit validation:

```sh
go test ./TreeDB/collections \
  -run 'TestColumnGraphRebuildVectorIndexPublishesPhysicalManifestV2A|TestColumnVectorIndexStateAdjacency' \
  -count=1

go test ./TreeDB/collections -run '^$' \
  -bench '^BenchmarkColumnGraphRebuildVectorIndexV2A$' \
  -benchmem -benchtime=3s -count=5 \
  > "$RUN_DIR/rebuild_storage_bench_after_edit.txt" 2>&1
```

## Before/after integrated search comparison

The before rows are the old graph-specific adjacency-source path from the issue body/artifacts. The after rows are the corrected vector-index state typed-list path, using the same benchmark names/fixtures/hardware where possible.

| Fixture | Path | Artifact | ns/op | ops/sec | B/op | allocs/op | Adjacency counters | Storage/accounting |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 1024 rows | old graph-specific source | `/tmp/treedb_search_profile_20260528_125524/bench.txt` | 12,542 | 79,732 | 0 | 0 | `adjacency_scratch_decodes/search=104`, `adjacency_direct_views/search=0`, `vector_direct_views/search=164` | `asset_B/row=747.1` |
| 1024 rows | corrected typed-list state | `/tmp/treedb_1990_after_20260529_013352/search_bench.txt` | 11,284 | 88,621 | 0 | 0 | `adjacency_typed_list_direct_views/search=104`, `adjacency_typed_list_mmap_direct/search=104`, `adjacency_scratch_decodes/search=0`, `adjacency_legacy_fallbacks/search=0`, `vector_direct_views/search=164` | `asset_B/row=747.1`; typed-list storage math covered below |
| 8192 rows | old graph-specific source | `/tmp/treedb_search_profile_prod_20260528_125618/bench.txt` | 14,288 | 69,989 | 0 | 0 | `adjacency_scratch_decodes/search=108`, `adjacency_direct_views/search=0`, `vector_direct_views/search=182` | `asset_B/row=747.2` |
| 8192 rows | corrected typed-list state | `/tmp/treedb_1990_after_20260529_013352/search_prod_bench.txt` | 11,884 | 84,147 | 0 | 0 | `adjacency_typed_list_direct_views/search=108`, `adjacency_typed_list_mmap_direct/search=108`, `adjacency_scratch_decodes/search=0`, `adjacency_legacy_fallbacks/search=0`, `vector_direct_views/search=182` | `asset_B/row=747.2`; typed-list storage math covered below |

Conclusion: integrated core search preserves/improves runtime while replacing per-expansion adjacency scratch decodes with direct typed-list offset/value views. The profiled small fixture improves by about 10.0%; the profiled 8192-row fixture improves by about 16.8%.

## Full corrected-path benchmark matrix

Median values from `/tmp/treedb_1990_after_20260529_013352/bench_after_benchstat.txt` (`count=5`; benchstat reports no confidence interval with fewer than 6 samples):

| Coverage | Benchmark | ns/op | ops/sec | B/op | allocs/op | Key counters |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Primitive fallback decode | `BenchmarkTypedColumnAdapterUint32ListPrimitive1985/fallback_decode` | 39,690 | 25,195 | 204,801 | 2 | `rows/op=8192`, `values/op=32761`, `read_bytes/op=196588` |
| Primitive direct open/validation | `BenchmarkTypedColumnAdapterUint32ListPrimitive1985/direct_open_validate` | 46,600 | 21,459 | 7,559 | 52 | `mmap_direct/op=1`, `rows/op=8192`, `values/op=32761` |
| Primitive row-slice direct view | `BenchmarkTypedColumnAdapterUint32ListPrimitive1985/row_slice_access_preopened` | 21,030 | 47,551 | 0 | 0 | `values[offsets[i]:offsets[i+1]]`; `rows/op=8192`, `values/op=32761` |
| Adjacency direct expansion | `BenchmarkTypedColumnAdapterVariableAdjacencyScan1917/mmap_direct_scan_preopened` | 20,890 | 47,870 | 0 | 0 | `adjacency_mmap_direct/op=1`, scratch `0` |
| Adjacency heap-copy typed view | `BenchmarkTypedColumnAdapterVariableAdjacencyScan1917/heap_copy_typed_view_scan_preopened` | 20,870 | 47,916 | 0 | 0 | `adjacency_heap_copy_typed_view/op=1`, scratch `0` |
| Adjacency fallback decode+scan | `BenchmarkTypedColumnAdapterVariableAdjacencyScan1917/scratch_fallback_decode_and_scan` | 60,150 | 16,625 | 204,800 | 2 | `adjacency_scratch_decode/op=1` |
| Core serial search | `BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3` | 10,560 | 94,697 | 0 | 0 | typed-list mmap `104/search`; scratch/legacy `0` |
| Core serial search, 8192 | `BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnProduction8192V3` | 11,630 | 85,985 | 0 | 0 | typed-list mmap `108/search`; scratch/legacy `0` |
| Core parallel search | `BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnV3` | 3,321 | 301,114 | 0 | 0 | typed-list mmap `104/search`; scratch/legacy `0` |
| Core parallel search, 8192 | `BenchmarkColumnVectorGraphNativeSearchCosineParallelTypedColumnProduction8192V3` | 3,507 | 285,144 | 0 | 0 | typed-list mmap `108/search`; scratch/legacy `0` |
| Public searcher | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4` | 12,710 | 78,678 | 784 | 2 | typed-list mmap `104/search`; scratch/legacy `0` |
| Public reusable searcher | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4` | 12,230 | 81,766 | 0 | 0 | typed-list mmap `104/search`; scratch/legacy `0` |
| Public parallel searcher | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderParallelV4` | 3,916 | 255,363 | 784 | 2 | typed-list mmap `104/search`; scratch/legacy `0` |
| Public reusable parallel searcher | `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferParallelV4` | 3,597 | 277,995 | 0 | 0 | typed-list mmap `104/search`; scratch/legacy `0` |
| Searcher open/setup | `BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderSetupV6` | 593,100 | 1,686 | 885 KiB | 937 | open-only; search counters zero |
| Status loaded | `BenchmarkColumnGraphVectorIndexStatusLoadedV2A` | 22,250 | 44,944 | 10.79 KiB | 139 | cheap manifest/ref/layer/schema checks |
| Status empty metadata | `BenchmarkVectorIndexStatusV2A` | 9,895 | 101,061 | 3.94 KiB | 62 | no payload scan |
| Rebuild | `BenchmarkColumnGraphRebuildVectorIndexV2A` | 19,670,000 | 50.85 | 2.11 MiB | 13,861 | state storage below |

## Storage overhead accounting

For v1 `raw_uint32_offsets_list`, storage overhead is explicit offsets bytes plus flattened values bytes:

```text
offsets bytes = (rows + 1) * 8
values bytes  = edges * 4
```

Observed storage counters:

| Benchmark/artifact | Rows | Values/edges | Offsets bytes | Values bytes | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| `BenchmarkTypedColumnAdapterVariableAdjacencyScan1917/*` | 8192 | 32761 | 65,544 | 131,044 | Primitive/adjacency row-slice fixture, `storage_B/op=526172`, padding `28` |
| `BenchmarkColumnGraphRebuildVectorIndexV2A` state layer 0 | 128 | 2048 | 1,032 | 8,192 | `state_layer0_offsets_B/op`, `state_layer0_values_B/op` |
| `BenchmarkColumnGraphRebuildVectorIndexV2A` state layer 1 | 128 | 176 | 1,032 | 704 | `state_layer1_offsets_B/op`, `state_layer1_values_B/op` |
| `BenchmarkColumnGraphRebuildVectorIndexV2A` state total | 128/layer | 2224 | 2,064 | 8,896 | two `hnsw/layer/<n>` adjacency assets, `state_adjacency_assets_B/op=24992` |

The benchmark helper validates each reported state adjacency asset as role `adjacency`, asset ID `hnsw/layer/<n>`, logical type `uint32_list`, physical encoding `raw_uint32_offsets_list`, and offset section length `(rows+1)*8` before reporting bytes.

## CPU profile summary

Old small profile (`/tmp/treedb_search_profile_20260528_125524/top.txt`) had adjacency retrieval as the top flat hotspot:

- `(*columnVectorGraphBlockView).adjacency`: 1.32s flat, 19.27%; 1.41s cumulative, 20.58%.
- `rawCandidateAdjacencyWithDirectView`: 1.49s cumulative, 21.75%.

Corrected small profile (`/tmp/treedb_1990_after_20260529_013352/search_cpu_top.txt`) no longer shows `columnVectorGraphBlockView.adjacency` as the decode hotspot. The top flat costs are search/scoring and SIMD dot product:

- `SearchCosine`: 2.14s flat, 21.62%; 8.11s cumulative.
- `dotProductFloat32NEON`: 2.02s flat, 20.40%.
- typed-list adjacency source calls are smaller: `(*columnVectorGraphLayer0AdjacencyDirectSource).Neighbors` 0.19s flat / 0.21s cumulative; `(*columnVectorGraphAdjacencyDirectSources).Neighbors` 0.08s flat / 0.29s cumulative.

Old production profile (`/tmp/treedb_search_profile_prod_20260528_125618/top.txt`) still showed `(*columnVectorGraphBlockView).adjacency` at 2.19s flat / 10.18%. Corrected production (`search_prod_cpu_top.txt`) shifts the main flat cost to scoring/SIMD; typed-list adjacency source calls remain small relative to scoring.

## Interpretation

Healthy search consumes TVIS adjacency assets directly: vector-index state role `adjacency`, asset IDs `hnsw/layer/<n>`, logical `uint32_list`, physical `raw_uint32_offsets_list`. The corrected path removes per-search adjacency scratch decodes (`0/search`) and row-image legacy fallback (`0/search`) while preserving zero allocation core search. Full payload validation remains on open/rebuild/integrity paths; `VectorIndexStatus` remains a cheap manifest/ref/layer/schema status check.
