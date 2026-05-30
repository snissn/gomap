# Search-native graph benchmark closeout (#1970)

Status: benchmark/threshold closeout for issue #1970.

This closeout captures integrated search-native benchmark evidence on the current typed-column vector-index state topology and records default decisions.

## Scope and guardrails

- No new format/kernel implementation in #1970.
- Default enablement decisions require integrated benchmark evidence.
- Indexed scoring from #1969 remains default-off unless integrated evidence proves a clear win on the current fallback backend.
- Current healthy search state target remains:
  - adjacency: TVIS `uint32_list` / `raw_uint32_offsets_list`
  - norms: TVIS `raw_float32`
  - row refs: TVIS `int64` / `raw_int64`
  - vectors: typed-column `float32_vector`

## Commits and artifact set

- Baseline commit: `c92f50d77b015087be1873b5210efc1e83eb9897`
- Candidate commit: `bb76ecc93555a6a8f94de55e9a36ac7d0227a342`
- Worktree branch: `snissn/1970-manager`
- Hardware/env capture: `artifacts/1970/hardware_env.txt`
- Benchmark-name verification: `artifacts/1970/benchmark_name_check.txt`

Main outputs:

- `artifacts/1970/bench_baseline_c92f50d77.txt`
- `artifacts/1970/bench_candidate_bb76ecc.txt`
- `artifacts/1970/benchstat_baseline_vs_candidate.txt`
- `artifacts/1970/benchmark_runtime_summary.md`
- `artifacts/1970/counter_extract.md`
- `artifacts/1970/cpu_core_typed_candidate_top.txt`
- `artifacts/1970/mem_core_typed_candidate_top.txt`
- `artifacts/1970/cpu_public_docs_candidate_top.txt`
- `artifacts/1970/mem_public_docs_candidate_top.txt`

## Focused matrix and runtime summary

Median runtime summary from `artifacts/1970/benchmark_runtime_summary.md`:

| Benchmark | baseline ns/op | candidate ns/op | delta |
| --- | ---: | ---: | ---: |
| `BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnV3` | 12,825 | 13,052 | +1.77% |
| `BenchmarkColumnVectorGraphNativeSearchCosineTypedColumnProduction8192V3` | 14,538 | 14,480 | -0.40% |
| `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderV4` | 12,938 | 15,232 | +17.73% |
| `BenchmarkOpenVectorIndexSearcherColumnGraphTypedColumnNativeReaderReusableBufferV4` | 12,800 | 15,540 | +21.41% |
| `BenchmarkSearchVectorIndexColumnGraphNativeReaderWithDocumentsV4` | 1,436,424 | 1,965,647 | +36.84% |
| `BenchmarkOpenVectorIndexSearcherColumnGraphNativeReaderSetupV6` | 914,056 | 1,356,669 | +48.42% |
| `BenchmarkColumnGraphRebuildVectorIndexV2A` | 20,300,682 | 23,315,928 | +14.85% |
| `BenchmarkVectorIndexStatusV2A` | 9,851 | 11,414 | +15.87% |

Allocation and storage notes:

- Search core typed and production rows remained zero-allocation (`B/op=0`, `allocs/op=0`).
- Rebuild storage counters remained stable (`graph_total_storage_B/op=102,938`; layer offsets/values unchanged).

## Counter highlights

From `artifacts/1970/counter_extract.md` and candidate benchmark rows:

- Adjacency typed-list direct path remained active:
  - `adjacency_typed_list_mmap_direct/search` non-zero on search benches.
- Legacy/scratch fallbacks remained clean on healthy search paths:
  - `adjacency_scratch_decodes/search=0`
  - `adjacency_legacy_fallbacks/search=0`
  - `norm_source_fallbacks/search=0`
- Row-ref state counters remained active where expected:
  - `row_ref_vector_source_state/search=1` (public typed searcher benches)
  - `row_ref_vector_source_legacy_graph_ids/search=0`
  - `row_ref_state_result_refs/search=10`
- Document path used row-ref state and no row-ref lookup fallback:
  - `doc_row_ref_state_fetches/search=10`
  - `doc_row_ref_lookup_fallbacks/search=0`
- Result IDs still sourced from graph row-id fallback path:
  - `result_id_graph_fallbacks/search=10`

## Indexed scoring decision (#1969)

Candidate-only indexed scoring microbench (same candidate commit):

- `BenchmarkVectorSearchReusableBufferSerialTypedColumnIndexedScoring1969/scalar`: median `17,833 ns/op`
- `BenchmarkVectorSearchReusableBufferSerialTypedColumnIndexedScoring1969/indexed`: median `15,671 ns/op`

However, the integrated baseline-vs-candidate matrix in this closeout does not show a net integrated search-path win sufficient to justify default-on indexed scoring for current fallback backend conditions. Historical #1969 evidence also documented fallback-backend regression risk.

Decision: keep indexed scoring default-off.

## Benchmark closeout conclusion

1. Healthy typed-state search counters remain in the intended state (typed-list adjacency direct, norm fallback zero, row-ref state active, legacy row-ref source zero).
2. Candidate shows material regressions in open/public/docs/setup/rebuild lanes versus baseline commit in this matrix.
3. No new default-on path is enabled in #1970 from this evidence set.
4. Indexed scoring remains default-off.

## Explicit remaining debt

This closeout does not claim complete topology debt elimination. The following remain explicit and deferred:

- #2010: result identity / row-id topology debt
- #2013: result ID source and graph-row-ID fallback retirement
- #2014: row payload compatibility debt retirement

`result_id_graph_fallbacks/search` remaining non-zero is expected until #2010/#2013 are completed.
