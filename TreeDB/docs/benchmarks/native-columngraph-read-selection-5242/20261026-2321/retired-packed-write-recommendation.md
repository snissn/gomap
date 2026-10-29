# Retired packed-norm writer result: source-bound disposition

**Recommendation:** remove the historical packed-norm +19.85% result as an unconditional current-source causal blocker. Keep the result FAILED / EXPERIMENT RETIRED, with all original rows and causal diagnostics intact. It is not passed, explained as noise, or evidence of current write performance. The actual current native/public write gate remains required.

Source-only inspection at base HEAD `99e71bbca7e01d429fe5704d5ea954a575636173` plus existing dirty/staged construction. No source mutation or runtime was performed. Exact reviewed on-disk hashes are in source-proof.json; no whole-dirty-tree identity is claimed.

## What actually failed

Entrypoint: `TreeDB/collections/text_v2_contract_bench_test.go:134`, selected `BenchmarkTextV2ContractWritePaths2623/insert_batch_text_v2_indexed` (185–213). Two fresh Go processes, before then after, fixed100x/GOMAXPROCS4, each operation inserts256 documents into a freshly created scalar tenant + V2 text title/body collection, with positions/offsets disabled. Only public Collection.InsertBatch is timed. Open/Create/index setup, storage statistics, high-DF inspection and Close are outside the timed interval. The fixture ignores DB.Close errors; do not upgrade this old diagnostic into cleanup qualification.

The durable/request premise needs correction: `openTextV2ContractDB2623` (1380) calls `backenddb.Open(Options{Dir: tb.TempDir()})` directly. It selects neither a public TreeDB durability profile nor a caching/native bridge or command-WAL request. `NewCollectionManager` (1127, api.go1738/1760) retains the legacy manager when legacy producer admission succeeds. This is the raw-backend-default **legacy collection** fixture, not native/durable service4096/fixed128. Existing contract-correction.md already records this distinction.

The original samples bind clean source551f4c5eb55df18366cd5ed88c2eb6c35bd9e8ad, tree5aae6400080770c3b44b09f8847020fe874ea57e. BEFORE overlays exactly the23e778845b1e7bd7ea27e15fe1ef2080aabf6e69 storage/search files; AFTER uses551 packed-norm writer. The receipts are not an original-main-versus-native pair. They run `go test`; unprofiled temporary test ELFs were not separately retained with ELF digests. Do not relabel the later profiled executables as those measured binaries.

The full original raw results are `evidence/native-dense-norm-codec1/write-before1.log` and `write-after1.log`; their original process/source/overlay/cleanup bindings are adjacent `*-receipt.json`. Checksums match their receipts. `evidence/native-dense-norm-write-cost1/outcomes.json` extracts the full rows: 4,048,225→4,851,628ns/op (+803,403ns, +19.85%), 3,638,363→3,610,660B/op, 21,653allocs unchanged. Both report256docs,601posting blocks,2norm blocks and other equal work counters; packed norm bytes/doc5.641→2.285. Nothing here replaces those numbers.

## The decisive current-source proof

Current whole `text_v2_storage.go` is byte-identical to the original BEFORE23e source: SHA256 `1d010ebc864dc980fbb8f829d0fa6c2f40b31aaba46afe1b4e169495ddae8b55`. The failed AFTER551 version was `11f22ae651ef938bfc3fc74fc481162643ccfda08772748239a2aa5b77a265ad`. Current `encodeTextV2NormBlockValue` (566) is the original V1 clone/sort/varint writer with no packed preflight/selection. `text_v2_dense_norm.go` is absent. Retirement decision/patch is `evidence/packed-norm-retirement1/` (all seven codec/reader/test/docs paths retired).

Current whole reducer `text_maintenance.go`, the unchanged benchmark harness, and `internal/memtable/cow_tree.go` are also byte-identical to23e, verified separately against551. Therefore the observed two-arm writer mechanism no longer differs on current source. This proves removal of that experimental delta, not equivalence of every current collection/runtime instruction or a timing pass.

Both historical551 samples already contained raw refinements4a84fb3c06c5647927d21a88d032714b696b2935 and7a18a3968d5db659dc29860de853239e55ac291f; cow_tree.go is identical BEFORE/AFTER/current (`e0743c9f1a7a7ea3e208546523fb306c54caf67437fb8ebe9db6e37f6bebc0e0`). The fixture directly opens backend DB rather than caching, so its public InsertBatch does not exercise COWWriter's cached raw incremental fallback. That refinement is neither the varying treatment nor a newly available rescue for this observation. Its separate actual-COW cost evidence remains separately owned.

## Existing causal evidence and selected next action

One existing fixed CPU pair already exists in native-dense-norm-write-cost1/: before.cpu.pprof/after.cpu.pprof, rendered before/after-insert-top.txt and profiles-manifest.json. The recorded profiled ELF digests are3b9c9831… (before) and86950d94… (after), bound to clean4ba5bb0ec4968d7990b6fd3cdbb1bd49f8546690 (production-equivalent551) plus the same23e overlay. These are causal diagnostics, not the unprofiled rows. InsertBatch-ancestry CPU420→490ms, unchanged textMutationNewStateAndAnalysis70→180ms; encoder has no observed samples. Setup/stats/Close, calibration and detached GC complicate whole-profile denominators. That evidence did not establish why the old single wall pair regressed; it supplies no safe writer patch.

Select **the structural retirement proof above**, not another profile of removed code. The small reproducible read-only check is:

```sh
git -C /mnt/fast4tb/gomap-mongo-execution-20261024/consumer-canonical diff --exit-code 23e778845b1e7bd7ea27e15fe1ef2080aabf6e69 -- TreeDB/collections/text_v2_storage.go TreeDB/collections/text_maintenance.go TreeDB/collections/text_v2_contract_bench_test.go TreeDB/internal/memtable/cow_tree.go
```

The source/hash comparison already established this result; no execution is requested. No new profile or favorable timing repeat is proposed to “clear” +19.85%. Root should update its causal gate/disposition to retired-source and retain the original negative. Before current feature promotion, separately freeze its actual current native/legacy public write contract and finite order/count/acceptance on the selected immutable source; this old raw-backend fixture cannot stand in for durable/current native InsertBatch authority or its production-cost qualification. That future actual feature gate is not satisfied by codec retirement, projection-reuse gains, or this artifact.
