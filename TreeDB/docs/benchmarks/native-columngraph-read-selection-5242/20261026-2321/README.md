# Native ColumnGraph read selection, PR #5242

These are bounded mechanism-selection measurements, not final default or G1 qualification. Every planned row, including unfavorable timings, is retained. Each comparison used four fresh processes in fixed before/after/after/before order, exactly1000 timed requests per query, zero retries, Go1.26.0 and GOMAXPROCS=4 on the same host. Source and binary hashes, exact argv/environment policy, activation and process cleanup appear in the adjacent plans and receipts. Setup, durable4096-document insertion in fixed128 batches and Optimize are outside query timing; query-time filtered results and final-document fetch remain inside it.

| Selected mechanism | Query | Median ns/op before → after | Median B/op before → after | Objects before → after |
| --- | --- | --- | --- | --- |
| Native forward cursor + selected-entry materialization | filtered exact Dense | 24852945.5 → 10536119.5 (-57.606%) | 7306650.5 → 7282342 (-0.333%) | 107050 → 102955 |
| Operation-local projection storage reuse | Keyword | 754571 → 756619 (+0.271%) | 301549.5 → 219230 (-27.299%) | 1888 → 1883 |
| Operation-local projection storage reuse | Hybrid | 824256 → 795035.5 (-3.545%) | 429340.5 → 347025.5 (-19.172%) | 1921 → 1916 |

Both compare native implementations under unchanged command_wal_durable, cow_btree, omitted cosine ColumnGraph, original corpus/filter/top-K/result assertions and work limits. Dense has no candidate/scanned-row counter: those values remain unavailable. Keyword retains640 postings/op; Hybrid retains1024 postings/op and10 fetched documents/op in all four cells. Algorithms, posting traversal and ranking remain unchanged. The projection before-ELF is the scan after-ELF; these are sequential experiments, not a simultaneous three-arm comparison.

The harness is `TreeDB/documentservice/shared_cow_consumers_bench_test.go` SHA256cc27ffef2e22d1e9c5aa0dc34c79990e6d204d02b27d555b1b8b7ec1357822a7. Reproduction uses the exact ELF argv in each receipt, with GOMAP_SERVICE_BENCH_BACKEND=native and GOMAP_SERVICE_BENCH_PROFILE=command_wal_durable. The equivalent source commands are:

```sh
GOMAXPROCS=4 GOMAP_SERVICE_BENCH_BACKEND=native GOMAP_SERVICE_BENCH_PROFILE=command_wal_durable go test ./TreeDB/documentservice -run '^$' -bench '^BenchmarkServiceSharedCOWConsumers/dense$' -benchmem -benchtime=1000x -count=1 -v
GOMAXPROCS=4 GOMAP_SERVICE_BENCH_BACKEND=native GOMAP_SERVICE_BENCH_PROFILE=command_wal_durable go test ./TreeDB/documentservice -run '^$' -bench '^BenchmarkServiceSharedCOWConsumers/(keyword|hybrid)$' -benchmem -benchtime=1000x -count=1 -v
```

The harness's legacy cow_btree selector resolves through backendOnly to mutable btree. A legacy/native comparison therefore measures different publication/read architectures; it cannot isolate COW cost. Neither comparison above uses that legacy arm. Earlier16-row collection lacked parent activation logs and remains rejected; later controls do not retroactively qualify it.

Correctness evidence includes the unchanged original4096/fixed128 native+legacy complete lifecycle, cold concurrent search-owner reuse, successful Optimize→immediate process exit→exact acknowledged graph reopen, projection ownership/admission/authentication race checks, and affected tree/root-reader/native text DDL/rewrite/evaluator/corruption/native-HNSW owning controls. The last owning selection passed57 top-level tests (176 test/subtest pass actions) across four packages. A native mutation triggers exactly one successful single-term fallback; exhaustive same-cut ranks/scores agree. Removing only immediate abandoned-cursor Close causes the targeted test to refuse a second projection allocation (RED); selected code passes (GREEN). This is process-exit recovery, not power-loss qualification.

The complete preprojection collections/documentservice invocation exited1:7285 functional pass entries,11 skips and one failing source-name inventory test. Git's index included a deleted packed-norm file. Staging the selected deletion and classifying22 actual changed/new public-API references repaired that inventory; all67 naming tests pass. Preserve the original exit1; do not claim a full successful invocation.

A separate exact four-path raw-refinement removal passes ordinary ColumnGraph4096 and selected text/finite-owner controls, but required explicit native_runtime4096 Optimize fails. A logging-only diagnostic unwraps immutable memtable capacity exhausted. Thus that capability still needs the refinement. PR #5244's strict PRIMARY/aggregate allocation failure remains an external acceptance hold under #5191; this result grants no waiver or retry.

The old +19.85% raw-backend write result belongs to the removed packed-codec experiment. It remains FAILED / EXPERIMENT RETIRED, with exact source disposition attached. Current feature durable/native writes, query/default performance, final five-sample G1, latest-head CI/review and all actual review threads remain unqualified. The selected source snapshot differs from the projection A/B only by the new targeted regression test; production is identical. Full local artifacts remain under /mnt/fast4tb/gomap-mongo-execution-20261024/evidence/ and the rolling parent checkpoint https://github.com/snissn/gomap/issues/5225#issuecomment-6392971639.
