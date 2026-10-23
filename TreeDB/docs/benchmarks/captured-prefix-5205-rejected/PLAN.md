Predeclared component comparison, 2026-10-23 UTC

Product baseline: 1c794e2a1358a573c7f55fbbf60acde5c7a75e8d (main after harness-only5211; runtime unchanged fromfc3c).
Candidate: 47862f151e78841cbca4161eeffbbcc41331d472 (captured-prefix engine).
Reviewed/landed harness: main1c794e2a, TreeDB/command_wal_parallel_sync_bench_test.go, Gitblob ba2aa3c9c1697a8508470f26fffe0f530f280d91. Identical harness overlay on candidate; no runtime edits.

Five alternating baseline/candidate pairs, pair1 B/C,2 C/B,3 B/C,4 C/B,5 B/C. All12 cells: btree and cow_ordered × Inline64 and forcedPointer4096 × G1,4,16. Fixed128x, GOMAXPROCS4, test.cpu4, one process at a time; no profiling in measured rows. Every raw stdout/stderr retained, errors/nulls not replaced. Workers hold all local builds/tests/server campaigns during collection. Builds finish before quiet check; VM/process/I/O context retained.

Report median time, throughput, B/op, allocs/op, per-call p50/95/99, physical WAL and value syncs, grouped participants. No synthetic grouping delay. Interpret speed and synchronization counts jointly. Singleton/allocation regression is a blocker requiring attribution; five-percent timing is an initial materiality bound, not waiver of existing gate. Concurrent improvement must exceed noise and preserve singleton behavior. This component comparison is engine viability evidence only; no Dgraph/RPC, sustained resource, crash/power-loss or final product qualification claim. PRIMARY and whole-owning-suite gates remain separate.

Infrastructure: local workstation, persistent shared task Go build/module caches and task fast-disk artifacts; no dedicated high-capacity benchmark runner provisioned. No machine isolation from external users is guaranteed; host observations determine limits.
