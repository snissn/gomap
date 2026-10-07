# M0 source applicability: f2c93cfc → 3182e15d

The new physical manifest-revision GC and its bounded-FD repeated-directory-scan schedule are excluded by the selected M0 fixture options. The broader f2c-to3182 production delta is not excluded: public vacuum reaches changed current-root resource capture. Eleven of the14 prior M0 bindings match and three differ. Source applicability alone proves neither binary equivalence nor stable performance. The one matched control and one candidate packet both failed unchanged noise qualification, so no cross-head ratio or causal regression/infrastructure attribution is accepted.

This persists the completed audit. It does not rerun or reinterpret any capture. The original hosted and matched FAILs remain FAIL/INCONCLUSIVE.

Control source: `f2c93cfcdf5f7ef54f6d7bc4ff9e6946fb21a72c`. Candidate source: `3182e15dfe1aa11120db9d309ad0d590283398d6`. Git objects read from `/tmp/gomap-r1-5067`.

## Exact fourteen-path bindings

| Path | Control Git blob | Candidate Git blob | Same |
| --- | --- | --- | --- |
| .github/workflows/treedb-vacuum-m0.yml | `b0ea24e0bf4d8a67ad39ab1668a39fcc69525508` | `b0ea24e0bf4d8a67ad39ab1668a39fcc69525508` | yes |
| scripts/treedb_vacuum_m0_capture.sh | `99e5a145e1a7ecf748b87ed957484b8214f6f8d9` | `99e5a145e1a7ecf748b87ed957484b8214f6f8d9` | yes |
| scripts/treedb_vacuum_m0_summarize.py | `431d8add7518e2195e5c5c73d2475d5862d2cf3f` | `431d8add7518e2195e5c5c73d2475d5862d2cf3f` | yes |
| scripts/treedb_vacuum_m0_summarize_test.py | `7a00d325f5eb035ad97670112457ee0c525de47e` | `7a00d325f5eb035ad97670112457ee0c525de47e` | yes |
| TreeDB/db/vacuum_collection_bench_test.go | `c5347bb64a3900d1a051576b93fc395db8912039` | `c5347bb64a3900d1a051576b93fc395db8912039` | yes |
| TreeDB/db/vacuum_collection_latency_external_bench_test.go | `62aa1ac379bbf2d20a0b83e5df9c0333bf68de53` | `62aa1ac379bbf2d20a0b83e5df9c0333bf68de53` | yes |
| TreeDB/db/vacuum_m0_fixture_test.go | `9c16343a393f95943e17f124f6d6c9ad7be4a7bb` | `9c16343a393f95943e17f124f6d6c9ad7be4a7bb` | yes |
| TreeDB/db/vacuum_online.go | `70d30583f9354b8284cf0303a90fa7627e92bd06` | `d90483b2bb34b394570f967857b0c5f8dae3b380` | no |
| TreeDB/db/leaf_generation_gc.go | `4559626cfefcacf4ead036eec69e98df49ea0c0a` | `5c961ae4f23666e519b08307c2869adbb148df7a` | no |
| TreeDB/db/db.go | `07c79f324245d3e89b742a85ecb81bf1940c9de2` | `07c79f324245d3e89b742a85ecb81bf1940c9de2` | yes |
| TreeDB/db/durable_root_runtime.go | `0a0ecefc05f8160dedb946a46d5835471c2d5b05` | `d92107d88821c59497a19e3aa816b5e5ca1fbb8c` | no |
| TreeDB/db/recoverable_root_set.go | `f47b5a0ce33f26e963d00a2db349d3738d875c17` | `f47b5a0ce33f26e963d00a2db349d3738d875c17` | yes |
| TreeDB/db/compact_storage.go | `e3b6328291b63e026d9bf68449cb067ce7b0b69e` | `e3b6328291b63e026d9bf68449cb067ce7b0b69e` | yes |
| TreeDB/db/vlog_rewrite.go | `43860615915a14752fb6aea084d4fb4e5b8a9b8c` | `43860615915a14752fb6aea084d4fb4e5b8a9b8c` | yes |

The JSON additionally records each object byte size and SHA256, exact per-path diff SHA256, the32-path whole delta, and bindings for additional changed production Go inputs. The previous095→852 all14-equal conclusion belongs to that pair alone.

## Fixture and executed branches

- [scripts/treedb_vacuum_m0_capture.sh:44](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/scripts/treedb_vacuum_m0_capture.sh#L44): Selected legacy and external public bytes_64x benchmarks; each sample fresh Go process, -benchtime=1x -count=1; ten interleaved pairs at line70.
- [TreeDB/db/vacuum_collection_bench_test.go:352](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_collection_bench_test.go#L352): Legacy fixture opens fresh TempDir with PointerThreshold4096 and no outer-leaf option; seeds1024 values and publishes OrderedRootStoragePagerLeaves.
- [TreeDB/db/vacuum_collection_bench_test.go:210](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_collection_bench_test.go#L210): Legacy fixture explicitly drains/stops root-publication coordinator and releases recovery handoff before the benchmark timer.
- [TreeDB/db/vacuum_collection_bench_test.go:112](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_collection_bench_test.go#L112): Legacy foreground timer starts after setup; b.StopTimer at177 precedes deferred DB.Close; small foreground Set values at260 are below threshold.
- [TreeDB/db/vacuum_collection_latency_external_bench_test.go:106](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_collection_latency_external_bench_test.go#L106): Public fixture opens fresh TempDir with PointerThreshold4096 and no outer-leaf option; seeds1024 JSON rows of selected valueSize1024.
- [TreeDB/db/vacuum_collection_latency_external_bench_test.go:29](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_collection_latency_external_bench_test.go#L29): Public timer starts after setup; b.StopTimer at55 precedes deferred DB.Close; public foreground Set values at218 are below threshold.
- [TreeDB/db/vacuum_collection_latency_external_bench_test.go:176](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_collection_latency_external_bench_test.go#L176): Timed public worker calls DB.VacuumIndexOnline;160 fixed foreground operations per round.
- [TreeDB/db/db.go:1324](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/db.go#L1324): IndexOuterLeavesInValueLog bool defaults false when omitted; assigned directly to db at2283, and zipper at2344.
- [TreeDB/db/leaf_generation_gc.go:61](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/leaf_generation_gc.go#L61): False indexOuterLeavesInValueLog returns before any leaf-generation/revision GC scan; gcLeafManifestRevisions call is later at88.
- [TreeDB/db/vacuum_online.go:1309](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_online.go#L1309): Public current-root capture calls changed projection/fallback helper with projectCurrentPacked=true; timer records capture duration.
- [TreeDB/db/durable_root_runtime.go:1220](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/durable_root_runtime.go#L1220): Current public capture executes new source.PhysicalDescriptors() pack check even for a no-pack source; cannot label all changed code unexercised.
- [TreeDB/db/durable_root_runtime.go:1305](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/durable_root_runtime.go#L1305): If exact fallback is selected, new identity maps and all inherited packed-descriptor validation are reached. No-pack fixtures do not enter pack-selection branch at1343.
- [TreeDB/db/vacuum_online.go:1737](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/vacuum_online.go#L1737): Older recovery-root reconstruction, if needed, calls changed helper with projectCurrentPacked=false; additional pack scan condition is skipped there.
- [TreeDB/db/wal_recovery.go:1256](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/db/wal_recovery.go#L1256): New retireRegisteredCreationMetadataLocked is called only after successful produced-pointer registration; fixtures use inline seed/churn sizes and pager leaves, excluding the identified large-value/outer-leaf hot path, not proving universal appender nonexecution.
- [TreeDB/internal/valuelog/manager.go:1758](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/internal/valuelog/manager.go#L1758): Changed Manager.Close now stops/joins retry workers; DB teardown calls vm.Close at db.go:3028. Fixture teardown follows StopTimer; source changes can still affect binary/layout or scheduling.
- [TreeDB/internal/rootpublication/resource_selector.go:1](https://github.com/snissn/gomap/blob/3182e15dfe1aa11120db9d309ad0d590283398d6/TreeDB/internal/rootpublication/resource_selector.go#L1): New exact physical-kind selector is consumed only by the pack-containing branch described at durable_root_runtime.go:1343; no packed resources configured in these fixtures.

The repeated revision-GC directory scan and its O(N²/16) maintenance cost are excluded for these fixtures. This does not exclude all production changes. Public current-root capture adds a descriptor census even when the fixture has no packs; its fallback may also reach changed map/identity validation. Older-root capture uses the changed helper with current packed selection disabled. I found no source-backed correctness defect or measured attribution in this bounded audit, and cannot exclude candidate public-vacuum overhead or indirect build/layout/scheduling effects.

## Preserved diagnostic disposition

The existing matched capture ran exactly one unmodified control followed by one unmodified candidate, each ten interleaved legacy/public samples. Existing analyzer results are copied below without recalculating or accepting cross-head ratios.

| Source | Vacuum-total CV | Max-writer-pause CV | Foreground-p99 CV | Existing noise gate |
| --- | ---: | ---: | ---: | --- |
| control | 11.14059484% | 17.74559793% | 42.73248721% | FAIL |
| candidate | 10.26783383% | 5.06418514% | 43.98511532% | FAIL |

Both packets retain zero-abort legacy completion and explicit production-index-vacuum-available classification. Both fail timing qualification. They establish neither a candidate regression nor its exclusion, and supply no causal infrastructure diagnosis. No accepted performance ratio is derived.

Root reports the [explicit auxiliary INCONCLUSIVE/nonblocking disposition](https://github.com/snissn/gomap/pull/5071#issuecomment-6015235219) under the inspected current required-gate policy. This is conceptually consistent when original FAILs remain visible, thresholds/results stay unchanged, the actual required TreeDB code gate and formal PR gates pass, and D/E finite-cost qualification remains open. This audit is not CI acceptance authority or a gate waiver.

No new run, retry, threshold change, outlier removal or evidence promotion is recommended. No Go/build/capture/GitHub action was performed. Original evidence and readiness handoff remain unchanged.

Original evidence byte bindings and the complete detailed handoff are in the adjacent JSON.
