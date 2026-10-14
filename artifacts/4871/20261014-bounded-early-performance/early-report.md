Bounded early performance inquiry; not product or final performance acceptance.

Baseline: 3f87d74b98e8b1e809d151611be03785f85e2283. Candidate: c3ba82a5ef6d6590f6c70734045c8d640c4c1255. ALL3184 producer source census matched after the two attributed diagnostic-test copies; production and prerequisites are published. Existing landed benchmark sources identical across matched sources. No new Go harness or source change.

Isolated .185, CPU0, GOMAXPROCS=1, Go1.26.3/CGO1, AB/BA two pairs, 200ms or Batch100x. Four candidate-only COW rows at16x. All21 invocations exited0, total15.3405 seconds wall. Raw receipts contain exact argv/env, source and binary hashes, sample counts and process snapshots. Modest swap activity and low sample count limit time inference. Initial mixed GOROOT failure retained; corrected builds allgreen.

Two concrete candidate concerns under the landed raw gate:
- GetVersioned: paired time +46.14%; baseline0B/0alloc, candidate112B/1alloc perop in both samples.
- BatchWrite: paired time +4.40%; baseline median152790.5B, candidate155006B (+2215.5B,+1.45%),281->286alloc.
- Snapshot seek, repeated iterator and durable ACK showed no crossing in this smoke. Durable ACK actual1append/1WALsync/1file-sync perop and0checkpoint. Time shifts are observational, not statistical passes.

Landed checker thresholds: time5%; nonzero B increase bounded by smaller of1% or64B; zeroB strict; no added allocations. Checker exited1 and reports GetVersioned and BatchWrite failures. Final full qualification remains unrun.

Candidate COW current-cost diagnostics only (main has no equivalent enabledPRIMARY): Inline64 CaptureReadRelease944.5ns/464B/4alloc; IncrementalWriteSync4.990923ms/10784B/71alloc. Pointer4096 CaptureReadRelease28.938us/43800B/17alloc; IncrementalWriteSync5.797791ms/555268B/117alloc. Actual mode/profile and entry-placement asserted by landed harness. Final-close lines allzero charges/cuts/sources/views/leases, with nonzero peaks. Fixture total publication/sync counters include seed; setup and teardown excluded timer. These rows do not accept default/full Dgraph/native behavior.

Source-only read-path followup: selector ./TreeDB/db ^BenchmarkGetVersioned$, conditional_kv_contract_bench_test.go:12. Seeds10000 keys with100B inline values, reuses dst capacity100. DB.GetVersionedAppend (api.go:553, unchanged) -> acquireOneShotReadOrErr (pools.go:45) -> captureSnapshotWithValueLogPublicationLockHeld -> tree.GetVersionedAppend -> deferred oneShotRead.close -> Snapshot.Close/finalizeCloseIfUnreferenced. Candidate adds unconditional Manager snapshot wrapper borrow/release. ReleaseOriginal (manager_wrapper_work.go:86) creates local OrdinalScanWork and passes &w through releaseStep/RemovalWork.ContinueWork/Owner.shrinkStepLocked, reaching interface WorkLease.ResizeWork(...,w). OrdinalScanWork has112-byte amd64 layout (three pointers, six uint64s,40-byte ChildRef), matching added112B/1alloc. This is a source-only escape hypothesis, not compiler/profile proof. No rerun, profiling, or implementation performed. Preserve original cleanup semantics when native investigates a synchronous no-work path or scratch lifetime.

All six measured binaries, driver, raw failure/noise logs and exact source/harness reconstruction retained. Only released task-owned checkouts/repo/fresh temporary DB roots may be deleted with verified zero consumers; cleanup receipt follows.
