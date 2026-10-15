# Admission callback and capture-fence audit

The a8 hosted ubuntu-latest-3 log stops at TestSnapshotAllocationAdmissionRefusesRefreshedExactView and is later cancelled. That fixture called RefreshValueLogSet synchronously from admission while the existing actual capture held rootReuseMu.RLock; Refresh requires Lock. This is a fixture self-deadlock, not a timeout qualification or a reason to loosen the gate. Root subsequently confirmed macos-latest-3 has the SAME last test and later cancellation (exact log retained). Windows5 remains unattributed without its exact log.

The repaired SAME named test launches a competing actual Refresh, checks that it cannot finish while admission holds the capture fence and that the selected original Set is still exactly singly publication-owned, then refuses admission with ErrSnapshotCapacity. After capture returns, it joins the SAME refresh and checks original refs0/Filesnil, actual replacement view, no registered reader, and a successful fresh retry on current Set. No Set/registry effect is allowed on refusal. The topology capture controls also retain their paused admission window for20ms before proceeding, avoiding an immediate default-only scheduling check. This20ms is a negative blocking observation; no existing operation deadline is extended.

Search: SnapshotAllocationSizes callbacks, AcquireSnapshotWithAllocationAdmission, AcquireStableSnapshotWithAllocationAdmission and BeginNativeMVCCSnapshotV6 across TreeDB production and tests.

| Callback family | Behavior under capture fence |
|---|---|
| db snapshot_allocation_admission_test | Size/capacity assertions, fixed refusal or acceptance, copy selected size metadata. No publication. |
| db bounded_read_owner_test / snapshot_primary_retention_test / primary_owned_promotion_v6_test | Refusal/acceptance only. Writes and physical captures are after callback returns. |
| db durable_root_bench_test cohort admission | Enroll original PRIMARY/registry metadata and acquire wrapper budget. Provider/source capture occurs later. |
| db native_mvcc_snapshot_capture_test | Budget wrapper acquire or flag/counter only. Actual Refresh is after Begin returns. |
| db state_value_log_lifetime_test | Owner assertions, or pause actual capture for competing publisher controlled by external test goroutine. No synchronous publish from callback. |
| caching cow_flush.go and cow_cut.go | Limits check and admitCOWBackendBasis budget/enrollment. Adopt and install occur after backend returns. |
| caching native_mvcc_prune_v6.go | Bounded budget/retention/enrollment effects only; no backend root publication. |
| caching cow_basis_completion_test | Original basis admission/enrollment only; owner Close/adopt/release after return. Test backend wrappers may close returned captures after real capture finishes, outside its callback. |
| caching custom refusing / maintenance / accepted-report backend fixtures | Forward bounded admission, fixed refusal or postcapture test handoff; no synchronous refresh in actual backend admission. |
| public cow_public_retention_test | Copies original metadata owner pointers then deliberately returns refusal. No backend publish. |
| internal/dictdb read_definition.go | Size bounds and external allocation admission/enrollment; owned Snapshot acquisition completes before dictionary reads/Close. External callback inherits the documented same-DB nonreentrant admission boundary. |

API and value-log-lifecycle docs now explicitly state this existing lock contract: callbacks may reserve charges but must not synchronously publish, refresh or close the same DB. No production locking or admission charge changed for this fixture repair.
