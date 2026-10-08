Online-vacuum v2 implementation handoff

Exact ZERO online-vacuum/source edits under grant v2. Investigation only; all local read commands joined. No Go, formatting, Git, remote operation, compilation or tests. All 10077 worktree source files match accepted phase2 checkpoint02. No partial/unformatted vacuum code exists to commit. The prior accepted dirty composition remains intact.

Current unfinished production behavior

* vacuum_online.go still unlinks stale files before raw pager.Open, constructs replacement with raw newIndexGen, and uses void local cleanup that ignores physical Close/unlink errors. Runtime/selection/handoff resource aliases can survive while that cleanup closes their Pager.
* Current error defers discard resource/runtime/handoff failures rather than anchoring a retryable exact replacement owner on DB. Rename uncertainty needs recovery-file preservation; poison is not completion.
* DB Close has no replacement-invocation admission/join or actual pending-replacement retry role.
* index_constructor_closure_v1_test.go still installs no immediate consumed-flag cleanup for its registered/acquired old-reader edge before the next fallible constructor. No fixture correction was applied.

Concrete next batch, subject to ROOT source resumption

1. Preborn fixed replacement custody in actual DB, charged by constructor DB class, retain SAME original constructor Scope before stale unlink/Open. Preserve exact partial Open owner. Replace generation constructor with newConstructorIndexGen before children/binding.
2. Transfer exact selected resources, runtime, recovery handoff, generation/Pager and namespace phase into that real custody. One checked cleanup sequence retains failed fields/cursors; stop/join and resource callbacks outside all inherited gates. Successful installation transfers roles once and retains old generation until last real reader.
3. Join only actual invocation in DB Close with maintenance-shared admission and explicit same-operation reentrant Busy refusal; never wait for unresolved debt. Close retries actual remaining custody before master/LOCK release.
4. Namespace phase records every rename attempt/result; ambiguous rename preserves files and exact physical owners for checked recovery. No optimistic unlink/close or highwater rollback.
5. Fix exact early-fatal reader cleanup; add public held-reader swap, owner/strict prebirth refusal, partial Open/Close, runtime/resource Pending, rename ambiguity, concurrent/reentrant Close and retry tests; update the three granted owning docs.

Two causal scope decisions required before claiming complete lifetime integration

* compact_storage.go holds maintenanceMu from lines420-422 and calls compactStorageVacuumIndexOnline with lockMaintenance=false (calls at663/762; delegate873). Thus cleanup cannot merely unlock the lock owned by vacuum without also preserving the remaining CompactStorage invocation against concurrent Close. Decide exact caller-wide admission handoff or grant a minimal CompactStorage caller change BEFORE editing that outside-grant path.
* recoverable_root_set.go Release lines695-724 marks Released first, ignores resource Set.Release and Snapshot.Close errors and clears fields. Vacuum's deferred recoverableRoots.Release therefore cannot acknowledge checked cleanup completion or retain that capability's actual remaining debt. Decide a checked actual-capability release/custody seam BEFORE editing that outside-grant path; do not treat the current void release as a cleanup certificate.

Finite/native public admission remains CLOSED; full 128MiB fit, hidden constructors/platform backing and performance remain uncertified. No next-step design here is implemented or qualified.
