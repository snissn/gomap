# Shared cowCutFixture seed-publication boundary

All 15 callers were inspected at HEAD30ee954 before the fixture repair. None requires backend.Set("disk","basis") publication to remain pending. The seed establishes the disk basis; asynchronous private COW-cut publication under test is separate.

| Caller | Tested boundary | Pending seed needed? |
|---|---|---|
| TestCOWCutPrivatePrepareDoesNotBlockOrMixReaders | private COW roots vs captured cut | No |
| TestCOWCutCancelledResourcesDrainOutsideWriter | callback and writer lock ownership | No |
| TestCOWCutPublicationCaptureCloseRace | COW publish/capture/close | No |
| TestCOWCutDBCloseExcludesAdmittedReadAndAllowsDelayedRelease | DB close and delayed capture release, exact final budget | No |
| TestCOWReadPressurePreservesCapacityError | view capacity refusal and disk reads | No |
| TestCOWSnapshotIteratorWrapperAdmissionAndLifetime | exact ExternalBytes and two wrapper leases | No; shares exact-baseline vulnerability |
| TestCOWCursorDeniedWrapperDoesNotAllocate | denied wrapper allocation | No; pending seed work can also contaminate allocations |
| TestCOWBatchCanonicalPreparationPublishesFinalRevisions | cached canonical frame and old/new cuts | No |
| TestCOWBatchFinalizerRefusalCancelsPrivateRoots | cancelled roots and exact external lease baseline | No; shared baseline should be quiescent |
| TestCOWLogicalSizeChangedKeysAndDuplicates | canonical frame logical size | No |
| TestCOWUnsupportedSurfacesRefuseBeforeEffects | unsupported operations leave cut unchanged | No |
| TestCOWRolloverPreservesFrozenPrefixAndLateWrites | COW frozen prefix and later writes | No |
| TestCOWRolloverRefusesBeforeFreeze | capacity refusal preserves writer and cut | No |
| TestCOWDictionaryCleanupPrepareRefusalTransfersOwner | partial dictionary capture/refusal/refund | No; hosted mismatch |
| TestCOWDictionaryCleanupPrivateCutOwnership | private dictionary owner cancellation/publication/refund | No; hosted mismatch |

Chosen boundary: original backend.Checkpoint immediately after original seed Set, before newCOWCache enrolls backend metadata. It waits for the existing publisher; it does not replace dictionary capture/release operations or weaken the exact whole-budget assertions. All original lease, byte, generation and release-count assertions remain.
