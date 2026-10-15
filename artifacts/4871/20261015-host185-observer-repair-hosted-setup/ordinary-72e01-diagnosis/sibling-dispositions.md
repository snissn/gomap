# Observer family audit

All30 installers in current TreeDB power-loss test files were inspected. This finite inventory concerns injected-cut/public second-admission classification; it is not a fresh qualification of every witness. No shared production or observer helper changes.

`TreeDB/cow_power_loss_test.go:51` — `func TestCOWPublicModeledStableImageCheckpointAndNoWALVolatileAck(t *testing.T)`

UNCHANGED:no injected cut/classification; exact DB plus mutex and active/drain; completed meta sync vs volatile image retained.

`TreeDB/cow_power_loss_test.go:214` — `func TestCOWPublicModeledInterruptedDependencyAndRootPublicationCuts(t *testing.T)`

REPAIR:36 exact DB/point/resource first completed cut; typed readiness alias permitted only with returned cause latch; image/group/root oracles unchanged.

`TreeDB/db/power_loss_certification_stale_build_test.go:105` — `func runStaleBuildBasePublicReopen(t *testing.T, capsule bool)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/db/power_loss_oracle_page_reuse_test.go:114` — `func TestPowerLossOracleCounterexampleRecoverablePageReuse(t *testing.T)`

UNCHANGED:backend/owned direct explicit-sync reused-page receipt; caller proves actual overwritten page and requires original cause.

`TreeDB/db/power_loss_oracle_poison_test.go:22` — `func TestPowerLossOraclePostMetaFailurePoisonsPublicHandle(t *testing.T)`

UNCHANGED:direct backend SetSync receipt requires BOTH original cut cause and recovery-required; subsequent admission independently requires recovery-required.

`TreeDB/power_loss_certification_api_test.go:99` — `func TestPowerLossCertificationOrdinaryWriteAndBatchPublicReopen(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_certification_api_test.go:190` — `func TestPowerLossCertificationAllExplicitSyncFormsPublicReopen(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_certification_api_test.go:260` — `func TestPowerLossCertificationEmptyExplicitSyncPublicReopen(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_certification_api_test.go:331` — `func TestPowerLossCertificationCleanClosePublicReopen(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_certification_api_test.go:404` — `func TestPowerLossCertificationPublicationFailureClassPublicReopen(t *testing.T)`

UNCHANGED:stage candidate before observer; synchronous checkpoint pre/post-meta witness requires original cause, retryability or poison separately, unchanged.

`TreeDB/power_loss_certification_api_test.go:479` — `func TestPowerLossCertificationDurablePrefixHolePublicReopen(t *testing.T)`

UNCHANGED:observation only (no injected sentinel); unchanged complete command/root hole assertions.

`TreeDB/power_loss_certification_api_test.go:597` — `func TestPowerLossCertificationMaintenanceUnlinkPublicReopen(t *testing.T)`

UNCHANGED:inject exact operation-owned unlink callback; direct maintenance/checkpoint receipt requires original cause AND typed poison.

`TreeDB/power_loss_certification_api_test.go:658` — `func TestPowerLossCertificationCommandWALUnlinkPublicReopen(t *testing.T)`

UNCHANGED:inject exact operation-owned unlink callback; direct maintenance/checkpoint receipt requires original cause AND typed poison.

`TreeDB/power_loss_certification_group_commit_test.go:58` — `func TestPowerLossCertificationGroupedAcknowledgementsPublicReopen(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_certification_group_commit_test.go:169` — `func TestPowerLossCertificationGroupedRotationExternalPublicReopen(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_certification_group_commit_test.go:257` — `func TestPowerLossCertificationGroupedLifecyclePublicReopen(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_certification_group_commit_test.go:418` — `func exercisePowerLossCertificationGroupedFailure(t *testing.T, point durabilitycut.Point, occurrence int) groupedFailureCutResult`

UNCHANGED:injected one actual grouped command-WAL receipt;4 waiters joined before restore; all4 must retain original cause. Later write separately requires typed poison.

`TreeDB/power_loss_certification_layout_test.go:152` — `func testPowerLossCertificationFreshLayout(t *testing.T, testCase freshLayoutNamespaceCase)`

UNCHANGED:exact path/resource/occurrence injected during synchronous Open before handle admission; direct Open cutErr required.

`TreeDB/power_loss_certification_layout_test.go:281` — `func TestPowerLossCertificationProvenLayoutDoesNotRewriteNamespace(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_certification_page_reuse_test.go:74` — `func TestPowerLossCertificationRecoverablePageReusePublicReopen(t *testing.T)`

UNCHANGED:backend/owned direct explicit-sync reused-page receipt; caller proves actual overwritten page and requires original cause.

`TreeDB/power_loss_certification_resources_test.go:99` — `func TestPowerLossCertificationAuthoritativeResourcesPublicReopen(t *testing.T)`

UNCHANGED:observer armed only for terminal checkpoint after authoritative witness preparation; exact main Root/meta sync/phase and original cause required.

`TreeDB/power_loss_oracle_internal_test.go:37` — `func TestPowerLossOracleCounterexampleSourceDeletionBeforeStableCoverage(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_oracle_internal_test.go:62` — `func TestPowerLossOracleCounterexampleSourceDeletionBeforeStableCoverage(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_oracle_test.go:370` — `func TestPowerLossOracleEnumerateCutPoints(t *testing.T)`

UNCHANGED:observation/recovery selection only, never returns injected cut; tracks multiple actual publications, rather than one interrupted cut.

`TreeDB/power_loss_oracle_test.go:570` — `func TestPowerLossOracleCounterexampleNewMetaMissingClosure(t *testing.T)`

UNCHANGED:already exact DB/PRIMARY ResourceMeta first successful Overlay+trace latch and drain;10 selective closure variants retained.

`TreeDB/power_loss_oracle_test.go:921` — `func TestPowerLossOracleAdversarialNewFileNamespaceMismatch(t *testing.T)`

UNCHANGED:actual SetSync new-file namespace receipt; Overlay+trace capture and direct cause, not a later checkpoint observer.

`TreeDB/power_loss_oracle_test.go:1213` — `func TestPowerLossOracleCounterexampleRelaxedCommandFrameMissingRID(t *testing.T)`

UNCHANGED:inject synchronous original command-WAL flush during b.Write; original receipt required before any second checkpoint admission.

`TreeDB/power_loss_oracle_test.go:1390` — `func TestPowerLossOracleCounterexampleChunkedSyncIntermediateRoot(t *testing.T)`

REPAIR:same async checkpoint lineage; exact PRIMARY ResourceMeta/AfterMetaWrite; first complete image latch; callback drain and no borrowed event handles;3 variants unchanged.

`TreeDB/power_loss_oracle_test.go:1623` — `func TestPowerLossOracleProductionOuterLeafAppendCuts(t *testing.T)`

UNCHANGED:observation only; no injected sentinel/interruption classification. Owns existing synchronous/counter/first-success freeze contract; no broadened error acceptance.

`TreeDB/power_loss_oracle_test.go:1816` — `func validateActualCutReopen(t *testing.T, model *powerlossoracle.Model, opts treedb.Options, cut powerlossoracle.CutPoint, occurrence int, readOnly bool, generations []powerlossoracle.Generation, latestSealedSequence uint64, expectedByAppliedLSN map[uint64]map[string]map[string]string, observedCommandFrames []observedPowerLossCommandFrame, durableSequence uint64, durableAcknowledged bool)`

UNCHANGED:observation/recovery selection only, never returns injected cut; tracks multiple actual publications, rather than one interrupted cut.
