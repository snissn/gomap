//go:build darwin || linux || freebsd || netbsd || openbsd

package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func requireStableColumnAuthorityTest(t *testing.T) {
	t.Helper()
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable column authority requires exact relative namespace support")
	}
}

func TestStableColumnAssetCreatesThroughCapturedParentAndSyncsOnce(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAssetCreatesThroughCapturedParentAndSyncsOnce(t)
}

func TestStableColumnAssetExistingUnknownNamespaceStabilizesThroughCapturedParent(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAssetExistingUnknownNamespaceStabilizesThroughCapturedParent(t)
}

func TestStableColumnAssetCreatedFailureRetainsOrphanAndRemainsRetryable(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAssetCreatedFailureRetainsOrphanAndRemainsRetryable(t)
}

func TestStableColumnAssetCaptureFailureInvalidatesPathSyncCache(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAssetCaptureFailureInvalidatesPathSyncCache(t)
}

func TestStableColumnAssetCaptureFailureResourcePlateau(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAssetCaptureFailureResourcePlateau(t)
}

func TestStableColumnAssetTokensCoalesceCreationNamespaceInEitherOrder(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAssetTokensCoalesceCreationNamespaceInEitherOrder(t)
}

func TestStableColumnAssetTokenBindsExactSegmentAndRange(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAssetTokenBindsExactSegmentAndRange(t)
}

func TestStableColumnAppendSessionReturnsCoalescedPinnedAuthority(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAppendSessionReturnsCoalescedPinnedAuthority(t)
}

func TestStableColumnAppendSessionNamespaceSyncProofTracksExactIdentity(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAppendSessionNamespaceSyncProofTracksExactIdentity(t)
}

func TestStableColumnAppendSessionFailureReleasesPinsAndNamespaceProof(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnAppendSessionFailureReleasesPinsAndNamespaceProof(t)
}

func TestColumnCommandWALPublishRetainsStableAssetAuthorityInDurableSlots(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnCommandWALPublishRetainsStableAssetAuthorityInDurableSlots(t)
}

func TestColumnAssetStableDeletePreservesReboundEntry(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnAssetStableDeletePreservesReboundEntry(t)
}

func TestStableColumnConstructionPinBlocksCrossManagerGC(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnConstructionPinBlocksCrossManagerGC(t)
}

func TestStableColumnConstructionRejectsUnlinkBeforeObserve(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testStableColumnConstructionRejectsUnlinkBeforeObserve(t)
}

func TestColumnAssetGCRejectsParentDirectoryRebindFromPlan(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnAssetGCRejectsParentDirectoryRebindFromPlan(t)
}

func TestColumnAssetGCRejectsChildRebindFromPlan(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnAssetGCRejectsChildRebindFromPlan(t)
}

func TestColumnAssetGCRejectsCompletedCrossManagerPublicationAfterPlan(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnAssetGCRejectsCompletedCrossManagerPublicationAfterPlan(t)
}

func TestColumnAssetGCRejectsAppendedCandidateFrontierAfterPlan(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnAssetGCRejectsAppendedCandidateFrontierAfterPlan(t)
}

func TestColumnAssetGCRejectsCommitAdvanceWithUnchangedCandidateFrontier(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnAssetGCRejectsCommitAdvanceWithUnchangedCandidateFrontier(t)
}

func TestColumnPublishStableAbandonPreservesSameSizeReboundSegment(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnPublishStableAbandonPreservesSameSizeReboundSegment(t)
}

func TestColumnStoreCompactStableAbandonPreservesSameSizeReboundSegment(t *testing.T) {
	requireStableColumnAuthorityTest(t)
	testColumnStoreCompactStableAbandonPreservesSameSizeReboundSegment(t)
}

func BenchmarkStableCentralColumnAppendSessionAuthority(b *testing.B) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		b.Skip("stable column authority requires exact relative namespace support")
	}
	benchmarkStableCentralColumnAppendSessionAuthority(b)
}
