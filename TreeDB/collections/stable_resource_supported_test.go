//go:build darwin || linux || freebsd || netbsd || openbsd

package collections

import "testing"

func TestStableColumnAssetCreatesThroughCapturedParentAndSyncsOnce(t *testing.T) {
	testStableColumnAssetCreatesThroughCapturedParentAndSyncsOnce(t)
}

func TestStableColumnAssetExistingUnknownNamespaceStabilizesThroughCapturedParent(t *testing.T) {
	testStableColumnAssetExistingUnknownNamespaceStabilizesThroughCapturedParent(t)
}

func TestStableColumnAssetCreatedFailureRetainsOrphanAndRemainsRetryable(t *testing.T) {
	testStableColumnAssetCreatedFailureRetainsOrphanAndRemainsRetryable(t)
}

func TestStableColumnAssetCaptureFailureInvalidatesPathSyncCache(t *testing.T) {
	testStableColumnAssetCaptureFailureInvalidatesPathSyncCache(t)
}

func TestStableColumnAssetCaptureFailureResourcePlateau(t *testing.T) {
	testStableColumnAssetCaptureFailureResourcePlateau(t)
}

func TestStableColumnAssetTokensCoalesceCreationNamespaceInEitherOrder(t *testing.T) {
	testStableColumnAssetTokensCoalesceCreationNamespaceInEitherOrder(t)
}

func TestStableColumnAssetTokenBindsExactSegmentAndRange(t *testing.T) {
	testStableColumnAssetTokenBindsExactSegmentAndRange(t)
}

func TestStableColumnAppendSessionReturnsCoalescedPinnedAuthority(t *testing.T) {
	testStableColumnAppendSessionReturnsCoalescedPinnedAuthority(t)
}

func TestStableColumnAppendSessionNamespaceSyncProofTracksExactIdentity(t *testing.T) {
	testStableColumnAppendSessionNamespaceSyncProofTracksExactIdentity(t)
}

func TestStableColumnAppendSessionFailureReleasesPinsAndNamespaceProof(t *testing.T) {
	testStableColumnAppendSessionFailureReleasesPinsAndNamespaceProof(t)
}

func TestColumnCommandWALPublishReleasesStableAssetAuthority(t *testing.T) {
	testColumnCommandWALPublishReleasesStableAssetAuthority(t)
}

func TestColumnAssetStableDeletePreservesReboundEntry(t *testing.T) {
	testColumnAssetStableDeletePreservesReboundEntry(t)
}
