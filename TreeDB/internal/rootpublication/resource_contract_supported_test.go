//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import "testing"

func TestStableResourceTokenSyncUsesPinnedIdentityAfterRenameRecreate(t *testing.T) {
	testStableResourceTokenSyncUsesPinnedIdentityAfterRenameRecreate(t)
}

func TestCloneStableResourceSetUsesExactHandlesAndIndependentPins(t *testing.T) {
	testCloneStableResourceSetUsesExactHandlesAndIndependentPins(t)
}

func TestCloneStableResourceSetFiltersExactLogicalObligationClosure(t *testing.T) {
	testCloneStableResourceSetFiltersExactLogicalObligationClosure(t)
}

func TestCloneStableResourceSetReportsPhysicalAndLogicalWorkSeparately(t *testing.T) {
	testCloneStableResourceSetReportsPhysicalAndLogicalWorkSeparately(t)
}

func TestAppendOnlyResourceClosureCloneWorkIsBoundedByMutation(t *testing.T) {
	testAppendOnlyResourceClosureCloneWorkIsBoundedByMutation(t)
}

func TestLogicalObligationRemovalMutationUsesExactFilter(t *testing.T) {
	testLogicalObligationRemovalMutationUsesExactFilter(t)
}

func TestStableResourceSetRejectsDataStableNamespaceUnstable(t *testing.T) {
	testStableResourceSetRejectsDataStableNamespaceUnstable(t)
}

func TestStableNamespacePinsExactLinkedParentAcrossRenameRecreate(t *testing.T) {
	testStableNamespacePinsExactLinkedParentAcrossRenameRecreate(t)
}

func TestStableNamespaceRejectsResourceOutsideExactParent(t *testing.T) {
	testStableNamespaceRejectsResourceOutsideExactParent(t)
}

func TestStableResourceNamespaceRequiresExactLinkedChild(t *testing.T) {
	testStableResourceNamespaceRequiresExactLinkedChild(t)
}

func TestStableResourceNamespaceAcceptsExactLinkedChild(t *testing.T) {
	testStableResourceNamespaceAcceptsExactLinkedChild(t)
}

func TestStableResourceMetricsSeparateFileAndNamespaceOperations(t *testing.T) {
	testStableResourceMetricsSeparateFileAndNamespaceOperations(t)
}
