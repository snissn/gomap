//go:build darwin || linux || freebsd || netbsd || openbsd

package rootpublication

import "testing"

func TestStableResourceTokenSyncUsesPinnedIdentityAfterRenameRecreate(t *testing.T) {
	testStableResourceTokenSyncUsesPinnedIdentityAfterRenameRecreate(t)
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
