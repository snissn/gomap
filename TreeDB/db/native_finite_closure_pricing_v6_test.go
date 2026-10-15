package db

import (
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// Source-layout evidence only. No DB, owner, file, publication or callback is
// constructed. These are containing allocations, not a transitive heap bound.
func TestNativeFiniteOriginalClosureSizesV6(t *testing.T) {
	alloc := retainedalloc.AllocationCharge
	visible := primaryVisibleMetadataV6() + 5*alloc(uint64(unsafe.Sizeof(primaryVisibilityCallbackEnvironmentV6{}))) + alloc(uint64(unsafe.Sizeof(rootPublicationVisibleInstallV1{})))
	for _, row := range []struct {
		name string
		size uintptr
	}{
		{"NativeMVCCCurrentDeleteV6", unsafe.Sizeof(NativeMVCCCurrentDeleteV6{})},
		{"NativePublicationV6", unsafe.Sizeof(rootpublication.NativePublicationV6{})},
		{"rootPublicationVisibleMemberV1", unsafe.Sizeof(rootPublicationVisibleMemberV1{})},
		{"rootPublicationVisibleInstallV1", unsafe.Sizeof(rootPublicationVisibleInstallV1{})},
		{"rootPublicationSealV1", unsafe.Sizeof(rootPublicationSealV1{})},
		{"primaryPublicationSealV5", unsafe.Sizeof(primaryPublicationSealV5{})},
		{"primaryDurableRuntimeV5", unsafe.Sizeof(primaryDurableRuntimeV5{})},
		{"Snapshot", unsafe.Sizeof(Snapshot{})},
		{"DBState", unsafe.Sizeof(DBState{})},
		{"finalizeCommitPost", unsafe.Sizeof(finalizeCommitPost{})},
		{"primaryVisibilityCallbackEnvironmentV6", unsafe.Sizeof(primaryVisibilityCallbackEnvironmentV6{})},
		{"primaryarena.ReleaseQueue", unsafe.Sizeof(primaryarena.ReleaseQueue{})},
		{"nativeAuthorityScopeV6", unsafe.Sizeof(nativeAuthorityScopeV6{})},
	} {
		t.Logf("layout sourceSymbol=%s size=%d allocationCharge=%d", row.name, row.size, alloc(uint64(row.size)))
	}
	t.Logf("containing-admission sourceSymbol=StepNativeMVCCCandidateV6.phase1 member+install+10callbacks+nestedAllocatorEnvironment=%d", visible)
	t.Logf("containing-admission sourceSymbol=primarySealMetadataV6 prefix=1 charge=%d", primarySealMetadataV6(1))
	r, b := nativeAuthorityAdmissionV6(1, 1, 1, 1)
	t.Logf("authority-runtime sourceSymbol=nativeAuthorityAdmissionV6 P=1 D=1 S=1 V=1 records=%d bytes=%d allocator-additional=15", r, b)
}
