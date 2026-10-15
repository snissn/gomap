package rootpublication

import (
	"fmt"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// EmptyDependencyManifestWithWorkV6 enters the original selected manifest
// constructor only after proving its actual fixed empty representation. The
// constructor's allocation remains under the SAME selected resource owner.
func EmptyDependencyManifestWithWorkV6(set *StableResourceSet, w *iterator.OrdinalScanWork) (*DependencyManifestV1, bool, error) {
	if !set.HasEmptyAdmittedDependencyClosureV6() || w == nil {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(4, 2*uint64(unsafe.Sizeof(DependencyManifestV1{}))+4*uint64(unsafe.Sizeof(resourceAllocation{}))+128) {
		return nil, false, nil
	}
	manifest, _, err := set.DependencyManifestV1()
	return manifest, err == nil, err
}

// HasEmptyAdmittedDependencyClosureV6 checks the actual immutable representation,
// not a caller-supplied resource maximum. Nonempty closures require the original
// retained resource derivation/validation and are deliberately excluded here.
func (set *StableResourceSet) HasEmptyAdmittedDependencyClosureV6() bool {
	if set == nil {
		return false
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	owner := set.Owner()
	if !set.selected || set.metadata == nil || set.physicalOnly || owner == ResourceOwnerReleased || owner == ResourceOwnerTransferred || len(set.entries) != 0 || len(set.pinHighWater) != 0 || set.kindViews != nil && len(set.kindViews.cells) != 0 {
		return false
	}
	if set.extras != nil {
		if len(set.extras.physical) != 0 || len(set.extras.logicalMembershipEvidence) != 0 {
			return false
		}
		if d := set.extras.empty; d != nil && (d.ref.PhysicalCount != 0 || d.ref.LogicalCount != 0) {
			return false
		}
	}
	return true
}

// A genuinely absent ordinary closure has no resource edge to import. Allocate
// its selected empty descriptor with the SAME maintained builder/import path;
// the caller owns the resulting original Set until binding or finite abort.
func PrepareEmptyStableResourceSetWithOrdinalWorkV6(owner *retainedalloc.Owner, source *StableResourceSet, w *iterator.OrdinalScanWork) (*StableResourceSet, bool, error) {
	if w == nil || owner == nil {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if source != nil {
		if !source.HasEmptyAdmittedDependencyClosureV6() {
			return nil, false, source.EmptyClosureRefusalV6()
		}
		return CloneAdmittedStableResourceSetWithOrdinalWorkV6(source, w)
	}
	bytes := 2*uint64(unsafe.Sizeof(StableResourceSetBuilder{})) + 2*uint64(unsafe.Sizeof(StableResourceSet{})) + 2*uint64(unsafe.Sizeof(resourceKindBacking{})) + 6*uint64(unsafe.Sizeof(resourceAllocation{}))
	if !w.Reserve(6, bytes) {
		return nil, false, nil
	}
	set, err := ImportStableResourceSetMetadata(owner, nil)
	return set, err == nil, err
}

func (set *StableResourceSet) EmptyClosureRefusalV6() error {
	if set == nil {
		return fmt.Errorf("%w: absent original resource descriptor", ErrNativeMVCCRootWorkRequiredV6)
	}
	set.mu.Lock()
	defer set.mu.Unlock()
	cells, physical, evidence := 0, 0, 0
	if set.kindViews != nil {
		cells = len(set.kindViews.cells)
	}
	if set.extras != nil {
		physical = len(set.extras.physical)
		evidence = len(set.extras.logicalMembershipEvidence)
	}
	return fmt.Errorf("%w: resource closure selected=%v admitted=%v entries=%d kinds=%d pins=%d physicalDirectories=%d evidenceKinds=%d", ErrNativeMVCCRootWorkRequiredV6, set.selected, set.metadata != nil, len(set.entries), cells, len(set.pinHighWater), physical, evidence)
}

// NewPreparedPrimaryEmptyCandidateWithWorkV6 enters the SAME maintained
// constructor after admitting its real fixed geometry. No opaque namespace/file
// validation is reachable for this actual empty closure. Nonempty resource and
// explicit-obligation shapes need their retained validator before this seam.
func NewPreparedPrimaryEmptyCandidateWithWorkV6(spec CandidateSpec, w *iterator.OrdinalScanWork) (*PreparedRootCandidate, bool, error) {
	if w == nil || spec.DurableRoot == nil || !spec.DurableRoot.RequiresNativeCallbackWorkV6() || len(spec.Obligations) != 0 || !spec.ResourceSet.HasEmptyAdmittedDependencyClosureV6() {
		return nil, false, ErrNativeMVCCRootWorkRequiredV6
	}
	if !w.Reserve(3, 2*uint64(unsafe.Sizeof(spec))+2*uint64(unsafe.Sizeof(PreparedRootCandidate{}))+2*uint64(unsafe.Sizeof(DurableRootTransaction{}))) {
		return nil, false, nil
	}
	candidate, err := NewPreparedRootCandidate(spec)
	return candidate, err == nil, err
}
