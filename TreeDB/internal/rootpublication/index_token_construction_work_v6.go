package rootpublication

import (
	"os"
	"strings"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

// StableIndexTokenConstructionWorkV6 is continuation storage on the original
// producer. It constructs the SAME admitted token and invokes the shared final
// initializer; no file pool, publication authority or replacement token exists.
type StableIndexTokenConstructionWorkV6 struct {
	token       *StableResourceToken
	spec        StableResourceSpec
	phase       uint8
	stringIndex uint8
}

func (s *StableIndexTokenConstructionWorkV6) Step(spec StableResourceSpec, w *iterator.OrdinalScanWork) (*StableResourceToken, bool, error) {
	if s == nil || w == nil {
		return nil, false, retainedalloc.ErrWorkRequired
	}
	for {
		switch s.phase {
		case 0:
			if !w.Reserve(2, 2*uint64(unsafe.Sizeof(spec))) {
				return nil, false, nil
			}
			if spec.MetadataOwner == nil || spec.OwnedOperations == nil || spec.OwnedOperations.MetadataOwner() != spec.MetadataOwner || spec.Namespace == nil || !spec.Namespace.metadataScoped || spec.Namespace.metadata != spec.MetadataOwner || spec.PinRegistry == nil || spec.File == nil || spec.Generation == 0 || spec.Kind != ResourceIndex || spec.Reachability != ReachabilityIndexFile || spec.OnRelease != nil || spec.FlushThrough != nil || spec.SyncThrough != nil || spec.OriginalObservation != nil || spec.StableIdentityOverride != (StableIdentity{}) || len(spec.LogicalObligations) != 0 || spec.Frontier.exactRIDs != nil || spec.Frontier.MaxRID != 0 || spec.Frontier.MaxLSN != 0 || spec.Frontier.RIDCount != 0 || spec.Frontier.RIDSetDigest != ([32]byte{}) || spec.physicalBacking != nil || spec.ContentSynced {
				return nil, false, retainedalloc.ErrWorkRequired
			}
			if spec.Kind == "" || spec.ResourceID == "" {
				return nil, false, ErrUnresolvedResource
			}
			if err := spec.Namespace.BindCleanupRegistry(spec.PinRegistry); err != nil {
				return nil, false, err
			}
			if _, ok := spec.OwnedOperations.(StableResourceOwnedOperationsWithWorkV6); !ok {
				return nil, false, retainedalloc.ErrWorkRequired
			}
			if err := validateDiagnosticPath(spec.DiagnosticPath); err != nil {
				return nil, false, err
			}
			if err := validateDurableFrontier(spec.Frontier); err != nil {
				return nil, false, err
			}
			// These are the original production policy checks. Account for both
			// actual inventory traversals before the shared constructor boundary.
			visited := uint64(0)
			for i := 0; i < len(canonicalReachabilityRequirements); i++ {
				if !w.Reserve(1, uint64(unsafe.Sizeof(canonicalReachabilityRequirements[0]))) {
					return nil, false, nil
				}
				requirement := canonicalReachabilityRequirements[i]
				visited++
				if requirement.Field == spec.Reachability {
					break
				}
			}
			if !w.Reserve(visited, visited*uint64(unsafe.Sizeof(canonicalReachabilityRequirements[0]))) {
				return nil, false, nil
			}
			policy, ok := StableResourcePolicyFor(spec.Reachability)
			if !ok || !policy.Registerable || policy.Producer != StableProducerDB || policy.Kind != spec.Kind || policy.Classification != "authoritative" {
				return nil, false, ErrResourceConflict
			}
			s.spec, s.phase = spec, 1
		case 1:
			charge, err := ownedTokenMetadataCharge(s.spec)
			if err != nil {
				return nil, false, err
			}
			fileBytes, err := StableFileMetadataCharge(uint64(len(s.spec.File.Name()) + len("#stable-pin")))
			if err != nil {
				return nil, false, err
			}
			// Original token/allocation, empty obligation/allocation and pinned
			// backing headers, plus their fixed pre-handle failure unwind.
			bytes := 2*(uint64(unsafe.Sizeof(StableResourceToken{}))+3*uint64(unsafe.Sizeof(resourceAllocation{}))+uint64(unsafe.Sizeof(resourceObligationBacking{}))+uint64(unsafe.Sizeof(resourcePinnedBacking{}))) + 2*fileBytes
			if !w.Reserve(12, bytes) {
				return nil, false, nil
			}
			// The SAME constructor immediately owns each successful allocation.
			// Later refusal/failure retains that partial token for its original
			// work-bearing disposer; no synchronous error refund is hidden here.
			if s.token == nil {
				a, err := newResourceAllocationWithWorkV6(s.spec.MetadataOwner, charge, w)
				if err != nil {
					return nil, false, err
				}
				s.token = &StableResourceToken{metadata: a}
				s.token.owner.Store(uint32(ResourceOwnerToken))
			}
			if s.token.obligationBacking == nil {
				// This native constructor already rejected every nonempty
				// logical obligation. Use the original empty backing shape.
				a, err := newResourceAllocationWithWorkV6(s.spec.MetadataOwner, retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(resourceObligationBacking{}))), w)
				if err != nil {
					return nil, false, err
				}
				s.token.obligationBacking = &resourceObligationBacking{allocation: a}
			}
			if s.token.physicalBacking == nil {
				physical, err := newResourcePinnedBackingWithWorkV6(s.spec.MetadataOwner, s.spec.PinRegistry, s.spec.File, false, w)
				if err != nil {
					return nil, false, err
				}
				s.token.physicalBacking = physical
			}
			s.phase = 2
		case 2:
			if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s.token))) {
				return nil, false, nil
			}
			if err := s.spec.OwnedOperations.Retain(); err != nil {
				return nil, false, err
			}
			s.token.ownedOperations = s.spec.OwnedOperations
			s.phase = 3
		case 3:
			// The original six owned descriptor strings are copied individually;
			// cancellation retains their token allocation through actual scrub.
			for s.stringIndex < 6 {
				var source string
				switch s.stringIndex {
				case 0:
					source = string(s.spec.Kind)
				case 1:
					source = s.spec.LogicalLane
				case 2:
					source = s.spec.ResourceID
				case 3:
					source = s.spec.DiagnosticPath
				case 4:
					source = string(s.spec.Reachability)
				case 5:
					source = s.spec.StableIdentityOverride.Platform
				}
				if !w.Reserve(1, 2*uint64(len(source))+2*uint64(unsafe.Sizeof(source))) {
					return nil, false, nil
				}
				copied := strings.Clone(source)
				switch s.stringIndex {
				case 0:
					s.spec.Kind = ResourceKind(copied)
					s.token.kind = s.spec.Kind
				case 1:
					s.spec.LogicalLane = copied
					s.token.logicalLane = copied
				case 2:
					s.spec.ResourceID = copied
					s.token.resourceID = copied
				case 3:
					s.spec.DiagnosticPath = copied
					s.token.diagnosticPath = copied
				case 4:
					s.spec.Reachability = ReachabilityField(copied)
					s.token.reachability = s.spec.Reachability
				case 5:
					s.spec.StableIdentityOverride.Platform = copied
				}
				s.stringIndex++
			}
			s.phase = 4
		case 4:
			fileBytes, err := StableFileMetadataCharge(uint64(len(s.spec.File.Name()) + len("#stable-pin")))
			if err != nil {
				return nil, false, err
			}
			if !w.Reserve(1, 2*fileBytes) {
				return nil, false, nil
			}
			pinned, err := duplicateStableFile(s.spec.File)
			if err != nil {
				return nil, false, err
			}
			s.token.physicalBacking.file = pinned
			s.token.pinned, s.token.pinnedRefs = pinned, &s.token.physicalBacking.refs
			s.phase = 5
		case 5:
			if !w.Reserve(2, 2*uint64(unsafe.Sizeof(StableIdentity{}))+256) {
				return nil, false, nil
			}
			stat, err := s.token.pinned.Stat()
			if err != nil {
				return nil, false, err
			}
			if s.spec.Frontier.Bytes > uint64(stat.Size()) {
				return nil, false, ErrFrontierBeyondResource
			}
			identity, err := stableIdentityFromFile(s.token.pinned)
			if err != nil {
				return nil, false, err
			}
			identity.Generation = s.spec.Generation
			s.token.identity = identity
			s.phase = 6
		case 6:
			pin, ready, err := s.spec.PinRegistry.PinWithWorkV6(s.token.identity, w)
			if !ready || err != nil {
				return nil, false, err
			}
			s.token.identityPin = pin
			s.phase = 7
		case 7:
			if !w.Reserve(3, 2*uint64(unsafe.Sizeof(*s.token))+2*uint64(unsafe.Sizeof(*s.spec.Namespace))) {
				return nil, false, nil
			}
			if err := s.spec.Namespace.validateLinkedResource(s.token.identity); err != nil {
				return nil, false, err
			}
			s.spec.physicalBacking = s.token.physicalBacking
			// Same original final descriptor initializer and producer callbacks.
			flush := func(*os.File, DurableFrontier) error { return nil }
			syncThrough := func(file *os.File, _ DurableFrontier) error { return stableio.SyncFile(file) }
			_, err := completeStableResourceTokenV6(s.spec, s.token.obligationBacking.values, s.token, ResourceMutableAppend, s.token.pinned, s.token.pinnedRefs, s.token.identity, s.token.identityPin, flush, syncThrough)
			if err != nil {
				return nil, false, err
			}
			s.phase = 8
		case 8:
			return s.token, true, nil
		default:
			return nil, false, ErrResourceOwnership
		}
	}
}

// DetachV6 transfers the SAME accepted or partially constructed token to its
// original disposing parent. Clear every borrowed constructor alias first.
func (s *StableIndexTokenConstructionWorkV6) DetachV6(w *iterator.OrdinalScanWork) (*StableResourceToken, bool, error) {
	if s == nil || w == nil {
		return nil, false, retainedalloc.ErrWorkRequired
	}
	if !w.Reserve(1, 2*uint64(unsafe.Sizeof(*s))) {
		return nil, false, nil
	}
	token := s.token
	*s = StableIndexTokenConstructionWorkV6{}
	return token, true, nil
}
