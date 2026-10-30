package rootpublication

import (
	"errors"
	"io"
	"math"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/stableio"
)

// OwnedPreparedReadRange is a private, partially accounted exact-handle read
// capability. It is not a namespace, sync, publication, immutable-content,
// cut, lineage, or recovery certificate. In particular, a pin does not stop
// the original producer from truncating its own file. A caller must hold the
// original source set throughout construction and must not concurrently
// release or transfer it, including by releasing escaped tokens.
//
// The caller supplies a live unpaired enrollment. Construction acquires an
// independently admitted edge to its owner and budget; that edge survives
// closure of the supplied enrollment and remains held in failure custody.
// The enrollment must be bound to an independently admitted budget; retaining
// an edge does not establish
// that the budget protects the original producer's progress. Original registry
// state retained by the pin remains charged
// to its original governing owners. The caller must separately account for
// source lookup, registry growth, producer progress and invocation identity.
// Its fixed charge does not bound syscall, stat, identity, transient name or
// error allocations, and therefore is not an end-to-end budget admission.
// ReadAt and Close serialize with one another; never copy this object.
type OwnedPreparedReadRange struct {
	mu         sync.Mutex
	self       *OwnedPreparedReadRange
	file       *os.File
	pin        *IdentityPin
	owner      *retainedalloc.Owner
	enrollment *retainedalloc.Enrollment
	custody    *OriginalCleanupCustody
	charge     uint64
	start      int64
	length     int64
	closed     bool
	failure    error
	duplicate  stableio.DuplicateCustody
}

// CaptureOwnedPreparedReadRange captures one already-certified logical range
// from a generic source set and its exact pinned handle. It exposes no handle
// or publication methods. It uses an independent file duplicate and extends
// the existing pin without creating a new producer observation. A 4096-byte
// maximum source handle name supplies a finite descriptor admission bound.
// Source construction and this function remain caller-serialized; this API
// does not admit or authorize a graph, and is not connected to one.
func CaptureOwnedPreparedReadRange(owner *retainedalloc.Owner, enrollment *retainedalloc.Enrollment, registry *IdentityPinRegistry, source *StableResourceSet, selector StableResourceSelector) (*OwnedPreparedReadRange, error) {
	if owner == nil || enrollment == nil || registry == nil || source == nil || owner == registry.MetadataOwner() ||
		selector.Kind == "" || selector.LogicalLane == "" || selector.ResourceID == "" || selector.PhysicalGeneration == 0 ||
		selector.Obligation.Offset < 0 || selector.Obligation.Length <= 0 || selector.Obligation.Offset > math.MaxInt64-selector.Obligation.Length {
		return nil, ErrResourceOwnership
	}
	for _, s := range []string{string(selector.Kind), selector.LogicalLane, selector.ResourceID, selector.Obligation.Class, selector.Obligation.Kind, selector.Obligation.Namespace, string(selector.Obligation.Reachability)} {
		if len(s) > 4096 {
			return nil, ErrResourceOwnership
		}
	}
	switch selector.Obligation.Reachability {
	case ReachabilityColumnManifest, ReachabilityTypedColumnMultipart, ReachabilityTypedColumnValue, ReachabilityTypedColumnCode, ReachabilityHNSWSearchPack:
	default:
		return nil, ErrResourceExcluded
	}
	ownedEnrollment, err := enrollment.RetainForOwner(owner)
	if err != nil {
		return nil, err
	}
	// Admission happens before source.mu. The fixed name cap covers the
	// duplicate-handle name independently of a borrowed token string.
	fileCharge, err := StableFileMetadataCharge(4096 + uint64(len("#stable-pin")))
	if err != nil {
		ownedEnrollment.Close()
		return nil, err
	}
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(OwnedPreparedReadRange{})))
	for _, n := range []uint64{retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(IdentityPin{}))), fileCharge} {
		if n > math.MaxUint64-charge {
			ownedEnrollment.Close()
			return nil, retainedalloc.ErrCapacity
		}
		charge += n
	}
	if err := owner.AddPending(charge); err != nil {
		ownedEnrollment.Close()
		return nil, err
	}
	reader := &OwnedPreparedReadRange{owner: owner, enrollment: ownedEnrollment, charge: charge, start: selector.Obligation.Offset, length: selector.Obligation.Length}
	reader.self = reader
	reader.custody, err = NewOriginalCleanupCustody(registry, owner)
	if err != nil {
		owner.RemovePending(charge)
		ownedEnrollment.Close()
		return nil, err
	}
	fail := func(cause error) (*OwnedPreparedReadRange, error) {
		if cleanupErr := reader.CloseWithError(); cleanupErr != nil {
			return nil, errors.Join(cause, cleanupErr)
		}
		return nil, cause
	}
	key := stableLogicalResourceKey{kind: selector.Kind, lane: selector.LogicalLane, resourceID: selector.ResourceID, generation: selector.PhysicalGeneration}
	source.mu.Lock()
	if source.selected || source.physicalOnly || source.Owner() == ResourceOwnerReleased || source.Owner() == ResourceOwnerTransferred {
		source.mu.Unlock()
		return fail(ErrResourceOwnership)
	}
	// The generic traversal helper may allocate a sorted kind slice and an
	// unbounded index stack. Use bounded searches for this optional capture.
	const maxReadRangeSearch = 64
	var entry *stableResourceEntry
	if source.kindViews != nil {
		if source.kindViews.allocation != nil || len(source.kindViews.cells) > maxReadRangeSearch {
			source.mu.Unlock()
			return fail(ErrResourceOwnership)
		}
		var root *stableResourceLogicalIndexNode
		seenKind := false
		for _, cell := range source.kindViews.cells {
			if cell.kind == selector.Kind {
				if seenKind {
					source.mu.Unlock()
					return fail(ErrResourceOwnership)
				}
				seenKind = true
				root = cell.view.logical
			}
		}
		for steps := 0; root != nil; steps++ {
			if steps == maxReadRangeSearch {
				source.mu.Unlock()
				return fail(ErrResourceOwnership)
			}
			if key == root.key {
				entry = root.entry
				break
			}
			if stableLogicalResourceKeyLess(key, root.key) {
				root = root.left
			} else {
				root = root.right
			}
		}
	} else {
		if len(source.entries) > maxReadRangeSearch {
			source.mu.Unlock()
			return fail(ErrResourceOwnership)
		}
		for i := range source.entries {
			candidate := &source.entries[i]
			if token := activeEntryToken(*candidate); token != nil && token.logicalKey() == key {
				if entry != nil {
					source.mu.Unlock()
					return fail(ErrResourceOwnership)
				}
				entry = candidate
			}
		}
	}
	if entry == nil {
		source.mu.Unlock()
		return fail(ErrUnresolvedResource)
	}
	view := entry.logicalObligations
	if view.directory != nil || view.ownedRemoved != nil || len(view.removed) != 0 {
		source.mu.Unlock()
		return fail(ErrResourceOwnership)
	}
	var obligation StableLogicalObligation
	found := false
	logicalKey := stableLogicalObligationKey(selector.Obligation)
	for node, steps := view.index, 0; node != nil; steps++ {
		if steps == maxReadRangeSearch {
			source.mu.Unlock()
			return fail(ErrResourceOwnership)
		}
		if logicalKey == node.key {
			obligation, found = node.obligation, true
			break
		}
		if stableLogicalObligationIndexLess(logicalKey, node.key) {
			node = node.left
		} else {
			node = node.right
		}
	}
	token := activeEntryToken(*entry)
	if !found || obligation != selector.Obligation || token == nil || token.released.Load() || token.pinned == nil ||
		token.identityPin == nil || token.identityPin.registry != registry || token.identityPin.identity.Platform != runtime.GOOS || len(token.pinned.Name()) > 4096 ||
		uint64(selector.Obligation.Offset+selector.Obligation.Length) > entry.frontier.Bytes {
		source.mu.Unlock()
		return fail(ErrUnresolvedResource)
	}
	reader.pin, err = token.identityPin.clone()
	if err == nil {
		// Replace a potentially borrowed platform string with the static Go
		// platform constant before the original source can be released.
		reader.pin.identity.Platform = runtime.GOOS
		reader.file, err = stableio.DuplicateFileWithCustody(token.pinned, token.pinned.Name()+"#stable-pin", &reader.duplicate)
	}
	source.mu.Unlock()
	if err != nil {
		if err == &reader.duplicate && !reader.duplicate.Ambiguous() {
			// The error otherwise points into reader and would keep the
			// refunded descriptor alive after successful cleanup.
			err = errors.Join(reader.duplicate.OperationError(), reader.duplicate.ControlError(), reader.duplicate.CleanupError())
			if err == nil {
				err = ErrResourceOwnership
			}
		}
		return fail(err)
	}
	identity, err := StableIdentityFromFile(reader.file)
	if err != nil || !SamePhysicalIdentity(identity, reader.pin.identity) {
		if err == nil {
			err = ErrResourceConflict
		}
		return fail(err)
	}
	stat, err := reader.file.Stat()
	if err != nil {
		return fail(err)
	}
	if stat.Size() < selector.Obligation.Offset+selector.Obligation.Length {
		return fail(ErrFrontierBeyondResource)
	}
	return reader, nil
}

// ReadAt uses an offset relative to the captured range and never returns data
// outside it. The consumer remains responsible for the canonical checksum.
func (reader *OwnedPreparedReadRange) ReadAt(dst []byte, offset int64) (int, error) {
	if reader == nil || reader.self != reader {
		return 0, ErrResourceOwnership
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed || reader.file == nil {
		return 0, ErrResourceOwnership
	}
	if offset < 0 || offset > reader.length || uint64(len(dst)) > uint64(reader.length-offset) {
		return 0, io.EOF
	}
	return reader.file.ReadAt(dst, reader.start+offset)
}

// CloseWithError makes one cleanup attempt. A failed duplicate-handle or pin
// cleanup transfers this exact object and its charge to preborn registry
// custody; a repeated call reports the original failure without retrying.
func (reader *OwnedPreparedReadRange) CloseWithError() error {
	if reader == nil {
		return nil
	}
	if reader.self != reader {
		return ErrResourceOwnership
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return reader.failure
	}
	reader.closed = true
	if reader.duplicate.Ambiguous() {
		// The OS may have consumed the raw descriptor despite the close
		// error; retain the exact one-shot record and pin, never retry it.
		reader.failure = &reader.duplicate
		reader.custody.Complete(reader, reader.failure)
		reader.custody = nil
		return reader.failure
	}
	if reader.file != nil {
		if err := reader.file.Close(); err != nil {
			reader.failure = err
			reader.custody.Complete(reader, err)
			reader.custody = nil
			return err
		}
		reader.file = nil
	}
	if reader.pin != nil {
		reader.pin.Release()
		if reader.pin.releaseError != nil {
			reader.failure = reader.pin.releaseError
			reader.custody.Complete(reader, reader.failure)
			reader.custody = nil
			return reader.failure
		}
		reader.pin = nil
	}
	reader.custody.Complete(nil, nil)
	reader.custody = nil
	reader.owner.RemovePending(reader.charge)
	reader.owner, reader.charge = nil, 0
	enrollment := reader.enrollment
	reader.enrollment = nil
	enrollment.Close()
	return nil
}
