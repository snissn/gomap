package caching

import (
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// cowFrontierRegistrationProbe is a test-only, caller-owned diagnostic. The
// caller exclusively installs it before checkpoint, then withdraws its owner
// token and cut edge after checkpoint exits. It is not transferred into the
// physical handoff, and this one slot does not enumerate older frontiers.
type cowFrontierRegistrationProbe struct {
	attempted bool
	token     cowPhysicalLifecycleToken
	err       error
}

// cowPhysicalCutCapture is an unconnected, caller-serialized experiment. It
// owns one independently retained cut edge, not a build, lineage, publication,
// graph or recovery receipt. The caller must clear all aliases before refund;
// its preborn original failure custody retains a failed finish. In particular,
// an empty lifecycle slot says nothing about other frozen or maintenance cuts.
type cowPhysicalCutCapture struct {
	self      *cowPhysicalCutCapture
	cache     *cowCache
	cut       *cowReadCut
	identity  cowPhysicalLifecycleObservation
	retention *memtable.COWSharedRetention
	custody   *rootpublication.OriginalCleanupCustody
	closed    bool
	failure   error
}

// newCOWPhysicalCutCapture admits the complete fixed wrapper before allocating
// it. The allocator must be an independently pre-reserved owner; using normal
// maintenance headroom here is not proof that the predecessor can progress.
// Construct outside writerMu and before any dependent logical acceptance.
func newCOWPhysicalCutCapture(allocator interface {
	AdmitNativeBytes(uint64) (*memtable.COWExternalLease, error)
}, registry *rootpublication.IdentityPinRegistry, metadata *retainedalloc.Owner) (*cowPhysicalCutCapture, error) {
	if registry == nil || metadata == nil {
		return nil, rootpublication.ErrResourceOwnership
	}
	charge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPhysicalCutCapture{})))
	retention, err := memtable.NewCOWSharedRetention(allocator, charge)
	if err != nil {
		return nil, err
	}
	// This is a fresh, exclusive failure edge. It cannot silently have been
	// completed already by another wrapper. The caller must hold and fund the
	// exact registry and metadata owner for the whole capture lifetime.
	custody, err := rootpublication.NewOriginalCleanupCustody(registry, metadata)
	if err != nil {
		retention.Release()
		return nil, err
	}
	owner := &cowPhysicalCutCapture{retention: retention, custody: custody}
	owner.self = owner
	return owner, nil
}

// joinLocked runs under writerMu, with opening independently retained by the
// caller. It transfers the exact extra cut edge returned by the existing join
// into the already-admitted owner. It does not allocate or wait for IO, and it
// never consumes the original physical owner's registration.
func (owner *cowPhysicalCutCapture) joinLocked(c *cowCache, opening *cowReadCut) cowPhysicalLifecycleJoin {
	if owner == nil || owner.self != owner || owner.closed || owner.cut != nil || c == nil {
		return cowPhysicalLifecycleUnavailable
	}
	identity, cut, outcome := c.joinPhysicalLifecycleLocked(opening)
	if outcome == cowPhysicalLifecycleJoined {
		owner.cache, owner.cut, owner.identity = c, cut, identity
	}
	return outcome
}

// finish consumes the retained cut once, outside writerMu. On success the
// caller MUST clear every wrapper alias before releasing the returned original
// retention receipt. On failure the preborn custody retains this same wrapper
// and its charge. Neither path retries releaseCut.
func (owner *cowPhysicalCutCapture) finish() (*memtable.COWSharedRetention, error) {
	if owner == nil || owner.self != owner {
		return nil, memtable.ErrCOWClosed
	}
	if owner.closed {
		return nil, memtable.ErrCOWClosed
	}
	owner.closed = true
	c, cut := owner.cache, owner.cut
	owner.cache, owner.cut, owner.identity = nil, nil, cowPhysicalLifecycleObservation{}
	if cut != nil {
		owner.failure = c.releaseCut(cut)
	}
	custody := owner.custody
	owner.custody = nil
	if owner.failure != nil {
		custody.Complete(owner, owner.failure)
		return nil, owner.failure
	}
	custody.Complete(nil, nil)
	retention := owner.retention
	owner.retention = nil
	owner.self = nil
	return retention, nil
}
