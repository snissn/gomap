package caching

import backenddb "github.com/snissn/gomap/TreeDB/db"

// cowPhysicalLifecycle is an original-cache-owned, single physical-producer
// registration. It is deliberately NOT a graph-source or handoff proof. The
// existing flush coordinator serializes the one registered physical owner;
// writerMu serializes registration, joins and retirement with cut changes.
// The cache's existing allocation receipt charges this embedded storage.
type cowPhysicalLifecycle struct {
	next uint64
	live uint64
	cut  *cowReadCut // one owned cut edge, withdrawn only by finish
}

// The cache identity is part of the token; equal counters from different
// caches never authorize withdrawal of each other's registration.
type cowPhysicalLifecycleToken struct {
	owner           *cowCache
	epoch           uint64
	ownerCapability struct{}
}

// Observation identifies a registration without giving the holder the owner's
// finish token by ordinary type conversion. It is not a receipt, a security
// boundary within this package, or permission to publish a root.
type cowPhysicalLifecycleObservation struct {
	owner *cowCache
	epoch uint64
}

type cowPhysicalLifecycleJoin uint8

const (
	cowPhysicalLifecycleNoRegistration cowPhysicalLifecycleJoin = iota
	cowPhysicalLifecycleJoined
	cowPhysicalLifecycleDifferentBasis
	cowPhysicalLifecycleUnavailable
)

// registerPhysicalLifecycleLocked requires writerMu and a physical owner that
// cannot start or finish a competing registration. Caller must already own a
// live edge to cut throughout this call. It retains this exact cut,
// never a sampled current root. The caller must finish the returned identity
// after its original physical operation and any accepted handoff terminate.
func (c *cowCache) registerPhysicalLifecycleLocked(cut *cowReadCut) (cowPhysicalLifecycleToken, error) {
	l := &c.physicalLifecycle
	if c.readClosed.Load() || l.live != 0 || l.cut != nil || l.next == ^uint64(0) || cut == nil || cut.cache != c || cut.basis == nil {
		return cowPhysicalLifecycleToken{}, backenddb.ErrNativeSemanticUnsupported
	}
	c.cutMu.Lock()
	if c.closed || cut.refs <= 0 || cut.refs == int(^uint(0)>>1) {
		c.cutMu.Unlock()
		return cowPhysicalLifecycleToken{}, backenddb.ErrNativeSemanticUnsupported
	}
	cut.refs++
	c.cutMu.Unlock()
	l.next++
	l.live, l.cut = l.next, cut
	return cowPhysicalLifecycleToken{owner: c, epoch: l.live}, nil
}

// joinPhysicalLifecycleLocked retains the registered original cut only when
// opening has the same owned physical basis. The caller must already own a
// live opening edge throughout this call. A call that observes an open cache
// while holding writerMu may complete even if Close begins concurrently.
// The caller owns the returned edge
// and must release it outside writerMu. NoRegistration only describes this
// slot; it does not prove there are no older frozen or maintenance sources.
func (c *cowCache) joinPhysicalLifecycleLocked(opening *cowReadCut) (cowPhysicalLifecycleObservation, *cowReadCut, cowPhysicalLifecycleJoin) {
	l := &c.physicalLifecycle
	if c.readClosed.Load() || opening == nil || opening.cache != c || opening.basis == nil {
		return cowPhysicalLifecycleObservation{}, nil, cowPhysicalLifecycleUnavailable
	}
	if l.live == 0 && l.cut == nil {
		return cowPhysicalLifecycleObservation{}, nil, cowPhysicalLifecycleNoRegistration
	}
	if l.live == 0 || l.cut == nil || l.cut.basis == nil {
		return cowPhysicalLifecycleObservation{}, nil, cowPhysicalLifecycleUnavailable
	}
	if opening.basis != l.cut.basis {
		return cowPhysicalLifecycleObservation{}, nil, cowPhysicalLifecycleDifferentBasis
	}
	c.cutMu.Lock()
	if c.closed || l.cut.refs <= 0 || l.cut.refs == int(^uint(0)>>1) {
		c.cutMu.Unlock()
		return cowPhysicalLifecycleObservation{}, nil, cowPhysicalLifecycleUnavailable
	}
	l.cut.refs++
	c.cutMu.Unlock()
	return cowPhysicalLifecycleObservation{owner: c, epoch: l.live}, l.cut, cowPhysicalLifecycleJoined
}

// finishPhysicalLifecycleLocked only withdraws the matching original owner.
// It returns the registration's edge for release outside writerMu, and does
// not revoke any independently retained joiner's cut.
func (c *cowCache) finishPhysicalLifecycleLocked(id cowPhysicalLifecycleToken) (*cowReadCut, error) {
	l := &c.physicalLifecycle
	if id.owner != c || id.epoch == 0 || l.live != id.epoch || l.cut == nil {
		return nil, backenddb.ErrNativeSemanticUnsupported
	}
	cut := l.cut
	l.live, l.cut = 0, nil
	return cut, nil
}
