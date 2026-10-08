package collections

import (
	"math"
	"sync"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

// The facets below adapt the existing request's disjoint credit tranches to the
// resource and allocator constructor hooks. They own no registry, scheduler or
// engine state. Callbacks only take this independent mutex; they never reenter
// the engine. A request cannot retire its credit before its last real consumer.
type nativeRequestCreditKind uint8

const (
	nativeRequestSourceCredit nativeRequestCreditKind = iota
	nativeRequestMetadataCredit
	nativeRequestAllocatorCredit
	nativeRequestCreditKinds
)

type nativeRequestCredit struct {
	mu       sync.Mutex
	limits   [nativeRequestCreditKinds]uint64
	used     [nativeRequestCreditKinds]uint64
	facets   [nativeRequestCreditKinds]nativeRequestCreditFacet
	retained uint64
	retired  bool
	closed   bool
}

type nativeRequestCreditFacet struct {
	owner    *nativeRequestCredit
	kind     nativeRequestCreditKind
	retained uint64
}

type nativeRequestCreditSnapshot struct {
	Reserved        [nativeRequestCreditKinds]uint64
	Debited         [nativeRequestCreditKinds]uint64
	Retained        uint64
	Retired, Closed bool
}

// limits must come from the selected source/publisher profile. This constructor
// alone cannot certify that profile or enable the closed finite engine route.
func newNativeRequestCredit(limits [nativeRequestCreditKinds]uint64) (*nativeRequestCredit, error) {
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(nativeRequestCredit{})), true)
	if err != nil {
		return nil, err
	}
	if control > limits[nativeRequestSourceCredit] {
		return nil, ErrPreparedInsertResourceLimit
	}
	credit := &nativeRequestCredit{limits: limits}
	credit.used[nativeRequestSourceCredit] = control
	for i := nativeRequestCreditKind(0); i < nativeRequestCreditKinds; i++ {
		credit.facets[i] = nativeRequestCreditFacet{owner: credit, kind: i}
	}
	return credit, nil
}

func (f *nativeRequestCreditFacet) reserve(n uint64) error {
	if f == nil || f.owner == nil {
		return ErrPreparedInsertResourceLimit
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.retired && f.retained == 0 || n > c.limits[f.kind]-c.used[f.kind] {
		return ErrPreparedInsertResourceLimit
	}
	// Operation debit is cumulative. Failure, retry, removal, publication and
	// transfer never restore request credit.
	c.used[f.kind] += n
	return nil
}
func (f *nativeRequestCreditFacet) retain() error {
	if f == nil || f.owner == nil {
		return ErrPreparedInsertResourceLimit
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.retired && f.retained == 0 || c.retained == math.MaxUint64 || f.retained == math.MaxUint64 {
		return ErrPreparedInsertResourceLimit
	}
	c.retained++
	f.retained++
	return nil
}
func (f *nativeRequestCreditFacet) release() {
	if f == nil || f.owner == nil {
		panic("collections: nil native creator credit")
	}
	c := f.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retained == 0 || f.retained == 0 {
		panic("collections: native creator credit imbalance")
	}
	c.retained--
	f.retained--
	if c.retired && c.retained == 0 {
		c.closed = true
	}
}
func (f *nativeRequestCreditFacet) ReserveStableMetadata(n uint64) error { return f.reserve(n) }
func (f *nativeRequestCreditFacet) RetainStableMetadata() error          { return f.retain() }
func (f *nativeRequestCreditFacet) ReleaseStableMetadata()               { f.release() }
func (f *nativeRequestCreditFacet) ReserveAllocation(n uint64) error     { return f.reserve(n) }
func (f *nativeRequestCreditFacet) RetainAllocationCredit() error        { return f.retain() }
func (f *nativeRequestCreditFacet) ReleaseAllocationCredit()             { f.release() }

func (c *nativeRequestCredit) retire() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retired {
		panic("collections: native request retired twice")
	}
	c.retired = true
	if c.retained == 0 {
		c.closed = true
	}
}
func (c *nativeRequestCredit) snapshot() nativeRequestCreditSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return nativeRequestCreditSnapshot{Reserved: c.limits, Debited: c.used, Retained: c.retained, Retired: c.retired, Closed: c.closed}
}

var _ rootpublication.StableMetadataAccount = (*nativeRequestCreditFacet)(nil)

// The valuelog facade accepts a Reserve callback, so its creator retention must
// be made explicit here. A function closure retaining Go memory is not credit.
type nativeStableMetadataCredit struct {
	metadata *valuelog.FiniteStableMetadata
	facet    *nativeRequestCreditFacet
}

func newNativeStableMetadataCredit(facet *nativeRequestCreditFacet, tokens, rotations uint64) (*nativeStableMetadataCredit, error) {
	control, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(nativeStableMetadataCredit{})), true)
	if err != nil {
		return nil, err
	}
	// Go1.26.3 represents this escaping method value as its code word and
	// receiver pointer. Both the wrapper and that retained callback backing are
	// born here; a function retaining the receiver does not pay for itself.
	callback, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(struct {
		code     uintptr
		receiver *nativeRequestCreditFacet
	}{})), true)
	if err != nil {
		return nil, err
	}
	if callback > math.MaxUint64-control {
		return nil, ErrPreparedInsertResourceLimit
	}
	if err = facet.ReserveStableMetadata(control + callback); err != nil {
		return nil, err
	}
	if err = facet.RetainStableMetadata(); err != nil {
		return nil, err
	}
	metadata, err := valuelog.NewFiniteStableMetadata(tokens, rotations, facet.ReserveStableMetadata)
	if err != nil {
		facet.ReleaseStableMetadata()
		return nil, err
	}
	return &nativeStableMetadataCredit{metadata: metadata, facet: facet}, nil
}
func (c *nativeStableMetadataCredit) close() error {
	if c == nil || c.metadata == nil {
		return nil
	}
	if err := c.metadata.Close(); err != nil {
		return err
	}
	facet := c.facet
	c.metadata = nil
	c.facet = nil
	facet.ReleaseStableMetadata()
	return nil
}

// Constructor installation is validated here. A request never creates or
// retrofits the installed owner; actual constructor coverage is still required
// by the separately closed finite admission route.
func ensureNativePublicationResidentOwner(db *backenddb.DB, source *nativeRequestCreditFacet) error {
	if db == nil || source == nil || !db.HasNativePublicationResidentOwnerV1() {
		return ErrPreparedInsertResourceLimit
	}
	return nil
}
