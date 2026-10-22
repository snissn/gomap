package cowbtree

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

// OwnedValueLifetime belongs to the original immutable descriptor owner. Retain
// is infallible and only attaches an edge; Release runs on the existing semantic
// cleanup worker and retains the exact failed object on error. Release returns
// the original independent completion instead of refunding the containing
// descriptor while the node's value slot still aliases it. No map callback may
// acquire publication or discover resources through current manager state.
type OwnedValueLifetime[V any] interface {
	Retain(V)
	Release(V) (OwnedMapCompletion, error)
}

type OwnedMapLease interface{ Close() }
type OwnedMapAdmit func(bytes uint64) (OwnedMapLease, error)

var ErrOwnedMap = errors.New("cowbtree: invalid owned map lifetime or mutation bound")

// One coarse original allocation receipt covers the private mutation cohort,
// its nodes/backing and ownership metadata. Sharing retains this receipt via
// actual node edges, never an ancestor EngineVersion or a resource registry.
// The receipt has no node/value/callback backpointer.
type ownedMapAllocation struct {
	refs   atomic.Int64
	refund OwnedMapRefund
}

func (a *ownedMapAllocation) retain() { a.refs.Add(1) }

// OwnedMapRefund is preborn accounting metadata. It refers only to the original
// independent lease and adjacent receipts, never to a descriptor or callback.
type OwnedMapRefund struct {
	lease OwnedMapLease
	next  *OwnedMapRefund
	used  bool
}

// Init binds the original separately admitted accounting lease before its
// descriptor is allocated or published. This receipt owns no descriptor alias.
func (r *OwnedMapRefund) Init(lease OwnedMapLease) {
	if r.lease != nil || r.used || lease == nil {
		panic("cowbtree: refund receipt initialized twice or without lease")
	}
	r.lease = lease
}

func (r *OwnedMapRefund) Completion() OwnedMapCompletion {
	if r == nil {
		return OwnedMapCompletion{}
	}
	if r.used {
		panic("cowbtree: refund receipt transferred twice")
	}
	r.used = true
	return OwnedMapCompletion{head: r, tail: r}
}

// OwnedMapCompletion contains only original independent accounting receipts.
// The containing map/builder and every parent edge must be detached before
// Complete. Successful sibling receipts may complete even when a cohort failed.
type OwnedMapCompletion struct{ head, tail *OwnedMapRefund }

// Append transfers only original independent receipts, without allocation.
func (c *OwnedMapCompletion) Append(other OwnedMapCompletion) {
	if other.head == nil {
		return
	}
	if c.tail == nil {
		c.head = other.head
	} else {
		c.tail.next = other.head
	}
	c.tail = other.tail
}

func (c OwnedMapCompletion) Complete() {
	head := c.head
	c.head, c.tail = nil, nil
	for head != nil {
		lease, next := head.lease, head.next
		head.lease, head.next = nil, nil
		head = nil
		if lease != nil {
			lease.Close()
		}
		head = next
	}
}
func (a *ownedMapAllocation) release(c *OwnedMapCompletion) {
	if a == nil || a.refs.Add(-1) != 0 {
		return
	}
	c.Append(a.refund.Completion())
}

// Metadata is attached only to sealed original nodes. Private copies have no
// metadata until sealing. Children/value slots are detached after their actual
// successful cleanup, so partial failure parks the exact remaining graph.
type ownedMapNode[K ordered, V any] struct {
	refs       atomic.Int64
	allocation *ownedMapAllocation
	lifetime   OwnedValueLifetime[V]
	err        error
	attempted  bool
}

// OwnedMap exposes only immutable operations. The private mutable Map header
// cannot escape; a new builder forks a fresh header without changing the
// published header's isolation identity. Last Release is constant work.
type OwnedMap[K ordered, V any] struct {
	refs       atomic.Int64
	tree       *Map[K, V]
	allocation *ownedMapAllocation
	lifetime   OwnedValueLifetime[V]
	attempted  bool
	err        error
}

type OwnedMapBuilder[K ordered, V any] struct {
	tree       *Map[K, V]
	allocation *ownedMapAllocation
	lifetime   OwnedValueLifetime[V]
	remaining  int
}

// NewOwnedMapBuilder reserves a conservative finite mutation cohort before any
// header/node/backing is allocated. The caller retains base until Seal or Cancel
// completes; this is the original opening cut, not another retained registry.
// Each Set/Delete consumes one mutation. The bound includes copies, splits,
// growth buffers, cleanup metadata and headers. Empty maps use degree 32, as the
// maintained Map mutation engine does on first insertion.
func NewOwnedMapBuilder[K ordered, V any](base *OwnedMap[K, V], mutations int, lifetime OwnedValueLifetime[V], admit OwnedMapAdmit) (*OwnedMapBuilder[K, V], error) {
	if mutations < 0 || lifetime == nil || admit == nil {
		return nil, ErrOwnedMap
	}
	count, degreeMax := 0, 63
	if base != nil {
		if base.refs.Load() <= 0 || base.tree == nil {
			return nil, ErrOwnedMap
		}
		count = base.tree.count
		if base.tree.max > degreeMax {
			degreeMax = base.tree.max
		}
	}
	if mutations > int(^uint(0)>>1)-count {
		return nil, ErrOwnedMap
	}
	charge, err := ownedMapBuilderCharge[K, V](count+mutations, degreeMax, mutations)
	if err != nil {
		return nil, err
	}
	lease, err := admit(charge)
	if err != nil {
		return nil, err
	}
	a := &ownedMapAllocation{}
	a.refund.Init(lease)
	a.refs.Store(1) // the eventual immutable header, or private cancellation
	var tree *Map[K, V]
	if base == nil {
		tree = NewMap[K, V](32)
	} else {
		tree = new(Map[K, V])
		*tree = *base.tree
		tree.isoid, tree.sealed = newIsoID(), false
	}
	return &OwnedMapBuilder[K, V]{tree: tree, allocation: a, lifetime: lifetime, remaining: mutations}, nil
}

// OwnedMapBuilderMaximumCharge bounds a sparse builder against any later
// degree-32 directory size representable by this process, when no narrower
// original charged-byte bound is available. It uses exactly the same
// node/cohort formula as NewOwnedMapBuilder; it allocates or retains nothing.
func OwnedMapBuilderMaximumCharge[K ordered, V any](mutations int) (uint64, error) {
	return ownedMapBuilderCharge[K, V](int(^uint(0)>>1), 63, mutations)
}

// OwnedMapBuilderBudgetCharge bounds a later degree-32 builder from the SAME
// governing charged-byte ceiling for directories built only by NewOwnedMapBuilder.
// Every item occupies one mapPair slot in a
// source-owned node backing array; structural sharing never duplicates a node
// position within one tree. Original cohorts retain their charge until their
// last actual node/header edge. Thus even aliased values and empty strings
// cannot create more live directory items than totalBytes/sizeof(mapPair).
//
// totalBytes includes the original maintenance parent reservation, rather than
// adding its child capacity a second time. This covers later catalog growth
// without relying on a separate descriptor per logical name or a future budget
// admission. Mutations are added because the maintained builder's conservative
// height formula includes count+mutations even for replacement-only changes.
func OwnedMapBuilderBudgetCharge[K ordered, V any](totalBytes uint64, mutations int) (uint64, error) {
	if mutations < 0 {
		return 0, ErrOwnedMap
	}
	itemBytes := uint64(unsafe.Sizeof(mapPair[K, V]{}))
	if itemBytes == 0 {
		return 0, ErrOwnedMap
	}
	count := totalBytes / itemBytes
	maximum := uint64(int(^uint(0) >> 1))
	if count > maximum || uint64(mutations) > maximum-count {
		return 0, ErrOwnedMap
	}
	return ownedMapBuilderCharge[K, V](int(count)+mutations, 63, mutations)
}

func ownedMapBuilderCharge[K ordered, V any](maxItems, degreeMax, mutations int) (uint64, error) {
	if maxItems < 0 || mutations < 0 || degreeMax < 63 {
		return 0, ErrOwnedMap
	}
	// Use the maintained degree's occupancy bound, rather than binary-tree
	// height. A degree-32 height-h map has at least 2*32^(h-1)-1 items.
	height, minimum := uint64(1), uint64(1)
	degree := uint64((degreeMax + 1) / 2)
	for minimum <= uint64(maxItems) {
		if minimum > (^uint64(0)-(degree-1))/degree {
			break
		}
		minimum = minimum*degree + degree - 1
		if minimum > uint64(maxItems) {
			break
		}
		height++
	}
	nodes := uint64(mutations) * (2*height + 3)
	if mutations != 0 && nodes/uint64(mutations) != 2*height+3 {
		return 0, ErrOwnedMap
	}
	// Growth allocates less than twice final capacity. Metadata and child
	// headers are charged even for leaves; this is coarse finite admission.
	nodeBytes := uint64(unsafe.Sizeof(mapNode[K, V]{})) + uint64(unsafe.Sizeof(ownedMapNode[K, V]{})) + 2*uint64(degreeMax)*uint64(unsafe.Sizeof(mapPair[K, V]{})) + 2*uint64(degreeMax+1)*uint64(unsafe.Sizeof(uintptr(0))) + uint64(unsafe.Sizeof([]*mapNode[K, V]{})) + 128
	baseBytes := uint64(unsafe.Sizeof(OwnedMapBuilder[K, V]{})) + uint64(unsafe.Sizeof(OwnedMap[K, V]{})) + uint64(unsafe.Sizeof(Map[K, V]{})) + uint64(unsafe.Sizeof(ownedMapAllocation{})) + 256
	if nodes > (^uint64(0)-baseBytes)/nodeBytes {
		return 0, ErrOwnedMap
	}
	return baseBytes + nodes*nodeBytes, nil
}

func (b *OwnedMapBuilder[K, V]) Set(key K, value V) error {
	if b == nil || b.tree == nil || b.remaining <= 0 {
		return ErrOwnedMap
	}
	b.remaining--
	b.tree.Set(key, value)
	return nil
}
func (b *OwnedMapBuilder[K, V]) Delete(key K) error {
	if b == nil || b.tree == nil || b.remaining <= 0 {
		return ErrOwnedMap
	}
	b.remaining--
	b.tree.Delete(key)
	return nil
}
func (b *OwnedMapBuilder[K, V]) Get(key K) (V, bool) {
	if b == nil || b.tree == nil {
		var zero V
		return zero, false
	}
	return b.tree.Get(key)
}

// Seal is infallible after coarse admission. It visits newly copied nodes only;
// each unchanged child is retained by one edge. No published header is mutated.
func (b *OwnedMapBuilder[K, V]) Seal() *OwnedMap[K, V] {
	if b == nil || b.tree == nil {
		return nil
	}
	sealOwnedNode(b.tree.root, b.allocation, b.lifetime)
	b.tree.sealed = true
	m := &OwnedMap[K, V]{tree: b.tree, allocation: b.allocation, lifetime: b.lifetime}
	m.refs.Store(1)
	b.tree, b.allocation, b.lifetime = nil, nil, nil
	return m
}
func sealOwnedNode[K ordered, V any](n *mapNode[K, V], a *ownedMapAllocation, lifetime OwnedValueLifetime[V]) {
	if n == nil {
		return
	}
	if n.ownership != nil {
		// The opening owner is still retained during private preparation.
		if n.ownership.refs.Add(1) <= 1 {
			panic("cowbtree: retaining dead owned node")
		}
		return
	}
	o := &ownedMapNode[K, V]{allocation: a, lifetime: lifetime}
	o.refs.Store(1)
	a.retain()
	n.ownership = o
	if n.children != nil {
		for _, child := range *n.children {
			sealOwnedNode(child, a, lifetime)
		}
	}
	for _, pair := range n.items {
		lifetime.Retain(pair.value)
	}
}

// Cancel owns no value/child edges: sealing has not happened. The borrowed
// opening cut stays live until this private successor has been discarded.
func (b *OwnedMapBuilder[K, V]) Cancel() (completion OwnedMapCompletion) {
	if b == nil || b.tree == nil {
		return completion
	}
	a := b.allocation
	b.tree, b.allocation, b.lifetime = nil, nil, nil
	a.release(&completion)
	return completion
}
func (m *OwnedMap[K, V]) Retain() {
	if m == nil || m.refs.Add(1) <= 1 {
		panic("cowbtree: retaining released owned map")
	}
}
func (m *OwnedMap[K, V]) Release() bool {
	if m == nil {
		return false
	}
	n := m.refs.Add(-1)
	if n < 0 {
		panic("cowbtree: owned map released twice")
	}
	return n == 0
}
func (m *OwnedMap[K, V]) Get(key K) (V, bool)                   { return m.tree.Get(key) }
func (m *OwnedMap[K, V]) Len() int                              { return m.tree.Len() }
func (m *OwnedMap[K, V]) Scan(visit func(K, V) bool)            { m.tree.Scan(visit) }
func (m *OwnedMap[K, V]) Ascend(pivot K, visit func(K, V) bool) { m.tree.Ascend(pivot, visit) }

// Finish is called once by the original EngineVersion work on the existing
// worker, after last Release. Failure keeps the original graph and charge and
// is never retried by error observation or another Close.
func (m *OwnedMap[K, V]) Finish() (completion OwnedMapCompletion, err error) {
	if m == nil {
		return completion, nil
	}
	if m.refs.Load() != 0 {
		return completion, ErrOwnedMap
	}
	if m.attempted {
		return completion, m.err
	}
	m.attempted = true
	if err := releaseOwnedNode(m.tree.root, &completion); err != nil {
		m.err = err
		return completion, err
	}
	a := m.allocation
	m.tree, m.allocation, m.lifetime = nil, nil, nil
	a.release(&completion)
	return completion, nil
}
func releaseOwnedNode[K ordered, V any](n *mapNode[K, V], completion *OwnedMapCompletion) error {
	if n == nil {
		return nil
	}
	o := n.ownership
	if o == nil {
		panic("cowbtree: cleanup of unsealed node")
	}
	refs := o.refs.Add(-1)
	if refs > 0 {
		return nil
	}
	if refs < 0 {
		panic("cowbtree: owned node released twice")
	}
	if o.attempted {
		return o.err
	}
	o.attempted = true
	if n.children != nil {
		for i, child := range *n.children {
			if child == nil {
				continue
			}
			if err := releaseOwnedNode(child, completion); err != nil {
				if o.err == nil {
					o.err = err
				}
				continue
			}
			(*n.children)[i] = nil
		}
	}
	for i := range n.items {
		childCompletion, err := o.lifetime.Release(n.items[i].value)
		completion.Append(childCompletion)
		if err != nil {
			if o.err == nil {
				o.err = err
			}
			continue
		}
		var zero mapPair[K, V]
		n.items[i] = zero
	}
	if o.err != nil {
		return o.err
	}
	a := o.allocation
	if n.children != nil {
		*n.children = nil
	}
	n.items, n.children, n.ownership = nil, nil, nil
	o.allocation, o.lifetime = nil, nil
	n, o = nil, nil
	a.release(completion)
	return nil
}
