package memtable

import (
	"fmt"
	"sync"
	"unsafe"

	btree "github.com/snissn/gomap/TreeDB/internal/cowbtree"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// COWMutation borrows input only during Prepare. Remove erases the physical key;
// FlagTombstone stores a cached physical-key deletion marker and its revision.
// MVCC logical tombstones remain separately encoded in ordinary value bytes.
type COWMutation struct {
	Key, Value []byte
	Ptr        page.ValuePtr
	Revision   page.EntryRevision
	Flags      byte
	Remove     bool
	// MVCC qualifies a canonical physical version for the logical-history
	// representation. Timestamp is application time, never revision or LSN.
	MVCC      bool
	Timestamp uint64
}

// COWRecord exposes immutable owned strings. Byte conversions produce copies.
// Pointer resolution is permitted only while the enclosing cut/view is owned.
type COWRecord struct {
	Key, Value string
	Ptr        page.ValuePtr
	Revision   page.EntryRevision
	Flags      byte
}

type cowPairLayout struct {
	value cowValue
	key   string
}
type cowNodeLayout struct {
	ownership unsafe.Pointer // optional cowbtree owned descriptor node metadata
	isoid     uint64
	count     int
	items     []cowPairLayout
	children  *[]*cowNodeLayout
}

// COWRetirement is a bounded ownership transfer, not a background queue.
// Drain runs outside publication/admission locks. Copies share a once-only
// cleanup claim; allocation/resource charges remain until callbacks complete.
type COWRetirement struct {
	generation *cowGeneration
	prepared   *COWPrepared
	viewLease  *cowViewReleaseLease
}

func (r *COWRetirement) Drain() {
	generation, prepared, lease := r.generation, r.prepared, r.viewLease
	r.generation, r.prepared, r.viewLease = nil, nil, nil
	if generation != nil {
		generation.drain()
	}
	generation = nil
	if prepared != nil {
		prepared.drain()
	}
	prepared = nil
	if lease != nil {
		lease.Close()
	}
}

// COWWriter is deliberately not Table: Reset, arena borrowing/stealing, mutable
// iterators and legacy mode dispatch cannot accidentally reach this capability.
// Only its private headers call Copy; published COWRoot headers never do.
type COWWriter struct {
	mu         sync.Mutex
	private    *btree.Map[string, cowValue]
	ordered    *cowOrderedTree
	generation *cowGeneration
	pending    *COWPrepared
	maxEntries int
	closed     bool
}

func NewCOWWriter(b *COWBudget) (*COWWriter, error) {
	return newCOWWriterWithConstruction(b, nil)
}

func NewCOWOrderedWriter(b *COWBudget) (*COWWriter, error) {
	return newCOWWriterMode(b, nil, true)
}

// ForkCOWWriter prepares a private successor from an owned immutable source.
// It shares only the original source generation, budget and resource owners;
// no ancestor engine, view or published mutable header is retained. Closing one
// fork cannot retire a generation while another private writer remains active.
// All fork/header storage is admitted before allocation and remains in the
// original finite generation history until its last actual owner is released.
func ForkCOWWriter(root *COWRoot) (*COWWriter, error) {
	if root == nil {
		return nil, ErrCOWClosed
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	if root.refs == 0 || root.tree == nil {
		return nil, ErrCOWClosed
	}
	g := root.generation
	b := g.budget
	charge := cowAllocation(uint64(unsafe.Sizeof(COWWriter{}))) + cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{})))
	if root.ordered != nil {
		charge += cowOrderedForkCharge()
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrCOWClosed
	}
	// Existing reservations may belong to another captured-root fork. Include
	// their full original charge conservatively; it contains every byte that
	// can become generation history, plus bounded preparation scratch.
	history := g.generationHistoryLocked()
	if g.frozen || !cowFits(b.limits.MaxGenerationBytes, history, g.pending) ||
		!cowFits(b.limits.MaxGenerationBytes, history+g.pending, charge) ||
		!b.retirementFitsLocked(charge) || !b.addLocked(charge) {
		b.mu.Unlock()
		return nil, ErrCOWCapacity
	}
	if g.retired {
		b.stats.RetiredBytes -= g.history - g.reservedCharge
		g.retired = false
	}
	g.history += charge
	b.stats.HistoryBytes += charge
	g.refs++
	g.activeWriters++
	b.mu.Unlock()
	w := &COWWriter{private: root.tree.ForkImmutable(), generation: g, maxEntries: root.tree.Len()}
	if root.ordered != nil {
		w.ordered = root.ordered.fork()
	}
	return w, nil
}

// NewCOWWriterWithMaintenanceConstruction admits only a fixed empty-source
// constructor from the original coarse reserve. Subsequent Prepare calls still
// use the producer budget, including payload, COW nodes, resources and scratch.
func NewCOWWriterWithMaintenanceConstruction(b, maintenance *COWBudget) (*COWWriter, error) {
	if maintenance == nil {
		return nil, ErrCOWCapacity
	}
	return newCOWWriterWithConstruction(b, maintenance)
}

func newCOWWriterWithConstruction(b, maintenance *COWBudget) (*COWWriter, error) {
	return newCOWWriterMode(b, maintenance, false)
}

func NewCOWOrderedWriterWithMaintenanceConstruction(b, maintenance *COWBudget) (*COWWriter, error) {
	if maintenance == nil {
		return nil, ErrCOWCapacity
	}
	return newCOWWriterMode(b, maintenance, true)
}

func newCOWWriterMode(b, maintenance *COWBudget, ordered bool) (*COWWriter, error) {
	if b == nil {
		return nil, fmt.Errorf("nil COW budget")
	}
	base := cowAllocation(uint64(unsafe.Sizeof(COWWriter{}))) + cowAllocation(uint64(unsafe.Sizeof(cowGeneration{}))) +
		cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) +
		cowAllocation(uint64(b.limits.MaxResources)*uint64(unsafe.Sizeof(COWResourceID{}))) +
		cowAllocation(uint64(b.limits.MaxResources)*uint64(unsafe.Sizeof(cowResourceOwner{})))
	if maintenance != nil {
		// Same immutable empty-root/map wrapper envelope as Prepare(nil), with
		// no mutable payload or resource-growth allowance hidden in the reserve.
		base += 2*cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) + cowAllocation(uint64(unsafe.Sizeof(COWRoot{})))
	}
	if ordered {
		base += cowOrderedConstructionCharge(maintenance != nil)
	}
	g, err := newCOWGenerationWithConstruction(b, base, maintenance)
	if err != nil {
		return nil, err
	}
	w := &COWWriter{private: btree.NewMap[string, cowValue](btreeDefaultDegree), generation: g}
	if ordered {
		w.ordered = newCOWOrderedTree()
	}
	return w, nil
}

// BindReservedEmptyRoot installs the one already-admitted generation resolver
// before this empty source can become visible. Original generation release owns
// both its callback and construction lease; no alternate finalizer is created.
func (w *COWWriter) BindReservedEmptyRoot(id COWResourceID, release func()) (*COWRoot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.pending != nil || w.private.Len() != 0 || (w.ordered != nil && w.ordered.len() != 0) || release == nil {
		return nil, ErrCOWPending
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	if b.closed || g.constructionLease == nil || g.frozen || len(g.ids) != 0 || cap(g.ids) == 0 || cap(g.owners) == 0 {
		b.mu.Unlock()
		return nil, ErrCOWCapacity
	}
	g.refs++
	g.ids = append(g.ids, id)
	g.owners = append(g.owners, cowResourceOwner{release: release})
	b.mu.Unlock()
	r := &COWRoot{tree: w.private.ImmutableCopy(), generation: g, refs: 1}
	if w.ordered != nil {
		r.ordered = w.ordered.immutableCopy()
	}
	return r, nil
}

// COWPrepareOptions reserves the later pre-frame resource binding and C2's
// publication vector/cut/backend/retirement wrappers before allocating them.
// ResourceBytes includes caller-owned closure/lease capacities, not file size.
type COWPrepareOptions struct {
	ResourceSlots             int
	ResourceBytes, ExtraBytes uint64
}

type COWCharge struct {
	Nodes, Payload, Wrappers, Resources, Extra, Temporary uint64
	Height                                                int
}

func (c COWCharge) History() uint64 { return c.Nodes + c.Payload + c.Wrappers + c.Resources + c.Extra }
func (c COWCharge) Total() uint64   { return c.History() + c.Temporary }

// The minimum cardinality of a degree-32 height-h tree is 2*32^(h-1)-1.
// This bounds height across the entire batch, including its transient inserts.
func cowMaximumHeight(n int) int {
	if n <= 0 {
		return 0
	}
	h := 1
	min := uint64(1)
	for min <= uint64(n) {
		if min > (^uint64(0)-31)/32 {
			break
		}
		min = min*32 + 31
		if min > uint64(n) {
			break
		}
		h++
	}
	return h
}

// Bound tidwall v1.8.1 Set/Delete allocation paths. Copies use existing capacity;
// growth occurs only at cap<=max, at most doubles, then allocator rounding.
// Splits share arrays; retries touch private nodes. Delete copies at most one
// extra sibling per level and a merge can grow arrays twice. Replacement alone
// neither splits nor grows and needs just the original path copies.
func cowNodeReserve(height int, replacement, remove bool) uint64 {
	return cowNodeReserveFor(height, replacement, remove, 2*btreeDefaultDegree-1)
}

func cowNodeReserveFor(height int, replacement, remove bool, maxEntries int) uint64 {
	if height < 1 {
		height = 1
	}
	items := 2*btreeDefaultDegree - 1
	if maxEntries < items {
		items = maxEntries
	}
	if items < 1 {
		items = 1
	}
	pairs := cowAllocation(uint64(2*items) * uint64(unsafe.Sizeof(cowPairLayout{})))
	children := cowAllocation(uint64(4*btreeDefaultDegree) * uint64(unsafe.Sizeof(uintptr(0))))
	header := cowAllocation(uint64(unsafe.Sizeof(cowNodeLayout{}))) + cowAllocation(uint64(unsafe.Sizeof([]uintptr{})))
	if height == 1 {
		children = 0
		header = cowAllocation(uint64(unsafe.Sizeof(cowNodeLayout{})))
	}
	full := header + pairs + children
	h := uint64(height)
	if replacement {
		return h * full
	}
	if remove {
		return 2*h*full + 2*h*(pairs+children)
	}
	return h*full + (h+1)*header + (h+1)*(pairs+children)
}

func (w *COWWriter) estimate(entries []COWMutation, opts COWPrepareOptions) (COWCharge, error) {
	var c COWCharge
	routing := w.ordered.routing(entries)
	if opts.ResourceSlots < 0 || opts.ResourceSlots > w.generation.budget.limits.MaxResources {
		return c, ErrCOWCapacity
	}
	if len(entries) > int(^uint(0)>>1)-w.private.Len() {
		return c, ErrCOWCapacity
	}
	c.Height = cowMaximumHeight(w.private.Len() + len(entries))
	if h := w.private.Height(); h > c.Height {
		c.Height = h
	}
	// A physical delete can invalidate a later replacement's membership proof.
	deletes := false
	allReplace := true
	allRemove := len(entries) > 0
	maxEntries := w.maxEntries
	if n := w.private.Len() + len(entries); n > maxEntries {
		maxEntries = n
	}
	for index, e := range entries {
		routed := routing.routes(index)
		rawRemove := e.Remove
		if routed {
			if !cowHasKey(w.private, e.Key) {
				continue
			}
			rawRemove = true
		}
		if !rawRemove {
			allRemove = false
		}
		if rawRemove {
			deletes = true
			allReplace = false
		}
	}
	add := func(dst *uint64, n uint64) bool { v, ok := cowAdd(*dst, n); *dst = v; return ok }
	var growth uint64
	fullCapacity := cowNodeReserveFor(2, true, false, maxEntries) / 2
	for index, e := range entries {
		if !e.Remove && e.Flags&node.FlagPointer != 0 && e.Flags&node.FlagTombstone != 0 {
			return c, fmt.Errorf("COW entry has pointer and tombstone flags")
		}
		if routing.routes(index) {
			if cowHasKey(w.private, e.Key) {
				reserve := cowNodeReserveFor(c.Height, false, true, maxEntries)
				if !add(&c.Nodes, reserve) {
					return c, ErrCOWCapacity
				}
				copies := 2 * cowNodeReserveFor(c.Height, true, false, maxEntries)
				if !add(&growth, reserve-copies) {
					return c, ErrCOWCapacity
				}
			}
			continue
		}
		exists := cowHasKey(w.private, e.Key)
		if !exists || e.Remove {
			allReplace = false
		}
		replacement := exists && !deletes && !e.Remove
		reserve := cowNodeReserveFor(c.Height, replacement, e.Remove, maxEntries)
		if !add(&c.Nodes, reserve) {
			return c, ErrCOWCapacity
		}
		if !replacement {
			copies := cowNodeReserveFor(c.Height, true, false, maxEntries)
			if e.Remove {
				copies *= 2
			}
			if !add(&growth, reserve-copies) {
				return c, ErrCOWCapacity
			}
		}
		if !e.Remove {
			valueSize, ok := cowValueSize(len(e.Value))
			if !ok {
				return c, ErrCOWCapacity
			}
			keyCharge := uint64(0)
			if !replacement {
				keyCharge = cowAllocation(uint64(len(e.Key)))
			}
			if !add(&c.Payload, keyCharge) || !add(&c.Payload, cowAllocation(uint64(valueSize))) {
				return c, ErrCOWCapacity
			}
		}
	}
	// Across any one fork, each original node can be copied only once, even
	// when deletes and inserts are interleaved. Fresh split nodes are private.
	if len(entries) > 0 {
		bound := uint64(w.private.Len()/31+1)*fullCapacity + growth
		if bound < c.Nodes {
			c.Nodes = bound
		}
	}
	// One batch forks only once. A shared original node is copied at most once;
	// without physical deletes nodes never disappear. This cardinality bound
	// avoids charging every operation as an independent full-height fork.
	if len(entries) > 0 && !deletes {
		maxNodes := uint64((w.private.Len()+len(entries))/31 + 1)
		oldNodes := uint64(w.private.Len()/31 + 1)
		pathNodes := uint64(len(entries)) * uint64(c.Height)
		if oldNodes > pathNodes {
			oldNodes = pathNodes
		}
		full := fullCapacity
		bound := oldNodes * full
		if !allReplace {
			// Geometric capacity growth plus split truncation is charged to
			// the surviving/new node; eight full capacities cover the two
			// arrays, rounding, both split sides and discarded root buffers.
			bound += 8 * maxNodes * full
		}
		if bound < c.Nodes {
			c.Nodes = bound
		}
	}
	if allRemove {
		// Delete-only batches copy each original node at most once. Each
		// merge removes one sibling and can allocate two array growths;
		// rotations add at most one per surviving node capacity progression.
		bound := 5 * uint64(w.private.Len()/31+1) * fullCapacity
		if bound < c.Nodes {
			c.Nodes = bound
		}
	}
	c.Wrappers = 2*cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) + cowAllocation(uint64(unsafe.Sizeof(COWRoot{})))
	c.Temporary = cowAllocation(uint64(unsafe.Sizeof(COWPrepared{}))) + cowAllocation(uint64(opts.ResourceSlots)*uint64(unsafe.Sizeof(COWResourceID{}))) +
		cowAllocation(uint64(opts.ResourceSlots)*uint64(unsafe.Sizeof(cowResourceOwner{})))
	c.Resources = opts.ResourceBytes
	c.Extra = opts.ExtraBytes
	if w.ordered != nil {
		ordered, err := w.ordered.estimate(entries)
		if err != nil {
			return c, err
		}
		for _, pair := range []struct {
			dst *uint64
			n   uint64
		}{
			{&c.Nodes, ordered.Nodes}, {&c.Payload, ordered.Payload},
			{&c.Wrappers, ordered.Wrappers}, {&c.Temporary, ordered.Temporary},
		} {
			if !add(pair.dst, pair.n) {
				return c, ErrCOWCapacity
			}
		}
		if ordered.Height > c.Height {
			c.Height = ordered.Height
		}
	}
	total := uint64(0)
	for _, n := range []uint64{c.Nodes, c.Payload, c.Wrappers, c.Temporary, c.Resources, c.Extra} {
		if !add(&total, n) {
			return c, ErrCOWCapacity
		}
	}
	return c, nil
}

// Estimate performs no node walk, payload copy or mutation. Caller input must
// remain stable until Prepare returns. Estimates include final batch height.
func (w *COWWriter) Estimate(entries []COWMutation, opts COWPrepareOptions) (COWCharge, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return COWCharge{}, ErrCOWClosed
	}
	return w.estimate(entries, opts)
}

type COWPrepared struct {
	writer         *COWWriter
	private        *btree.Map[string, cowValue]
	ordered        *cowOrderedTree
	root           *COWRoot
	charge         COWCharge
	ids            []COWResourceID
	owners         []cowResourceOwner
	maxEntries     int
	deferredBudget *COWBudget
	deferredCharge uint64
	drainClaimed   bool
}

func (p *COWPrepared) Charge() COWCharge { return p.charge }

// Root is the already allocated staged header for constructing C2's successor
// cut before a frame. It acquires its first source reference only at Publish.
// Before then it must not be exposed, retained, or used for pointer resolution.
func (p *COWPrepared) Root() *COWRoot { return p.root }

func (w *COWWriter) Prepare(entries []COWMutation, opts COWPrepareOptions) (*COWPrepared, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrCOWClosed
	}
	if w.pending != nil {
		return nil, ErrCOWPending
	}
	c, err := w.estimate(entries, opts)
	if err != nil {
		return nil, err
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, ErrCOWClosed
	}
	// Distinct private forks may prepare ordinary rows concurrently. New
	// resource identities still use the original single-writer reservation
	// protocol; do not race two private resource ledgers in this generation.
	if opts.ResourceSlots != 0 && g.activeWriters != 1 {
		b.mu.Unlock()
		return nil, ErrCOWPending
	}
	total := c.Total()
	if g.frozen || !cowFits(b.limits.MaxInFlightBytes, b.stats.ReservedBytes, total) ||
		!cowFits(b.limits.MaxGenerationBytes, g.generationHistoryLocked(), g.pending) ||
		!cowFits(b.limits.MaxGenerationBytes, g.generationHistoryLocked()+g.pending, c.History()) ||
		!b.retirementFitsLocked(total) ||
		opts.ResourceSlots > b.limits.MaxResources-len(g.ids) || !b.addLocked(total) {
		b.mu.Unlock()
		return nil, ErrCOWCapacity
	}
	g.pending += total
	b.stats.ReservedBytes += total
	b.mu.Unlock()
	p := &COWPrepared{writer: w, private: w.private.Copy(), charge: c,
		ids: make([]COWResourceID, 0, opts.ResourceSlots), owners: make([]cowResourceOwner, 0, opts.ResourceSlots), maxEntries: w.private.Len() + len(entries)}
	if w.ordered != nil {
		p.ordered = w.ordered.fork()
		p.ordered.applyValidatedFrom(w.ordered, entries)
	}
	routing := w.ordered.routing(entries)
	for index, e := range entries {
		if routing.routes(index) {
			// A newly qualified version replaces any prior raw incarnation.
			// Deleting a missing key can still copy its raw search path. Only
			// real migration work is included in the original reservation; use
			// the staged tree because earlier operations may insert this key.
			if p.private.Len() != 0 && cowHasKey(p.private, e.Key) {
				cowDeleteKey(p.private, e.Key)
			}
			continue
		}
		// Lookup returns an existing owned key; safe builds compare bytes
		// directly, without temporary key conversions before or after admission.
		if e.Remove {
			cowDeleteKey(p.private, e.Key)
			continue
		}
		ownedKey, _, exists := cowLookupGE(p.private, e.Key)
		exists = exists && cowKeyCompare(ownedKey, e.Key) == 0
		// Reuse an owned immutable key on replacement; new payloads copy once.
		if !exists {
			ownedKey = string(e.Key)
		}
		p.private.Set(ownedKey, cowEncodeValue(e))
	}
	p.root = &COWRoot{tree: p.private.ImmutableCopy(), generation: g}
	if p.ordered != nil {
		p.root.ordered = p.ordered.immutableCopy()
	}
	actualNodes := p.private.AllocationCharge()
	if p.ordered != nil {
		orderedNodes := p.ordered.nodeAllocationCharge()
		if orderedNodes > ^uint64(0)-actualNodes {
			actualNodes = ^uint64(0)
		} else {
			actualNodes += orderedNodes
		}
	}
	if err := p.reconcileNodes(actualNodes); err != nil {
		// No resource callback or published source edge exists yet. End every
		// private postimage alias before returning the original reservation.
		p.private, p.ordered, p.root, p.ids, p.owners, p.writer = nil, nil, nil, nil, nil, nil
		b.mu.Lock()
		g.pending -= total
		b.stats.ReservedBytes -= total
		b.stats.TotalBytes -= total
		b.mu.Unlock()
		return nil, err
	}
	w.pending = p
	return p, nil
}

// reconcileNodes returns only an unspent part of the original proven node
// reservation. The counter includes every actual copied/grown backing, even
// ones discarded while preparing. Payload, wrappers and resource envelopes
// retain their original charge. Caller holds the writer lock before publication.
func (p *COWPrepared) reconcileNodes(actual uint64) error {
	if actual > p.charge.Nodes {
		return fmt.Errorf("COW node allocation charge %d exceeds reservation %d: %w", actual, p.charge.Nodes, ErrCOWCapacity)
	}
	unused := p.charge.Nodes - actual
	if unused == 0 {
		return nil
	}
	g := p.writer.generation
	b := g.budget
	b.mu.Lock()
	g.pending -= unused
	b.stats.ReservedBytes -= unused
	b.stats.TotalBytes -= unused
	p.charge.Nodes = actual
	b.mu.Unlock()
	return nil
}

func (w *COWWriter) HasResource(id COWResourceID) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	b := w.generation.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, got := range w.generation.ids {
		if got == id {
			return true
		}
	}
	return false
}

// AttachResources transfers one independently retained existing physical owner.
// IDs must all be newly introduced, distinct identities. Capacity was reserved
// by Prepare; no append here can allocate. On refusal ownership stays with caller.
// The callback is executed once on cancellation or last generation release.
func (p *COWPrepared) AttachResources(ids []COWResourceID, release func()) error {
	w := p.writer
	if w == nil {
		return ErrCOWClosed
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending != p {
		return ErrCOWClosed
	}
	if len(ids) == 0 || release == nil {
		return fmt.Errorf("COW resource owner requires distinct identities and release")
	}
	if len(ids) > cap(p.ids)-len(p.ids) || len(p.owners) == cap(p.owners) {
		return ErrCOWCapacity
	}
	b := w.generation.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, id := range ids {
		for _, old := range w.generation.ids {
			if old == id {
				return fmt.Errorf("COW resource already retained")
			}
		}
		for _, old := range p.ids {
			if old == id {
				return fmt.Errorf("COW resource already staged")
			}
		}
		for _, old := range ids[:i] {
			if old == id {
				return fmt.Errorf("duplicate COW resource identity")
			}
		}
	}
	p.ids = append(p.ids, ids...)
	p.owners = append(p.owners, cowResourceOwner{release: release})
	return nil
}

// Publish only transfers already allocated state. The external writer sequencer
// must keep the prepared basis exclusive through its WAL/ACK and complete-cut
// swap. This function has no fallible allocation, callback, IO or rebase.
func (p *COWPrepared) Publish() *COWRoot {
	w := p.writer
	if w == nil {
		panic("COW preparation consumed")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending != p {
		panic("COW preparation is not pending")
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	g.pending -= p.charge.Total()
	b.stats.ReservedBytes -= p.charge.Total()
	b.stats.TotalBytes -= p.charge.Temporary
	g.history += p.charge.History()
	b.stats.HistoryBytes += p.charge.History()
	g.refs++
	g.ids = append(g.ids, p.ids...)
	g.owners = append(g.owners, p.owners...)
	b.mu.Unlock()
	w.private = p.private
	w.ordered = p.ordered
	if n := p.maxEntries; n > w.maxEntries {
		w.maxEntries = n
	}
	w.pending = nil
	root := p.root
	root.mu.Lock()
	root.refs = 1
	root.mu.Unlock()
	p.writer = nil
	p.private = nil
	p.ordered = nil
	p.root = nil
	p.ids = nil
	p.owners = nil
	return root
}

func (p *COWPrepared) Cancel() COWRetirement {
	w := p.writer
	if w == nil {
		return COWRetirement{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.pending != p {
		return COWRetirement{}
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	g.pending -= p.charge.Total()
	b.stats.ReservedBytes -= p.charge.Total()
	deferred := uint64(0)
	if len(p.owners) != 0 {
		// The prepared wrapper is also the shared cleanup claim. Retain its
		// full staging charge and resource owners until callbacks finish.
		deferred = p.charge.Temporary + p.charge.Resources
		p.deferredBudget = b
		p.deferredCharge = deferred
		b.stats.DeferredBytes += deferred
		b.stats.RetiredBytes += deferred
	}
	b.stats.TotalBytes -= p.charge.Total() - deferred
	b.mu.Unlock()
	w.pending = nil
	r := COWRetirement{}
	if deferred != 0 {
		r.prepared = p
	} else {
		p.owners = nil
	}
	p.writer = nil
	p.private = nil
	p.ordered = nil
	p.root = nil
	p.ids = nil
	return r
}

func (p *COWPrepared) drain() {
	b := p.deferredBudget
	b.mu.Lock()
	if p.drainClaimed {
		b.mu.Unlock()
		return
	}
	p.drainClaimed = true
	b.mu.Unlock()
	cowReleaseOwners(p.owners)
	b.mu.Lock()
	charge := p.deferredCharge
	p.owners, p.deferredCharge = nil, 0
	p = nil
	b.stats.TotalBytes -= charge
	b.stats.DeferredBytes -= charge
	b.stats.RetiredBytes -= charge
	b.releaseControlLocked()
	b.mu.Unlock()
	b.releaseMaintenanceReservation()
}

// ReserveRawRolloverSource reserves the cache generation's source before its
// first dirty acceptance. Standalone writers retain Freeze-time admission.
// The original generation owns the slot through all actual root/view edges.
func (w *COWWriter) ReserveRawRolloverSource() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.pending != nil {
		return ErrCOWPending
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrCOWClosed
	}
	if g.frozen || g.rawSourceReserved || b.stats.Sources >= b.limits.MaxSources {
		return ErrCOWCapacity
	}
	g.rawSourceReserved = true
	b.stats.Sources++
	return nil
}

// CancelRawRolloverSource unwinds only an unpublished first-dirty preparation.
// The cache serializes this operation with all writes and rollover.
func (w *COWWriter) CancelRawRolloverSource() {
	w.mu.Lock()
	defer w.mu.Unlock()
	g := w.generation
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if g.frozen || !g.rawSourceReserved || w.pending != nil {
		panic("invalid raw source cancellation")
	}
	g.rawSourceReserved = false
	b.stats.Sources--
}

// Freeze is pre-frame admission for a write-side source rollover. C2 reserves
// its complete successor cut and fresh writer before making this transition.
// Freeze cannot run while a preparation is pending. It never walks records.
func (w *COWWriter) Freeze() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrCOWClosed
	}
	if w.pending != nil {
		return ErrCOWPending
	}
	g := w.generation
	b := g.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if g.frozen {
		return nil
	}
	if g.activeWriters != 1 || g.pending != 0 {
		return ErrCOWPending
	}
	if b.closed {
		return ErrCOWClosed
	}
	if (!g.rawSourceReserved && b.stats.Sources >= b.limits.MaxSources) || !cowFits(b.limits.MaxRetiredBytes, b.stats.RetiredBytes, g.history-g.reservedCharge) {
		return ErrCOWCapacity
	}
	g.frozen = true
	g.retired = true
	if !g.rawSourceReserved {
		b.stats.Sources++
	}
	b.stats.RetiredBytes += g.history - g.reservedCharge
	return nil
}

func (w *COWWriter) Close() COWRetirement {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return COWRetirement{}
	}
	if w.pending != nil {
		panic("cancel COW preparation before writer Close")
	}
	w.closed = true
	w.private = nil
	w.ordered = nil
	g := w.generation
	b := g.budget
	b.mu.Lock()
	g.activeWriters--
	if g.activeWriters < 0 {
		panic("COW generation writer released twice")
	}
	if g.activeWriters == 0 && !g.retired {
		g.retired = true
		b.stats.RetiredBytes += g.history - g.reservedCharge
	}
	b.mu.Unlock()
	w.generation = nil
	return g.release()
}

// COWRoot is one immutable read header. Retain/Release are source/cut ownership,
// not public independently closable views. Release returns deferred IO work.
type COWRoot struct {
	mu         sync.Mutex
	tree       *btree.Map[string, cowValue]
	ordered    *cowOrderedTree
	generation *cowGeneration
	refs       int
}

// AcquireCapturedSource claims one already admitted original source cohort AND
// one exact root edge against this root's SAME budget. The caller owns both on
// success. It admits no producer bytes after accepted mutation. It does not
// freeze shared generations or alter private fork publication/cancellation.
// The caller attaches this receipt to the original source, shares that owner
// across views, and closes it only after detaching its last root/resource edge.
func (r *COWRoot) AcquireCapturedSource(bytes uint64) (*COWNativeSourceReservation, error) {
	if r == nil {
		return nil, ErrCOWClosed
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 || r.tree == nil {
		return nil, ErrCOWClosed
	}
	cell := r.generation.nativeSource
	if cell == nil {
		return nil, ErrCOWCapacity
	}
	b := r.generation.budget
	b.mu.Lock()
	err := cell.claimLocked(bytes)
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	r.refs++ // belongs to this already admitted original source, even if Close follows
	return cell, nil
}

func (r *COWRoot) Retain() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 {
		return false
	}
	b := r.generation.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return false
	}
	r.refs++
	return true
}

// RetainOwned shares an immutable root already owned by the caller. Native
// maintenance uses it to transfer an existing source edge into its preadmitted
// successor, including after budget Close. It acquires no generation, source
// slot, pin, allocation, or admission authority. The caller must keep its own
// root edge through this call; a released root cannot be revived.
func (r *COWRoot) RetainOwned() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 {
		return false
	}
	r.refs++
	return true
}
func (r *COWRoot) Release() COWRetirement {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refs == 0 {
		panic("COW root released twice")
	}
	r.refs--
	if r.refs != 0 {
		return COWRetirement{}
	}
	generation := r.generation
	r.tree, r.ordered, r.generation = nil, nil, nil
	return generation.release()
}
func (r *COWRoot) Len() int {
	n := r.tree.Len()
	if r.ordered != nil {
		n += r.ordered.len()
	}
	return n
}
func (r *COWRoot) Height() int {
	h := r.tree.Height()
	if r.ordered != nil && r.ordered.height() > h {
		h = r.ordered.height()
	}
	return h
}

// OrderedCounts reports representation activation while the caller owns this
// immutable root. It creates no cursor or new ownership edge.
func (r *COWRoot) OrderedCounts() (logicalKeys, physicalVersions int) {
	if r == nil || r.ordered == nil {
		return 0, 0
	}
	return r.ordered.keys.Len(), r.ordered.len()
}
func (r *COWRoot) Get(key []byte) (COWRecord, bool) {
	record, ok := r.SeekGE(key, nil)
	if !ok || cowKeyCompare(record.Key, key) != 0 {
		return COWRecord{}, false
	}
	return record, true
}

// ScanOwned scans this immutable root in key order using the caller's existing
// source/cut edge. It admits no view or cursor and retains no callback or record.
// The caller must keep that edge until every borrowed key/value alias has been
// cleared. Callbacks run outside publication/cut gates and must not release the
// enclosing source while scanning. Returning false stops the scan immediately.
func (r *COWRoot) ScanOwned(iter func(COWRecord) bool) {
	if r.ordered != nil && r.ordered.len() != 0 {
		// Immutable projections share the original generation owner.
		r.scanOrderedMerged(iter)
		return
	}
	r.tree.Scan(func(key string, value cowValue) bool {
		return iter(value.record(key))
	})
}

// SeekGE uses the already owned cut/source reference and allocates no cursor,
// view or key conversion. Default builds search one path; treedb_safe uses
// read-only rank binary search, O(log N * height), to avoid unsafe conversions.
// The returned strings are immutable generation-owned data.
func (r *COWRoot) SeekGE(start, end []byte) (COWRecord, bool) {
	key, v, found := cowLookupGE(r.tree, start)
	if r.ordered != nil {
		if candidate, ok := r.ordered.seekGE(start, end); ok && (!found || candidate.Key < key) {
			return candidate, true
		}
	}
	if !found || (end != nil && cowKeyCompare(key, end) >= 0) {
		return COWRecord{}, false
	}
	return v.record(key), true
}

// SeekLT is the borrowed strict predecessor in [start,end); nil end means last.
// It has the same original source lifetime and allocation contract as SeekGE.
func (r *COWRoot) SeekLT(start, end []byte) (COWRecord, bool) {
	key, v, found := cowLookupLT(r.tree, end)
	if r.ordered != nil {
		if candidate, ok := r.ordered.seekLT(start, end); ok && (!found || candidate.Key > key) {
			return candidate, true
		}
	}
	if !found || (start != nil && cowKeyCompare(key, start) < 0) {
		return COWRecord{}, false
	}
	return v.record(key), true
}
