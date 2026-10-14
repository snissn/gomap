package memtable

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/tidwall/btree"
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
}

// COWRecord exposes immutable owned strings. Byte conversions produce copies.
// Pointer resolution is permitted only while the enclosing cut/view is owned.
type COWRecord struct {
	Key, Value string
	Ptr        page.ValuePtr
	Revision   page.EntryRevision
	Flags      byte
}

type cowValue struct {
	value    string
	ptr      page.ValuePtr
	revision page.EntryRevision
	flags    byte
}

type cowPairLayout struct {
	value cowValue
	key   string
}
type cowNodeLayout struct {
	isoid    uint64
	count    int
	items    []cowPairLayout
	children *[]*cowNodeLayout
}

// COWRetirement is a bounded ownership transfer, not a background queue.
// Drain runs outside publication/admission locks. Copies share a once-only
// cleanup claim; allocation/resource charges remain until callbacks complete.
type COWRetirement struct {
	generation *cowGeneration
	prepared   *COWPrepared
}

func (r *COWRetirement) Drain() {
	if r.generation != nil {
		r.generation.drain()
	}
	if r.prepared != nil {
		r.prepared.drain()
	}
}

// COWWriter is deliberately not Table: Reset, arena borrowing/stealing, mutable
// iterators and legacy mode dispatch cannot accidentally reach this capability.
// Only its private headers call Copy; published COWRoot headers never do.
type COWWriter struct {
	mu         sync.Mutex
	private    *btree.Map[string, cowValue]
	generation *cowGeneration
	pending    *COWPrepared
	maxEntries int
	closed     bool
}

func NewCOWWriter(b *COWBudget) (*COWWriter, error) {
	if b == nil {
		return nil, fmt.Errorf("nil COW budget")
	}
	base := cowAllocation(uint64(unsafe.Sizeof(COWWriter{}))) + cowAllocation(uint64(unsafe.Sizeof(cowGeneration{}))) +
		cowAllocation(uint64(unsafe.Sizeof(btree.Map[string, cowValue]{}))) +
		cowAllocation(uint64(b.limits.MaxResources)*uint64(unsafe.Sizeof(COWResourceID{}))) +
		cowAllocation(uint64(b.limits.MaxResources)*uint64(unsafe.Sizeof(cowResourceOwner{})))
	g, err := newCOWGeneration(b, base)
	if err != nil {
		return nil, err
	}
	return &COWWriter{private: btree.NewMap[string, cowValue](btreeDefaultDegree), generation: g}, nil
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
	for _, e := range entries {
		if !e.Remove {
			allRemove = false
		}
		if e.Remove {
			deletes = true
		}
	}
	add := func(dst *uint64, n uint64) bool { v, ok := cowAdd(*dst, n); *dst = v; return ok }
	var growth uint64
	fullCapacity := cowNodeReserveFor(2, true, false, maxEntries) / 2
	for _, e := range entries {
		if !e.Remove && e.Flags&node.FlagPointer != 0 && e.Flags&node.FlagTombstone != 0 {
			return c, fmt.Errorf("COW entry has pointer and tombstone flags")
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
			keyCharge := uint64(0)
			if !replacement {
				keyCharge = cowAllocation(uint64(len(e.Key)))
			}
			if !add(&c.Payload, keyCharge) || !add(&c.Payload, cowAllocation(uint64(len(e.Value)))) {
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
	total := c.Total()
	if g.frozen || !cowFits(b.limits.MaxInFlightBytes, b.stats.ReservedBytes, total) ||
		!cowFits(b.limits.MaxGenerationBytes, g.history, c.History()) ||
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
	for _, e := range entries {
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
		p.private.Set(ownedKey, cowValue{value: string(e.Value), ptr: e.Ptr, flags: e.Flags, revision: e.Revision})
	}
	p.root = &COWRoot{tree: p.private.Copy(), generation: g}
	w.pending = p
	return p, nil
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
	defer b.mu.Unlock()
	b.stats.TotalBytes -= p.deferredCharge
	b.stats.DeferredBytes -= p.deferredCharge
	b.stats.RetiredBytes -= p.deferredCharge
	p.owners = nil
	b.releaseControlLocked()
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
	if b.closed {
		return ErrCOWClosed
	}
	if b.stats.Sources >= b.limits.MaxSources || !cowFits(b.limits.MaxRetiredBytes, b.stats.RetiredBytes, g.history) {
		return ErrCOWCapacity
	}
	g.frozen = true
	g.retired = true
	b.stats.Sources++
	b.stats.RetiredBytes += g.history
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
	g := w.generation
	b := g.budget
	b.mu.Lock()
	if !g.retired {
		g.retired = true
		b.stats.RetiredBytes += g.history
	}
	b.mu.Unlock()
	return g.release()
}

// COWRoot is one immutable read header. Retain/Release are source/cut ownership,
// not public independently closable views. Release returns deferred IO work.
type COWRoot struct {
	mu         sync.Mutex
	tree       *btree.Map[string, cowValue]
	generation *cowGeneration
	refs       int
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
	return r.generation.release()
}
func (r *COWRoot) Len() int    { return r.tree.Len() }
func (r *COWRoot) Height() int { return r.tree.Height() }
func (r *COWRoot) Get(key []byte) (COWRecord, bool) {
	record, ok := r.SeekGE(key, nil)
	if !ok || cowKeyCompare(record.Key, key) != 0 {
		return COWRecord{}, false
	}
	return record, true
}

// SeekGE uses the already owned cut/source reference and allocates no cursor,
// view or key conversion. Default builds search one path; treedb_safe uses
// read-only rank binary search, O(log N * height), to avoid unsafe conversions.
// The returned strings are immutable generation-owned data.
func (r *COWRoot) SeekGE(start, end []byte) (COWRecord, bool) {
	key, v, found := cowLookupGE(r.tree, start)
	if !found || (end != nil && cowKeyCompare(key, end) >= 0) {
		return COWRecord{}, false
	}
	return COWRecord{Key: key, Value: v.value, Ptr: v.ptr, Revision: v.revision, Flags: v.flags}, true
}
