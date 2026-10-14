// Package primaryarena owns immutable primary directory and component banks.
// Custody is explicit: current roots, both sealed slots and old readers retain
// independent root handles. Release is incremental; zero-ref banks cannot be
// reused while outgoing custody is waiting to be dropped.
package primaryarena

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sync"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

const Namespace uint64 = page.PrimaryBankNamespace
const groupSize = node.PrimaryDirectoryMaxEntries
const releaseCells = 8
const groupCount = (node.PrimaryDirectoryMaxEntries + groupSize - 1) / groupSize
const chunkBanks = 256
const maxChunks = 4096

var ErrStale = errors.New("primary arena: stale ownership")
var ErrFull = errors.New("primary arena: physical bank capacity exhausted")
var ErrUnrecovered = errors.New("primary arena: durable custody not recovered")
var ErrNeedsConsolidation = errors.New("primary arena: current custody group requires flattening")
var ErrFormat = errors.New("primary arena: invalid physical format")

type Class uint8

const (
	Component  Class = 1
	Directory  Class = 2
	Dependency Class = 3
	Manifest   Class = 4
	Record     Class = 5
)

type Ref struct{ PageID, Incarnation uint64 }
type Group struct {
	owner        *Arena
	refs         uint64
	components   [groupSize]Ref
	overrideMask uint64
	base         *Group // Always flat; maximum custody depth is one.
	releaseNext  *Group
	releaseCell  uint8
	releaseBase  bool
}

func (g *Group) componentAt(slot int) Ref {
	r := g.components[slot]
	if g.base != nil && g.overrideMask&(uint64(1)<<uint(slot)) == 0 {
		return g.base.components[slot]
	}
	return r
}

type bank struct {
	incarnation, refs  uint64
	class              Class
	sealed             bool
	digest             [32]byte
	directory          node.PrimaryDirectoryView
	image              []byte
	groups             [groupCount]*Group
	next               uint64
	releaseGroup       uint8
	edges              []Ref
	releaseEdge        uint32
	pair               uint64
	reusable           bool
	freeNext           uint64
	genericListed      bool
	privatePublication bool
	construction       ReadRootConstructionV6
}
type bankChunk [chunkBanks]bank

// Counters report actual custody and bank work separately from pager receipts.
// Every method reserves before the operation; a failed admission changes no
// custody. Callers include these and pager work in the same whole-call ledger.
type Counters struct {
	Claims, RootAcquires, RootDrops, EdgeReads, EdgeWrites, BanksFreed, PagesWritten, Bytes uint64
	GroupsCreated, GroupsFreed                                                              uint64
}
type Arena struct {
	mu                sync.Mutex
	pager             *pager.Pager
	uuid              [16]byte
	chunks            [maxChunks]*bankChunk
	next, free, epoch uint64
	ordinaryRelease   ReleaseQueue
	recovered         bool
	counters          Counters
	metadataDecoder   MetadataDecoder
	firstBank         uint64
	capsuleFormat     bool
	readSerial        uint64
	readRoots         map[uint64]*bank
	freePair          uint64
	repackCursor      uint64
	repackEpoch       uint64
}

func IsPage(id uint64) bool                                 { return id&Namespace != 0 && id < Namespace*2 }
func Local(id uint64) uint64                                { return id &^ Namespace }
func reserve(w *iterator.OrdinalScanWork, r, b uint64) bool { return w == nil || w.Reserve(r, b) }
func (a *Arena) slot(id uint64) *bank {
	local := Local(id)
	if IsPage(id) && local >= pager.PrimaryReadRootLocalBaseV6 {
		return a.readRoots[local]
	}
	if !IsPage(id) || local < a.firstBank || local >= a.next {
		return nil
	}
	c := a.chunks[local/chunkBanks]
	if c == nil {
		return nil
	}
	return &c[local%chunkBanks]
}
func (a *Arena) valid(r Ref) *bank {
	b := a.slot(r.PageID)
	if b == nil || b.incarnation != r.Incarnation || b.refs == 0 {
		return nil
	}
	return b
}
func Open(path string) (*Arena, error) { return open(path, false, false) }

// OpenReadOnly admits an existing header without growing or modifying the file.
func OpenReadOnly(path string) (*Arena, error) { return open(path, true, false) }

func open(path string, readOnly bool, capsule bool) (*Arena, error) {
	var p *pager.Pager
	var err error
	if readOnly {
		p, err = pager.OpenReadOnly(path, chunkBanks*page.PageSize)
	} else {
		p, err = pager.Open(path, chunkBanks*page.PageSize)
	}
	if err != nil {
		return nil, err
	}
	firstBank := uint64(2)
	version := uint16(1)
	magic := "TDPRBANK"
	if capsule {
		firstBank = 8
		version = 6
		magic = "TDPRCAPB"
	}
	a := &Arena{pager: p, next: firstBank, firstBank: firstBank, epoch: 1, capsuleFormat: capsule}
	if capsule {
		a.readRoots = make(map[uint64]*bank)
	}
	a.ordinaryRelease.owner = a
	if p.PageCount() == 0 {
		if readOnly {
			p.Close()
			return nil, ErrFormat
		}
		if _, err = rand.Read(a.uuid[:]); err != nil {
			p.Close()
			return nil, err
		}
		if err = p.GrowTo(firstBank); err != nil {
			p.Close()
			return nil, err
		}
		image := make([]byte, page.PageSize)
		copy(image[16:24], []byte(magic))
		binary.LittleEndian.PutUint16(image[24:26], version)
		copy(image[32:48], a.uuid[:])
		page.UpdateChecksum(image)
		if err = p.Write(0, image); err != nil {
			p.Close()
			return nil, err
		}
		p.SetPageCount(firstBank)
		a.recovered = true
	} else {
		image, e := p.Get(0)
		if e != nil || !page.VerifyChecksumNonMutating(image) || string(image[16:24]) != magic || binary.LittleEndian.Uint16(image[24:26]) != version || p.PageCount() < firstBank {
			p.Close()
			return nil, ErrFormat
		}
		copy(a.uuid[:], image[32:48])
		if a.uuid == ([16]byte{}) {
			p.Close()
			return nil, ErrFormat
		}
		// Allocation remains disabled until Recover closes the exact durable roots.
		a.next = p.PageCount()
	}
	return a, nil
}
func (a *Arena) UUID() [16]byte      { return a.uuid }
func (a *Arena) Pager() *pager.Pager { return a.pager }
func (a *Arena) Counters() Counters  { a.mu.Lock(); defer a.mu.Unlock(); return a.counters }
func (a *Arena) Close() error        { return a.pager.Close() }

// Claim is private destination construction. Its expected epoch/extent prevent
// two prepared claims from publishing the same bank. A caller may cancel before
// installation; an installed claim must drop its own independent owner.
type Claim struct {
	arena            *Arena
	epoch, local     uint64
	class            Class
	growth           *pager.Growth
	chunk            *bankChunk
	ready, cancelled bool
	ref              Ref
	splitPair        bool
}

func (a *Arena) PrepareClaim(class Class, w *iterator.OrdinalScanWork) (*Claim, bool, error) {
	if class != Component && class != Directory && class != Dependency && class != Manifest && class != Record {
		return nil, false, ErrFormat
	}
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(Claim{}))+128) {
		return nil, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.recovered {
		return nil, false, ErrUnrecovered
	}
	if class >= Dependency && a.metadataDecoder == nil {
		return nil, false, ErrFormat
	}
	for a.free != 0 {
		b := &a.chunks[a.free/chunkBanks][a.free%chunkBanks]
		if b.pair == 0 && b.refs == 0 && b.reusable {
			break
		}
		if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bank{}))+64) {
			return nil, false, nil
		}
		a.free = b.freeNext
		b.freeNext = 0
		b.genericListed = false
		a.epoch++
	}
	local := a.free
	splitPair := false
	if local == 0 {
		local = a.freePair
		splitPair = local != 0
		if local == 0 {
			local = a.next
		}
	}
	if local >= maxChunks*chunkBanks {
		return nil, false, ErrFull
	}
	c := &Claim{arena: a, epoch: a.epoch, local: local, class: class, splitPair: splitPair}
	if a.chunks[local/chunkBanks] == nil {
		if !reserve(w, 1, 2*uint64(unsafe.Sizeof(bankChunk{}))+128) {
			return nil, false, nil
		}
		c.chunk = new(bankChunk)
	}
	if local == a.next {
		g, ok, e := pager.NewGrowth(local+1, w)
		if !ok || e != nil {
			return nil, false, e
		}
		c.growth = g
	}
	return c, true, nil
}
func (c *Claim) Step(w *iterator.OrdinalScanWork) (Ref, bool, error) {
	if c == nil || c.cancelled {
		return Ref{}, false, ErrStale
	}
	if c.ready {
		return c.ref, true, nil
	}
	a := c.arena
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.epoch != c.epoch {
		return Ref{}, false, ErrStale
	}
	install := func() {
		if c.chunk != nil {
			a.chunks[c.local/chunkBanks] = c.chunk
		}
		b := &a.chunks[c.local/chunkBanks][c.local%chunkBanks]
		inc := b.incarnation + 1
		next, listed, freeNext := b.next, b.genericListed, b.freeNext
		*b = bank{incarnation: inc, refs: 1, class: c.class, genericListed: listed, freeNext: freeNext}
		if c.splitPair {
			a.freePair = next
			otherLocal := c.local + 1
			other := &a.chunks[otherLocal/chunkBanks][otherLocal%chunkBanks]
			other.pair = 0
			if !other.genericListed {
				other.freeNext = a.free
				other.genericListed = true
				a.free = otherLocal
			}
		} else if a.free != 0 {
			a.free = freeNext
			b.genericListed = false
			b.freeNext = 0
		} else {
			a.next++
		}
		a.epoch++
		c.ref = Ref{Namespace + c.local, inc}
		c.ready = true
		a.counters.Claims++
		a.counters.Bytes += 2*uint64(unsafe.Sizeof(bank{})) + 64
		if c.splitPair {
			a.counters.Bytes += 2 * uint64(unsafe.Sizeof(bank{}))
		}
	}
	r, b := uint64(1), 2*uint64(unsafe.Sizeof(bank{}))+64
	if c.splitPair {
		r++
		b += 2 * uint64(unsafe.Sizeof(bank{}))
	}
	if c.growth != nil {
		ok, e := c.growth.StepWithInstallAtCount(a.pager, c.local, w, r, b, install)
		return c.ref, ok, e
	}
	if !reserve(w, r, b) {
		return Ref{}, false, nil
	}
	install()
	return c.ref, true, nil
}
func (c *Claim) Cancel(w *iterator.OrdinalScanWork) (bool, error) {
	if c == nil || c.cancelled {
		return true, nil
	}
	if c.ready {
		return false, ErrStale
	}
	if c.growth != nil {
		ok, e := c.growth.Cancel(w)
		if !ok || e != nil {
			return ok, e
		}
	}
	c.chunk = nil
	c.cancelled = true
	return true, nil
}

func (a *Arena) SealComponent(r Ref, image []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if !reserve(w, 1, 2*page.PageSize+2*uint64(unsafe.Sizeof(bank{}))) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil || b.class != Component || b.sealed || len(image) != page.PageSize || page.DecodeHeader(image).PageID != r.PageID || !page.VerifyChecksumNonMutating(image) {
		return false, ErrFormat
	}
	n := node.NewNode(image)
	if n.Type() != page.PageTypeLeaf || n.Count() != 1 {
		return false, ErrFormat
	}
	physical, ok, e := a.pager.WriteViewWithWork(Local(r.PageID), image, w)
	if !ok || e != nil {
		return ok, e
	}
	b.image = physical
	b.digest = sha256.Sum256(b.image)
	b.sealed = true
	a.counters.PagesWritten++
	a.counters.Bytes += 2*page.PageSize + 2*uint64(unsafe.Sizeof(bank{}))
	return true, nil
}

// NewGroup creates independent custody of at most eight exact immutable banks.
// Groups are runtime edge sets, not persisted ancestry or recovery authority.
func (a *Arena) NewGroup(refs [groupSize]Ref, w *iterator.OrdinalScanWork) (*Group, bool, error) {
	count := uint64(0)
	for _, r := range refs {
		if r != (Ref{}) {
			count++
		}
	}
	bytes := 2*uint64(unsafe.Sizeof(Group{})) + count*2*uint64(unsafe.Sizeof(bank{}))
	if !reserve(w, 1+count, bytes) {
		return nil, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, r := range refs {
		if r == (Ref{}) {
			continue
		}
		b := a.valid(r)
		if b == nil || !b.sealed || b.class != Component || b.refs == ^uint64(0) {
			return nil, false, ErrStale
		}
	}
	g := &Group{owner: a, refs: 1, components: refs}
	a.counters.GroupsCreated++
	for _, r := range refs {
		if r == (Ref{}) {
			continue
		}
		a.valid(r).refs++
	}
	a.counters.EdgeReads += count
	a.counters.EdgeWrites += count
	a.counters.Bytes += bytes
	return g, true, nil
}
func (a *Arena) GroupComponents(g *Group, w *iterator.OrdinalScanWork) ([groupSize]Ref, bool, error) {
	var empty [groupSize]Ref
	if !reserve(w, 1, 2*uint64(unsafe.Sizeof(Group{}))) {
		return empty, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if g == nil || g.owner != a || g.refs == 0 {
		return empty, false, ErrStale
	}
	a.counters.EdgeReads++
	a.counters.Bytes += 2 * uint64(unsafe.Sizeof(Group{}))
	if g.base != nil {
		if !reserve(w, 1, 2*uint64(unsafe.Sizeof(Group{}))) {
			return empty, false, nil
		}
		if g.base.owner != a || g.base.refs == 0 || g.base.base != nil {
			return empty, false, ErrStale
		}
	}
	var refs [groupSize]Ref
	for i := range refs {
		refs[i] = g.componentAt(i)
	}
	return refs, true, nil
}
func (a *Arena) SealDirectory(r Ref, image []byte, groups [groupCount]*Group, w *iterator.OrdinalScanWork) (bool, error) {
	return a.sealDirectory(r, image, groups, nil, w)
}
func (a *Arena) sealDirectory(r Ref, image []byte, groups [groupCount]*Group, record *bundleRecord, w *iterator.OrdinalScanWork) (bool, error) {
	return a.sealDirectoryConstructedV6(r, image, groups, record, nil, nil, w)
}
func (a *Arena) sealDirectoryConstructedV6(r Ref, image []byte, groups [groupCount]*Group, record *bundleRecord, owner ReadRootConstructionV6, cert **ReadRootCertificateV6, w *iterator.OrdinalScanWork) (bool, error) {
	count := uint64(0)
	for _, g := range groups {
		if g != nil {
			count++
		}
	}
	bytes := 10*page.PageSize + 2*uint64(unsafe.Sizeof(bank{})) + count*2*uint64(unsafe.Sizeof(Group{})) + node.PrimaryDirectoryMaxEntries*2*uint64(unsafe.Sizeof(bank{}))
	if !reserve(w, 1+count+node.PrimaryDirectoryMaxEntries, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil || b.class != Directory || b.sealed || len(image) != page.PageSize || page.DecodeHeader(image).PageID != r.PageID {
		return false, ErrStale
	}
	if owner != nil && (b.construction != owner || !a.capsuleFormat) {
		return false, ErrStale
	}
	d, e := node.DecodePrimaryDirectory(image)
	if e != nil {
		return false, e
	}
	// Every physical component class must agree with its independently retained
	// edge. Validation here is ordinary full construction; native certified-copy
	// installation must charge its own checked delta and shared group custody.
	for i := 0; i < d.Count(); i++ {
		entry, _ := d.Entry(i)
		if entry.InlineAbsence() {
			continue
		}
		slot := int(entry.ClassSlot)
		g := groups[slot/groupSize]
		if g == nil || g.owner != a || g.refs == 0 {
			return false, ErrStale
		}
		component := g.componentAt(slot % groupSize)
		cb := a.valid(component)
		if component.PageID != entry.Operand.Ref.Page || cb == nil || !cb.sealed || cb.digest != entry.Operand.Digest {
			return false, ErrFormat
		}
	}
	for slot := 0; slot < groupCount*groupSize; slot++ {
		g := groups[slot/groupSize]
		if g != nil && slot >= d.Count() && g.componentAt(slot%groupSize) != (Ref{}) {
			return false, ErrFormat
		}
	}
	for _, g := range groups {
		if g != nil && (g.owner != a || g.refs == 0 || g.refs == ^uint64(0)) {
			return false, ErrStale
		}
	}
	physical, ok, e := a.writeBundleLocked(r, image, record, w)
	if !ok || e != nil {
		return ok, e
	}
	for _, g := range groups {
		if g != nil {
			g.refs++
		}
	}
	b.groups = groups
	b.image = physical
	b.directory = d.WithPhysicalImage(physical)
	b.digest = sha256.Sum256(b.image)
	b.sealed = true
	if owner != nil {
		b.privatePublication = false
		*cert = &ReadRootCertificateV6{arena: a, ref: r, directory: b.directory, image: b.image, digest: b.digest}
	}
	a.counters.EdgeReads += uint64(d.Count()) + count
	a.counters.EdgeWrites += count
	if Local(r.PageID) < pager.PrimaryReadRootLocalBaseV6 {
		a.counters.PagesWritten++
	}
	a.counters.Bytes += bytes
	return true, nil
}

// RefAcquireBytesV6 describes the actual fixed bank operand; it reads no state.
func RefAcquireBytesV6() uint64 { return 2 * uint64(unsafe.Sizeof(bank{})) }

func (a *Arena) Acquire(r Ref, w *iterator.OrdinalScanWork) (bool, error) {
	bytes := 2 * uint64(unsafe.Sizeof(bank{}))
	if !reserve(w, 1, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil || !b.sealed || b.refs == ^uint64(0) {
		return false, ErrStale
	}
	b.refs++
	a.counters.RootAcquires++
	a.counters.Bytes += bytes
	return true, nil
}
func (a *Arena) enqueue(local uint64, b *bank) { a.enqueueOn(&a.ordinaryRelease, local, b) }
func (a *Arena) enqueueOn(queue *ReleaseQueue, local uint64, b *bank) {
	b.next = queue.bankHead
	queue.bankHead = local
}
func (a *Arena) Drop(r Ref, w *iterator.OrdinalScanWork) (bool, error) {
	return a.DropToReleaseQueue(r, &a.ordinaryRelease, w)
}

// DropToReleaseQueue retires only this owner's edge. Descendant work remains on
// its explicit queue; another publisher cannot drain it through ordinary work.
func (a *Arena) DropToReleaseQueue(r Ref, queue *ReleaseQueue, w *iterator.OrdinalScanWork) (bool, error) {
	bytes := 2*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(ReleaseQueue{}))
	if !reserve(w, 2, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil || queue == nil || queue.owner != a {
		return false, ErrStale
	}
	b.refs--
	if b.refs == 0 {
		a.enqueueOn(queue, Local(r.PageID), b)
	}
	a.counters.RootDrops++
	a.counters.Bytes += bytes
	return true, nil
}
func (a *Arena) dropGroup(g *Group) { a.dropGroupOn(&a.ordinaryRelease, g) }
func (a *Arena) dropGroupOn(queue *ReleaseQueue, g *Group) {
	g.refs--
	a.counters.EdgeWrites++
	if g.refs == 0 {
		g.releaseNext = queue.groupHead
		queue.groupHead = g
	}
}
func (a *Arena) DropGroup(g *Group, w *iterator.OrdinalScanWork) (bool, error) {
	return a.DropGroupToReleaseQueue(g, &a.ordinaryRelease, w)
}
func (a *Arena) DropGroupToReleaseQueue(g *Group, queue *ReleaseQueue, w *iterator.OrdinalScanWork) (bool, error) {
	bytes := 2*uint64(unsafe.Sizeof(Group{})) + 2*uint64(unsafe.Sizeof(ReleaseQueue{}))
	if !reserve(w, 2, bytes) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if g == nil || g.owner != a || g.refs == 0 || queue == nil || queue.owner != a {
		return false, ErrStale
	}
	a.dropGroupOn(queue, g)
	a.counters.Bytes += bytes
	return true, nil
}

// ReleaseStep drops one group or frees one edge-free bank. It makes real
// progress without ordinary writer help, and never walks historical roots.
func (a *Arena) ReleaseStep(w *iterator.OrdinalScanWork) (bool, bool, error) {
	return a.ReleaseQueueStep(&a.ordinaryRelease, w)
}
func (a *Arena) ReleaseQueueStep(queue *ReleaseQueue, w *iterator.OrdinalScanWork) (bool, bool, error) {
	queueBytes := 2 * uint64(unsafe.Sizeof(ReleaseQueue{}))
	bytes := 2*uint64(unsafe.Sizeof(bank{})) + 2*uint64(unsafe.Sizeof(Group{})) + releaseCells*2*uint64(unsafe.Sizeof(bank{})) + queueBytes
	if !reserve(w, 2+releaseCells, bytes) {
		return false, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if queue == nil || queue.owner != a {
		return false, false, ErrStale
	}
	if queue.bankHead == 0 && queue.groupHead == nil {
		a.counters.Bytes += queueBytes
		return true, false, nil
	}

	if g := queue.groupHead; g != nil {
		if g.refs != 0 || g.owner != a {
			return false, false, ErrStale
		}
		if !g.releaseBase && g.base != nil {
			base := g.base
			if base.owner != a || base.refs == 0 || base.base != nil {
				return false, false, ErrStale
			}
			queue.groupHead = g.releaseNext
			g.releaseNext = nil
			g.releaseBase = true
			a.dropGroupOn(queue, base)
			g.releaseNext = queue.groupHead
			queue.groupHead = g
			a.counters.Bytes += bytes
			return true, true, nil
		}
		end := min(int(g.releaseCell)+releaseCells, groupSize)
		for int(g.releaseCell) < end {
			r := g.components[g.releaseCell]
			g.releaseCell++
			if r == (Ref{}) {
				continue
			}
			child := a.valid(r)
			if child == nil {
				return false, false, ErrStale
			}
			child.refs--
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
			if child.refs == 0 {
				a.enqueueOn(queue, Local(r.PageID), child)
			}
		}
		if int(g.releaseCell) < groupSize {
			a.counters.Bytes += bytes
			return true, true, nil
		}
		queue.groupHead = g.releaseNext
		g.releaseNext = nil
		g.base = nil
		a.counters.GroupsFreed++
		a.counters.Bytes += bytes
		return true, true, nil
	}
	local := queue.bankHead
	if local == 0 {
		return true, false, nil
	}
	b := a.slot(Namespace + local)
	if b == nil || b.refs != 0 {
		return false, false, ErrStale
	}
	// Detach the current bank before any last-edge drop enqueues a child. A
	// partially processed bank is requeued with its finite cursor unchanged.
	queue.bankHead = b.next
	b.next = 0
	for int(b.releaseGroup) < groupCount {
		i := b.releaseGroup
		b.releaseGroup++
		if g := b.groups[i]; g != nil {
			b.groups[i] = nil
			a.dropGroupOn(queue, g)
			a.enqueueOn(queue, local, b)
			a.counters.Bytes += bytes
			return true, true, nil
		}
	}
	if int(b.releaseEdge) < len(b.edges) {
		end := min(int(b.releaseEdge)+releaseCells, len(b.edges))
		for _, r := range b.edges[b.releaseEdge:end] {
			child := a.valid(r)
			if child == nil {
				return false, false, ErrStale
			}
			child.refs--
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
			if child.refs == 0 {
				a.enqueueOn(queue, Local(r.PageID), child)
			}
		}
		b.releaseEdge = uint32(end)
		a.enqueueOn(queue, local, b)
		a.counters.Bytes += bytes
		return true, true, nil
	}
	if local >= pager.PrimaryReadRootLocalBaseV6 {
		ok, e := a.pager.RemovePrimaryReadRootV6(local, w)
		if !ok || e != nil {
			a.enqueueOn(queue, local, b)
			return ok, false, e
		}
		delete(a.readRoots, local)
		a.epoch++
		a.counters.BanksFreed++
		a.counters.Bytes += bytes
		return true, true, nil
	}
	b.sealed = false
	b.class = 0
	b.digest = [32]byte{}
	b.directory = node.PrimaryDirectoryView{}
	b.image = nil
	b.edges = nil
	b.releaseEdge = 0
	b.reusable = true
	if b.pair != 0 {
		first := b.pair
		otherLocal := first
		if local == first {
			otherLocal++
		}
		other := &a.chunks[otherLocal/chunkBanks][otherLocal%chunkBanks]
		if other.reusable && other.refs == 0 {
			pair := &a.chunks[first/chunkBanks][first%chunkBanks]
			pair.next = a.freePair
			a.freePair = first
		}
	} else if !b.genericListed {
		b.freeNext = a.free
		b.genericListed = true
		a.free = local
	}
	a.epoch++
	a.counters.BanksFreed++
	a.counters.Bytes += bytes
	return true, true, nil
}

// RecoveredRoot is an independently complete slot's physical primary claim.
// No predecessor root participates in reconstruction.
type RecoveredRoot struct {
	PageID uint64
	Digest [32]byte
	Class  Class // Zero denotes Directory for existing callers.
}

func (a *Arena) Recover(extent uint64, roots []RecoveredRoot) ([]Ref, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.recovered || extent < a.firstBank || extent > a.pager.PageCount() || extent > maxChunks*chunkBanks || len(roots) == 0 {
		return nil, ErrFormat
	}
	a.next = extent
	register := func(id uint64, class Class, digest [32]byte) (*bank, error) {
		local := Local(id)
		if !IsPage(id) || local < a.firstBank || local >= extent || digest == ([32]byte{}) {
			return nil, ErrFormat
		}
		if a.chunks[local/chunkBanks] == nil {
			a.chunks[local/chunkBanks] = new(bankChunk)
		}
		b := &a.chunks[local/chunkBanks][local%chunkBanks]
		if b.sealed {
			if b.class != class || b.digest != digest {
				return nil, ErrFormat
			}
			return b, nil
		}
		*b = bank{incarnation: 1, class: class, sealed: true, digest: digest}
		return b, nil
	}
	// Walk only physical edges in each independently complete root. Metadata
	// decoder rejects malformed pages and returns canonical actual child classes.
	// Parent durable-proof fields are not ancestry edges; callers retain those
	// finite proof record images as additional independent RecoveredRoot handles.
	visiting := make(map[uint64]bool)
	closed := make(map[uint64]bool)
	var closeBank func(uint64, Class, [32]byte) (*bank, error)
	closeBank = func(id uint64, class Class, expected [32]byte) (*bank, error) {
		if !IsPage(id) || Local(id) < a.firstBank || Local(id) >= extent || visiting[id] {
			return nil, ErrFormat
		}
		image, e := a.pager.Get(Local(id))
		if e != nil || len(image) != page.PageSize || !page.VerifyChecksumNonMutating(image) || page.DecodeHeader(image).PageID != id {
			return nil, ErrFormat
		}
		digest := sha256.Sum256(image)
		if expected != ([32]byte{}) && expected != digest {
			return nil, ErrFormat
		}
		b, e := register(id, class, digest)
		if e != nil {
			return nil, e
		}
		if closed[id] {
			return b, nil
		}
		visiting[id] = true
		b.image = image
		switch class {
		case Component:
			n := node.NewNode(image)
			if n.Type() != page.PageTypeLeaf || n.Count() != 1 {
				return nil, ErrFormat
			}
		case Directory:
			d, e := node.DecodePrimaryDirectory(image)
			if e != nil {
				return nil, e
			}
			for i := 0; i < d.Count(); i++ {
				entry, _ := d.Entry(i)
				if entry.InlineAbsence() {
					continue
				}
				cb, e := closeBank(entry.Operand.Ref.Page, Component, entry.Operand.Digest)
				if e != nil || node.ValidatePrimaryComponent(entry, cb.image) != nil {
					return nil, ErrFormat
				}
				slot := int(entry.ClassSlot)
				g := b.groups[slot/groupSize]
				if g == nil {
					g = &Group{owner: a, refs: 1}
					a.counters.GroupsCreated++
					b.groups[slot/groupSize] = g
				}
				g.components[slot%groupSize] = Ref{entry.Operand.Ref.Page, cb.incarnation}
				cb.refs++
				a.counters.EdgeReads++
				a.counters.EdgeWrites++
			}
			b.directory = d
		case Dependency, Manifest, Record:
			if a.metadataDecoder == nil {
				return nil, ErrFormat
			}
			edges, e := a.metadataDecoder(class, image)
			if e != nil {
				return nil, e
			}
			b.edges = make([]Ref, len(edges))
			for i, edge := range edges {
				if !validMetadataEdge(class, edge.Class) {
					return nil, ErrFormat
				}
				child, e := closeBank(edge.PageID, edge.Class, edge.Digest)
				if e != nil {
					return nil, e
				}
				b.edges[i] = Ref{edge.PageID, child.incarnation}
				child.refs++
				a.counters.EdgeReads++
				a.counters.EdgeWrites++
			}
		default:
			return nil, ErrFormat
		}
		if class == Record && len(b.edges) > 0 {
			directory := a.slot(b.edges[0].PageID)
			first := Local(b.edges[0].PageID)
			if directory != nil && directory.class == Directory && first%2 == 0 && Local(id) == first+1 {
				b.pair = first
				directory.pair = first
			}
		}
		delete(visiting, id)
		closed[id] = true
		return b, nil
	}
	out := make([]Ref, 0, len(roots))
	for _, root := range roots {
		class := root.Class
		if class == 0 {
			class = Directory
		}
		if root.Digest == ([32]byte{}) {
			return nil, ErrFormat
		}
		b, e := closeBank(root.PageID, class, root.Digest)
		if e != nil {
			return nil, e
		}
		b.refs++
		out = append(out, Ref{root.PageID, b.incarnation})
	}
	// Only the exact closed slot projection protects banks. Unpublished private
	// tails are free; no age, ancestry or mutable current-head lookup is used.
	for local := a.firstBank; local < extent; local++ {
		if a.chunks[local/chunkBanks] == nil {
			a.chunks[local/chunkBanks] = new(bankChunk)
		}
		b := &a.chunks[local/chunkBanks][local%chunkBanks]
		if b.refs != 0 {
			continue
		}
		b.reusable = true
		if local%2 == 0 && local+1 < extent {
			other := &a.chunks[local/chunkBanks][local%chunkBanks+1]
			if other.refs == 0 {
				b.pair = local
				other.pair = local
				other.reusable = true
				b.next = a.freePair
				a.freePair = local
				local++
				continue
			}
		}
		b.freeNext = a.free
		b.genericListed = true
		a.free = local
	}
	a.pager.SetPageCount(extent)
	a.recovered = true
	a.epoch++
	return out, nil
}
func (a *Arena) RootGroups(r Ref, w *iterator.OrdinalScanWork) ([groupCount]*Group, bool, error) {
	var empty [groupCount]*Group
	bytes := 2 * uint64(unsafe.Sizeof(bank{}))
	if !reserve(w, 1, bytes) {
		return empty, false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.valid(r)
	if b == nil || b.class != Directory || !b.sealed {
		return empty, false, ErrStale
	}
	a.counters.EdgeReads++
	a.counters.Bytes += bytes
	return b.groups, true, nil
}
func (a *Arena) Get(pageID uint64) ([]byte, error) { return a.pager.Get(Local(pageID)) }

// SealExactReplacement is the bounded physical installation for an existing
// class under a caller's fresh current-root guard. The source directory is
// rehashed against its actual sealed bank certificate, the exact selected leaf
// is revalidated, and only the selected immutable custody group is rebuilt.
// expectedRevision is a predicate, not authority: current bytes must match it.
// All destination ownership already exists privately; no old root is retained
// across a yield. A refusal changes no custody or page bytes.
func (a *Arena) SealExactReplacement(destination, current Ref, entry node.PrimaryDirectoryEntry, expectedRevision page.EntryRevision, scratch []byte, w *iterator.OrdinalScanWork) (bool, error) {
	return a.sealExactReplacement(destination, current, entry, expectedRevision, scratch, nil, w)
}
func (a *Arena) sealExactReplacement(destination, current Ref, entry node.PrimaryDirectoryEntry, expectedRevision page.EntryRevision, scratch []byte, record *bundleRecord, w *iterator.OrdinalScanWork) (bool, error) {
	return a.sealExactAbsence(destination, current, entry, expectedRevision, nil, scratch, record, w)
}

// SealBaseAbsence installs a private directory over the exact pinned DATA base.
// It uses the same fresh predicate and custody installation as the durable pair.
func (a *Arena) SealBaseAbsence(destination, current Ref, entry node.PrimaryDirectoryEntry, proof *BaseKeyProofV5, scratch []byte, w *iterator.OrdinalScanWork) (bool, error) {
	if proof == nil {
		return false, ErrStale
	}
	return a.sealExactAbsence(destination, current, entry, proof.revision, proof, scratch, nil, w)
}

// SealBaseAbsenceDurableBundle consumes an actual generation-owned base proof
// and a fresh negative directory lookup. No prepared directory position, old
// neighbor or caller-supplied digest can authorize the current absence append.
func (a *Arena) SealBaseAbsenceDurableBundle(bundle PublicationBundle, current Ref, entry node.PrimaryDirectoryEntry, proof *BaseKeyProofV5, scratch []byte, encode BundleRecordEncoder, w *iterator.OrdinalScanWork) (bool, error) {
	if proof == nil {
		return false, ErrStale
	}
	return a.sealExactAbsence(bundle.Directory, current, entry, proof.revision, proof, scratch, &bundleRecord{ref: bundle.Record, encode: encode, durable: true}, w)
}

func (a *Arena) sealExactAbsence(destination, current Ref, entry node.PrimaryDirectoryEntry, expectedRevision page.EntryRevision, baseProof *BaseKeyProofV5, scratch []byte, record *bundleRecord, w *iterator.OrdinalScanWork) (bool, error) {
	const fixedRecords = 7
	bytesCost := uint64(24*page.PageSize) + 4*uint64(unsafe.Sizeof(bank{})) + 3*uint64(unsafe.Sizeof(Group{})) + uint64(groupSize)*2*uint64(unsafe.Sizeof(bank{})) + uint64(groupCount)*2*uint64(unsafe.Sizeof(Group{}))
	// Page reads/writes reserve in the ordinary pager, independently below.
	if !reserve(w, fixedRecords, bytesCost) {
		return false, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	dst, src := a.valid(destination), a.valid(current)
	if dst == nil || src == nil || dst.class != Directory || dst.sealed || src.class != Directory || !src.sealed || len(scratch) != page.PageSize {
		return false, ErrStale
	}
	if int(entry.ClassSlot) >= groupSize {
		if baseProof != nil {
			return false, ErrFull
		}
		return false, ErrStale
	}
	if entry.Kind != node.PrimaryAbsence {
		return false, ErrStale
	}
	var old node.PrimaryDirectoryEntry
	var e error
	if baseProof == nil {
		old, e = src.directory.ClassEntry(entry.ClassSlot)
		if e != nil || !bytes.Equal(old.Key, entry.Key) || old.Revision != expectedRevision {
			return false, ErrStale
		}
	} else {
		base, sequence := src.directory.Base()
		matched, admitted := baseProof.matches(base, sequence, entry.Key, expectedRevision, w)
		if !admitted {
			return false, nil
		}
		if int(entry.ClassSlot) != src.directory.Count() || !matched {
			return false, ErrStale
		}
	}
	oldImage := src.image
	if sha256.Sum256(oldImage) != src.digest || !page.VerifyChecksumNonMutating(oldImage) {
		return false, ErrFormat
	}
	if baseProof == nil && !old.InlineAbsence() {
		selectedBank := a.slot(old.Operand.Ref.Page)
		if selectedBank == nil || selectedBank.refs == 0 || !selectedBank.sealed || selectedBank.digest != old.Operand.Digest {
			return false, ErrStale
		}
		if e = node.ValidatePrimaryComponent(old, selectedBank.image); e != nil {
			return false, e
		}
	}
	gi, offset := int(entry.ClassSlot)/groupSize, int(entry.ClassSlot)%groupSize
	oldGroup := src.groups[gi]
	if oldGroup == nil {
		if baseProof == nil || src.directory.Count() != 0 {
			return false, ErrStale
		}
	} else if oldGroup.owner != a || oldGroup.refs == 0 {
		return false, ErrStale
	}
	var replacement *bank
	if !entry.InlineAbsence() {
		replacement = a.slot(entry.Operand.Ref.Page)
		if replacement == nil || !replacement.sealed || replacement.class != Component || replacement.digest != entry.Operand.Digest || replacement.refs == 0 {
			return false, ErrStale
		}
		if e = node.ValidatePrimaryComponent(entry, replacement.image); e != nil {
			return false, e
		}
	}
	g := &Group{owner: a, refs: 1, base: oldGroup}
	if oldGroup != nil && oldGroup.base != nil {
		g.base = oldGroup.base
		g.components = oldGroup.components
		g.overrideMask = oldGroup.overrideMask
	}
	g.overrideMask |= uint64(1) << uint(offset)
	if replacement != nil {
		g.components[offset] = Ref{entry.Operand.Ref.Page, replacement.incarnation}
	} else {
		g.components[offset] = Ref{}
	}
	overrides := uint64(0)
	for slot := 0; slot < groupSize; slot++ {
		if g.overrideMask&(uint64(1)<<uint(slot)) != 0 {
			overrides++
		}
	}
	// A third distinct override performs explicit current-group flattening in a
	// separate charged return. Repeated same-class replacement stays bounded.
	if overrides > 2 {
		return false, ErrNeedsConsolidation
	}
	baseEdges := uint64(0)
	if g.base != nil {
		baseEdges = 1
	}
	if !reserve(w, baseEdges+overrides+groupCount-1, 2*uint64(unsafe.Sizeof(Group{}))) {
		return false, nil
	}
	if g.base != nil && (g.base.owner != a || g.base.refs == 0 || g.base.base != nil || g.base.refs == ^uint64(0)) {
		return false, ErrStale
	}
	for _, r := range g.components {
		if r == (Ref{}) {
			continue
		}
		b := a.valid(r)
		if b == nil || !b.sealed || b.class != Component || b.refs == ^uint64(0) {
			return false, ErrStale
		}
	}
	groups := src.groups
	groups[gi] = g
	for i, shared := range groups {
		if i == gi || shared == nil {
			continue
		}
		if shared.owner != a || shared.refs == 0 || shared.refs == ^uint64(0) {
			return false, ErrStale
		}
	}
	var view node.PrimaryDirectoryView
	if baseProof == nil {
		view, e = src.directory.ReplacePrimaryComponent(scratch, destination.PageID, entry)
		if e != nil {
			return false, e
		}
	} else {
		var admitted bool
		view, admitted, e = src.directory.InsertPrimaryComponentWithVisit(scratch, destination.PageID, entry, func(observed node.PrimaryDirectoryEntry) bool {
			return reserve(w, 1, 2*uint64(unsafe.Sizeof(observed))+2*uint64(len(observed.Key))+2*uint64(len(entry.Key)))
		})
		if e != nil || !admitted {
			return false, e
		}
	}
	physical, ok, e := a.writeBundleLocked(destination, scratch, record, w)
	if !ok || e != nil {
		return ok, e
	}
	// The remaining custody installation is infallible, pre-admitted above.
	if g.base != nil {
		g.base.refs++
		a.counters.EdgeReads++
		a.counters.EdgeWrites++
	}
	for _, r := range g.components {
		if r != (Ref{}) {
			a.valid(r).refs++
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
		}
	}
	for i, shared := range groups {
		if i != gi && shared != nil {
			shared.refs++
			a.counters.EdgeReads++
			a.counters.EdgeWrites++
		}
	}
	a.counters.GroupsCreated++
	dst.groups = groups
	dst.image = physical
	// The certificate must borrow immutable physical bank bytes, never caller scratch.
	dst.directory = view.WithPhysicalImage(dst.image)
	dst.digest = sha256.Sum256(dst.image)
	dst.sealed = true
	a.counters.PagesWritten++
	a.counters.Bytes += bytesCost
	return true, nil
}
