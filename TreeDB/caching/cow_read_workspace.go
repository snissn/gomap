package caching

import (
	"errors"
	"os"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"

	"github.com/snissn/gomap/TreeDB/internal/dictdb"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

type cowReadAllocation struct {
	lease *memtable.COWExternalLease
	next  *cowReadAllocation
}
type cowReadCodec struct {
	id         uint64
	codec      valuelog.BlockCodec
	decoder    *valuelog.COWDecoder
	definition *dictdb.DictionaryReadDefinition // nil when borrowed from the cut
	input, raw []byte
	lease      *memtable.COWExternalLease
}
type cowPointScratch struct {
	lease     *memtable.COWExternalLease
	key, leaf [page.PageSize]byte
	successor tree.OwnedSuccessorScratch
}

type cowReadWorkspace struct {
	snapshot      *Snapshot
	basis         *backenddb.Snapshot // borrowed from the original basis owner, never retained here
	budget        *memtable.COWBudget
	limits        valuelog.COWReadLimits
	lease         *memtable.COWExternalLease
	allocations   *cowReadAllocation
	codecs        []*cowReadCodec
	input, output []byte
	point         *cowPointScratch
}

// Cut-local workspaces are serialized by cowReadMu; basis traversal uses
// basis.readMu. Dictionary-free codecs use the original cache's physicalMu and
// retain no Snapshot, file, pointer or dictionary authority. Their admission
// survives through the cache's original last-cut/closed cleanup gate.
func (s *Snapshot) cowWorkspaceLocked() (*cowReadWorkspace, error) {
	if s.cowReader != nil {
		return s.cowReader, nil
	}
	w, err := newCOWReadWorkspace(s)
	if err == nil {
		s.cowReader = w
	}
	return w, err
}

func newCOWReadWorkspace(s *Snapshot) (*cowReadWorkspace, error) {
	w, err := admitCOWReadWorkspace(s.cowCache.budget, s.cowCache.readLimits())
	if err == nil {
		w.snapshot = s
	}
	return w, err
}

func admitCOWReadWorkspace(budget *memtable.COWBudget, limits valuelog.COWReadLimits) (*cowReadWorkspace, error) {
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) +
		memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0))))
	// Inspection precedes record/decoder growth. Reserve its peak heap metadata
	// first; cowReadMu serializes reuse and Close owns the envelope's lifetime.
	for _, size := range valuelog.COWInspectionMetadataAllocationSizes() {
		bytes += memtable.COWAllocationCharge(size)
	}
	lease, err := budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	w := &cowReadWorkspace{budget: budget, limits: limits, lease: lease}
	return w, nil
}

// physicalWorkspaceLocked requires physicalMu. Only the original cache owns
// this admitted scratch; snapshot/basis remain nil even across physical handoffs.
func (c *cowCache) physicalWorkspaceLocked(limits valuelog.COWReadLimits) (*cowReadWorkspace, error) {
	if c.physicalReader == nil {
		w, err := admitCOWReadWorkspace(c.budget, limits)
		if err != nil {
			return nil, err
		}
		c.physicalReader = w
	}
	c.physicalReader.limits = limits
	return c.physicalReader, nil
}

// Only the winner of an original cache refund gate calls this, after its last
// cut has withdrawn and before refunding the containing cache allocation.
func (c *cowCache) closePhysicalReader() {
	c.physicalMu.Lock()
	reader := c.physicalReader
	c.physicalReader = nil
	if reader != nil {
		reader.close()
		reader = nil
	}
	c.physicalMu.Unlock()
}

// Point traversal needs page scratch; a winning cached pointer does not.
func (w *cowReadWorkspace) pointScratch() (*cowPointScratch, error) {
	if w.point == nil {
		lease, err := w.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))))
		if err != nil {
			return nil, err
		}
		w.point = &cowPointScratch{lease: lease}
	}
	return w.point, nil
}

// Codec arrays grow only for actual dependencies. All discarded arrays stay
// charged through read-owner close, matching the existing buffer growth rule.
func (w *cowReadWorkspace) addCodecSlot() (int, error) {
	limit := w.budget.Limits().MaxResources
	if len(w.codecs) >= limit {
		return 0, memtable.ErrCOWCapacity
	}
	if len(w.codecs) == cap(w.codecs) {
		capacity := min(max(1, 2*cap(w.codecs)), limit)
		bytes := memtable.COWAllocationCharge(uint64(capacity)*uint64(unsafe.Sizeof((*cowReadCodec)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadAllocation{})))
		lease, err := w.budget.AcquireExternal(bytes)
		if err != nil {
			return 0, err
		}
		codecs := make([]*cowReadCodec, len(w.codecs), capacity)
		copy(codecs, w.codecs)
		w.codecs = codecs
		w.allocations = &cowReadAllocation{lease: lease, next: w.allocations}
	}
	slot := len(w.codecs)
	w.codecs = append(w.codecs, nil)
	return slot, nil
}

func cowReadCapacity(n uint64) uint64 {
	capacity := uint64(1024)
	for capacity < n {
		capacity *= 2
	}
	return capacity
}

func (w *cowReadWorkspace) grow(input, output uint64) error {
	in, out := uint64(cap(w.input)), uint64(cap(w.output))
	if input <= in && output <= out {
		return nil
	}
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadAllocation{})))
	if input > in {
		in = cowReadCapacity(input)
		bytes += memtable.COWAllocationCharge(in)
	}
	if output > out {
		out = cowReadCapacity(output)
		bytes += memtable.COWAllocationCharge(out)
	}
	lease, err := w.budget.AcquireExternal(bytes)
	if err != nil {
		return err
	}
	w.allocations = &cowReadAllocation{lease: lease, next: w.allocations}
	if input > uint64(cap(w.input)) {
		w.input = make([]byte, in)
	}
	if output > uint64(cap(w.output)) {
		w.output = make([]byte, out)
	}
	// Superseded capacities remain conservatively charged to this read owner;
	// geometric growth makes the cumulative envelope finite and explicit.
	return nil
}

func (w *cowReadWorkspace) file(ptr page.ValuePtr) (*os.File, error) {
	if w.basis != nil {
		f, _, err := w.basis.PinnedValueLogFile(ptr.FileID)
		return f, err
	}
	if w.snapshot == nil {
		return nil, valuelog.ErrCOWReadUnsupported
	}
	cut := w.snapshot.cowCut
	id := memtable.COWResourceID{Kind: cowResourceValueLogFile, ID: uint64(ptr.FileID)}
	for i := range cut.shards {
		if r := cut.shards[i].resources.get(id); r != nil {
			f := r.set.Files[ptr.FileID]
			if f != nil {
				return f.File, nil
			}
		}
	}
	for i := len(cut.frozen) - 1; i >= 0; i-- {
		if r := cut.frozen[i].resources.get(id); r != nil {
			f := r.set.Files[ptr.FileID]
			if f != nil {
				return f.File, nil
			}
		}
	}
	f, _, err := cut.basis.snapshot.PinnedValueLogFile(ptr.FileID)
	return f, err
}

func (w *cowReadWorkspace) definition(id uint64) ([]byte, *dictdb.DictionaryReadDefinition, error) {
	if w.snapshot == nil {
		return nil, nil, valuelog.ErrCOWReadUnsupported
	}
	cut := w.snapshot.cowCut
	resourceID := memtable.COWResourceID{Kind: cowResourceDictionary, ID: id}
	for i := range cut.shards {
		if r := cut.shards[i].resources.get(resourceID); r != nil {
			return r.definition.Bytes, nil, nil
		}
	}
	for i := len(cut.frozen) - 1; i >= 0; i-- {
		if r := cut.frozen[i].resources.get(resourceID); r != nil {
			return r.definition.Bytes, nil, nil
		}
	}
	provider, ok := w.snapshot.db.dictStore.(cowDictionaryReadProvider)
	if !ok {
		return nil, nil, ErrCOWUnsupported
	}
	owner, err := provider.PrepareDictionaryReadDefinition(id, w.limits, w.budget.Limits().MaxResources, w.snapshot.cowCache.admitDictionaryRead)
	if err != nil {
		owner.Close()
		return nil, nil, err
	}
	owner.ReleaseCapture() // read route holds no writer/admission/cut locks
	return owner.Bytes, owner, nil
}

func (w *cowReadWorkspace) codec(shape valuelog.COWRecordShape) (*cowReadCodec, error) {
	var slot int = -1
	for i, c := range w.codecs {
		if c == nil {
			if slot < 0 {
				slot = i
			}
			continue
		}
		if c.id == shape.DictID && c.codec == shape.Codec {
			if uint64(cap(c.input)) >= shape.PayloadBytes() && uint64(cap(c.raw)) >= shape.RawBytes {
				return c, nil
			}
			slot = i
			break
		}
	}
	if slot < 0 {
		var err error
		slot, err = w.addCodecSlot()
		if err != nil {
			return nil, err
		}
	}
	input, raw := cowReadCapacity(shape.PayloadBytes()), cowReadCapacity(shape.RawBytes)
	if old := w.codecs[slot]; old != nil {
		// The original read mutex excludes decoder/output borrowers. Keep both
		// dimensions monotone, but retire scratch before replacement admission:
		// two whole decoder envelopes need not fit the original finite budget.
		input, raw = max(input, uint64(cap(old.input))), max(raw, uint64(cap(old.raw)))
		w.codecs[slot] = nil
		old.close()
	}
	// Refusal leaves the slot empty; the original cut/definition authority
	// remains intact and a later read can retry normal admission.
	var definition []byte
	var owner *dictdb.DictionaryReadDefinition
	var err error
	if shape.DictID != 0 {
		definition, owner, err = w.definition(shape.DictID)
		if err != nil {
			return nil, err
		}
	}
	blockOnly := shape.DictID == 0 && (shape.Codec == valuelog.BlockCodecSnappy || shape.Codec == valuelog.BlockCodecLZ4)
	var sizes valuelog.COWDecoderAllocationSizes
	if blockOnly {
		sizes, err = valuelog.COWBlockDecoderRetentionSizes(shape.Codec, raw, input)
	} else {
		sizes, err = valuelog.COWDecoderRetentionSizes(uint64(len(definition)), raw, input, max(raw, 2048))
	}
	if err != nil {
		owner.Close()
		return nil, err
	}
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadCodec{}))) +
		memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0)))) +
		memtable.COWAllocationCharge(sizes.OwnerWrapper) + memtable.COWAllocationCharge(sizes.DefinitionCopy) +
		memtable.COWAllocationCharge(sizes.RawBacking) + memtable.COWAllocationCharge(sizes.InputBacking) +
		memtable.COWAllocationCharge(sizes.SequenceBacking) + uint64(sizes.FixedAllocationCount)*memtable.COWAllocationCharge(sizes.FixedAllocationBytes)
	lease, err := w.budget.AcquireExternal(bytes)
	if err != nil {
		owner.Close()
		return nil, err
	}
	var decoder *valuelog.COWDecoder
	if blockOnly {
		decoder, err = valuelog.NewCOWBlockDecoder(shape.Codec, raw, input, func(valuelog.COWDecoderAllocationSizes) error { return nil })
	} else {
		decoder, err = valuelog.NewCOWDecoder(shape.DictID, definition, raw, input, max(raw, 2048), func(valuelog.COWDecoderAllocationSizes) error { return nil })
	}
	if err != nil {
		lease.Close()
		owner.Close()
		return nil, err
	}
	c := &cowReadCodec{id: shape.DictID, codec: shape.Codec, decoder: decoder, definition: owner, input: make([]byte, input), raw: make([]byte, raw), lease: lease}
	w.codecs[slot] = c
	return c, nil
}

func (c *cowReadCodec) close() {
	c.decoder.Close()
	c.decoder = nil
	c.input = nil
	c.raw = nil
	c.definition.Close()
	c.definition = nil
	lease := c.lease
	c.lease = nil
	lease.Close()
}
func (w *cowReadWorkspace) close() {
	if w == nil {
		return
	}
	if w.point != nil {
		point := w.point
		w.point = nil
		lease := point.lease
		point.lease = nil
		point = nil
		lease.Close()
	}
	for i, c := range w.codecs {
		w.codecs[i] = nil
		if c != nil {
			c.close()
		}
	}
	w.codecs = nil
	w.input = nil
	w.output = nil
	for w.allocations != nil {
		a := w.allocations
		w.allocations = a.next
		lease := a.lease
		a.next, a.lease = nil, nil
		a = nil
		lease.Close()
	}
	w.snapshot, w.basis, w.budget = nil, nil, nil
	lease := w.lease
	w.lease = nil
	lease.Close()
}

func (w *cowReadWorkspace) read(ptr page.ValuePtr, leaf bool, dst []byte) ([]byte, error) {
	f, err := w.file(ptr)
	if err != nil {
		return nil, err
	}
	var shape valuelog.COWRecordShape
	if leaf {
		shape, err = valuelog.InspectCOWLeafRecord(f, ptr, w.limits)
	} else {
		shape, err = valuelog.InspectCOWRecord(f, ptr, w.limits)
	}
	if err != nil {
		return nil, err
	}
	if w.basis != nil && shape.Compressed && (shape.DictID != 0 || (shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4)) {
		return nil, valuelog.ErrCOWReadUnsupported
	}
	if shape.Compressed && shape.DictID == 0 && w.snapshot != nil {
		return w.readSharedRecord(f, ptr, shape, leaf, dst)
	}
	return w.readInspected(f, ptr, shape, leaf, dst)
}

// The caller's admitted read owns f and the inspected pointer; neither escapes
// this call. Dictionary-free codec/input/raw backing belongs to the SAME cache
// across bases. Value copy-out and leaf decode into caller dst finish under its
// mutex; the shared workspace never retains caller authority or output aliases.
func (w *cowReadWorkspace) readSharedRecord(f *os.File, ptr page.ValuePtr, shape valuelog.COWRecordShape, leaf bool, dst []byte) ([]byte, error) {
	c := w.snapshot.cowCache
	c.physicalMu.Lock()
	defer c.physicalMu.Unlock()
	shared, err := c.physicalWorkspaceLocked(w.limits)
	if err != nil {
		return nil, err
	}
	value, err := shared.readInspected(f, ptr, shape, leaf, dst)
	if err != nil {
		return nil, err
	}
	if leaf {
		return value, nil // ReadCOWLeafPage returns only the original caller dst.
	}
	if err = w.grow(0, uint64(len(value))); err != nil {
		return nil, err
	}
	out := w.output[:len(value)]
	copy(out, value)
	return out, nil
}

// readInspected shares the maintained admission/codec/CRC path. The inspector
// and its caller's policy have already validated this exact file and shape.
func (w *cowReadWorkspace) readInspected(f *os.File, ptr page.ValuePtr, shape valuelog.COWRecordShape, leaf bool, dst []byte) ([]byte, error) {
	var err error
	var codec *cowReadCodec
	var decode valuelog.COWDecodeFunc
	if shape.Compressed {
		codec, err = w.codec(shape)
		if err != nil {
			return nil, err
		}
		decode = codec.decoder.Decode
	}
	inputNeed := shape.PayloadBytes()
	if codec != nil {
		inputNeed = 0
	}
	outputNeed := shape.ValueBytes
	if leaf {
		outputNeed = 0
	}
	if err = w.grow(inputNeed, outputNeed); err != nil {
		return nil, err
	}
	input, raw := w.input, []byte(nil)
	if codec != nil {
		input, raw = codec.input, codec.raw
	}
	if leaf {
		return valuelog.ReadCOWLeafPage(f, ptr, shape, true, input, raw, dst, decode)
	}
	return valuelog.ReadCOWRecord(f, ptr, shape, true, input, raw, w.output, decode)
}

func (w *cowReadWorkspace) readLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return w.read(ptr.ValuePtr(), true, dst)
}

// Dictionary definitions belong to the caller's complete cut, never the
// shared physical basis. This private signal retries only the physical lookup
// through the original cut-local workspace; no public error is substituted.
var errCOWPhysicalDictionary = errors.New("cow physical leaf requires cut dictionary")

// Both cowReadMu and basis.readMu are held. No caller cut is stored in reader.
func (s *Snapshot) cowPhysicalLeafLocked(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return s.cowCut.basis.readPhysicalLeafLocked(s.cowCache, ptr, dst, false)
}

// basis.readMu protects traversal dst. Physical decoding takes physicalMu only
// after that mutex; the cache scratch never takes a basis lock in return.
func (b *cowBackendBasis) readPhysicalLeafLocked(c *cowCache, ptr page.LeafLogPtr, dst []byte, native bool) ([]byte, error) {
	w := b.reader
	f, identity, err := b.snapshot.PinnedValueLogFile(ptr.ValuePtr().FileID)
	if err != nil {
		return nil, err
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), w.limits)
	if err != nil {
		return nil, err
	}
	if !native && shape.DictID != 0 {
		return nil, errCOWPhysicalDictionary
	}
	if native && shape.Compressed && (shape.DictID != 0 || (shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4)) {
		return nil, valuelog.ErrCOWReadUnsupported
	}
	if data, hit, err := b.snapshot.CopyVerifiedCachedLeafPage(ptr, f, identity, dst); err != nil || hit {
		return data, err
	}
	var data []byte
	if shape.Compressed {
		c.physicalMu.Lock()
		shared, workspaceErr := c.physicalWorkspaceLocked(w.limits)
		if workspaceErr != nil {
			c.physicalMu.Unlock()
			return nil, workspaceErr
		}
		data, err = shared.readInspected(f, ptr.ValuePtr(), shape, true, dst)
		c.physicalMu.Unlock()
	} else {
		data, err = w.readInspected(f, ptr.ValuePtr(), shape, true, dst)
	}
	if err != nil {
		return nil, err
	}
	if err = b.snapshot.MarkCachedLeafRecordVerified(ptr, f, identity, data); err != nil {
		return nil, err
	}
	return data, nil
}

// Physical entries borrow page scratch. Copy an inline value into the original
// cut's admitted output before releasing the shared basis lock; the existing
// cowEntryLocked callers may borrow that output only under cowReadMu.
func (s *Snapshot) cowPhysicalEntryLocked(key []byte) (node.LeafEntry, error) {
	basis := s.cowCut.basis
	basis.readMu.Lock()
	defer basis.readMu.Unlock()
	w, err := basis.physicalWorkspaceLocked(s.cowCache.budget, s.cowCache.readLimits())
	if err != nil {
		return node.LeafEntry{}, err
	}
	point, err := w.pointScratch()
	if err != nil {
		return node.LeafEntry{}, err
	}
	entry, err := basis.snapshot.GetEntryExactWithFixedScratch(key, point.key[:], point.leaf[:], s.cowPhysicalLeafLocked)
	if err != nil {
		return node.LeafEntry{}, err
	}
	entry.Key = key
	if entry.Flags&(node.FlagPointer|node.FlagTombstone) != 0 {
		entry.Value = nil
		return entry, nil
	}
	local, err := s.cowWorkspaceLocked()
	if err != nil {
		return node.LeafEntry{}, err
	}
	if err = local.grow(0, uint64(len(entry.Value))); err != nil {
		return node.LeafEntry{}, err
	}
	value := local.output[:len(entry.Value)]
	copy(value, entry.Value)
	entry.Value = value
	return entry, nil
}

func (s *Snapshot) cowEntryLocked(key []byte) (node.LeafEntry, error) {
	val, ptr, flags, revision, found := s.lookupCachedRootDomainEntryWithRevision(key)
	if found {
		if flags&(node.FlagPointer|node.FlagTombstone) == 0 {
			val = normalizeRawKVValue(val)
		}
		// Internal point reads borrow the normalized query key under beginRead.
		// GetEntry alone copies it into the caller-owned returned entry.
		return node.LeafEntry{Key: key, Value: val, ValuePtr: ptr, Flags: flags, Revision: revision}, nil
	}
	entry, err := s.cowPhysicalEntryLocked(key)
	if err != errCOWPhysicalDictionary {
		return entry, err
	}
	// Preserve the original dictionary definition/resource lifetime and policies.
	w, err := s.cowWorkspaceLocked()
	if err != nil {
		return node.LeafEntry{}, err
	}
	point, err := w.pointScratch()
	if err != nil {
		return node.LeafEntry{}, err
	}
	return s.cowCut.basis.snapshot.GetEntryExactWithFixedScratch(key, point.key[:], point.leaf[:], w.readLeaf)
}

func (s *Snapshot) cowGetVersionedAppendOpen(key, dst []byte) ([]byte, page.EntryRevision, error) {
	s.cowReadMu.Lock()
	defer s.cowReadMu.Unlock()
	value, revision, err := s.cowValueLocked(key)
	if err != nil {
		return dst, revision, err
	}
	return append(dst, value...), revision, nil
}

// cowValueLocked borrows one winning value under cowReadMu and beginRead.
func (s *Snapshot) cowValueLocked(key []byte) ([]byte, page.EntryRevision, error) {
	entry, err := s.cowEntryLocked(key)
	if err != nil {
		return nil, 0, err
	}
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, entry.Revision, tree.ErrKeyNotFound
	}
	value := entry.Value
	if entry.Flags&node.FlagPointer != 0 {
		w, workspaceErr := s.cowWorkspaceLocked()
		if workspaceErr != nil {
			return nil, entry.Revision, workspaceErr
		}
		value, err = w.read(entry.ValuePtr, false, nil)
	}
	return value, entry.Revision, err
}

// A view callback runs after read gates and workspace locks are released. Its
// single owned value is admitted before copying and refunded after the callback.
func (s *Snapshot) cowViewCopy(key []byte) ([]byte, *memtable.COWExternalLease, bool, error) {
	if err := s.beginRead(); err != nil {
		return nil, nil, false, err
	}
	defer s.endRead()
	s.cowReadMu.Lock()
	defer s.cowReadMu.Unlock()
	value, _, err := s.cowValueLocked(normalizeRawKVPointKey(key))
	if err == tree.ErrKeyNotFound {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	lease, err := s.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(len(value))))
	if err != nil {
		return nil, nil, false, err
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out, lease, true, nil
}
