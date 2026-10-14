package caching

import (
	"os"
	"unsafe"

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
}

type cowReadWorkspace struct {
	snapshot      *Snapshot
	lease         *memtable.COWExternalLease
	allocations   *cowReadAllocation
	codecs        []*cowReadCodec
	input, output []byte
	point         *cowPointScratch
}

// Read owners are private to one snapshot and serialized by cowReadMu. Each
// codec reuses its admitted input/raw buffers across frames. Full backing and
// decoder admission survives until Close; no global codec cache is consulted.
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
	bytes := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) +
		memtable.COWAllocationCharge(4*uint64(unsafe.Sizeof(uintptr(0))))
	// Inspection precedes record/decoder growth. Reserve its peak heap metadata
	// first; cowReadMu serializes reuse and Close owns the envelope's lifetime.
	for _, size := range valuelog.COWInspectionMetadataAllocationSizes() {
		bytes += memtable.COWAllocationCharge(size)
	}
	lease, err := s.cowCache.budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	w := &cowReadWorkspace{snapshot: s, lease: lease}
	return w, nil
}

// Point traversal needs page scratch; a winning cached pointer does not.
func (w *cowReadWorkspace) pointScratch() (*cowPointScratch, error) {
	if w.point == nil {
		lease, err := w.snapshot.cowCache.budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))))
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
	limit := w.snapshot.cowCache.budget.Limits().MaxResources
	if len(w.codecs) >= limit {
		return 0, memtable.ErrCOWCapacity
	}
	if len(w.codecs) == cap(w.codecs) {
		capacity := min(max(1, 2*cap(w.codecs)), limit)
		bytes := memtable.COWAllocationCharge(uint64(capacity)*uint64(unsafe.Sizeof((*cowReadCodec)(nil)))) + memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadAllocation{})))
		lease, err := w.snapshot.cowCache.budget.AcquireExternal(bytes)
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
	lease, err := w.snapshot.cowCache.budget.AcquireExternal(bytes)
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
	owner, err := provider.PrepareDictionaryReadDefinition(id, w.snapshot.cowCache.readLimits(), w.snapshot.cowCache.budget.Limits().MaxResources, w.snapshot.cowCache.admitDictionaryRead)
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
	var definition []byte
	var owner *dictdb.DictionaryReadDefinition
	var err error
	if shape.DictID != 0 {
		definition, owner, err = w.definition(shape.DictID)
		if err != nil {
			return nil, err
		}
	}
	input, raw := cowReadCapacity(shape.PayloadBytes()), cowReadCapacity(shape.RawBytes)
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
	lease, err := w.snapshot.cowCache.budget.AcquireExternal(bytes)
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
	if old := w.codecs[slot]; old != nil {
		old.close()
	}
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
	c.lease.Close()
}
func (w *cowReadWorkspace) close() {
	if w == nil {
		return
	}
	if w.point != nil {
		w.point.lease.Close()
		w.point = nil
	}
	for _, c := range w.codecs {
		if c != nil {
			c.close()
		}
	}
	w.codecs = nil
	w.input = nil
	w.output = nil
	for a := w.allocations; a != nil; a = a.next {
		a.lease.Close()
	}
	w.allocations = nil
	w.lease.Close()
}

func (w *cowReadWorkspace) read(ptr page.ValuePtr, leaf bool, dst []byte) ([]byte, error) {
	f, err := w.file(ptr)
	if err != nil {
		return nil, err
	}
	var shape valuelog.COWRecordShape
	if leaf {
		shape, err = valuelog.InspectCOWLeafRecord(f, ptr, w.snapshot.cowCache.readLimits())
	} else {
		shape, err = valuelog.InspectCOWRecord(f, ptr, w.snapshot.cowCache.readLimits())
	}
	if err != nil {
		return nil, err
	}
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
