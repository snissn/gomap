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

// Per-call borrowed capability only; codec storage retains neither provider
// nor cache through this descriptor. Definitions have independent owned bytes.
type cowCacheDictionaryRead struct {
	provider cowDictionaryReadProvider
	cache    *cowCache
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

// Cut-local workspaces are serialized by cowReadMu. Their fixed traversal and
// small native block decoders are independent of other retained readers.
// Generic codecs use the original cache's physicalMu when their definition can
// be retained independently. Borrowed results remain in this read owner's bytes.
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
		cowReadWorkspaceInspectionCharge()
	lease, err := budget.AcquireExternal(bytes)
	if err != nil {
		return nil, err
	}
	w := &cowReadWorkspace{budget: budget, limits: limits, lease: lease}
	return w, nil
}

// Standalone workspaces and physical iterator owners both reserve this whole
// envelope before any inspector can run. Embedded storage itself is covered
// by the iterator's actual composite allocation size, never charged twice.
func cowReadWorkspaceInspectionCharge() uint64 {
	bytes := memtable.COWAllocationCharge(4 * uint64(unsafe.Sizeof(uintptr(0))))
	// Inspection precedes record/decoder growth. Reserve its peak heap metadata
	// first; the read owner serializes reuse and Close ends its lifetime.
	for _, size := range valuelog.COWInspectionMetadataAllocationSizes() {
		bytes += memtable.COWAllocationCharge(size)
	}
	return bytes
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
	return w.codecWithDictionary(shape, nil)
}

func (w *cowReadWorkspace) codecWithDictionary(shape valuelog.COWRecordShape, independent *cowCacheDictionaryRead) (*cowReadCodec, error) {
	// The cache physical mutex excludes every borrower of this workspace.
	// A generic decoder has a large admitted envelope: keep only the current
	// producer here, retiring incompatible idle storage before new admission.
	// Cut-local borrowed readers retain their original independent lifetimes.
	blockOnly := shape.DictID == 0 && (shape.Codec == valuelog.BlockCodecSnappy || shape.Codec == valuelog.BlockCodecLZ4)
	if !blockOnly && w.snapshot == nil && w.basis == nil {
		for i, c := range w.codecs {
			if c != nil && !(c.id == 0 && (c.codec == valuelog.BlockCodecSnappy || c.codec == valuelog.BlockCodecLZ4)) && (c.id != shape.DictID || c.codec != shape.Codec) {
				w.codecs[i] = nil
				c.close()
			}
		}
	}
	var slot int = -1
	for i, c := range w.codecs {
		if c == nil {
			if slot < 0 {
				slot = i
			}
			continue
		}
		if c.id == shape.DictID && c.codec == shape.Codec {
			if c.definition != nil && uint64(len(c.definition.Bytes)) > w.limits.MaxValueBytes {
				return nil, dictdb.ErrReadDefinitionCapacity
			}
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
		if independent != nil {
			// Shared physical scratch must own its definition independently. Never
			// install a borrowed cut's bytes into a decoder that outlives that cut.
			owner, err = independent.provider.PrepareDictionaryReadDefinition(shape.DictID, w.limits, w.budget.Limits().MaxResources, independent.cache.admitDictionaryRead)
			if owner != nil {
				owner.ReleaseCapture()
				definition = owner.Bytes
			}
		} else {
			definition, owner, err = w.definition(shape.DictID)
		}
		if err != nil {
			owner.Close()
			return nil, err
		}
	}
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
	if errors.Is(err, memtable.ErrCOWCapacity) && !blockOnly && w.snapshot != nil && w.snapshot.cowCache.physicalMu.TryLock() {
		// A borrowed reader still owns its original decoder. Before refusing it,
		// refund only idle cache scratch; physicalMu proves no decoder borrower
		// survives, and close drops references before releasing its receipt.
		c := w.snapshot.cowCache
		refunded := false
		if shared := c.physicalReader; shared != nil {
			for i, codec := range shared.codecs {
				if codec != nil && !(codec.id == 0 && (codec.codec == valuelog.BlockCodecSnappy || codec.codec == valuelog.BlockCodecLZ4)) {
					shared.codecs[i] = nil
					codec.close()
					refunded = true
				}
			}
		}
		c.physicalMu.Unlock()
		if refunded {
			lease, err = w.budget.AcquireExternal(bytes)
		}
	}
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
	return w.readWithInspection(f, ptr, shape, leaf, dst)
}

// Preserve the original read-owner dispatch after an inspection handoff. The
// retained cut pins f throughout; shared dictionary storage owns its definition
// independently and never retains this cut or its borrowed result bytes.
func (w *cowReadWorkspace) readWithInspection(f *os.File, ptr page.ValuePtr, shape valuelog.COWRecordShape, leaf bool, dst []byte) ([]byte, error) {
	if w.basis != nil && shape.Compressed && (shape.DictID != 0 || (shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4)) {
		return nil, valuelog.ErrCOWReadUnsupported
	}
	if shape.Compressed && shape.DictID == 0 && w.snapshot != nil {
		return w.readSharedRecord(f, ptr, shape, leaf, dst, nil)
	}
	if provider, ok := w.sharedDictionaryProvider(shape); ok {
		definition := cowCacheDictionaryRead{provider: provider, cache: w.snapshot.cowCache}
		return w.readSharedRecord(f, ptr, shape, leaf, dst, &definition)
	}
	return w.readInspected(f, ptr, shape, leaf, dst, nil)
}

// Matching local storage may already be required by a legacy captured-only
// provider. Reuse it first. Otherwise the same independent-definition capability
// serves owned points, Snapshot values, and each independently moving iterator.
func (w *cowReadWorkspace) sharedDictionaryProvider(shape valuelog.COWRecordShape) (cowDictionaryReadProvider, bool) {
	if !shape.Compressed || shape.DictID == 0 || w.snapshot == nil {
		return nil, false
	}
	for _, codec := range w.codecs {
		if codec != nil && codec.id == shape.DictID && codec.codec == shape.Codec && uint64(cap(codec.input)) >= shape.PayloadBytes() && uint64(cap(codec.raw)) >= shape.RawBytes {
			return nil, false
		}
	}
	provider, ok := w.snapshot.db.dictStore.(cowDictionaryReadProvider)
	return provider, ok
}

// cowSuccessorInspection is caller-local metadata, never retained by a slot.
// Its file is pinned by the original basis until the admitted read completes.
type cowSuccessorInspection struct {
	file  *os.File
	shape valuelog.COWRecordShape
}

// Uncompressed physical pointer winners can reuse their basis slot's input and
// output. Refused growth remains explicit until the caller refunds a fresh slot.
// Compressed values and capacity fallbacks hand off this same inspection to the
// original complete-cut resolver without transferring dictionary authority.
func (w *cowReadWorkspace) readUncompressedOwned(ptr page.ValuePtr, inspected *cowSuccessorInspection) ([]byte, bool, error) {
	f, err := w.file(ptr)
	if err != nil {
		return nil, false, err
	}
	shape, err := valuelog.InspectCOWRecord(f, ptr, w.limits)
	if err != nil {
		return nil, false, err
	}
	*inspected = cowSuccessorInspection{file: f, shape: shape}
	if shape.Compressed {
		return nil, false, nil
	}
	value, err := w.readInspected(f, ptr, shape, false, nil, nil)
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), value...), true, nil
}

// The caller's admitted read owns f and the inspected pointer; neither escapes
// this call. Generic codec/input/raw backing belongs to the SAME cache
// across bases. Value copy-out and leaf decode into caller dst finish under its
// mutex; the shared workspace never retains caller authority or output aliases.
func (w *cowReadWorkspace) readSharedRecord(f *os.File, ptr page.ValuePtr, shape valuelog.COWRecordShape, leaf bool, dst []byte, independent *cowCacheDictionaryRead) ([]byte, error) {
	if !leaf {
		if err := w.grow(0, shape.ValueBytes); err != nil {
			return nil, err
		}
	}
	c := w.snapshot.cowCache
	c.physicalMu.Lock()
	defer c.physicalMu.Unlock()
	shared, err := c.physicalWorkspaceLocked(w.limits)
	if err != nil {
		return nil, err
	}
	if leaf {
		// ReadCOWLeafPage returns only the original caller dst.
		return shared.readInspected(f, ptr, shape, true, dst, independent)
	}
	// The maintained decoder verifies the whole record before append. Decode
	// and copy finish before unlock, into this iterator/Snapshot's admitted
	// output rather than an alias of shared decoder or shared output storage.
	return shared.appendInspected(f, ptr, shape, w.output[:0], independent)
}

// readInspected shares the maintained admission/codec/CRC path. The inspector
// and its caller's policy have already validated this exact file and shape.
func (w *cowReadWorkspace) readInspected(f *os.File, ptr page.ValuePtr, shape valuelog.COWRecordShape, leaf bool, dst []byte, independent *cowCacheDictionaryRead) ([]byte, error) {
	outputNeed := shape.ValueBytes
	if leaf {
		outputNeed = 0
	}
	input, raw, codec, err := w.prepareRead(shape, outputNeed, independent)
	if err != nil {
		return nil, err
	}
	var decode valuelog.COWDecodeFunc
	if codec != nil {
		decode = codec.decoder.Decode
	}
	if leaf {
		return valuelog.ReadCOWLeafPage(f, ptr, shape, true, input, raw, dst, decode)
	}
	return valuelog.ReadCOWRecord(f, ptr, shape, true, input, raw, w.output, decode)
}

// prepareRead preserves one admission/codec path for borrowed materialization
// and synchronous owned copy-out. The latter needs no retained output buffer;
// input/raw and dictionary/decoder ownership are unchanged.
func (w *cowReadWorkspace) prepareRead(shape valuelog.COWRecordShape, outputNeed uint64, independent *cowCacheDictionaryRead) ([]byte, []byte, *cowReadCodec, error) {
	var codec *cowReadCodec
	if shape.Compressed {
		var err error
		codec, err = w.codecWithDictionary(shape, independent)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	inputNeed := shape.PayloadBytes()
	if codec != nil {
		inputNeed = 0
	}
	if err := w.grow(inputNeed, outputNeed); err != nil {
		return nil, nil, nil, err
	}
	input, raw := w.input, []byte(nil)
	if codec != nil {
		input, raw = codec.input, codec.raw
	}
	return input, raw, codec, nil
}

// appendPointer finishes the owned public point result before releasing the
// exact cut or its original decoder owner. It never changes borrowed Snapshot,
// iterator, View or leaf materialization and retains no caller destination.
func (w *cowReadWorkspace) appendPointer(ptr page.ValuePtr, dst []byte) ([]byte, error) {
	f, err := w.file(ptr)
	if err != nil {
		return dst, err
	}
	shape, err := valuelog.InspectCOWRecord(f, ptr, w.limits)
	if err != nil {
		return dst, err
	}
	return w.appendWithInspection(f, ptr, shape, dst)
}

func (w *cowReadWorkspace) appendWithInspection(f *os.File, ptr page.ValuePtr, shape valuelog.COWRecordShape, dst []byte) ([]byte, error) {
	if w.basis != nil && shape.Compressed && (shape.DictID != 0 || (shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4)) {
		return dst, valuelog.ErrCOWReadUnsupported
	}
	provider, dictionary := w.sharedDictionaryProvider(shape)
	if (shape.Compressed && shape.DictID == 0 && w.snapshot != nil) || dictionary {
		c := w.snapshot.cowCache
		c.physicalMu.Lock()
		defer c.physicalMu.Unlock()
		shared, err := c.physicalWorkspaceLocked(w.limits)
		if err != nil {
			return dst, err
		}
		if dictionary {
			definition := cowCacheDictionaryRead{provider: provider, cache: c}
			return shared.appendInspected(f, ptr, shape, dst, &definition)
		}
		return shared.appendInspected(f, ptr, shape, dst, nil)
	}
	return w.appendInspected(f, ptr, shape, dst, nil)
}

// readOwnedSuccessor inspects through the exact cut and completes caller
// copy-out while its original physical owner is locked. Shared dictionary
// storage owns its definition independently of this cut; no borrowed output or
// file capability escapes into that storage.
func (w *cowReadWorkspace) readOwnedSuccessor(ptr page.ValuePtr, leaf bool, dst []byte, inspected *cowSuccessorInspection) ([]byte, error) {
	var f *os.File
	var shape valuelog.COWRecordShape
	var err error
	if inspected != nil && inspected.file != nil {
		f, shape = inspected.file, inspected.shape
	} else {
		f, err = w.file(ptr)
		if err != nil {
			return nil, err
		}
		if leaf {
			shape, err = valuelog.InspectCOWLeafRecord(f, ptr, w.limits)
		} else {
			shape, err = valuelog.InspectCOWRecord(f, ptr, w.limits)
		}
		if err != nil {
			return nil, err
		}
	}
	if !shape.Compressed || shape.DictID == 0 {
		value, err := w.readWithInspection(f, ptr, shape, leaf, dst)
		if err != nil || leaf {
			return value, err
		}
		return append(dst, value...), nil
	}
	if leaf {
		return w.readWithInspection(f, ptr, shape, true, dst)
	}
	return w.appendWithInspection(f, ptr, shape, dst)
}

func (w *cowReadWorkspace) readOwnedSuccessorLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	if w.snapshot != nil {
		return w.snapshot.cowCut.basis.readPhysicalLeafWithWorkspace(w.snapshot.cowCache, w, ptr, dst, false, true)
	}
	return w.readOwnedSuccessor(ptr.ValuePtr(), true, dst, nil)
}

func (w *cowReadWorkspace) appendInspected(f *os.File, ptr page.ValuePtr, shape valuelog.COWRecordShape, dst []byte, independent *cowCacheDictionaryRead) ([]byte, error) {
	input, raw, codec, err := w.prepareRead(shape, 0, independent)
	if err != nil {
		return dst, err
	}
	var decode valuelog.COWDecodeFunc
	if codec != nil {
		decode = codec.decoder.Decode
	}
	return valuelog.AppendCOWRecord(f, ptr, shape, true, input, raw, dst, decode)
}

func (w *cowReadWorkspace) readLeaf(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	return w.read(ptr.ValuePtr(), true, dst)
}

// Captured-only dictionary providers still require the complete caller cut.
// This private signal preserves their original Snapshot admission/fallback;
// independently retained definitions can finish the original basis traversal.
var errCOWPhysicalDictionary = errors.New("cow physical leaf requires cut dictionary")

// Only the synchronous basis-point caller needs to hand dictionary admission
// back to its original complete cut. Other callers retain ordinary capacity
// matching through Unwrap and their existing local fallback/refund policy.
type cowPhysicalDictionaryCapacity struct{ cause error }

func (e *cowPhysicalDictionaryCapacity) Error() string { return e.cause.Error() }
func (e *cowPhysicalDictionaryCapacity) Unwrap() error { return e.cause }

// cowReadMu owns this caller's admitted scratch; the retained cut owns the
// exact physical basis throughout traversal and copy-out.
func (s *Snapshot) cowPhysicalLeafLocked(ptr page.LeafLogPtr, dst []byte) ([]byte, error) {
	w, err := s.cowWorkspaceLocked()
	if err != nil {
		return nil, err
	}
	return s.cowCut.basis.readPhysicalLeafWithWorkspace(s.cowCache, w, ptr, dst, false, true)
}

// basis.readMu protects traversal dst. Physical decoding takes physicalMu only
// after that mutex; the cache scratch never takes a basis lock in return.
func (b *cowBackendBasis) readPhysicalLeafLocked(c *cowCache, ptr page.LeafLogPtr, dst []byte, native bool) ([]byte, error) {
	return b.readPhysicalLeafWithWorkspace(c, b.reader, ptr, dst, native, false)
}

// Caller-owned workspaces hold no new physical authority. Small native block
// decoders may belong to the caller; generic codecs retain the original shared
// envelope and admission policy. Every path still inspects before a cache hit.
func (b *cowBackendBasis) readPhysicalLeafWithWorkspace(c *cowCache, w *cowReadWorkspace, ptr page.LeafLogPtr, dst []byte, native, localBlock bool) ([]byte, error) {
	var independent *cowCacheDictionaryRead
	if w.snapshot != nil {
		if provider, ok := w.snapshot.db.dictStore.(cowDictionaryReadProvider); ok {
			definition := cowCacheDictionaryRead{provider: provider, cache: c}
			independent = &definition
		}
	}
	return b.readPhysicalLeafWithDictionary(c, w, ptr, dst, native, localBlock, independent)
}

// independent is borrowed only for this traversal. Neither the basis nor its
// reusable scratch retains a caller/provider; the shared codec owns its checked
// definition independently. Captured-only providers keep their local decoder.
func (b *cowBackendBasis) readPhysicalLeafWithDictionary(c *cowCache, w *cowReadWorkspace, ptr page.LeafLogPtr, dst []byte, native, localBlock bool, independent *cowCacheDictionaryRead) ([]byte, error) {
	f, identity, err := b.snapshot.PinnedValueLogFile(ptr.ValuePtr().FileID)
	if err != nil {
		return nil, err
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), w.limits)
	if err != nil {
		return nil, err
	}
	if native && shape.Compressed && (shape.DictID != 0 || (shape.Codec != valuelog.BlockCodecSnappy && shape.Codec != valuelog.BlockCodecLZ4)) {
		return nil, valuelog.ErrCOWReadUnsupported
	}
	if shape.DictID != 0 && independent == nil {
		if w.snapshot == nil {
			return nil, errCOWPhysicalDictionary
		}
		// Without independently retained definition capability, preserve the
		// complete cut's dictionary policy and local admission before any hit.
		return w.readInspected(f, ptr.ValuePtr(), shape, true, dst, nil)
	}
	// Ordinary COW limits are fixed for this cache. A fully CRC/producer-
	// verified page is sufficient immutable authority without retaining a
	// decoder or recapturing its dictionary on every hit. Private callers using
	// different limits must pass their original decode/definition policy again.
	if shape.DictID == 0 || w.limits == c.readLimits() {
		if data, hit, err := b.snapshot.CopyVerifiedCachedLeafPage(ptr, f, identity, dst); err != nil || hit {
			return data, err
		}
	}
	var data []byte
	// Reuse a matching local dictionary codec first, as on the original
	// borrowed path. Otherwise the shared generic owner supplies decode storage.
	localDictionary := false
	if shape.DictID != 0 && w.snapshot != nil {
		_, shared := w.sharedDictionaryProvider(shape)
		localDictionary = !shared
	}
	if shape.Compressed && !localDictionary && !(localBlock && shape.DictID == 0 && (shape.Codec == valuelog.BlockCodecSnappy || shape.Codec == valuelog.BlockCodecLZ4)) {
		c.physicalMu.Lock()
		shared, workspaceErr := c.physicalWorkspaceLocked(w.limits)
		if workspaceErr != nil {
			c.physicalMu.Unlock()
			if shape.DictID != 0 && errors.Is(workspaceErr, memtable.ErrCOWCapacity) {
				return nil, &cowPhysicalDictionaryCapacity{cause: workspaceErr}
			}
			return nil, workspaceErr
		}
		data, err = shared.readInspected(f, ptr.ValuePtr(), shape, true, dst, independent)
		c.physicalMu.Unlock()
	} else {
		data, err = w.readInspected(f, ptr.ValuePtr(), shape, true, dst, nil)
	}
	if err != nil {
		if shape.DictID != 0 && errors.Is(err, memtable.ErrCOWCapacity) {
			return nil, &cowPhysicalDictionaryCapacity{cause: err}
		}
		return nil, err
	}
	if err = b.snapshot.AdmitVerifiedCachedLeafPage(ptr, f, identity, data); err != nil {
		return nil, err
	}
	return data, nil
}

// Physical entries borrow this snapshot's admitted page scratch. cowReadMu
// excludes its reuse until the existing caller has completed copy-out.
func (s *Snapshot) cowPhysicalEntryLocked(key []byte) (node.LeafEntry, error) {
	basis := s.cowCut.basis
	w, err := s.cowWorkspaceLocked()
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

// cowOwnedPointAppendOpen is only the DB point-read continuation. Exported
// Snapshot APIs keep their original borrowed materialization/admission route.
func (s *Snapshot) cowOwnedPointAppendOpen(key, dst []byte) ([]byte, page.EntryRevision, error) {
	s.cowReadMu.Lock()
	defer s.cowReadMu.Unlock()
	entry, err := s.cowEntryLocked(key)
	if err != nil {
		return dst, 0, err
	}
	return s.cowAppendPointEntryLocked(entry, dst)
}

func (s *Snapshot) cowAppendPointEntryLocked(entry node.LeafEntry, dst []byte) ([]byte, page.EntryRevision, error) {
	if entry.Flags&node.FlagTombstone != 0 {
		return dst, entry.Revision, tree.ErrKeyNotFound
	}
	if entry.Flags&node.FlagPointer != 0 {
		w, err := s.cowWorkspaceLocked()
		if err != nil {
			return dst, entry.Revision, err
		}
		out, err := w.appendPointer(entry.ValuePtr, dst)
		return out, entry.Revision, err
	}
	return append(dst, entry.Value...), entry.Revision, nil
}

// cowValueLocked borrows one winning value under cowReadMu and beginRead.
func (s *Snapshot) cowValueLocked(key []byte) ([]byte, page.EntryRevision, error) {
	entry, err := s.cowEntryLocked(key)
	if err != nil {
		return nil, 0, err
	}
	return s.cowValueFromEntryLocked(entry)
}

// cowValueFromEntryLocked materializes metadata already selected from this
// snapshot's exact cut. The caller holds cowReadMu and the cache read gate.
func (s *Snapshot) cowValueFromEntryLocked(entry node.LeafEntry) ([]byte, page.EntryRevision, error) {
	if entry.Flags&node.FlagTombstone != 0 {
		return nil, entry.Revision, tree.ErrKeyNotFound
	}
	if entry.Flags&node.FlagPointer != 0 {
		w, workspaceErr := s.cowWorkspaceLocked()
		if workspaceErr != nil {
			return nil, entry.Revision, workspaceErr
		}
		value, err := w.read(entry.ValuePtr, false, nil)
		return value, entry.Revision, err
	}
	return entry.Value, entry.Revision, nil
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
