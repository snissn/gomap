package valuelog

import (
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"math"
	"os"
	"unsafe"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/gomap/TreeDB/page"
)

// ErrFiniteWriterLoan refuses an unowned writer route. This component does not
// certify codec/rotation/registry admission for the public prepared publisher.
var ErrFiniteWriterLoan = errors.New("valuelog: finite writer loan refused")

type finiteWriterEntry struct {
	writer     *Writer // ordinary prototype only; selected collector loans keep this nil
	owner      *rootpublication.StableSegmentOwner
	capacities [11]uint64 // full charged classes, never capacity deltas
	serials    [11]uint64
}

// FiniteWriterBacking owns one request's concrete writer-capacity ledger. The
// caller serializes all accesses with the existing lane mutex. This is not a
// writer lock, registry, codec selector, or public admission certificate.
type FiniteWriterBacking struct {
	entries     []finiteWriterEntry
	collector   *rootpublication.StableSegmentFrontierCollector
	activeLoans uint64
	selected    bool
	terminal    bool
	maxWriters  uint64
	reserve     func(uint64) error
	bytes       uint64
	closed      bool
}

func NewFiniteWriterBacking(maxWriters uint64, reserve func(uint64) error) (*FiniteWriterBacking, error) {
	size := uint64(unsafe.Sizeof(finiteWriterEntry{}))
	if reserve == nil || maxWriters == 0 || maxWriters > uint64(math.MaxInt)/size {
		return nil, ErrFiniteWriterLoan
	}
	n, err := rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(FiniteWriterBacking{})), true)
	if err != nil {
		return nil, err
	}
	if err := reserve(n); err != nil {
		return nil, err
	}
	return &FiniteWriterBacking{maxWriters: maxWriters, reserve: reserve, bytes: n}, nil
}
func (b *FiniteWriterBacking) charge(n uint64) error {
	if b == nil || b.closed || n > math.MaxUint64-b.bytes {
		return ErrFiniteWriterLoan
	}
	if n == 0 {
		return nil
	}
	if err := b.reserve(n); err != nil {
		return err
	}
	b.bytes += n
	return nil
}

// BindSegmentCollector narrows retained ledger keys to the real physical
// owner already anchored by this same request's collector. No Writer or producer
// callback is retained after a scoped append loan ends.
func (b *FiniteWriterBacking) chargeAllocation(n uint64, scan bool) error {
	class, err := rootpublication.StableBackingClassBytes(n, scan)
	if err != nil {
		return err
	}
	return b.charge(class)
}
func (b *FiniteWriterBacking) BindSegmentCollector(c *rootpublication.StableSegmentFrontierCollector) error {
	if b == nil || b.closed || c == nil || b.collector != nil || len(b.entries) != 0 || b.activeLoans != 0 {
		return ErrFiniteWriterLoan
	}
	b.collector = c
	b.selected = true
	return nil
}

// ForgetSegmentKeys runs before collector Freeze/Close transfers or drops its
// actual source edges. Allocated ledger capacity and cumulative credit remain.
func (b *FiniteWriterBacking) ForgetSegmentKeys() error {
	if b == nil || b.closed || b.activeLoans != 0 {
		return ErrFiniteWriterLoan
	}
	if b.collector != nil {
		clear(b.entries)
		b.entries = b.entries[:0]
		b.collector = nil
		b.terminal = true
	}
	return nil
}

func (b *FiniteWriterBacking) BackingBytes() uint64 {
	if b == nil {
		return 0
	}
	return b.bytes
}
func (b *FiniteWriterBacking) Close() error {
	if b == nil || b.closed {
		return nil
	}
	if b.activeLoans != 0 {
		return ErrFiniteWriterLoan
	}
	for _, e := range b.entries {
		if e.writer != nil && e.writer.finiteLoan != nil {
			return ErrFiniteWriterLoan
		}
	}
	clear(b.entries)
	b.entries = nil
	b.reserve = nil
	b.collector = nil
	b.closed = true
	return nil
}

// FiniteWriterLoan borrows persistent writer scratch under lane.vlogMu. Ending a
// loan clears only the scoped authority, never pending append bytes or writer
// buffers. Constructor/file/rotation metadata is an additional caller debit.
type FiniteWriterLoan struct {
	writer             *Writer
	backing            *FiniteWriterBacking
	entry              int
	maxRecords, maxRaw int
	closed             bool
}

// BeginFiniteWriterLoan binds a concrete file writer after authoritative reload.
// maxRecords and maxRaw derive from the admitted actual append batch. It does
// not allocate a fresh append buffer: existing persistent backing is retained.
func (w *Writer) BeginFiniteWriterLoan(b *FiniteWriterBacking, maxRecords, maxRaw int) (*FiniteWriterLoan, error) {
	if w == nil || w.f == nil || w.bw != nil || w.finiteLoan != nil || b == nil || b.closed || b.terminal || maxRecords < 1 || maxRecords > MaxFrameK || maxRaw < 0 || maxRaw > maxRecords*page.PageSize {
		return nil, ErrFiniteWriterLoan
	}
	var owner *rootpublication.StableSegmentOwner
	if b.collector != nil {
		owner = w.stableSegmentOwner
		if !b.collector.HasRetainedSegmentOwner(owner) {
			return nil, ErrFiniteWriterLoan
		}
	}
	if _, err := w.finiteWriterAllocationCensus(b.selected); err != nil {
		return nil, err
	}
	if w.codecs != nil || w.dictEncoder != nil {
		return nil, ErrFiniteWriterLoan
	}
	i := -1
	for j := range b.entries {
		if owner != nil && b.entries[j].owner == owner || owner == nil && b.entries[j].writer == w {
			i = j
			break
		}
	}
	if i < 0 {
		if err := b.admitEntrySlot(); err != nil {
			return nil, err
		}
		entry := finiteWriterEntry{owner: owner}
		if owner == nil {
			entry.writer = w
		}
		b.entries = append(b.entries, entry)
		i = len(b.entries) - 1
	}
	if err := b.chargeAllocation(uint64(unsafe.Sizeof(FiniteWriterLoan{})), true); err != nil {
		return nil, err
	}
	l := &FiniteWriterLoan{writer: w, backing: b, entry: i, maxRecords: maxRecords, maxRaw: maxRaw}
	if err := l.observe(); err != nil {
		return nil, err
	}
	// The writer is exclusively borrowed and these tables are inactive scratch.
	// Clearing full capacities drops historical caller aliases, including tails
	// left by a shorter previous writev. Actual table backing remains charged.
	clear(w.rawWritevIovs[:cap(w.rawWritevIovs)])
	clear(w.rawWritevVecs[:cap(w.rawWritevVecs)])
	w.encLimiter.buf = nil
	w.encLimiter.limit = 0
	if b.activeLoans == math.MaxUint64 {
		return nil, ErrFiniteWriterLoan
	}
	b.activeLoans++
	w.finiteLoan = l
	return l, nil
}
func (l *FiniteWriterLoan) Close() error {
	if l == nil || l.closed {
		return nil
	}
	if l.writer == nil || l.writer.finiteLoan != l {
		return ErrFiniteWriterLoan
	}
	if l.backing == nil || l.backing.activeLoans == 0 {
		return ErrFiniteWriterLoan
	}
	l.backing.activeLoans--
	l.writer.finiteLoan = nil
	l.writer = nil
	l.backing = nil
	l.closed = true
	return nil
}
func (l *FiniteWriterLoan) observe() error {
	census, err := l.writer.finiteWriterAllocationCensus(l.backing.selected)
	if err != nil {
		return err
	}
	e := &l.backing.entries[l.entry]
	for i, stamp := range census {
		if stamp.class == 0 {
			continue
		}
		if stamp.serial == 0 || stamp.serial != e.serials[i] || stamp.class > e.capacities[i] {
			// A new actual allocation is charged in full while preceding backing may
			// remain live. An equal class replacement is still a distinct birth.
			if err := l.backing.charge(stamp.class); err != nil {
				return err
			}
			e.serials[i], e.capacities[i] = stamp.serial, stamp.class
		}
	}
	return nil
}

func (l *FiniteWriterLoan) grow(index int, p *[]byte, n int) error {
	if n < 0 || n > math.MaxInt {
		return ErrFiniteWriterLoan
	}
	if cap(*p) >= n {
		return nil
	}
	// Cumulative allocation admits the whole new array while old backing remains
	// reachable; it does not merely debit newCapacity-oldCapacity.
	if err := l.backing.chargeAllocation(uint64(n), false); err != nil {
		return err
	}
	next := make([]byte, len(*p), n)
	copy(next, *p)
	l.writer.setWriterByteBacking(index, p, next)
	stamp := l.writer.backingAllocations[index]
	l.backing.entries[l.entry].capacities[index] = stamp.class
	l.backing.entries[l.entry].serials[index] = stamp.serial
	return nil
}
func (l *FiniteWriterLoan) admitFrame(dictID uint64, dict []byte, records []Record) error {
	if l == nil || l.closed || l.writer == nil || l.writer.finiteLoan != l || dictID != 0 || len(dict) != 0 || len(records) == 0 || len(records) > l.maxRecords {
		return ErrFiniteWriterLoan
	}
	raw := 0
	for _, r := range records {
		if r.RID == 0 || len(r.Value) > page.PageSize || len(r.Value) > l.maxRaw-raw {
			return ErrFiniteWriterLoan
		}
		raw += len(r.Value)
	}
	w := l.writer
	if w.blockCompression && w.blockCodec != BlockCodecSnappy && w.blockCodec != BlockCodecLZ4 {
		return ErrFiniteWriterLoan
	}
	prefix := FrameHeaderSize + 8*len(records) + 4*(len(records)+1)
	record := HeaderSize + prefix + raw
	// A no-dictionary uncompressed fallback may construct the whole record in
	// rawScratch. Provisioning it also covers multi-record raw concatenation.
	if err := l.grow(1, &w.scratch, record); err != nil {
		return err
	}
	if err := l.grow(2, &w.rawScratch, record); err != nil {
		return err
	}
	if err := l.grow(5, &w.prefixBuf, prefix); err != nil {
		return err
	}
	if w.blockCompression {
		bound := snappy.MaxEncodedLen(raw)
		if w.blockCodec == BlockCodecLZ4 {
			bound = lz4.CompressBlockBound(raw)
		}
		if bound < 0 {
			return ErrFiniteWriterLoan
		}
		if err := l.grow(4, &w.blockScratch, bound); err != nil {
			return err
		}
		if w.blockCodec == BlockCodecLZ4 && w.blockCodecScratch.lz4Compressor == nil {
			if err := l.backing.chargeAllocation(uint64(unsafe.Sizeof(lz4.Compressor{})), false); err != nil {
				return err
			}
			w.blockCodecScratch.lz4Compressor = &lz4.Compressor{}
			w.recordWriterBackingBirth(6, uint64(unsafe.Sizeof(lz4.Compressor{})), false)
			w.backingAllocations[6].base = unsafe.Pointer(w.blockCodecScratch.lz4Compressor)
			stamp := w.backingAllocations[6]
			l.backing.entries[l.entry].capacities[6], l.backing.entries[l.entry].serials[6] = stamp.class, stamp.serial
		}
	}
	return l.admitAppend()
}
func (l *FiniteWriterLoan) admitEncoded(body []byte, k int) error {
	if l == nil || l.closed || l.writer == nil || l.writer.finiteLoan != l || k < 1 || k > l.maxRecords || len(body) < FrameHeaderSize+8*k+4*(k+1) || len(body) > FrameHeaderSize+8*k+4*(k+1)+l.maxRaw {
		return ErrFiniteWriterLoan
	}
	// Only the already-selected raw/LZ4/Snappy no-dictionary frame can use this
	// backing. Encoded frame validation still runs in the existing append method.
	if body[0] != FrameVersion || int(body[2]) != k {
		return ErrFiniteWriterLoan
	}
	if body[1] == 0 {
		if body[3] != 0 {
			return ErrFiniteWriterLoan
		}
	} else if body[1] == FrameFlagCompressed {
		if BlockCodec(body[3]) != BlockCodecSnappy && BlockCodec(body[3]) != BlockCodecLZ4 {
			return ErrFiniteWriterLoan
		}
	} else {
		return ErrFiniteWriterLoan
	}
	for i := 0; i < k; i++ {
		if binary.LittleEndian.Uint64(body[FrameHeaderSize+8*i:]) == 0 {
			return ErrFiniteWriterLoan
		}
	}
	for _, b := range body[4:12] {
		if b != 0 {
			return ErrFiniteWriterLoan
		}
	}
	if body[1] != 0 && body[1] != FrameFlagCompressed {
		return ErrFiniteWriterLoan
	}
	offsetStart := FrameHeaderSize + 8*k
	previous := uint32(0)
	for i := 0; i <= k; i++ {
		n := binary.LittleEndian.Uint32(body[offsetStart+4*i:])
		if i == 0 && n != 0 || n < previous || n > uint32(l.maxRaw) || i > 0 && n-previous > page.PageSize {
			return ErrFiniteWriterLoan
		}
		previous = n
	}
	if body[1] == 0 && int(previous) != len(body)-(offsetStart+4*(k+1)) {
		return ErrFiniteWriterLoan
	}
	if err := l.grow(1, &l.writer.scratch, HeaderSize+len(body)); err != nil {
		return err
	}
	return l.admitAppend()
}
func (l *FiniteWriterLoan) admitAppend() error {
	w := l.writer
	max := w.appendMax
	if max <= 0 {
		max = defaultBufferSize
	}
	// Ordinary raw frames always use persistent buffering when they fit. Provision
	// the same buffer only if absent; no pool acquisition or per-request copy.
	if err := l.grow(0, &w.appendBuf, max); err != nil {
		return err
	}
	return nil
}

// NewWriterWithFiniteBacking admits the concrete Writer and prefix array before
// those allocations or opening a file. Namespace/file/registry construction
// backing requires the enclosing cached owner to debit its existing authority
// path as well; this factory alone is not full constructor admission.
func NewWriterWithFiniteBacking(path string, fileID uint32, registry *rootpublication.IdentityPinRegistry, b *FiniteWriterBacking) (*Writer, error) {
	if b == nil {
		return nil, ErrFiniteWriterLoan
	}
	return newFileWriter(path, fileID, true, func(f *os.File) error { return f.Sync() }, registry, b)
}
func (b *FiniteWriterBacking) admitConstructor() error {
	if b == nil || b.selected || b.terminal {
		return ErrFiniteWriterLoan
	}
	if err := b.admitEntrySlot(); err != nil {
		return err
	}
	if err := b.chargeAllocation(uint64(unsafe.Sizeof(Writer{})), true); err != nil {
		return err
	}
	return b.chargeAllocation(uint64(FrameHeaderSize+MaxFrameK*8+(MaxFrameK+1)*4), false)
}
func (b *FiniteWriterBacking) recordConstructor(w *Writer) {
	// admitConstructor proves the existing table has room; this performs no
	// allocation and is invoked before further constructor allocations.
	e := finiteWriterEntry{writer: w}
	e.capacities[5], _ = rootpublication.StableBackingClassBytes(uint64(FrameHeaderSize+MaxFrameK*8+(MaxFrameK+1)*4), false)
	e.capacities[7], _ = rootpublication.StableBackingClassBytes(uint64(unsafe.Sizeof(Writer{})), true)
	b.entries = append(b.entries, e)
}

func (b *FiniteWriterBacking) admitEntrySlot() error {
	if b == nil || b.closed || uint64(len(b.entries)) >= b.maxWriters {
		return ErrFiniteWriterLoan
	}
	if len(b.entries) < cap(b.entries) {
		return nil
	}
	count := uint64(len(b.entries)) + 1
	size := uint64(unsafe.Sizeof(finiteWriterEntry{}))
	if count > uint64(math.MaxInt)/size {
		return ErrFiniteWriterLoan
	}
	if err := b.chargeAllocation(count*size, true); err != nil {
		return err
	}
	next := make([]finiteWriterEntry, len(b.entries), int(count))
	copy(next, b.entries)
	clear(b.entries[:cap(b.entries)])
	b.entries = next
	return nil
}

// admitRawBatch follows the same adaptive writev/buffered decision as the
// ordinary writer. The scratch widths below are derived from its existing
// maxIovs/minimum-meta policy and the actual admitted frame partition.
func (l *FiniteWriterLoan) admitRawBatch(records []Record, k int, buffered bool) error {
	if l == nil || l.closed || l.writer == nil || l.writer.finiteLoan != l || l.writer.blockCompression || k < 1 || k > MaxFrameK || len(records) == 0 || len(records) > l.maxRecords {
		return ErrFiniteWriterLoan
	}
	raw := 0
	metaBytes := 0
	for start := 0; start < len(records); {
		end := start + k
		if end > len(records) {
			end = len(records)
		}
		for _, r := range records[start:end] {
			if r.RID == 0 || len(r.Value) > page.PageSize || len(r.Value) > l.maxRaw-raw {
				return ErrFiniteWriterLoan
			}
			raw += len(r.Value)
		}
		if err := l.admitFrame(0, nil, records[start:end]); err != nil {
			return err
		}
		n := HeaderSize + FrameHeaderSize + 8*(end-start) + 4*(end-start+1)
		if n > math.MaxInt-metaBytes {
			return ErrFiniteWriterLoan
		}
		metaBytes += n
		start = end
	}
	w := l.writer
	if buffered {
		if metaBytes < 4096 {
			metaBytes = 4096
		}
		return l.grow(10, &w.rawWritevMeta, metaBytes)
	}
	if !w.shouldUseRawWritev(records, k, raw) {
		return nil
	}
	_, maxIovs := rawWritevLimits(w)
	// A frame must fit the existing writev boundary; refusing this component
	// never substitutes a different K or changes the ordinary strategy.
	if maxIovs < 2 || k > maxIovs-2 {
		return ErrFiniteWriterLoan
	}
	if maxIovs > cap(w.rawWritevIovs) {
		size := uint64(unsafe.Sizeof([]byte{}))
		if uint64(maxIovs) > uint64(math.MaxInt)/size {
			return ErrFiniteWriterLoan
		}
		if err := l.backing.chargeAllocation(uint64(maxIovs)*size, true); err != nil {
			return err
		}
		next := make([][]byte, 0, maxIovs)
		clear(w.rawWritevIovs[:cap(w.rawWritevIovs)])
		w.setWriterIovBacking(next)
		stamp := w.backingAllocations[8]
		l.backing.entries[l.entry].capacities[8], l.backing.entries[l.entry].serials[8] = stamp.class, stamp.serial
	}
	if maxIovs > cap(w.rawWritevVecs) {
		size := uint64(unsafe.Sizeof(writevIovec{}))
		if uint64(maxIovs) > uint64(math.MaxInt)/size {
			return ErrFiniteWriterLoan
		}
		if err := l.backing.chargeAllocation(uint64(maxIovs)*size, false); err != nil {
			return err
		}
		next := make([]writevIovec, 0, maxIovs)
		clear(w.rawWritevVecs[:cap(w.rawWritevVecs)])
		w.setWriterVecBacking(next)
		stamp := w.backingAllocations[9]
		l.backing.entries[l.entry].capacities[9], l.backing.entries[l.entry].serials[9] = stamp.class, stamp.serial
	}
	if metaBytes < 4096 {
		metaBytes = 4096
	}
	return l.grow(10, &w.rawWritevMeta, metaBytes)
}

func (b *FiniteWriterBacking) finishConstructor(w *Writer) {
	for i := range b.entries {
		e := &b.entries[i]
		if e.writer == w {
			for _, j := range [...]int{5, 7} {
				e.serials[j] = w.backingAllocations[j].serial
				e.capacities[j] = w.backingAllocations[j].class
			}
			return
		}
	}
	panic("valuelog: missing prepaid writer constructor")
}
