package node

import (
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

var ErrBuilderPacking = errors.New("node: builder packing owns scratch")

// The original Builder owns this continuation and its original pooled arenas.
// An unfinished Add borrows the caller's immutable entry until it returns ready.
// An unfinished Finish excludes Add/Reset; ReleaseScratch explicitly abandons it.
type columnarPrefixWork struct {
	packing, packed       bool
	phase                 uint8
	index, valOff, keyOff int
	adding                bool
	probe                 int
	prefixReady           bool
	key, value            []byte
	flags                 byte
	ptr                   page.ValuePtr
	revision              page.EntryRevision
	scratchReady          bool
}

// ResetWithOptionsWithWork admits scratch acquisition before resetting the
// original builder. Pool bounds include the largest backing the original pools
// can retain, even on a cold pool. No old image or scratch is changed on refusal.
func (b *Builder) ResetWithOptionsWithWork(data []byte, kind page.PageType, opts BuilderOptions, w *iterator.OrdinalScanWork) (bool, error) {
	if b.columnarWork.packing || b.columnarWork.adding {
		return false, ErrBuilderPacking
	}
	bytes := uint64(unsafe.Sizeof(Builder{}))
	records := uint64(1)
	if kind == page.PageTypeLeaf && opts.LeafColumnar && opts.LeafPrefixCompression {
		bytes += uint64(leafColumnarEntriesPoolMaxCap) * uint64(unsafe.Sizeof(leafColumnarPrefixV2Entry{}))
		records += 2 // pool acquisition and possible cold backing allocation
	}
	if len(b.internalBaseEntries) != 0 {
		records += uint64(len(b.internalBaseEntries))
		bytes += uint64(len(b.internalBaseEntries)) * uint64(unsafe.Sizeof(internalBaseDeltaEntry{}))
	}
	if ok, e := builderReserve(w, records, bytes); !ok || e != nil {
		return false, e
	}
	b.ResetWithOptions(data, kind, opts)
	return true, nil
}

// Admit the original scratch backing before the original append helpers can
// acquire it. A complete page bounds each byte arena by len(data). Reserving
// the pool's maximum capacity also covers zeroing on a cold pool. Entry capacity
// is established at empty-builder birth, so later appends cannot hide a growing
// metadata copy. A nonempty foreign builder that needs growth refuses honestly.
func (b *Builder) prepareColumnarAddScratch(value []byte, flags byte, w *iterator.OrdinalScanWork) (bool, error) {
	if b.columnarWork.scratchReady && (flags&(FlagPointer|FlagTombstone) != 0 || b.leafColumnarPrefixV2ValueArena != nil || len(value) == 0) {
		return true, nil
	}
	maxEntries := (len(b.data) - NodeHeaderSize) / leafColumnarPrefixV2MetaSize
	needEntries := cap(b.leafColumnarPrefixV2Entries) < maxEntries
	if needEntries && b.count != 0 {
		return false, ErrBuilderWorkFormat
	}
	records, bytes := uint64(0), uint64(0)
	if needEntries {
		records++
		bytes += uint64(maxEntries) * uint64(unsafe.Sizeof(leafColumnarPrefixV2Entry{}))
	}
	needKeys := b.leafColumnarV2Arena == nil
	needValues := flags&(FlagPointer|FlagTombstone) == 0 && len(value) > 0 && b.leafColumnarPrefixV2ValueArena == nil
	if needKeys {
		records += 2
		bytes += builderByteArenaPoolMaxCap + uint64(unsafe.Sizeof(byteArenaHandle{}))
	}
	if needValues {
		records += 2
		bytes += builderByteArenaPoolMaxCap + uint64(unsafe.Sizeof(byteArenaHandle{}))
	}
	if ok, e := builderReserve(w, records, bytes); !ok || e != nil {
		return false, e
	}
	if needEntries {
		b.leafColumnarPrefixV2Entries = make([]leafColumnarPrefixV2Entry, 0, maxEntries)
		if b.leafColumnarPrefixV2EntriesH == nil {
			b.leafColumnarPrefixV2EntriesH = &leafColumnarPrefixV2EntriesHandle{}
		}
	}
	if needKeys {
		h := leafColumnarV2ArenaPool.Get().(*byteArenaHandle)
		if cap(h.buf) < len(b.data) {
			h.buf = make([]byte, 0, len(b.data))
		}
		b.leafColumnarV2ArenaH = h
		b.leafColumnarV2Arena = h.buf[:0]
	}
	if needValues {
		h := leafColumnarPrefixV2ValueArenaPool.Get().(*byteArenaHandle)
		if cap(h.buf) < len(b.data) {
			h.buf = make([]byte, 0, len(b.data))
		}
		b.leafColumnarPrefixV2ValueArenaH = h
		b.leafColumnarPrefixV2ValueArena = h.buf[:0]
	}
	b.columnarWork.scratchReady = true
	return true, nil
}

func sameBuilderBorrow(a, b []byte) bool { return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0]) }

func (b *Builder) addColumnarPrefixWithWork(key, value []byte, flags byte, ptr page.ValuePtr, revision page.EntryRevision, reserveBytes int, w *iterator.OrdinalScanWork) (bool, error) {
	c := &b.columnarWork
	if c.packing || c.packed {
		return false, ErrBuilderPacking
	}
	if !c.adding {
		if ok, e := builderReserve(w, 1, 64); !ok || e != nil {
			return false, e
		}
		c.adding = true
		c.key = key
		c.value = value
		c.flags = flags
		c.ptr = ptr
		c.revision = revision
		c.probe = 0
		c.prefixReady = b.leafIndex%leafPrefixRestartInterval == 0
	} else if !sameBuilderBorrow(key, c.key) || !sameBuilderBorrow(value, c.value) || flags != c.flags || ptr != c.ptr || revision != c.revision {
		return false, ErrBuilderPacking
	}
	if !c.prefixReady {
		n := min(len(key), len(b.leafPrevKey))
		for c.probe < n {
			if ok, e := builderReserve(w, 1, 2); !ok || e != nil {
				return false, e
			}
			if key[c.probe] != b.leafPrevKey[c.probe] {
				c.prefixReady = true
				break
			}
			c.probe++
		}
		if c.probe == n {
			c.prefixReady = true
		}
	}
	prefix := c.probe
	// Exact no-effect capacity check precedes scratch birth and original mutation.
	valSize := 0
	if flags&FlagPointer != 0 {
		valSize = page.ValuePtrSize
		if b.leafPackedValuePtr {
			valSize = page.PackedValuePtrSize
		}
	} else if flags&FlagTombstone == 0 {
		valSize = len(value)
	}
	revisions := b.leafEntryRevisions || revision != page.LegacyEntryRevision
	meta := (int(b.count)+1)*leafColumnarPrefixV2MetaSize + NodeHeaderSize
	if revisions {
		meta += (int(b.count) + 1) * page.EntryRevisionSize
	}
	entrySize := len(key) - prefix + valSize + leafColumnarPrefixV2MetaSize
	if revisions {
		entrySize += page.EntryRevisionSize
	}
	if b.count != 0 && reserveBytes > 0 && b.FreeSpace() < entrySize+DirectoryEntrySize+reserveBytes {
		b.clearColumnarAdd()
		return false, ErrNodeFull
	}
	if len(key)-prefix > int(^uint16(0)) || prefix > int(^uint16(0)) {
		b.clearColumnarAdd()
		return false, ErrKeyTooLarge
	}
	if len(b.data)-b.leafColumnarPrefixV2KeyBytes-(len(key)-prefix)-b.leafColumnarPrefixV2ValBytes-valSize < meta || (!b.leafEntryRevisions && revision != page.LegacyEntryRevision && b.count != 0) {
		b.clearColumnarAdd()
		return false, ErrNodeFull
	}
	if ok, e := b.prepareColumnarAddScratch(value, flags, w); !ok || e != nil {
		return false, e
	}
	// Original append's suffix, value, previous-key copy, metadata and transitions.
	if ok, e := builderReserve(w, 6, uint64(2*len(key)+len(value)+valSize)+uint64(unsafe.Sizeof(leafColumnarPrefixV2Entry{}))+64); !ok || e != nil {
		return false, e
	}
	err := b.AddLeafEntryWithPrefixRevision(key, value, flags, ptr, revision, 0, prefix, len(key)-prefix)
	b.clearColumnarAdd()
	return err == nil, err
}
func (b *Builder) clearColumnarAdd() {
	c := &b.columnarWork
	c.adding = false
	c.key = nil
	c.value = nil
	c.probe = 0
	c.prefixReady = false
	c.ptr = page.ValuePtr{}
	c.revision = 0
	c.flags = 0
}

// packLeafColumnarPrefixV2 is the original combined-V2 packing engine. Both
// ordinary Finish and work-bearing Finish use these same offsets and stores.
// Whole-arena copies are byte-admitted, metadata and pointer stores per entry.
func (b *Builder) packLeafColumnarPrefixV2(w *iterator.OrdinalScanWork) (bool, error) {
	c := &b.columnarWork
	if c.adding {
		return false, ErrBuilderPacking
	}
	if c.packed {
		return true, nil
	}
	count := int(b.count)
	keyDir := NodeHeaderSize
	valDir := keyDir + count*2
	flagDir := valDir + count*2
	prefixDir := flagDir + count
	revDir := prefixDir + count*2
	metaEnd := revDir
	if b.leafEntryRevisions {
		metaEnd += count * page.EntryRevisionSize
	}
	suffixStart := len(b.data) - b.leafColumnarPrefixV2KeyBytes
	valuesStart := suffixStart - b.leafColumnarPrefixV2ValBytes
	if valuesStart < metaEnd {
		return false, ErrCorruptedNode
	}
	if !c.packing {
		if ok, e := builderReserve(w, 1, 64); !ok || e != nil {
			return false, e
		}
		c.packing = true
		c.valOff = valuesStart
		c.keyOff = suffixStart
	}
	if c.phase == 0 {
		if b.leafColumnarPrefixV2AllInline {
			if len(b.leafColumnarPrefixV2ValueArena) != b.leafColumnarPrefixV2ValBytes {
				return false, ErrCorruptedNode
			}
			if ok, e := builderReserve(w, 1, uint64(2*len(b.leafColumnarPrefixV2ValueArena))); !ok || e != nil {
				return false, e
			}
			copy(b.data[valuesStart:suffixStart], b.leafColumnarPrefixV2ValueArena)
		}
		c.phase = 1
	}
	if c.phase == 1 {
		if b.leafColumnarPrefixV2AllInline || b.leafColumnarPrefixV2AllPointer {
			if len(b.leafColumnarV2Arena) != b.leafColumnarPrefixV2KeyBytes {
				return false, ErrCorruptedNode
			}
			if ok, e := builderReserve(w, 1, uint64(2*len(b.leafColumnarV2Arena))); !ok || e != nil {
				return false, e
			}
			copy(b.data[suffixStart:], b.leafColumnarV2Arena)
		}
		c.phase = 2
	}
	for c.phase == 2 && c.index < count {
		end := count
		if w != nil {
			e := &b.leafColumnarPrefixV2Entries[c.index]
			payloadBytes := int(e.suffixLen) + int(e.valueLen) + page.ValuePtrSize
			if ok, err := builderReserve(w, 8, uint64(2*payloadBytes+leafColumnarPrefixV2MetaSize+page.EntryRevisionSize)); !ok || err != nil {
				return false, err
			}
			end = c.index + 1
		}
		b.packColumnarPrefixEntries(c.index, end, keyDir, valDir, flagDir, prefixDir, revDir, suffixStart, valuesStart)
		c.index = end
	}

	if c.phase == 2 {
		if !b.leafColumnarPrefixV2AllInline && (c.valOff != suffixStart || (!b.leafColumnarPrefixV2AllPointer && c.keyOff != len(b.data))) {
			return false, ErrCorruptedNode
		}
		if ok, e := builderReserve(w, 3, 64); !ok || e != nil {
			return false, e
		}
		b.dirEnd = metaEnd
		b.heapStart = valuesStart
		b.releaseLeafColumnarPrefixV2Scratch()
		c.packing = false
		c.packed = true
		c.phase = 3
	}
	return true, nil
}

// The synchronous driver enters once for the entire range; a bounded driver
// enters only for its admitted range. Keep original native-endian directory
// stores, all-inline bulk copies (in the preceding stage), packed pointer loop,
// and mixed payload placement. There is no per-entry Work call on the nil path.
func (b *Builder) packColumnarPrefixEntries(start, end, keyDir, valDir, flagDir, prefixDir, revDir, suffixStart, valuesStart int) {
	c := &b.columnarWork
	entries := b.leafColumnarPrefixV2Entries
	flags := b.data[flagDir:prefixDir]
	var keys, values, prefixes []uint16
	if builderNativeLittleEndian {
		keys = bytesAsUint16Slice(b.data[keyDir:valDir])
		values = bytesAsUint16Slice(b.data[valDir:flagDir])
		if prefixDir&1 == 0 {
			prefixes = bytesAsUint16Slice(b.data[prefixDir:revDir])
		}
	}
	writeMeta := func(i, ko, vo int, e *leafColumnarPrefixV2Entry) {
		if builderNativeLittleEndian {
			keys[i] = uint16(ko)
			values[i] = uint16(vo)
		} else {
			putUint16At(b.data, keyDir+i*2, uint16(ko))
			putUint16At(b.data, valDir+i*2, uint16(vo))
		}
		flags[i] = e.flags
		if prefixes != nil {
			prefixes[i] = e.prefixLen
		} else {
			putUint16At(b.data, prefixDir+i*2, e.prefixLen)
		}
		if b.leafEntryRevisions {
			binary.LittleEndian.PutUint64(b.data[revDir+i*page.EntryRevisionSize:], uint64(e.revision))
		}
	}
	if b.leafColumnarPrefixV2AllInline {
		for i := start; i < end; i++ {
			e := &entries[i]
			writeMeta(i, suffixStart+int(e.suffixOff), valuesStart+int(e.valueOff), e)
		}
		return
	}
	if b.leafColumnarPrefixV2AllPointer {
		for i := start; i < end; i++ {
			e := &entries[i]
			writeMeta(i, suffixStart+int(e.suffixOff), c.valOff, e)
			if b.leafPackedValuePtr {
				encodePackedValuePtrAt(b.data, c.valOff, e.valPtr)
				c.valOff += page.PackedValuePtrSize
			} else {
				encodeValuePtrAt(b.data, c.valOff, e.valPtr)
				c.valOff += page.ValuePtrSize
			}
		}
		return
	}
	for i := start; i < end; i++ {
		e := &entries[i]
		writeMeta(i, c.keyOff, c.valOff, e)
		if e.flags&FlagPointer != 0 {
			if b.leafPackedValuePtr {
				encodePackedValuePtrAt(b.data, c.valOff, e.valPtr)
				c.valOff += page.PackedValuePtrSize
			} else {
				encodeValuePtrAt(b.data, c.valOff, e.valPtr)
				c.valOff += page.ValuePtrSize
			}
		} else if e.flags&FlagTombstone == 0 {
			n := int(e.valueLen)
			copy(b.data[c.valOff:c.valOff+n], b.leafColumnarPrefixV2ValueArena[int(e.valueOff):int(e.valueOff)+n])
			c.valOff += n
		}
		n := int(e.suffixLen)
		copy(b.data[c.keyOff:c.keyOff+n], b.leafColumnarV2Arena[int(e.suffixOff):int(e.suffixOff)+n])
		c.keyOff += n
	}
}
