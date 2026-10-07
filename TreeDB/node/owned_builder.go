package node

import (
	"errors"
	"math"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/page"
)

var ErrOwnedBuilderScratch = errors.New("node: owned builder scratch limit")

type ownedBuilderScratch struct {
	typ                                       page.PageType
	opts                                      BuilderOptions
	maxKey                                    int
	err                                       error
	leafEntries                               []leafColumnarV2Entry
	prefixEntries                             []leafColumnarPrefixV2Entry
	internalEntries                           []internalBaseDeltaEntry
	keyArena, valueArena, previous, low, high []byte
}

// NewOwnedBuilderWithOptions charges visible backing before allocation. The
// enclosing owner already owns data and must prove its global builder lifetime.
// No shared pool or backing growth is allowed. maxKey includes reconstructed
// old keys and all new keys. Heap rounding is a separate whole-call bound.
func NewOwnedBuilderWithOptions(data []byte, typ page.PageType, opts BuilderOptions, maxKey int, reserve func(uint64) error) (*Builder, error) {
	return NewOwnedBuilderWithEntryLimit(data, typ, opts, maxKey, math.MaxInt, reserve)
}

// NewOwnedBuilderWithEntryLimit narrows typed scratch to the admitted producer's
// remaining entry count, capped by the existing format geometry. Every split
// gets its own exact remaining-count admission; Reset retains this capacity and
// cannot replenish it. The caller must prove the count before allocation.
func NewOwnedBuilderWithEntryLimit(data []byte, typ page.PageType, opts BuilderOptions, maxKey, maxEntries int, reserve func(uint64) error) (*Builder, error) {
	if maxEntries < 0 || reserve == nil || len(data) != page.PageSize || cap(data) != page.PageSize || maxKey < 0 || maxKey > math.MaxUint16 ||
		(typ != page.PageTypeLeaf && typ != page.PageTypeInternal) {
		return nil, ErrOwnedBuilderScratch
	}
	var leafCount, prefixCount, internalCount, arenaSize, valueSize, previousSize, fenceSize int
	if typ == page.PageTypeLeaf && opts.LeafColumnar {
		arenaSize = page.PageSize
		if opts.LeafPrefixCompression {
			// Prefix-columnar Add admits at least seven metadata bytes per slot.
			prefixCount, valueSize = (page.PageSize-NodeHeaderSize)/leafColumnarPrefixV2MetaSize, page.PageSize
		} else {
			// Non-prefix columnar Add charges metadata plus its offset slot.
			leafCount = (page.PageSize - NodeHeaderSize) / (leafColumnarV2MetaSize + DirectoryEntrySize)
		}
	}
	if typ == page.PageTypeLeaf && opts.LeafPrefixCompression {
		previousSize = maxKey
	}
	if typ == page.PageTypeInternal {
		fenceSize = maxKey
	}
	if typ == page.PageTypeInternal && opts.InternalBaseDelta {
		// Base-delta Add reserves its footer, two-byte key length,
		// minimum two-byte child delta and one directory slot per entry.
		// Common-prefix compression can remove all suffix bytes, so full
		// reconstructed keys still need internalCount*maxKey owned backing.
		internalCount, fenceSize = (page.PageSize-NodeHeaderSize-internalBaseDeltaFooterTailSize)/(DirectoryEntrySize+2+2), maxKey
		if maxKey > math.MaxInt/internalCount {
			return nil, ErrOwnedBuilderScratch
		}
		arenaSize = internalCount * maxKey // full keys can exceed the compressed page
	}
	if leafCount > maxEntries {
		leafCount = maxEntries
	}
	if prefixCount > maxEntries {
		prefixCount = maxEntries
	}
	if internalCount > maxEntries {
		internalCount = maxEntries
		arenaSize = internalCount * maxKey // multiplication already checked at geometric ceiling
	}
	parts := [...]struct {
		count int
		size  uintptr
	}{
		{leafCount, unsafe.Sizeof(leafColumnarV2Entry{})},
		{prefixCount, unsafe.Sizeof(leafColumnarPrefixV2Entry{})},
		{internalCount, unsafe.Sizeof(internalBaseDeltaEntry{})},
		{arenaSize, 1}, {valueSize, 1}, {previousSize, 1}, {fenceSize, 1}, {fenceSize, 1},
	}
	total := uint64(unsafe.Sizeof(Builder{})) + uint64(unsafe.Sizeof(ownedBuilderScratch{}))
	for _, part := range parts {
		if part.count < 0 || uint64(part.count) > uint64(math.MaxInt)/uint64(part.size) {
			return nil, ErrOwnedBuilderScratch
		}
		n := uint64(part.count) * uint64(part.size)
		if n > math.MaxUint64-total {
			return nil, ErrOwnedBuilderScratch
		}
		total += n
	}
	if err := reserve(total); err != nil {
		return nil, err
	}
	s := &ownedBuilderScratch{typ: typ, opts: opts, maxKey: maxKey}
	if leafCount != 0 {
		s.leafEntries = make([]leafColumnarV2Entry, 0, leafCount)
	}
	if prefixCount != 0 {
		s.prefixEntries = make([]leafColumnarPrefixV2Entry, 0, prefixCount)
	}
	if internalCount != 0 {
		s.internalEntries = make([]internalBaseDeltaEntry, 0, internalCount)
	}
	if arenaSize != 0 {
		s.keyArena = make([]byte, 0, arenaSize)
	}
	if valueSize != 0 {
		s.valueArena = make([]byte, 0, valueSize)
	}
	if previousSize != 0 {
		s.previous = make([]byte, previousSize)
	}
	if fenceSize != 0 {
		s.low = make([]byte, 0, fenceSize)
		s.high = make([]byte, 0, fenceSize)
	}
	b := &Builder{ownedScratch: s}
	b.ResetWithOptions(data, typ, opts)
	return b, nil
}

// OwnedScratch prevents transferring an isolated builder to a generic pool.
func (b *Builder) OwnedScratch() bool { return b != nil && b.ownedScratch != nil }

func (s *ownedBuilderScratch) clear() {
	clear(s.leafEntries[:cap(s.leafEntries)])
	clear(s.prefixEntries[:cap(s.prefixEntries)])
	clear(s.internalEntries[:cap(s.internalEntries)])
	clear(s.keyArena[:cap(s.keyArena)])
	clear(s.valueArena[:cap(s.valueArena)])
	clear(s.previous)
	clear(s.low[:cap(s.low)])
	clear(s.high[:cap(s.high)])
}

func (b *Builder) bindOwnedScratch(typ page.PageType, opts BuilderOptions) {
	s := b.ownedScratch
	if s == nil {
		return
	}
	original, current := s.opts, opts
	original.EntryRevisions, current.EntryRevisions = false, false
	if typ != s.typ || current != original || len(b.data) != page.PageSize || cap(b.data) != page.PageSize {
		s.err = ErrOwnedBuilderScratch
		return
	}
	b.leafColumnarV2Entries = s.leafEntries[:0]
	b.leafColumnarPrefixV2Entries = s.prefixEntries[:0]
	if typ == page.PageTypeLeaf && opts.LeafColumnar && opts.LeafPrefixCompression {
		// Match the generic constructor's classification before the first Add.
		b.leafColumnarPrefixV2AllInline = true
		b.leafColumnarPrefixV2AllPointer = true
	}
	b.internalBaseEntries = s.internalEntries[:0]
	b.internalBaseArena = s.keyArena[:0]
	if typ == page.PageTypeLeaf {
		b.leafColumnarV2Arena = s.keyArena[:0]
	}
	b.leafColumnarPrefixV2ValueArena = s.valueArena[:0]
	if opts.LeafPrefixCompression {
		b.leafPrevKey = s.previous[:0]
	}
	b.internalFenceLow, b.internalFenceHigh = s.low[:0], s.high[:0]
}

func (b *Builder) ownedKeyCheck(key []byte) error {
	if b.ownedScratch == nil {
		return nil
	}
	if b.ownedScratch.err != nil {
		return b.ownedScratch.err
	}
	if len(key) > b.ownedScratch.maxKey {
		return ErrOwnedBuilderScratch
	}
	return nil
}

// CloseOwnedScratch is called only after output consumers have drained. It
// clears and drops every owned scratch backing while retaining a sticky closed
// marker, so Reset cannot regain a generic pool path.
func (b *Builder) CloseOwnedScratch() {
	if b == nil || b.ownedScratch == nil {
		return
	}
	b.ReleaseScratch()
	s := b.ownedScratch
	s.leafEntries, s.prefixEntries, s.internalEntries = nil, nil, nil
	s.keyArena, s.valueArena, s.previous, s.low, s.high = nil, nil, nil, nil, nil
	s.err = ErrOwnedBuilderScratch
}

// OwnedScratchError must be checked before finalizing output when an owned
// Reset/fence setter has no error return in the existing Builder API.
func (b *Builder) OwnedScratchError() error {
	if b == nil || b.ownedScratch == nil {
		return nil
	}
	return b.ownedScratch.err
}
