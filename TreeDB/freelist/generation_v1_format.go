package freelist

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
)

const (
	generationHeaderSize  = 192
	indexHeaderSize       = 80
	indexEntrySize        = 32
	chunkHeaderSize       = 96
	reservationHeaderSize = 176
	reservationEntrySize  = 24
)

var (
	generationMagic  = [8]byte{'F', 'L', 'G', 'E', 'N', 'V', '2', 0}
	indexMagic       = [8]byte{'F', 'L', 'I', 'D', 'X', 'V', '2', 0}
	chunkMagic       = [8]byte{'F', 'L', 'C', 'H', 'K', 'V', '2', 0}
	reservationMagic = [8]byte{'F', 'L', 'R', 'S', 'V', 'V', '1', 0}
)

type PageSource interface {
	ReadPage(pageID uint64) ([]byte, error)
}

type AppendPageSink interface {
	WritePage(pageID uint64, data []byte) error
}

// indexPagePlanV1 owns the canonical fixed-width header and child descriptors.
// It has no state-tree pointers: a retained view cannot keep a generation alive.
type indexPagePlanV1 struct {
	prefix [indexHeaderSize + 16*indexEntrySize]byte
}

// CandidatePageViewV1 is a read-only view of one candidate-owned page. Index
// pages retain an immutable encoding plan; other pages retain owned bytes.
// Ordinary views may be retained indefinitely. Finite generic exports refuse.
type CandidatePageViewV1 struct {
	data  []byte
	index *indexPagePlanV1
}

func (view CandidatePageViewV1) Len() int {
	if view.index != nil {
		return page.PageSize
	}
	return len(view.data)
}

func (view CandidatePageViewV1) CopyTo(dst []byte) error {
	if view.Len() != page.PageSize || len(dst) != page.PageSize {
		return fmt.Errorf("%w: invalid candidate page copy", ErrGenerationFormat)
	}
	if view.index != nil {
		prefix := view.index.prefix[:]
		// The plan owns the exact canonical prefix; all remaining page bytes
		// are logically zero. Validate that complete page before touching dst.
		stored := binary.LittleEndian.Uint32(prefix[8:12])
		if page.CalculateChecksumWithZeroGap(prefix, page.PageSize-len(prefix), nil) != stored {
			return fmt.Errorf("%w: candidate index page %d", ErrGenerationChecksum, binary.LittleEndian.Uint64(prefix[:8]))
		}
		clear(dst)
		copy(dst, prefix)
		return nil
	}
	copy(dst, view.data)
	return nil
}

// WriteCandidatePageToPagerV1 copies an opaque candidate page into pager-owned
// mmap storage. A plan is fully encoded and checked before Pager.Write, which
// holds the pager lock for the complete copy. Neither path retains scratch.
func WriteCandidatePageToPagerV1(dst *pager.Pager, pageID uint64, view CandidatePageViewV1) error {
	if dst == nil {
		return fmt.Errorf("%w: missing candidate pager", ErrGenerationFormat)
	}
	if view.index != nil {
		if pageID != binary.LittleEndian.Uint64(view.index.prefix[:8]) {
			return fmt.Errorf("%w: candidate page identity", ErrGenerationFormat)
		}
		var encoded [page.PageSize]byte
		if err := view.CopyTo(encoded[:]); err != nil {
			return err
		}
		return dst.Write(pageID, encoded[:])
	}
	if len(view.data) != page.PageSize {
		return fmt.Errorf("%w: invalid candidate page copy", ErrGenerationFormat)
	}
	return dst.Write(pageID, view.data)
}

// CandidatePageWriterV1 receives an opaque read-only page view. It may retain
// an ordinary view. Finite candidates use the concrete synchronous pager bridge.
type CandidatePageWriterV1 interface {
	WriteCandidatePageV1(pageID uint64, view CandidatePageViewV1) error
}

type GenerationRefV1 struct {
	HeaderPageID uint64
	GenerationID uint64
	CommitSeq    uint64
	HighWater    uint64
	Digest       [32]byte
}

type PageImageV1 struct {
	PageID uint64
	Data   []byte
}

type MemoryPageStoreV1 struct {
	Pages map[uint64][]byte
	Reads uint64
}

func NewMemoryPageStoreV1() *MemoryPageStoreV1 {
	return &MemoryPageStoreV1{Pages: make(map[uint64][]byte)}
}

// CandidatePageSinkV1 validates ordinary candidate writes without retaining
// their bytes. It allows the materialized candidate to take ownership of each
// freshly encoded non-index page or immutable index plan instead of making a
// validation-store copy.
type CandidatePageSinkV1 struct {
	pageIDs *numericRadixV1[uint64, struct{}]
}

func NewCandidatePageSinkV1() *CandidatePageSinkV1 {
	return &CandidatePageSinkV1{pageIDs: newPageRadixV1[struct{}]()}
}

// ownedCandidatePageSinkV1 has no retained fields or directory. Its exact value
// is accepted only by the materializer, whose recording sink checks the complete
// reserved interval before every callback and before exposing the candidate.
// It never receives physical storage authority.
type ownedCandidatePageSinkV1 struct{}

// NewOwnedCandidatePageSinkV1 selects candidate-owned immutable page images.
// IDs are validated by MaterializeCandidate against its exact reservation;
// callers needing an independent duplicate-checking sink use CandidatePageSinkV1.
func NewOwnedCandidatePageSinkV1() AppendPageSink { return ownedCandidatePageSinkV1{} }

func (ownedCandidatePageSinkV1) WritePage(id uint64, data []byte) error {
	if id == 0 || len(data) != page.PageSize {
		return ErrGenerationFormat
	}
	return nil
}

func candidateOwnedImagesV1(sink AppendPageSink) bool {
	switch sink.(type) {
	case *CandidatePageSinkV1, ownedCandidatePageSinkV1:
		return true
	default:
		return false
	}
}

func (s *CandidatePageSinkV1) WritePage(id uint64, data []byte) error {
	if s == nil || len(data) != page.PageSize {
		return fmt.Errorf("%w: invalid candidate page write", ErrGenerationFormat)
	}
	if s.pageIDs == nil {
		s.pageIDs = newPageRadixV1[struct{}]()
	}
	if _, exists := s.pageIDs.Get(id); exists {
		return fmt.Errorf("%w: page %d rewritten", ErrGenerationFormat, id)
	}
	s.pageIDs.Set(id, struct{}{})
	return nil
}

func (s *MemoryPageStoreV1) WritePage(id uint64, data []byte) error {
	if s == nil || len(data) != page.PageSize {
		return fmt.Errorf("%w: invalid page write", ErrGenerationFormat)
	}
	if _, exists := s.Pages[id]; exists {
		return fmt.Errorf("%w: page %d rewritten", ErrGenerationFormat, id)
	}
	s.Pages[id] = append([]byte(nil), data...)
	return nil
}

func (s *MemoryPageStoreV1) ReadPage(id uint64) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: nil page source", ErrGenerationFormat)
	}
	s.Reads++
	b, ok := s.Pages[id]
	if !ok {
		return nil, fmt.Errorf("%w: missing page %d", ErrGenerationFormat, id)
	}
	return append([]byte(nil), b...), nil
}

func encodePageHeader(dst []byte, id uint64, typ page.PageType, count uint16) {
	header := page.PageHeader{PageID: id, Flags: uint16(typ), Count: count}
	header.Encode(dst)
}

func finishPage(dst []byte) {
	page.UpdateChecksum(dst)
}

func readTypedPage(src PageSource, id uint64, typ page.PageType, highWater uint64) ([]byte, error) {
	if err := validateManagedPageID(id, highWater); err != nil {
		return nil, err
	}
	b, err := src.ReadPage(id)
	if err != nil {
		return nil, err
	}
	if len(b) != page.PageSize {
		return nil, fmt.Errorf("%w: page %d has size %d", ErrGenerationFormat, id, len(b))
	}
	h := page.DecodeHeader(b)
	if h.PageID != id || h.Flags != uint16(typ) {
		return nil, fmt.Errorf("%w: page %d header/type", ErrGenerationFormat, id)
	}
	if !page.VerifyChecksumNonMutating(b) {
		return nil, fmt.Errorf("%w: page %d", ErrGenerationChecksum, id)
	}
	return b, nil
}

func zeroTail(b []byte, used int) bool {
	if used < 0 || used > len(b) {
		return false
	}
	for _, v := range b[used:] {
		if v != 0 {
			return false
		}
	}
	return true
}

func encodeChunkPage(id, generationID uint64, chunk *stateChunk) []byte {
	b := make([]byte, page.PageSize)
	encodePageHeader(b, id, page.PageTypeFreelistChunk, freelistChunkSize)
	copy(b[16:24], chunkMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 2)
	binary.LittleEndian.PutUint16(b[26:28], chunkHeaderSize)
	binary.LittleEndian.PutUint64(b[32:40], generationID)
	binary.LittleEndian.PutUint64(b[40:48], chunk.chunkNo)
	binary.LittleEndian.PutUint32(b[48:52], uint32(chunk.freeCount()))
	retiredCount, minSeq := chunk.retiredSummary()
	binary.LittleEndian.PutUint32(b[52:56], uint32(retiredCount))
	binary.LittleEndian.PutUint64(b[56:64], minSeq)
	for i, word := range chunk.free {
		binary.LittleEndian.PutUint64(b[64+i*8:], word)
	}
	for i, seq := range chunk.retired {
		binary.LittleEndian.PutUint64(b[chunkHeaderSize+i*8:], seq)
	}
	finishPage(b)
	return b
}

func decodeChunkPage(b []byte, maxGeneration, expectedChunk uint64) (*stateChunk, error) {
	pageGeneration := binary.LittleEndian.Uint64(b[32:40])
	if !bytes.Equal(b[16:24], chunkMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 2 || binary.LittleEndian.Uint16(b[26:28]) != chunkHeaderSize || !zeroTail(b[28:32], 0) || pageGeneration == 0 || pageGeneration > maxGeneration {
		return nil, ErrGenerationFormat
	}
	chunkNo := binary.LittleEndian.Uint64(b[40:48])
	if chunkNo >= 1<<56 || chunkNo != expectedChunk || !zeroTail(b, chunkHeaderSize+freelistChunkSize*8) {
		return nil, ErrGenerationFormat
	}
	h := page.DecodeHeader(b)
	if h.Count != freelistChunkSize {
		return nil, ErrGenerationFormat
	}
	c := &stateChunk{ownedRefs: 1, pageID: h.PageID, checksum: h.Checksum, chunkNo: chunkNo}
	for i := range c.free {
		c.free[i] = binary.LittleEndian.Uint64(b[64+i*8:])
	}
	for i := range c.retired {
		c.retired[i] = binary.LittleEndian.Uint64(b[chunkHeaderSize+i*8:])
		if c.retired[i] != 0 && c.isFree(uint64(i)) {
			return nil, ErrGenerationFormat
		}
	}
	retiredCount, minSeq := c.retiredSummary()
	if c.freeCount() != uint64(binary.LittleEndian.Uint32(b[48:52])) || retiredCount != uint64(binary.LittleEndian.Uint32(b[52:56])) || minSeq != binary.LittleEndian.Uint64(b[56:64]) {
		return nil, ErrGenerationFormat
	}
	refreshChunkSummaryV1(c)
	return c, nil
}

func encodeIndexPage(id, generationID uint64, n *stateNode, depth int) ([]byte, error) {
	b := make([]byte, page.PageSize)
	if err := encodeIndexPageInto(b, id, generationID, n, depth); err != nil {
		return nil, err
	}
	return b, nil
}

// encodeIndexPageInto overwrites the complete destination, including reserved
// bytes and unused entries, so a reused buffer encodes the same canonical page.
func encodeIndexPageInto(b []byte, id, generationID uint64, n *stateNode, _ int) error {
	if len(b) != page.PageSize || n == nil || int(n.depth) >= chunkTrieDepth || generationID == 0 {
		return ErrGenerationFormat
	}
	clear(b)
	count := 0
	for _, child := range n.child {
		if child.zero() {
			continue
		}
		if !child.validKind() || child.pageID() == 0 || child.freeCount()+child.retiredCount() == 0 {
			return ErrGenerationFormat
		}
		count++
	}
	if count == 1 || (count == 0 && (n.depth != 0 || n.prefix != 0 || n.freePages+n.retiredPages != 0)) {
		return ErrGenerationFormat
	}
	encodePageHeader(b, id, page.PageTypeFreelistIndex, uint16(count))
	copy(b[16:24], indexMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 2)
	binary.LittleEndian.PutUint16(b[26:28], indexHeaderSize)
	b[28] = n.depth
	if count == 0 {
		b[29] = 1
	} // sole empty exact-generation root sentinel
	binary.LittleEndian.PutUint16(b[30:32], indexEntrySize)
	binary.LittleEndian.PutUint64(b[32:40], generationID)
	binary.LittleEndian.PutUint64(b[40:48], n.freePages)
	binary.LittleEndian.PutUint64(b[48:56], n.retiredPages)
	binary.LittleEndian.PutUint64(b[56:64], n.minSeq)
	binary.LittleEndian.PutUint64(b[64:72], n.prefix)
	o := indexHeaderSize
	for slot, child := range n.child {
		if child.zero() {
			continue
		}
		if child.freeCount() > uint64(^uint32(0)) || child.retiredCount() > uint64(^uint32(0)) {
			return ErrGenerationFormat
		}
		b[o], b[o+1] = byte(slot), child.kind()
		binary.LittleEndian.PutUint32(b[o+2:o+6], child.checksum())
		binary.LittleEndian.PutUint64(b[o+8:o+16], child.pageID())
		binary.LittleEndian.PutUint32(b[o+16:o+20], uint32(child.freeCount()))
		binary.LittleEndian.PutUint32(b[o+20:o+24], uint32(child.retiredCount()))
		binary.LittleEndian.PutUint64(b[o+24:o+32], child.minRetiredSeq())
		o += indexEntrySize
	}
	finishPage(b)
	return nil
}

func encodeGenerationPage(id uint64, g *FreelistGenerationV1, rootCRC uint32) []byte {
	b := make([]byte, page.PageSize)
	encodePageHeader(b, id, page.PageTypeFreelistGeneration, 1)
	copy(b[16:24], generationMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 2)
	binary.LittleEndian.PutUint16(b[26:28], generationHeaderSize)
	b[28] = freelistChunkShift
	b[29] = g.root.kind()
	binary.LittleEndian.PutUint64(b[32:40], g.generationID)
	binary.LittleEndian.PutUint64(b[40:48], g.commitSeq)
	binary.LittleEndian.PutUint64(b[48:56], g.parentGenerationID)
	binary.LittleEndian.PutUint64(b[56:64], g.parentCommitSeq)
	binary.LittleEndian.PutUint64(b[64:72], g.root.pageID())
	binary.LittleEndian.PutUint64(b[72:80], g.highWater)
	binary.LittleEndian.PutUint64(b[80:88], g.root.freeCount())
	binary.LittleEndian.PutUint64(b[88:96], g.root.retiredCount())
	binary.LittleEndian.PutUint64(b[96:104], g.record.pageID)
	binary.LittleEndian.PutUint32(b[104:108], uint32(len(g.metadataPages)))
	binary.LittleEndian.PutUint32(b[108:112], uint32(g.record.pendingMetadataCountV1()))
	binary.LittleEndian.PutUint32(b[112:116], rootCRC)
	copy(b[120:152], g.record.digest[:])
	digest := generationDigest(b)
	copy(b[152:184], digest[:])
	finishPage(b)
	return b
}

func generationDigest(b []byte) [32]byte {
	var scratch [generationHeaderSize]byte
	copy(scratch[:], b[:generationHeaderSize])
	canonical := scratch[:]
	for i := 8; i < 12; i++ {
		canonical[i] = 0
	}
	for i := 152; i < 184; i++ {
		canonical[i] = 0
	}
	return sha256.Sum256(canonical)
}

func LoadGenerationV1(src PageSource, ref GenerationRefV1) (*FreelistGenerationV1, error) {
	g, err := loadGenerationOwnedV1(src, ref)
	if err == nil {
		g.markOrdinaryEscapeV1()
	}
	return g, err
}

func loadGenerationOwnedV1(src PageSource, ref GenerationRefV1) (*FreelistGenerationV1, error) {
	if ref.HeaderPageID < 2 || ref.HighWater <= ref.HeaderPageID {
		return nil, ErrGenerationFormat
	}
	b, err := readTypedPage(src, ref.HeaderPageID, page.PageTypeFreelistGeneration, ref.HighWater)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(b[16:24], generationMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 2 || binary.LittleEndian.Uint16(b[26:28]) != generationHeaderSize || b[28] != freelistChunkShift || b[29] > 1 || !zeroTail(b[30:32], 0) || !zeroTail(b[116:120], 0) || !zeroTail(b[184:192], 0) || !zeroTail(b, generationHeaderSize) {
		return nil, ErrGenerationFormat
	}
	if page.DecodeHeader(b).Count != 1 {
		return nil, ErrGenerationFormat
	}
	digest := generationDigest(b)
	if digest != ref.Digest || !bytes.Equal(b[152:184], digest[:]) {
		return nil, ErrGenerationDigest
	}
	g := &FreelistGenerationV1{
		ownedRefs:          1,
		generationID:       binary.LittleEndian.Uint64(b[32:40]),
		commitSeq:          binary.LittleEndian.Uint64(b[40:48]),
		parentGenerationID: binary.LittleEndian.Uint64(b[48:56]),
		parentCommitSeq:    binary.LittleEndian.Uint64(b[56:64]),
		highWater:          binary.LittleEndian.Uint64(b[72:80]),
	}
	accepted := false
	defer func() {
		if !accepted {
			releaseGenerationV1(g)
		}
	}()
	if g.generationID != ref.GenerationID || g.commitSeq != ref.CommitSeq || g.highWater != ref.HighWater {
		return nil, ErrGenerationFormat
	}
	recordPageID := binary.LittleEndian.Uint64(b[96:104])
	record, err := loadReservationRecord(src, recordPageID, g.highWater)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(record.digest[:], b[120:152]) {
		return nil, ErrGenerationDigest
	}
	// V2 emits one contiguous current-generation metadata reservation. Its
	// exact interval is the ownership witness; inherited state stays outside.
	var target ReservationExtentV1
	for _, extent := range record.Extents {
		if extent.Kind != ReservationTargetMetadata {
			continue
		}
		if target.Count != 0 {
			return nil, ErrGenerationFormat
		}
		target = extent
	}
	if target.Count == 0 {
		return nil, ErrGenerationFormat
	}
	ownership := metadataOwnershipV2{generation: g.generationID, start: target.StartPageID, end: target.StartPageID + uint64(target.Count)}
	if !ownership.contains(ref.HeaderPageID) {
		return nil, ErrGenerationFormat
	}
	ownership.pages = 1
	g.record = record
	if record.GenerationID != g.generationID || record.BaseID != g.parentGenerationID || target.Count != binary.LittleEndian.Uint32(b[104:108]) || uint32(len(record.pendingMetadata())) != binary.LittleEndian.Uint32(b[108:112]) {
		return nil, ErrGenerationFormat
	}
	seen := newPageRadixV1[struct{}]()
	defer seen.Clear()
	rootID := binary.LittleEndian.Uint64(b[64:72])
	seen.Set(ref.HeaderPageID, struct{}{})
	for _, reservationID := range record.pageIDs {
		if _, duplicate := seen.Get(reservationID); duplicate || !ownership.contains(reservationID) {
			return nil, ErrGenerationFormat
		}
		ownership.pages++
		seen.Set(reservationID, struct{}{})
	}
	g.root, err = loadStateRefV2(src, rootID, b[29], g.generationID, true, true, g.highWater, seen, -1, &ownership)
	if err != nil {
		return nil, err
	}
	if ownership.pages != uint64(target.Count) {
		return nil, ErrGenerationFormat
	}
	rootPage, err := src.ReadPage(rootID)
	if err != nil || len(rootPage) != page.PageSize || binary.LittleEndian.Uint32(rootPage[8:12]) != binary.LittleEndian.Uint32(b[112:116]) {
		return nil, ErrGenerationDigest
	}
	if g.root.freeCount() != binary.LittleEndian.Uint64(b[80:88]) || g.root.retiredCount() != binary.LittleEndian.Uint64(b[88:96]) {
		return nil, ErrGenerationFormat
	}
	g.ref = ref
	g.metadataPages = append([]uint64(nil), record.metadataPages()...)
	if err := g.Validate(); err != nil {
		return nil, err
	}
	accepted = true
	return g, nil
}

// Scalar ownership accounting proves all current metadata pages belong to and
// exhaust their pre-reserved interval without constructing another inventory.
type metadataOwnershipV2 struct {
	generation, start, end, pages uint64
}

func (m *metadataOwnershipV2) contains(id uint64) bool { return id >= m.start && id < m.end }

// The declared kind is checked before decoding, so a child cannot masquerade as
// another physical page type. Header and reservation IDs are already in seen.
// Branch depth is checked before recursion, bounding even malformed graphs.
func loadStateRefV2(src PageSource, id uint64, kind byte, maxGeneration uint64, exactGeneration, root bool, highWater uint64, seen *numericRadixV1[uint64, struct{}], parentDepth int, ownership *metadataOwnershipV2) (stateRefV1, error) {
	fail := func() (stateRefV1, error) { return stateRefV1{}, ErrGenerationFormat }
	if kind > 1 {
		return fail()
	}
	if _, duplicate := seen.Get(id); duplicate {
		return fail()
	}
	seen.Set(id, struct{}{})
	typ := page.PageTypeFreelistIndex
	if kind == 1 {
		typ = page.PageTypeFreelistChunk
	}
	b, err := readTypedPage(src, id, typ, highWater)
	if err != nil {
		return stateRefV1{}, err
	}
	generation := binary.LittleEndian.Uint64(b[32:40])
	if generation == 0 || generation > maxGeneration || exactGeneration && generation != maxGeneration {
		return fail()
	}
	if ownership != nil {
		current := generation == ownership.generation
		if current != ownership.contains(id) {
			return fail()
		}
		if current {
			ownership.pages++
		}
	}
	if kind == 1 {
		c, err := decodeChunkPage(b, maxGeneration, binary.LittleEndian.Uint64(b[40:48]))
		if err != nil {
			return stateRefV1{}, err
		}
		if c.freeCount()+c.retiredPages == 0 {
			releaseStateChunkV1(c)
			return fail()
		}
		return stateRefV1{chunk: c}, nil
	}
	h := page.DecodeHeader(b)
	depth := int(b[28])
	prefix := binary.LittleEndian.Uint64(b[64:72])
	empty := h.Count == 0
	if !bytes.Equal(b[16:24], indexMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 2 ||
		binary.LittleEndian.Uint16(b[26:28]) != indexHeaderSize || depth >= chunkTrieDepth || depth <= parentDepth ||
		binary.LittleEndian.Uint16(b[30:32]) != indexEntrySize || h.Count > 16 || h.Count == 1 ||
		!zeroTail(b[72:80], 0) || !zeroTail(b, indexHeaderSize+int(h.Count)*indexEntrySize) ||
		prefix>>uint(4*depth) != 0 || (empty && (!root || depth != 0 || prefix != 0 || b[29] != 1)) ||
		(!empty && b[29] != 0) {
		return fail()
	}
	n := &stateNode{ownedRefs: 1, pageID: id, checksum: h.Checksum, depth: uint8(depth), prefix: prefix}
	r := stateRefV1{branch: n}
	accepted := false
	defer func() {
		if !accepted {
			releaseStateNodeV1(r)
		}
	}()
	last := -1
	for i := 0; i < int(h.Count); i++ {
		o := indexHeaderSize + i*indexEntrySize
		slot := int(b[o])
		if slot <= last || slot > 15 || !zeroTail(b[o+6:o+8], 0) {
			return fail()
		}
		last = slot
		child, err := loadStateRefV2(src, binary.LittleEndian.Uint64(b[o+8:o+16]), b[o+1], generation, false, false, highWater, seen, depth, ownership)
		if err != nil {
			return stateRefV1{}, err
		}
		n.child[slot] = child
		if child.checksum() != binary.LittleEndian.Uint32(b[o+2:o+6]) {
			return stateRefV1{}, ErrGenerationDigest
		}
		if child.freeCount() != uint64(binary.LittleEndian.Uint32(b[o+16:o+20])) ||
			child.retiredCount() != uint64(binary.LittleEndian.Uint32(b[o+20:o+24])) ||
			child.minRetiredSeq() != binary.LittleEndian.Uint64(b[o+24:o+32]) ||
			chunkPrefixV1(child.minChunk(), depth) != prefix || chunkPrefixV1(child.maxChunk(), depth) != prefix ||
			chunkNibble(child.minChunk(), depth) != slot || chunkNibble(child.maxChunk(), depth) != slot ||
			child.branch != nil && int(child.branch.depth) <= depth {
			return fail()
		}
	}
	recomputeStateNode(n, depth)
	if n.freePages != binary.LittleEndian.Uint64(b[40:48]) || n.retiredPages != binary.LittleEndian.Uint64(b[48:56]) ||
		n.minSeq != binary.LittleEndian.Uint64(b[56:64]) ||
		!empty && firstDifferingDepthV1(n.lowChunk, n.highChunk) != depth {
		return fail()
	}
	accepted = true
	return r, nil
}

func sortedUnique(ids []uint64) []uint64 {
	out := append([]uint64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	w := 0
	for _, id := range out {
		if w == 0 || out[w-1] != id {
			out[w] = id
			w++
		}
	}
	return out[:w]
}
