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
	generationMagic  = [8]byte{'F', 'L', 'G', 'E', 'N', 'V', '1', 0}
	indexMagic       = [8]byte{'F', 'L', 'I', 'D', 'X', 'V', '1', 0}
	chunkMagic       = [8]byte{'F', 'L', 'C', 'H', 'K', 'V', '1', 0}
	reservationMagic = [8]byte{'F', 'L', 'R', 'S', 'V', 'V', '1', 0}
)

type PageSource interface {
	ReadPage(pageID uint64) ([]byte, error)
}

type AppendPageSink interface {
	WritePage(pageID uint64, data []byte) error
}

// CandidatePageViewV1 is a read-only view of one candidate-owned page. It can
// copy into caller-owned storage without exposing the candidate's byte slice.
type CandidatePageViewV1 struct {
	data []byte
}

func (view CandidatePageViewV1) Len() int { return len(view.data) }

func (view CandidatePageViewV1) CopyTo(dst []byte) error {
	if len(view.data) != page.PageSize || len(dst) != len(view.data) {
		return fmt.Errorf("%w: invalid candidate page copy", ErrGenerationFormat)
	}
	copy(dst, view.data)
	return nil
}

// WriteCandidatePageToPagerV1 copies an opaque candidate page directly into
// pager-owned mmap storage. Pager.Write holds the pager lock for the complete
// copy, preventing a concurrent Sync from draining a partially populated page.
func WriteCandidatePageToPagerV1(dst *pager.Pager, pageID uint64, view CandidatePageViewV1) error {
	if dst == nil {
		return fmt.Errorf("%w: missing candidate pager", ErrGenerationFormat)
	}
	if len(view.data) != page.PageSize {
		return fmt.Errorf("%w: invalid candidate page copy", ErrGenerationFormat)
	}
	return dst.Write(pageID, view.data)
}

// CandidatePageWriterV1 receives an opaque read-only page view. It may retain
// the view, but cannot mutate or alias candidate-owned bytes through this API.
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

// CandidatePageSinkV1 validates production candidate writes without retaining
// their bytes. It allows the materialized candidate to take ownership of each
// freshly encoded page instead of making a validation-store copy.
type CandidatePageSinkV1 struct {
	pageIDs map[uint64]struct{}
}

func NewCandidatePageSinkV1() *CandidatePageSinkV1 {
	return &CandidatePageSinkV1{pageIDs: make(map[uint64]struct{})}
}

func (s *CandidatePageSinkV1) WritePage(id uint64, data []byte) error {
	if s == nil || len(data) != page.PageSize {
		return fmt.Errorf("%w: invalid candidate page write", ErrGenerationFormat)
	}
	if s.pageIDs == nil {
		s.pageIDs = make(map[uint64]struct{})
	}
	if _, exists := s.pageIDs[id]; exists {
		return fmt.Errorf("%w: page %d rewritten", ErrGenerationFormat, id)
	}
	s.pageIDs[id] = struct{}{}
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
	if h.PageID != id || page.PageType(h.Flags&0xff) != typ {
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
	if err := encodeChunkPageInto(b, id, generationID, chunk); err != nil {
		panic(err)
	}
	return b
}

func encodeChunkPageInto(b []byte, id, generationID uint64, chunk *stateChunk) error {
	var work statePageEncodeWorkV1
	_, err := work.chunkPage(b, id, generationID, chunk, nil)
	return err
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
	c := &stateChunk{pageID: h.PageID, checksum: h.Checksum, chunkNo: chunkNo}
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
	var work statePageEncodeWorkV1
	_, err := work.indexPage(b, id, generationID, n, nil)
	return err
}

func encodeGenerationPage(id uint64, g *FreelistGenerationV1, rootCRC uint32) []byte {
	b := make([]byte, page.PageSize)
	encodePageHeader(b, id, page.PageTypeFreelistGeneration, 1)
	copy(b[16:24], generationMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 2)
	binary.LittleEndian.PutUint16(b[26:28], generationHeaderSize)
	b[28] = freelistChunkShift
	b[29] = stateNodeKindV2(g.root)
	binary.LittleEndian.PutUint64(b[32:40], g.generationID)
	binary.LittleEndian.PutUint64(b[40:48], g.commitSeq)
	binary.LittleEndian.PutUint64(b[48:56], g.parentGenerationID)
	binary.LittleEndian.PutUint64(b[56:64], g.parentCommitSeq)
	binary.LittleEndian.PutUint64(b[64:72], g.root.pageID)
	binary.LittleEndian.PutUint64(b[72:80], g.highWater)
	binary.LittleEndian.PutUint64(b[80:88], g.root.freeCount)
	binary.LittleEndian.PutUint64(b[88:96], g.root.retiredCount)
	binary.LittleEndian.PutUint64(b[96:104], g.record.pageID)
	binary.LittleEndian.PutUint32(b[104:108], uint32(len(g.metadataPages)))
	binary.LittleEndian.PutUint32(b[108:112], uint32(len(g.record.pendingMetadata())))
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
	if ref.HeaderPageID < 2 || ref.HighWater <= ref.HeaderPageID {
		return nil, ErrGenerationFormat
	}
	b, err := readTypedPage(src, ref.HeaderPageID, page.PageTypeFreelistGeneration, ref.HighWater)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(b[16:24], generationMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 2 || binary.LittleEndian.Uint16(b[26:28]) != generationHeaderSize || b[28] != freelistChunkShift || (b[29] > 1 || !zeroTail(b[30:32], 0)) || !zeroTail(b[116:120], 0) || !zeroTail(b[184:192], 0) || !zeroTail(b, generationHeaderSize) {
		return nil, ErrGenerationFormat
	}
	digest := generationDigest(b)
	if digest != ref.Digest || !bytes.Equal(b[152:184], digest[:]) {
		return nil, ErrGenerationDigest
	}
	g := &FreelistGenerationV1{
		generationID:       binary.LittleEndian.Uint64(b[32:40]),
		commitSeq:          binary.LittleEndian.Uint64(b[40:48]),
		parentGenerationID: binary.LittleEndian.Uint64(b[48:56]),
		parentCommitSeq:    binary.LittleEndian.Uint64(b[56:64]),
		highWater:          binary.LittleEndian.Uint64(b[72:80]),
	}
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
	g.record = record
	if record.GenerationID != g.generationID || record.BaseID != g.parentGenerationID || uint32(len(record.metadataPages())) != binary.LittleEndian.Uint32(b[104:108]) || uint32(len(record.pendingMetadata())) != binary.LittleEndian.Uint32(b[108:112]) {
		return nil, ErrGenerationFormat
	}
	var target ReservationExtentV1
	targets := 0
	for _, extent := range record.Extents {
		if extent.Kind == ReservationTargetMetadata {
			target = extent
			targets++
		}
	}
	if targets != 1 || target.Count == 0 {
		return nil, ErrGenerationFormat
	}
	ownership := &metadataOwnershipV2{generation: g.generationID, start: target.StartPageID, end: target.StartPageID + uint64(target.Count)}
	if !ownership.contains(ref.HeaderPageID) {
		return nil, ErrGenerationFormat
	}
	ownership.pages = 1
	seen := make(map[uint64]struct{})
	rootID := binary.LittleEndian.Uint64(b[64:72])
	seen[ref.HeaderPageID] = struct{}{}
	for _, id := range record.pageIDs {
		if !ownership.contains(id) {
			return nil, ErrGenerationFormat
		}
		ownership.pages++
		seen[id] = struct{}{}
	}
	g.root, err = loadStateRefV2(src, rootID, b[29], g.generationID, true, true, g.highWater, seen, -1, ownership)
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
	if g.root.freeCount != binary.LittleEndian.Uint64(b[80:88]) || g.root.retiredCount != binary.LittleEndian.Uint64(b[88:96]) {
		return nil, ErrGenerationFormat
	}
	g.ref = ref
	g.metadataPages = append([]uint64(nil), record.metadataPages()...)
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return g, nil
}

func stateNodeKindV2(n *stateNode) byte {
	if n != nil && n.chunk != nil {
		return 1
	}
	return 0
}

// metadataOwnershipV2 accounts for the pre-reserved current-generation interval
// without a second inventory. Shared ancestors belong to older generations.
type metadataOwnershipV2 struct{ generation, start, end, pages uint64 }

func (m *metadataOwnershipV2) contains(id uint64) bool { return id >= m.start && id < m.end }

func loadStateRefV2(src PageSource, id uint64, kind byte, maxGeneration uint64, exactGeneration, root bool, highWater uint64, seen map[uint64]struct{}, parentDepth int, ownershipArgs ...*metadataOwnershipV2) (*stateNode, error) {
	fail := func() (*stateNode, error) { return nil, ErrGenerationFormat }
	if kind > 1 {
		return fail()
	}
	if _, duplicate := seen[id]; duplicate {
		return fail()
	}
	seen[id] = struct{}{}
	typ := page.PageTypeFreelistIndex
	if kind == 1 {
		typ = page.PageTypeFreelistChunk
	}
	b, err := readTypedPage(src, id, typ, highWater)
	if err != nil {
		return nil, err
	}
	generation := binary.LittleEndian.Uint64(b[32:40])
	if generation == 0 || generation > maxGeneration || exactGeneration && generation != maxGeneration {
		return fail()
	}
	var ownership *metadataOwnershipV2
	if len(ownershipArgs) != 0 {
		ownership = ownershipArgs[0]
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
			return nil, err
		}
		retired, _ := c.retiredSummary()
		if c.freeCount()+retired == 0 {
			return fail()
		}
		n := &stateNode{pageID: c.pageID, checksum: c.checksum, chunk: c}
		recomputeStateNode(n, 0)
		return n, nil
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
	n := &stateNode{pageID: id, checksum: h.Checksum, depth: uint8(depth), prefix: prefix}
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
			return nil, err
		}
		n.child[slot] = child
		if child.checksum != binary.LittleEndian.Uint32(b[o+2:o+6]) {
			return nil, ErrGenerationDigest
		}
		if child.freeCount != uint64(binary.LittleEndian.Uint32(b[o+16:o+20])) ||
			child.retiredCount != uint64(binary.LittleEndian.Uint32(b[o+20:o+24])) ||
			child.minRetiredSeq != binary.LittleEndian.Uint64(b[o+24:o+32]) ||
			chunkPrefixV1(child.minChunk, depth) != prefix || chunkPrefixV1(child.maxChunk, depth) != prefix ||
			chunkNibble(child.minChunk, depth) != slot || chunkNibble(child.maxChunk, depth) != slot ||
			child.chunk == nil && int(child.depth) <= depth {
			return fail()
		}
	}
	recomputeStateNode(n, depth)
	if n.freeCount != binary.LittleEndian.Uint64(b[40:48]) || n.retiredCount != binary.LittleEndian.Uint64(b[48:56]) ||
		n.minRetiredSeq != binary.LittleEndian.Uint64(b[56:64]) ||
		!empty && firstDifferingDepthV1(n.minChunk, n.maxChunk) != depth {
		return fail()
	}
	return n, nil
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
