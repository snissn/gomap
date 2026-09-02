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
	indexHeaderSize       = 64
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
	encodePageHeader(b, id, page.PageTypeFreelistChunk, freelistChunkSize)
	copy(b[16:24], chunkMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 1)
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
	if !bytes.Equal(b[16:24], chunkMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 1 || binary.LittleEndian.Uint16(b[26:28]) != chunkHeaderSize || !zeroTail(b[28:32], 0) || pageGeneration == 0 || pageGeneration > maxGeneration {
		return nil, ErrGenerationFormat
	}
	chunkNo := binary.LittleEndian.Uint64(b[40:48])
	if chunkNo != expectedChunk || !zeroTail(b, chunkHeaderSize+freelistChunkSize*8) {
		return nil, ErrGenerationFormat
	}
	h := page.DecodeHeader(b)
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
	count := 0
	if depth == chunkTrieDepth {
		if n.chunk == nil || n.chunk.pageID == 0 {
			return nil, ErrGenerationFormat
		}
		count = 1
	} else {
		for _, child := range n.child {
			if child != nil && (child.freeCount != 0 || child.retiredCount != 0) {
				if child.pageID == 0 {
					return nil, ErrGenerationFormat
				}
				count++
			}
		}
	}
	encodePageHeader(b, id, page.PageTypeFreelistIndex, uint16(count))
	copy(b[16:24], indexMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 1)
	binary.LittleEndian.PutUint16(b[26:28], indexHeaderSize)
	b[28] = byte(depth)
	binary.LittleEndian.PutUint16(b[30:32], indexEntrySize)
	binary.LittleEndian.PutUint64(b[32:40], generationID)
	binary.LittleEndian.PutUint64(b[40:48], n.freeCount)
	binary.LittleEndian.PutUint64(b[48:56], n.retiredCount)
	binary.LittleEndian.PutUint64(b[56:64], n.minRetiredSeq)
	o := indexHeaderSize
	write := func(slot byte, kind byte, childChecksum uint32, childID, freeCount, retiredCount, minSeq uint64) error {
		if freeCount > uint64(^uint32(0)) || retiredCount > uint64(^uint32(0)) {
			return ErrGenerationFormat
		}
		b[o], b[o+1] = slot, kind
		binary.LittleEndian.PutUint32(b[o+2:o+6], childChecksum)
		binary.LittleEndian.PutUint64(b[o+8:o+16], childID)
		binary.LittleEndian.PutUint32(b[o+16:o+20], uint32(freeCount))
		binary.LittleEndian.PutUint32(b[o+20:o+24], uint32(retiredCount))
		binary.LittleEndian.PutUint64(b[o+24:o+32], minSeq)
		o += indexEntrySize
		return nil
	}
	if depth == chunkTrieDepth {
		retiredCount, minSeq := n.chunk.retiredSummary()
		if err := write(0, 1, n.chunk.checksum, n.chunk.pageID, n.chunk.freeCount(), retiredCount, minSeq); err != nil {
			return nil, err
		}
	} else {
		for slot, child := range n.child {
			if child != nil && (child.freeCount != 0 || child.retiredCount != 0) {
				if err := write(byte(slot), 0, child.checksum, child.pageID, child.freeCount, child.retiredCount, child.minRetiredSeq); err != nil {
					return nil, err
				}
			}
		}
	}
	finishPage(b)
	return b, nil
}

func encodeGenerationPage(id uint64, g *FreelistGenerationV1, rootCRC uint32) []byte {
	b := make([]byte, page.PageSize)
	encodePageHeader(b, id, page.PageTypeFreelistGeneration, 1)
	copy(b[16:24], generationMagic[:])
	binary.LittleEndian.PutUint16(b[24:26], 1)
	binary.LittleEndian.PutUint16(b[26:28], generationHeaderSize)
	b[28] = freelistChunkShift
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
	canonical := append([]byte(nil), b[:generationHeaderSize]...)
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
	if !bytes.Equal(b[16:24], generationMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 1 || binary.LittleEndian.Uint16(b[26:28]) != generationHeaderSize || b[28] != freelistChunkShift || !zeroTail(b[29:32], 0) || !zeroTail(b[116:120], 0) || !zeroTail(b[184:192], 0) || !zeroTail(b, generationHeaderSize) {
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
	seen := make(map[uint64]struct{})
	rootID := binary.LittleEndian.Uint64(b[64:72])
	g.root, err = loadStateNode(src, rootID, 0, 0, g.generationID, true, g.highWater, seen)
	if err != nil {
		return nil, err
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

func loadStateNode(src PageSource, id uint64, depth int, prefix, maxGeneration uint64, exactGeneration bool, highWater uint64, seen map[uint64]struct{}) (*stateNode, error) {
	if depth > chunkTrieDepth {
		return nil, ErrGenerationFormat
	}
	if _, duplicate := seen[id]; duplicate {
		return nil, fmt.Errorf("%w: duplicate or cyclic page %d", ErrGenerationFormat, id)
	}
	seen[id] = struct{}{}
	b, err := readTypedPage(src, id, page.PageTypeFreelistIndex, highWater)
	if err != nil {
		return nil, err
	}
	h := page.DecodeHeader(b)
	pageGeneration := binary.LittleEndian.Uint64(b[32:40])
	if !bytes.Equal(b[16:24], indexMagic[:]) || binary.LittleEndian.Uint16(b[24:26]) != 1 || binary.LittleEndian.Uint16(b[26:28]) != indexHeaderSize || int(b[28]) != depth || b[29] != 0 || binary.LittleEndian.Uint16(b[30:32]) != indexEntrySize || pageGeneration == 0 || pageGeneration > maxGeneration || (exactGeneration && pageGeneration != maxGeneration) || int(h.Count) > 16 || !zeroTail(b, indexHeaderSize+int(h.Count)*indexEntrySize) {
		return nil, ErrGenerationFormat
	}
	n := &stateNode{pageID: id, checksum: h.Checksum}
	lastSlot := -1
	for i := 0; i < int(h.Count); i++ {
		o := indexHeaderSize + i*indexEntrySize
		slot, kind := int(b[o]), b[o+1]
		if slot <= lastSlot || slot > 15 {
			return nil, ErrGenerationFormat
		}
		lastSlot = slot
		entryChecksum := binary.LittleEndian.Uint32(b[o+2 : o+6])
		if !zeroTail(b[o+6:o+8], 0) {
			return nil, ErrGenerationFormat
		}
		childID := binary.LittleEndian.Uint64(b[o+8 : o+16])
		entryFree := uint64(binary.LittleEndian.Uint32(b[o+16 : o+20]))
		entryRetired := uint64(binary.LittleEndian.Uint32(b[o+20 : o+24]))
		entryMinSeq := binary.LittleEndian.Uint64(b[o+24 : o+32])
		childPrefix := (prefix << 4) | uint64(slot)
		if depth == chunkTrieDepth {
			if h.Count != 1 || kind != 1 || slot != 0 {
				return nil, ErrGenerationFormat
			}
			chunkPage, err := readTypedPage(src, childID, page.PageTypeFreelistChunk, highWater)
			if err != nil {
				return nil, err
			}
			if _, duplicate := seen[childID]; duplicate {
				return nil, ErrGenerationFormat
			}
			seen[childID] = struct{}{}
			if page.DecodeHeader(chunkPage).Checksum != entryChecksum {
				return nil, ErrGenerationDigest
			}
			n.chunk, err = decodeChunkPage(chunkPage, pageGeneration, prefix)
			if err != nil {
				return nil, err
			}
			retiredCount, minSeq := n.chunk.retiredSummary()
			if n.chunk.freeCount() != entryFree || retiredCount != entryRetired || minSeq != entryMinSeq {
				return nil, ErrGenerationFormat
			}
		} else {
			if kind != 0 {
				return nil, ErrGenerationFormat
			}
			child, err := loadStateNode(src, childID, depth+1, childPrefix, pageGeneration, false, highWater, seen)
			if err != nil {
				return nil, err
			}
			n.child[slot] = child
			if child.checksum != entryChecksum {
				return nil, ErrGenerationDigest
			}
			if child.freeCount != entryFree || child.retiredCount != entryRetired || child.minRetiredSeq != entryMinSeq {
				return nil, ErrGenerationFormat
			}
		}
	}
	recomputeStateNode(n, depth)
	if n.freeCount != binary.LittleEndian.Uint64(b[40:48]) || n.retiredCount != binary.LittleEndian.Uint64(b[48:56]) || n.minRetiredSeq != binary.LittleEndian.Uint64(b[56:64]) {
		return nil, ErrGenerationFormat
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
