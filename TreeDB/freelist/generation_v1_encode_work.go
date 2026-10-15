package freelist

import (
	"encoding/binary"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

// statePageEncodeWorkV1 is the original canonical encoder's retained control.
// It borrows an immutable private node/chunk and caller-owned output; neither
// may be reset, published or reused until encoding completes. Candidate output
// bytes remain owned by the candidate after this control finishes.
type statePageEncodeWorkV1 struct {
	output               []byte
	chunk                *stateChunk
	node                 *stateNode
	id, generation       uint64
	retiredCount, minSeq uint64
	index, offset, count int
	phase                uint8
}

func stateEncodeEntriesV1(w *iterator.OrdinalScanWork, remaining int, bytes uint64) (int, error) {
	if remaining == 0 {
		return 0, nil
	}
	if w == nil {
		return remaining, nil
	}
	n := uint64(remaining)
	if w.RecordLimit != 0 {
		n = min(n, w.RecordLimit-w.Records)
	}
	if w.ByteLimit != 0 {
		n = min(n, (w.ByteLimit-w.Bytes)/bytes)
	}
	if n == 0 {
		_, err := appendReserve(w, 1, bytes)
		return 0, err
	}
	if ok, err := appendReserve(w, n, n*bytes); !ok || err != nil {
		return 0, err
	}
	return int(n), nil
}
func (s *statePageEncodeWorkV1) bound(b []byte, id, generation uint64) bool {
	return len(b) == page.PageSize && len(s.output) == len(b) && &s.output[0] == &b[0] && s.id == id && s.generation == generation
}
func (s *statePageEncodeWorkV1) chunkPage(b []byte, id, generation uint64, c *stateChunk, w *iterator.OrdinalScanWork) (bool, error) {
	if s.phase == 0 {
		if len(b) != page.PageSize || c == nil || generation == 0 || c.chunkNo >= 1<<56 {
			return false, ErrGenerationFormat
		}
		if ok, e := appendReserve(w, 1, page.PageSize+chunkHeaderSize); !ok || e != nil {
			return false, e
		}
		s.output, s.chunk, s.id, s.generation = b, c, id, generation
		clear(b)
		encodePageHeader(b, id, page.PageTypeFreelistChunk, freelistChunkSize)
		copy(b[16:24], chunkMagic[:])
		binary.LittleEndian.PutUint16(b[24:26], 2)
		binary.LittleEndian.PutUint16(b[26:28], chunkHeaderSize)
		binary.LittleEndian.PutUint64(b[32:40], generation)
		binary.LittleEndian.PutUint64(b[40:48], c.chunkNo)
		binary.LittleEndian.PutUint32(b[48:52], uint32(c.freeCount()))
		for i, word := range c.free {
			binary.LittleEndian.PutUint64(b[64+i*8:], word)
		}
		s.phase = 1
	}
	if !s.bound(b, id, generation) || s.chunk != c || s.node != nil {
		return false, ErrGenerationFormat
	}
	for {
		switch s.phase {
		case 1:
			n, e := stateEncodeEntriesV1(w, freelistChunkSize-s.index, 16)
			if e != nil || n == 0 && s.index < freelistChunkSize {
				return false, e
			}
			end := s.index + n
			// The nil-policy ordinary driver packs the whole remaining range in one
			// tight loop. Finite callers use this same loop over the admitted range.
			for ; s.index < end; s.index++ {
				seq := c.retired[s.index]
				binary.LittleEndian.PutUint64(b[chunkHeaderSize+s.index*8:], seq)
				if seq != 0 {
					s.retiredCount++
					if s.minSeq == 0 || seq < s.minSeq {
						s.minSeq = seq
					}
				}
			}
			if s.index < freelistChunkSize {
				return false, nil
			}
			s.phase = 2
		case 2:
			if ok, e := appendReserve(w, 1, 12); !ok || e != nil {
				return false, e
			}
			binary.LittleEndian.PutUint32(b[52:56], uint32(s.retiredCount))
			binary.LittleEndian.PutUint64(b[56:64], s.minSeq)
			s.phase = 3
		case 3:
			if ok, e := appendReserve(w, 1, page.PageSize); !ok || e != nil {
				return false, e
			}
			finishPage(b)
			s.phase = 4
		case 4:
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}
func (s *statePageEncodeWorkV1) indexPage(b []byte, id, generation uint64, n *stateNode, w *iterator.OrdinalScanWork) (bool, error) {
	if s.phase == 0 {
		if len(b) != page.PageSize || n == nil || n.chunk != nil || int(n.depth) >= chunkTrieDepth || generation == 0 {
			return false, ErrGenerationFormat
		}
		if ok, e := appendReserve(w, 1, page.PageSize); !ok || e != nil {
			return false, e
		}
		s.output, s.node, s.id, s.generation = b, n, id, generation
		clear(b)
		s.offset = indexHeaderSize
		s.phase = 1
	}
	if !s.bound(b, id, generation) || s.node != n || s.chunk != nil {
		return false, ErrGenerationFormat
	}
	for {
		switch s.phase {
		case 1:
			length, e := stateEncodeEntriesV1(w, len(n.child)-s.index, 80)
			if e != nil || length == 0 && s.index < len(n.child) {
				return false, e
			}
			end := s.index + length
			for ; s.index < end; s.index++ {
				child := n.child[s.index]
				if child == nil {
					continue
				}
				if child.pageID == 0 || child.freeCount+child.retiredCount == 0 || child.freeCount > uint64(^uint32(0)) || child.retiredCount > uint64(^uint32(0)) {
					return false, ErrGenerationFormat
				}
				o := s.offset
				b[o], b[o+1] = byte(s.index), stateNodeKindV2(child)
				binary.LittleEndian.PutUint32(b[o+2:o+6], child.checksum)
				binary.LittleEndian.PutUint64(b[o+8:o+16], child.pageID)
				binary.LittleEndian.PutUint32(b[o+16:o+20], uint32(child.freeCount))
				binary.LittleEndian.PutUint32(b[o+20:o+24], uint32(child.retiredCount))
				binary.LittleEndian.PutUint64(b[o+24:o+32], child.minRetiredSeq)
				s.offset += indexEntrySize
				s.count++
			}
			if s.index < len(n.child) {
				return false, nil
			}
			if s.count == 1 || s.count == 0 && (n.depth != 0 || n.prefix != 0 || n.freeCount+n.retiredCount != 0) {
				return false, ErrGenerationFormat
			}
			s.phase = 2
		case 2:
			if ok, e := appendReserve(w, 1, indexHeaderSize); !ok || e != nil {
				return false, e
			}
			encodePageHeader(b, id, page.PageTypeFreelistIndex, uint16(s.count))
			copy(b[16:24], indexMagic[:])
			binary.LittleEndian.PutUint16(b[24:26], 2)
			binary.LittleEndian.PutUint16(b[26:28], indexHeaderSize)
			b[28] = n.depth
			if s.count == 0 {
				b[29] = 1
			}
			binary.LittleEndian.PutUint16(b[30:32], indexEntrySize)
			binary.LittleEndian.PutUint64(b[32:40], generation)
			binary.LittleEndian.PutUint64(b[40:48], n.freeCount)
			binary.LittleEndian.PutUint64(b[48:56], n.retiredCount)
			binary.LittleEndian.PutUint64(b[56:64], n.minRetiredSeq)
			binary.LittleEndian.PutUint64(b[64:72], n.prefix)
			s.phase = 3
		case 3:
			if ok, e := appendReserve(w, 1, page.PageSize); !ok || e != nil {
				return false, e
			}
			finishPage(b)
			s.phase = 4
		case 4:
			return true, nil
		default:
			return false, ErrGenerationFormat
		}
	}
}
