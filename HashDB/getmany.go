package hashdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/snissn/compress/s2"
)

type slabReadRef struct {
	i      int
	key    []byte
	offset SlabOffset
}

type slabSegmentRef struct {
	i      int
	key    []byte
	offset int64
}

var getManyChunkBufPool sync.Pool

func getManyChunkBuf(n int64) []byte {
	if n <= 0 || n > int64(int(^uint(0)>>1)) {
		return nil
	}
	b, _ := getManyChunkBufPool.Get().([]byte)
	if cap(b) < int(n) {
		return make([]byte, int(n))
	}
	return b[:int(n)]
}

func putManyChunkBuf(b []byte) {
	// Keep pool bounded: we never intentionally read chunks > 1MB.
	const maxKeep = 1 << 20
	if cap(b) > maxKeep {
		return
	}
	getManyChunkBufPool.Put(b[:0])
}

func (h *DB) getManyWithHashes(keys [][]byte, hashes []Hash) ([][]byte, []error) {
	values := make([][]byte, len(keys))
	errs := make([]error, len(keys))

	if len(keys) == 0 {
		return values, errs
	}

	// Find slab offsets for all found keys without reading values.
	found := make([]slabReadRef, 0, len(keys))

	for i, key := range keys {
		keyHash := hashes[i]

		if len(h.keys) > 0 && h.capacity > 0 {
			idx, ok, err := h.probeIndexWithHash(h.keys, h.controls, h.capacity, key, keyHash)
			if err != nil {
				errs[i] = err
				continue
			}
			if ok {
				found = append(found, slabReadRef{i: i, key: key, offset: h.keys[idx].slabOffset})
				continue
			}
		}

		if h.rehashInProgress && h.rehashOldCapacity > 0 && len(h.rehashOldKeys) > 0 {
			idx, ok, err := h.probeIndexWithHash(h.rehashOldKeys, h.rehashOldControls, h.rehashOldCapacity, key, keyHash)
			if err != nil {
				errs[i] = err
				continue
			}
			if ok {
				found = append(found, slabReadRef{i: i, key: key, offset: h.rehashOldKeys[idx].slabOffset})
				continue
			}
		}
	}

	if len(found) == 0 {
		return values, errs
	}

	// Group found items by segment so we can coalesce ReadAt calls.
	bySegment := make(map[uint16][]slabSegmentRef)
	for _, f := range found {
		segmentID := uint16(uint64(f.offset) >> OffsetBits)
		localOffset := int64(uint64(f.offset) & ((1 << OffsetBits) - 1))
		bySegment[segmentID] = append(bySegment[segmentID], slabSegmentRef{i: f.i, key: f.key, offset: localOffset})
	}

	const (
		firstReadSize = int64(4096)
		maxChunkSize  = int64(1 << 20) // 1MB
	)

	for segmentID, refs := range bySegment {
		f := h.slabFiles[segmentID]
		if f == nil {
			for _, r := range refs {
				errs[r.i] = fmt.Errorf("segment %d not found", segmentID)
			}
			continue
		}

		sort.Slice(refs, func(i, j int) bool { return refs[i].offset < refs[j].offset })

		// Coalesce reads: read a window that covers multiple nearby offsets when possible.
		for i := 0; i < len(refs); {
			chunkStart := refs[i].offset
			if chunkStart < 0 {
				errs[refs[i].i] = fmt.Errorf("negative offset: %d", chunkStart)
				i++
				continue
			}
			chunkEnd := chunkStart + firstReadSize

			j := i + 1
			for j < len(refs) && refs[j].offset < chunkEnd {
				wantEnd := refs[j].offset + firstReadSize
				if wantEnd-chunkStart > maxChunkSize {
					break
				}
				if wantEnd > chunkEnd {
					chunkEnd = wantEnd
				}
				j++
			}

			chunkLen := chunkEnd - chunkStart
			if chunkLen <= 0 {
				for ; i < j; i++ {
					errs[refs[i].i] = fmt.Errorf("invalid chunk length: %d", chunkLen)
				}
				continue
			}

			var (
				buf    []byte
				n      int
				err    error
				pooled bool
			)

			// Fast-path: sealed segments may be mmapped read-only.
			if segmentID < h.activeSegmentID {
				if m, mapErr := h.slabReadOnlyMap(segmentID); mapErr == nil && m != nil {
					if chunkStart >= 0 && chunkEnd >= chunkStart && chunkEnd <= int64(len(m)) {
						if chunkStart <= int64(int(^uint(0)>>1)) && chunkEnd <= int64(int(^uint(0)>>1)) {
							buf = m[int(chunkStart):int(chunkEnd)]
						}
						n = len(buf)
						err = nil
					}
				}
			}

			if buf == nil {
				buf = getManyChunkBuf(chunkLen)
				if buf == nil {
					for ; i < j; i++ {
						errs[refs[i].i] = fmt.Errorf("chunk too large: %d", chunkLen)
					}
					i = j
					continue
				}
				pooled = true

				n, err = readAt(f, buf, chunkStart)
				if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
					putManyChunkBuf(buf)
					for ; i < j; i++ {
						errs[refs[i].i] = err
					}
					i = j
					continue
				}
				if n < 0 {
					n = 0
				}
				if n > len(buf) {
					n = len(buf)
				}
				buf = buf[:n]
			}

			for k := i; k < j; k++ {
				ref := refs[k]
				rel := ref.offset - chunkStart
				val, ok, need, err := decodeValueFromChunk(buf, rel, ref.key)
				if err != nil {
					errs[ref.i] = err
					continue
				}
				if ok {
					values[ref.i] = val
					continue
				}

				// If we saw the record header but didn't have enough bytes in this chunk,
				// issue an exact read for the full record and decode from that.
				if need > 0 {
					record, recErr := h.readSlabRecord(segmentID, ref.offset, need)
					if recErr != nil {
						errs[ref.i] = recErr
						continue
					}
					val, ok, _, recErr := decodeValueFromChunk(record, 0, ref.key)
					if recErr != nil {
						errs[ref.i] = recErr
						continue
					}
					if ok {
						values[ref.i] = val
						continue
					}
				}

				// Fallback to the existing per-key reader (handles more edge cases).
				item, err := h.unmarshalItemFromSlab(Key{slabOffset: SlabOffset((uint64(segmentID) << OffsetBits) | uint64(ref.offset))})
				if err != nil {
					errs[ref.i] = err
					continue
				}
				values[ref.i] = append([]byte(nil), item.Value...)
			}

			if pooled {
				putManyChunkBuf(buf)
			}

			i = j
		}
	}

	return values, errs
}

func decodeValueFromChunk(buf []byte, rel int64, key []byte) ([]byte, bool, int64, error) {
	if rel < 0 || rel+16 > int64(len(buf)) {
		return nil, false, 0, nil
	}

	keyLen := binary.LittleEndian.Uint64(buf[rel : rel+8])
	if keyLen == slabKeyLenControl {
		return nil, false, 0, fmt.Errorf("unexpected control record in slab chunk")
	}

	valLenPacked := binary.LittleEndian.Uint64(buf[rel+8 : rel+16])
	if valLenPacked == ^uint64(0) {
		// Delete record.
		totalLen := int64(16) + int64(keyLen)
		if rel+totalLen > int64(len(buf)) {
			return nil, false, totalLen, nil
		}
		return nil, true, totalLen, nil
	}
	valLen, flags := unpackLength(valLenPacked)

	totalLen := int64(16) + int64(keyLen) + int64(valLen)
	if rel+totalLen > int64(len(buf)) {
		return nil, false, totalLen, nil
	}

	if keyLen != uint64(len(key)) {
		return nil, false, 0, nil
	}

	keyStart := rel + 16
	keyEnd := keyStart + int64(keyLen)
	if !bytes.Equal(buf[keyStart:keyEnd], key) {
		return nil, false, 0, nil
	}

	valStart := keyEnd
	valEnd := valStart + int64(valLen)
	valBytes := buf[valStart:valEnd]

	if flags&FlagCompressed != 0 {
		decompressed, err := s2.Decode(nil, valBytes)
		if err != nil {
			return nil, false, 0, err
		}
		return decompressed, true, totalLen, nil
	}

	return append([]byte(nil), valBytes...), true, totalLen, nil
}

func (h *DB) readSlabRecord(segmentID uint16, localOffset, n int64) ([]byte, error) {
	if n <= 0 {
		return nil, fmt.Errorf("record length must be positive")
	}
	if localOffset < 0 {
		return nil, fmt.Errorf("negative offset: %d", localOffset)
	}
	end := localOffset + n
	if end < localOffset {
		return nil, fmt.Errorf("offset overflow")
	}

	// Sealed segment: slice directly from mmap (no syscall, no allocation for record bytes).
	if segmentID < h.activeSegmentID {
		if m, err := h.slabReadOnlyMap(segmentID); err == nil && m != nil {
			if end <= int64(len(m)) && localOffset <= int64(int(^uint(0)>>1)) && end <= int64(int(^uint(0)>>1)) {
				return m[int(localOffset):int(end)], nil
			}
		}
	}

	f := h.slabFiles[segmentID]
	if f == nil {
		return nil, fmt.Errorf("segment %d not found", segmentID)
	}
	if n > int64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("record too large: %d", n)
	}

	buf := make([]byte, int(n))
	readN, err := readAt(f, buf, localOffset)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	// If we couldn't read the full record, treat as incomplete.
	if readN != len(buf) {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}
