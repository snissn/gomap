package hashdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
	"github.com/snissn/compress/s2"
)

// FlagCompressed marks slab records with s2-compressed payloads.
const FlagCompressed = 0x80

func packLength(len uint64, flags uint8) uint64 {
	return len | (uint64(flags) << 56)
}

func unpackLength(packed uint64) (uint64, uint8) {
	len := packed & 0x00FFFFFFFFFFFFFF
	flags := uint8(packed >> 56)
	return len, flags
}

func (h *DB) writeSlab(buf []byte) error {
	// Check if active segment is full using in-memory size accounting
	f := h.slabFiles[h.activeSegmentID]
	maxSegmentSize := atomic.LoadInt64(&MaxSegmentSize)
	if h.activeSegmentSize+int64(len(buf)) > maxSegmentSize {
		// Rotate to a new segment
		h.activeSegmentID++
		newFilename := fmt.Sprintf("%s/slab-%d", h.dir, h.activeSegmentID)
		newF, err := os.OpenFile(newFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		h.slabFiles[h.activeSegmentID] = newF
		f = newF

		*h.slabOffset = SlabOffset(uint64(h.activeSegmentID) << OffsetBits)
		h.activeSegmentSize = 0
	}

	if _, err := writeAll(f, buf); err != nil {
		return err
	}
	h.activeSegmentSize += int64(len(buf))
	return nil
}

// ReadBytes reads raw bytes from the slab at the given offset.
func (h *DB) ReadBytes(offset SlabOffset, n int64) ([]byte, error) {
	segmentID := uint16(uint64(offset) >> OffsetBits)
	localOffset := int64(uint64(offset) & ((1 << OffsetBits) - 1))

	// Fast-path: sealed segments may be mmapped read-only.
	if segmentID < h.activeSegmentID {
		if m, err := h.slabReadOnlyMap(segmentID); err == nil && m != nil {
			if localOffset < 0 {
				return nil, fmt.Errorf("negative offset: %d", localOffset)
			}
			end := localOffset + n
			if end < localOffset {
				return nil, fmt.Errorf("offset overflow")
			}
			if end > int64(len(m)) {
				return nil, io.EOF
			}
			if localOffset > int64(int(^uint(0)>>1)) || end > int64(int(^uint(0)>>1)) {
				return nil, fmt.Errorf("offset too large")
			}
			b := make([]byte, n)
			copy(b, m[int(localOffset):int(end)])
			return b, nil
		}
	}

	f, ok := h.slabFiles[segmentID]
	if !ok {
		return nil, fmt.Errorf("segment %d not found", segmentID)
	}

	bytes := make([]byte, n)
	_, err := readAt(f, bytes, localOffset)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (h *DB) addSlab(item Item) (Key, error) {
	key := item.Key
	val := item.Value

	// Compress value?
	var flags uint8
	if compressed, ok := compressValueIfEnabled(h.compressionEnabled, val); ok {
		val = compressed
		flags |= FlagCompressed
	}

	keylen := len(key)
	vallen := len(val)
	actualTotalLength := 16 + keylen + vallen

	if cap(h.slabData) < actualTotalLength {
		h.slabData = make([]byte, 0, actualTotalLength)
	} else {
		h.slabData = h.slabData[:0]
	}

	var scratch [16]byte
	// Pack flags into KeyLen (since we retrieve KeyLen first usually, wait, unmarshal reads both)
	// Let's pack flags into ValLen? Or KeyLen?
	// unmarshal reads header.
	// If we compress Value, flag should be on Value Length?
	// Yes.

	binary.LittleEndian.PutUint64(scratch[:8], uint64(keylen))
	binary.LittleEndian.PutUint64(scratch[8:], packLength(uint64(vallen), flags))
	h.slabData = append(h.slabData, scratch[:]...)

	h.slabData = append(h.slabData, key...)
	h.slabData = append(h.slabData, val...)

	writeOffset, err := h.writeSlabAndRotate(h.slabData)
	if err != nil {
		return Key{}, err
	}

	*h.slabOffset += SlabOffset(actualTotalLength)
	return Key{slabOffset: writeOffset, hash: hash(key)}, nil
}

func (h *DB) writeSlabAndRotate(buf []byte) (SlabOffset, error) {
	f := h.slabFiles[h.activeSegmentID]
	maxSegmentSize := atomic.LoadInt64(&MaxSegmentSize)
	if h.activeSegmentSize+int64(len(buf)) > maxSegmentSize {
		h.activeSegmentID++
		newFilename := fmt.Sprintf("%s/slab-%d", h.dir, h.activeSegmentID)
		newF, err := os.OpenFile(newFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return 0, err
		}
		h.slabFiles[h.activeSegmentID] = newF
		f = newF

		*h.slabOffset = SlabOffset(uint64(h.activeSegmentID) << OffsetBits)
		h.activeSegmentSize = 0
	}

	offset := *h.slabOffset
	if _, err := writeAll(f, buf); err != nil {
		return 0, err
	}
	h.activeSegmentSize += int64(len(buf))
	return offset, nil
}

func (h *DB) addManySlabs(items []Item) ([]Key, error) {
	if len(items) == 0 {
		return nil, nil
	}

	if offsets, ok, err := h.addManySlabsDirect(items); err != nil {
		return nil, err
	} else if ok {
		return offsets, nil
	}

	return h.addManySlabsBuffered(items)
}

// addManySlabsDirect attempts a no-copy batch write using writev (net.Buffers).
// It only succeeds when the entire batch fits in the current slab segment.
func (h *DB) addManySlabsDirect(items []Item) ([]Key, bool, error) {
	f := h.slabFiles[h.activeSegmentID]
	if f == nil {
		return nil, false, fmt.Errorf("missing slab-%d", h.activeSegmentID)
	}

	maxSegmentSize := atomic.LoadInt64(&MaxSegmentSize)
	available := maxSegmentSize - h.activeSegmentSize
	if available <= 0 {
		return nil, false, nil
	}

	if cap(h.slabOffsets) < len(items) {
		h.slabOffsets = make([]Key, len(items))
	} else {
		h.slabOffsets = h.slabOffsets[:len(items)]
	}
	slabOffsets := h.slabOffsets

	currentOffset := *h.slabOffset
	totalBytes := 0

	headerBytes := len(items) * 16
	if cap(h.slabHeaders) < headerBytes {
		h.slabHeaders = make([]byte, headerBytes)
	} else {
		h.slabHeaders = h.slabHeaders[:headerBytes]
	}

	if cap(h.slabBuffers) < len(items)*3 {
		h.slabBuffers = make(net.Buffers, 0, len(items)*3)
	} else {
		h.slabBuffers = h.slabBuffers[:0]
	}

	for i, item := range items {
		keyBytes := item.Key
		valueBytes := item.Value

		var flags uint8
		if compressed, ok := compressValueIfEnabled(h.compressionEnabled, valueBytes); ok {
			valueBytes = compressed
			flags |= FlagCompressed
		}

		recordLen := 16 + len(keyBytes) + len(valueBytes)
		totalBytes += recordLen
		if int64(totalBytes) > available {
			return nil, false, nil
		}

		slabOffsets[i] = Key{slabOffset: currentOffset, hash: hash(keyBytes)}

		hdrOff := i * 16
		binary.LittleEndian.PutUint64(h.slabHeaders[hdrOff:hdrOff+8], uint64(len(keyBytes)))
		binary.LittleEndian.PutUint64(h.slabHeaders[hdrOff+8:hdrOff+16], packLength(uint64(len(valueBytes)), flags))
		h.slabBuffers = append(h.slabBuffers, h.slabHeaders[hdrOff:hdrOff+16], keyBytes, valueBytes)

		currentOffset += SlabOffset(recordLen)
	}

	if _, err := h.slabBuffers.WriteTo(f); err != nil {
		return nil, false, err
	}
	h.activeSegmentSize += int64(totalBytes)
	*h.slabOffset = currentOffset

	return slabOffsets, true, nil
}

func (h *DB) addManySlabsBuffered(items []Item) ([]Key, error) {
	if cap(h.slabOffsets) < len(items) {
		h.slabOffsets = make([]Key, len(items))
	} else {
		h.slabOffsets = h.slabOffsets[:len(items)]
	}
	slabOffsets := h.slabOffsets
	if cap(h.slabData) < 1<<20 {
		h.slabData = make([]byte, 0, 1<<20) // 1MB starting point; grows as needed.
	} else {
		h.slabData = h.slabData[:0]
	}

	f := h.slabFiles[h.activeSegmentID]
	if f == nil {
		return nil, fmt.Errorf("missing slab-%d", h.activeSegmentID)
	}

	maxSegmentSize := atomic.LoadInt64(&MaxSegmentSize)

	currentOffset := *h.slabOffset
	var scratch [16]byte

	flush := func() error {
		if len(h.slabData) == 0 {
			return nil
		}
		if _, err := writeAll(f, h.slabData); err != nil {
			return err
		}
		h.activeSegmentSize += int64(len(h.slabData))
		h.slabData = h.slabData[:0]
		*h.slabOffset = currentOffset
		return nil
	}

	rotate := func() error {
		if err := flush(); err != nil {
			return err
		}
		if err := h.rotateSlabSegment(); err != nil {
			return err
		}
		f = h.slabFiles[h.activeSegmentID]
		if f == nil {
			return fmt.Errorf("missing slab-%d after rotation", h.activeSegmentID)
		}
		currentOffset = *h.slabOffset
		return nil
	}

	for i, item := range items {
		keyBytes := item.Key
		valueBytes := item.Value

		var flags uint8
		if compressed, ok := compressValueIfEnabled(h.compressionEnabled, valueBytes); ok {
			valueBytes = compressed
			flags |= FlagCompressed
		}

		recordLen := 16 + len(keyBytes) + len(valueBytes)

		// Ensure we don't write a record across a segment boundary. If it doesn't fit,
		// flush current buffer and rotate to a new segment.
		if h.activeSegmentSize+int64(len(h.slabData))+int64(recordLen) > maxSegmentSize {
			if err := rotate(); err != nil {
				return nil, err
			}
		}

		slabOffsets[i] = Key{slabOffset: currentOffset, hash: hash(keyBytes)}

		binary.LittleEndian.PutUint64(scratch[:8], uint64(len(keyBytes)))
		binary.LittleEndian.PutUint64(scratch[8:], packLength(uint64(len(valueBytes)), flags))
		h.slabData = append(h.slabData, scratch[:]...)
		h.slabData = append(h.slabData, keyBytes...)
		h.slabData = append(h.slabData, valueBytes...)

		currentOffset += SlabOffset(recordLen)
	}

	if err := flush(); err != nil {
		return nil, err
	}

	return slabOffsets, nil
}

func (h *DB) addDeleteSlab(key []byte) error {
	keylen := len(key)
	actualTotalLength := 16 + keylen

	if cap(h.slabData) < actualTotalLength {
		h.slabData = make([]byte, 0, actualTotalLength)
	} else {
		h.slabData = h.slabData[:0]
	}

	var scratch [16]byte
	binary.LittleEndian.PutUint64(scratch[:8], uint64(keylen))
	binary.LittleEndian.PutUint64(scratch[8:], ^uint64(0))
	h.slabData = append(h.slabData, scratch[:]...)
	h.slabData = append(h.slabData, key...)

	_, err := h.writeSlabAndRotate(h.slabData)
	if err != nil {
		return err
	}
	*h.slabOffset += SlabOffset(actualTotalLength)
	return nil
}

func (h *DB) openSlabSegments() error {
	h.slabFiles = make(map[uint16]*os.File)

	files, err := os.ReadDir(h.dir)
	if err != nil {
		return err
	}

	maxID := -1
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "slab-") && !strings.HasSuffix(file.Name(), "-real") {
			var id int
			_, err := fmt.Sscanf(file.Name(), "slab-%d", &id)
			if err == nil {
				f, err := os.OpenFile(filepath.Join(h.dir, file.Name()), os.O_RDWR|os.O_APPEND, 0644)
				if err != nil {
					return err
				}
				h.slabFiles[uint16(id)] = f
				if id > maxID {
					maxID = id
				}
			}
		}
	}

	oldReal := h.dir + "/slab-real"
	if doesFileExist(oldReal) && maxID == -1 {
		os.Rename(oldReal, h.dir+"/slab-0")
		maxID = 0
		f, err := os.OpenFile(h.dir+"/slab-0", os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		h.slabFiles[0] = f
	}

	if maxID == -1 {
		maxID = 0
		f, err := os.Create(h.dir + "/slab-0")
		if err != nil {
			return err
		}
		h.slabFiles[0] = f
		h.activeSegmentSize = 0
	} else {
		// Initialize active segment size from the last segment on disk.
		lastFile := fmt.Sprintf("%s/slab-%d", h.dir, maxID)
		fi, err := os.Stat(lastFile)
		if err != nil {
			return err
		}
		h.activeSegmentSize = fi.Size()
	}

	h.activeSegmentID = uint16(maxID)
	return nil
}

func (h *DB) unmarshalKeyFromSlab(slabValues Key) ([]byte, error) {
	header, err := h.ReadBytes(slabValues.slabOffset, 16)
	if err != nil {
		return nil, err
	}

	keyLength, _ := decodeuint64(header[0:8])
	if keyLength == slabKeyLenControl {
		return nil, fmt.Errorf("unexpected control record at offset %d", slabValues.slabOffset)
	}
	if keyLength == 0 {
		return nil, nil
	}

	keyBytes, err := h.ReadBytes(slabValues.slabOffset+16, int64(keyLength))
	if err != nil {
		return nil, err
	}
	return keyBytes, nil
}

func (h *DB) unmarshalItemFromSlab(slabValues Key) (Item, error) {
	// Optimistic read: pull a full page to cover header + typical key/value with one syscall.
	const firstReadSize = int64(4096)
	buf, err := h.ReadBytes(slabValues.slabOffset, firstReadSize)
	if err != nil {
		// Fall back to just the header.
		buf, err = h.ReadBytes(slabValues.slabOffset, 16)
		if err != nil {
			return Item{}, err
		}
	}

	keyLength, _ := decodeuint64(buf[0:8])
	valueLengthPacked, _ := decodeuint64(buf[8:16])
	valueLength, flags := unpackLength(valueLengthPacked)

	totalLen := int64(16) + int64(keyLength) + int64(valueLength)

	var valuesBytes []byte
	if totalLen <= int64(len(buf)) {
		valuesBytes = buf[16:totalLen]
	} else {
		valuesBytes, err = h.ReadBytes(slabValues.slabOffset+16, int64(keyLength+valueLength))
		if err != nil {
			return Item{}, err
		}
	}

	key := valuesBytes[0:keyLength]
	val := valuesBytes[keyLength:]

	if flags&FlagCompressed != 0 {
		decompressed, err := s2.Decode(nil, val)
		if err != nil {
			return Item{}, err
		}
		val = decompressed
	}

	return Item{
		Key:   key,
		Value: val,
	}, nil
}

func decodeuint64(input []byte) (uint64, int) {
	return binary.LittleEndian.Uint64(input), 8
}

func (h *DB) openMetadata() (mmap.MMap, *os.File, error) {
	filename := h.dir + "/metadata"
	size := int64(4096)

	if !doesFileExist(filename) {
		f, err := os.Create(filename)
		if err != nil {
			return nil, nil, errors.Wrap(err, 1)
		}
		f.Seek(size-1, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)
		f.Sync()
		f.Close()
	}

	f, err := os.OpenFile(filename, os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, errors.Wrap(err, 1)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, nil, errors.Wrap(err, 1)
	}

	if size > fi.Size() {
		f.Seek(size-1, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)
		f.Sync()
	}

	data, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("failed to mmap file %s: %w", filename, err)
	}

	applyMadvise(data)
	return data, f, nil
}
