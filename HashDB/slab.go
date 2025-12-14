package hashdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
	"github.com/klauspost/compress/s2"
)

const FlagCompressed = 0x80

func packLength(len uint64, flags uint8) uint64 {
	return len | (uint64(flags) << 56)
}

func unpackLength(packed uint64) (uint64, uint8) {
	len := packed & 0x00FFFFFFFFFFFFFF
	flags := uint8(packed >> 56)
	return len, flags
}

func (h *Hashmap) writeSlab(buf []byte) error {
	// Check if active segment is full using in-memory size accounting
	f := h.slabFiles[h.activeSegmentId]
	if h.activeSegmentSize+int64(len(buf)) > MaxSegmentSize {
		// Rotate to a new segment
		h.activeSegmentId++
		newFilename := fmt.Sprintf("%s/slab-%d", h.Folder, h.activeSegmentId)
		newF, err := os.OpenFile(newFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		h.slabFiles[h.activeSegmentId] = newF
		f = newF

		*h.slabOffset = SlabOffset(uint64(h.activeSegmentId) << OffsetBits)
		h.activeSegmentSize = 0
	}

	n, err := f.Write(buf)
	if err == nil {
		h.activeSegmentSize += int64(n)
	}
	return err
}

func (h *Hashmap) ReadBytes(offset SlabOffset, n int64) ([]byte, error) {
	segmentID := uint16(uint64(offset) >> OffsetBits)
	localOffset := int64(uint64(offset) & ((1 << OffsetBits) - 1))

	f, ok := h.slabFiles[segmentID]
	if !ok {
		return nil, fmt.Errorf("segment %d not found", segmentID)
	}

	bytes := make([]byte, n)
	_, err := f.ReadAt(bytes, localOffset)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (h *Hashmap) addSlab(item Item) (Key, error) {
	key := item.Key
	val := item.Value

	// Compress value?
	var flags uint8
	if h.CompressionEnabled && len(val) > 32 { // Only try compressing if > 32 bytes
		compressed := s2.Encode(nil, val)
		if len(compressed) < len(val) {
			val = compressed
			flags |= FlagCompressed
		}
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

func (h *Hashmap) writeSlabAndRotate(buf []byte) (SlabOffset, error) {
	f := h.slabFiles[h.activeSegmentId]
	if h.activeSegmentSize+int64(len(buf)) > MaxSegmentSize {
		h.activeSegmentId++
		newFilename := fmt.Sprintf("%s/slab-%d", h.Folder, h.activeSegmentId)
		newF, err := os.OpenFile(newFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return 0, err
		}
		h.slabFiles[h.activeSegmentId] = newF
		f = newF

		*h.slabOffset = SlabOffset(uint64(h.activeSegmentId) << OffsetBits)
		h.activeSegmentSize = 0
	}

	offset := *h.slabOffset
	n, err := f.Write(buf)
	if err == nil {
		h.activeSegmentSize += int64(n)
	}
	return offset, err
}

func (h *Hashmap) addManySlabs(items []Item) ([]Key, error) {
	slabOffsets := make([]Key, len(items))
	if cap(h.slabData) < len(items)*2048 {
		h.slabData = make([]byte, 0, len(items)*2048)
	} else {
		h.slabData = h.slabData[:0]
	}

	var scratch [16]byte

	totalBatchSize := 0
	for _, item := range items {
		totalBatchSize += 16 + len(item.Key) + len(item.Value)
	}

	// Check rotation once for batch using in-memory size.
	// If batch > MaxSegmentSize, this logic needs to be smarter (split batch).
	// For now assume batch fits.
	if h.activeSegmentSize+int64(totalBatchSize) > MaxSegmentSize {
		h.activeSegmentId++
		newFilename := fmt.Sprintf("%s/slab-%d", h.Folder, h.activeSegmentId)
		newF, err := os.OpenFile(newFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		h.slabFiles[h.activeSegmentId] = newF
		*h.slabOffset = SlabOffset(uint64(h.activeSegmentId) << OffsetBits)
		h.activeSegmentSize = 0
	}

	f := h.slabFiles[h.activeSegmentId]
	startOffset := *h.slabOffset
	currentOffset := startOffset

	for i, item := range items {
		keyBytes := item.Key
		valueBytes := item.Value

		var flags uint8
		if h.CompressionEnabled && len(valueBytes) > 32 {
			compressed := s2.Encode(nil, valueBytes)
			if len(compressed) < len(valueBytes) {
				valueBytes = compressed
				flags |= FlagCompressed
			}
		}

		totalLength := 16 + len(keyBytes) + len(valueBytes)

		slabOffsets[i] = Key{slabOffset: currentOffset, hash: hash(keyBytes)}

		binary.LittleEndian.PutUint64(scratch[:8], uint64(len(keyBytes)))
		binary.LittleEndian.PutUint64(scratch[8:], packLength(uint64(len(valueBytes)), flags))
		h.slabData = append(h.slabData, scratch[:]...)
		h.slabData = append(h.slabData, keyBytes...)
		h.slabData = append(h.slabData, valueBytes...)

		currentOffset += SlabOffset(totalLength)
	}

	n, err := f.Write(h.slabData)
	if err != nil {
		return nil, err
	}
	h.activeSegmentSize += int64(n)
	*h.slabOffset = currentOffset
	return slabOffsets, nil
}

func (h *Hashmap) addDeleteSlab(key []byte) error {
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

func (h *Hashmap) openSlabSegments() error {
	h.slabFiles = make(map[uint16]*os.File)

	files, err := os.ReadDir(h.Folder)
	if err != nil {
		return err
	}

	maxID := -1
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "slab-") && !strings.HasSuffix(file.Name(), "-real") {
			var id int
			_, err := fmt.Sscanf(file.Name(), "slab-%d", &id)
			if err == nil {
				f, err := os.OpenFile(filepath.Join(h.Folder, file.Name()), os.O_RDWR|os.O_APPEND, 0644)
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

	oldReal := h.Folder + "/slab-real"
	if doesFileExist(oldReal) && maxID == -1 {
		os.Rename(oldReal, h.Folder+"/slab-0")
		maxID = 0
		f, err := os.OpenFile(h.Folder+"/slab-0", os.O_RDWR|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		h.slabFiles[0] = f
	}

	if maxID == -1 {
		maxID = 0
		f, err := os.Create(h.Folder + "/slab-0")
		if err != nil {
			return err
		}
		h.slabFiles[0] = f
		h.activeSegmentSize = 0
	} else {
		// Initialize active segment size from the last segment on disk.
		lastFile := fmt.Sprintf("%s/slab-%d", h.Folder, maxID)
		fi, err := os.Stat(lastFile)
		if err != nil {
			return err
		}
		h.activeSegmentSize = fi.Size()
	}

	h.activeSegmentId = uint16(maxID)
	return nil
}

func (h *Hashmap) unmarshalItemFromSlab(slabValues Key) (Item, error) {
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

func (h *Hashmap) openMetadata() (mmap.MMap, *os.File, error) {
	filename := h.Folder + "/metadata"
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

	f, err := os.OpenFile(filename, os.O_RDWR, 0655)
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
