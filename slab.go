package gomap

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
)

func (h *Hashmap) writeSlab(buf []byte) error {
	_, err := h.realSlabFILE.Write(buf)
	return err
}

func (h *Hashmap) ReadBytes(offset SlabOffset, n int64) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := h.realSlabFILE.ReadAt(bytes, int64(offset))
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (h *Hashmap) addSlab(item Item) (Key, error) {
	offset := *h.slabOffset
	key := item.Key
	val := item.Value
	keylen := len(key)
	vallen := len(val)
	actualTotalLength := 16 + keylen + vallen
	if cap(h.slabData) < actualTotalLength {
		h.slabData = make([]byte, 0, actualTotalLength)
	} else {
		h.slabData = h.slabData[:0]
	}
	h.slabData = append(h.slabData, encodeuint64(uint64(keylen))...)
	h.slabData = append(h.slabData, encodeuint64(uint64(vallen))...)
	h.slabData = append(h.slabData, key...)
	h.slabData = append(h.slabData, val...)
	err := h.writeSlab(h.slabData)
	if err != nil {
		return Key{}, err
	}
	*h.slabOffset += SlabOffset(actualTotalLength)
	return Key{slabOffset: offset, hash: hash(key)}, nil
}

func (h *Hashmap) addManySlabs(items []Item) ([]Key, error) {
	slabOffsets := make([]Key, len(items))
	if cap(h.slabData) < len(items)*2048 {
		h.slabData = make([]byte, 0, len(items)*2048)
	} else {
		h.slabData = h.slabData[:0]
	}
	offset := *h.slabOffset
	for i, item := range items {
		keyBytes := item.Key
		valueBytes := item.Value
		totalLength := len(keyBytes) + len(valueBytes) + 16
		slabOffsets[i] = Key{slabOffset: offset, hash: hash(keyBytes)}
		h.slabData = append(h.slabData, encodeuint64(uint64(len(keyBytes)))...)
		h.slabData = append(h.slabData, encodeuint64(uint64(len(valueBytes)))...)
		h.slabData = append(h.slabData, keyBytes...)
		h.slabData = append(h.slabData, valueBytes...)
		offset += SlabOffset(totalLength)
	}
	err := h.writeSlab(h.slabData)
	if err != nil {
		return nil, err
	}
	*h.slabOffset += SlabOffset(len(h.slabData))
	return slabOffsets, nil
}

func (h *Hashmap) unmarshalItemFromSlab(slabValues Key) (Item, error) {
	headerBytes, err := h.ReadBytes(slabValues.slabOffset, int64(16))
	if err != nil {
		return Item{}, err
	}
	keyLength, _ := decodeuint64(headerBytes[0:8])
	valueLength, _ := decodeuint64(headerBytes[8:16])
	valuesBytes, err := h.ReadBytes(slabValues.slabOffset+16, int64(keyLength+valueLength))
	if err != nil {
		return Item{}, err
	}
	return Item{
		Key:   valuesBytes[0:keyLength],
		Value: valuesBytes[keyLength:],
	}, nil
}

func decodeuint64(input []byte) (uint64, int) {
	return binary.LittleEndian.Uint64(input), 8
}

func encodeuint64(input uint64) []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, input)
	return ret
}

func encodeLEB128(slab []byte, input uint64) int {
	var i int
	for input >= 0x80 {
		slab[i] = byte(input&0x7F | 0x80)
		input >>= 7
		i++
	}
	slab[i] = byte(input)
	return i + 1
}

func decodeLEB128(input []byte) (uint64, int) {
	var result uint64
	var shift uint
	var length int
	for {
		b := input[length]
		length++
		result |= (uint64(b&0x7F) << shift)
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return result, length
}

func (h *Hashmap) openMetadata() (mmap.MMap, *os.File, error) {
	filename := h.Folder + "/metadata"
	// We only need a small space for Count and SlabOffset. 4KB is plenty (OS page size).
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

	// applyFadvise(int(f.Fd()), fi.Size()) // Optional for small metadata

	data, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("failed to mmap file %s: %w", filename, err)
	}

	applyMadvise(data)
	return data, f, nil
}


func (h *Hashmap) openDataFile() (*os.File, error) {
	filename := h.Folder + "/slab-real"
	if !doesFileExist(filename) {
		_, _ = os.Create(filename)
	}
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, errors.Wrap(err, 1)
	}
	return file, nil
}
