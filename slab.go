package gomap

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
	"golang.org/x/sys/unix"
)

func (h *Hashmap) writeSlab(buf []byte) {
	_, err := h.realSlabFILE.Write(buf) // Write the buffer to the file
	if err != nil {
		panic(err)
	}

}

// ReadBytes reads N bytes from a given offset in the file
func (h *Hashmap) ReadBytes(offset SlabOffset, n int64) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := h.realSlabFILE.ReadAt(bytes, int64(offset))

	if err != nil {
		return nil, err
	}

	return bytes, nil
}

func (h *Hashmap) addSlab(item Item) Key {
	offset := *h.slabOffset
	key := item.Key
	val := item.Value
	keylen := len(key)
	vallen := len(val)
	actualTotalLength := 16 + keylen + vallen
	slabData := make([]byte, 0, actualTotalLength)
	// Write key length
	slabData = append(slabData, encodeuint64(uint64(keylen))...)
	slabData = append(slabData, encodeuint64(uint64(vallen))...)
	slabData = append(slabData, key...)
	slabData = append(slabData, val...)
	h.writeSlab(slabData)
	*h.slabOffset += SlabOffset(actualTotalLength)
	ret := Key{slabOffset: offset, hash: hash(key)} // todo only actually compute hash() once
	return ret
}

func (h *Hashmap) addManySlabs(items []Item) []Key {
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
		totalLength := len(keyBytes) + len(valueBytes) + 16 // 10 is the maximum length of LEB128 encoded uint64

		slabOffsets[i] = Key{slabOffset: offset, hash: hash(keyBytes)} // todo only actually compute hash() once

		// Write key length
		h.slabData = append(h.slabData, encodeuint64(uint64(len(keyBytes)))...)
		h.slabData = append(h.slabData, encodeuint64(uint64(len(valueBytes)))...)
		h.slabData = append(h.slabData, keyBytes...)
		h.slabData = append(h.slabData, valueBytes...)
		offset += SlabOffset(totalLength)

	}
	h.writeSlab(h.slabData)
	*h.slabOffset += SlabOffset(len(h.slabData))
	return slabOffsets

}
func (h *Hashmap) unmarshalItemFromSlab(slabValues Key) Item {
	var ret Item

	headerBytes, err := h.ReadBytes(slabValues.slabOffset, int64(16))
	if err != nil {
		fmt.Println("slabValues", slabValues)
		panic(err)
	}
	keyLength, _ := decodeuint64(headerBytes[0:8])
	valueLength, _ := decodeuint64(headerBytes[8:16])

	valuesBytes, err := h.ReadBytes(slabValues.slabOffset+16, int64(keyLength+valueLength))
	if err != nil {
		panic(err)
	}

	ret.Key = valuesBytes[0:keyLength]
	ret.Value = valuesBytes[keyLength:]

	return ret
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

func (h *Hashmap) openMmapSlab(slabSize int64) (mmap.MMap, *os.File, error) {
	var f *os.File
	var err error

	err = os.MkdirAll(h.Folder, 0755)
	if err != nil {
		log.Fatal("1", h.Folder, "2", errors.Wrap(err, 1))
	}

	filename := h.Folder + "/slab"

	realfilename := filename + "-real"
	if !doesFileExist(realfilename) {
		_, _ = os.Create(realfilename)
	}
	fmt.Println(realfilename)

	file, err := os.OpenFile(realfilename, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal("2", errors.Wrap(err, 1))
	}
	h.realSlabFILE = file

	if !doesFileExist(filename) {
		f, err = os.Create(filename)

		if err != nil {
			log.Fatal("2", errors.Wrap(err, 1))
		}
		f.Seek(slabSize-1, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)
		f.Sync()
		f.Close()
	}
	f, err = os.OpenFile(filename, os.O_RDWR, 0655)
	//todo test:
	//    f, err = os.OpenFile(filename, os.O_RDWR|os.O_SYNC, 0655)
	if err != nil {
		log.Fatal("3", errors.Wrap(err, 1))
	}

	fi, err := f.Stat()
	if err != nil {
		log.Fatal("4", errors.Wrap(err, 1))
	}
	if slabSize > fi.Size() { // need to expand file
		f.Seek(slabSize-1, 0)
		f.Write([]byte("\x00"))
		f.Seek(0, 0)
		f.Sync()
	}

	// Advise the kernel that we intend to access the file sequentially.
	// This will enable the kernel to do read-ahead and improve write performance.
	if err := unix.Fadvise(int(f.Fd()), 0, int64(fi.Size()), unix.FADV_SEQUENTIAL); err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("failed to advise kernel for file %s: %w", filename, err)
	}

	// mmap the whole file into memory with read-write permissions.
	// As the file is larger than memory, it won't be fully loaded into memory.
	// Instead, the kernel will load and unload parts of the file as needed.
	ret, err := unix.Mmap(int(f.Fd()), 0, int(fi.Size()), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, nil, fmt.Errorf("failed to mmap file %s: %w", filename, err)
	}

	// Advise the kernel that the mapped memory will be accessed soon.
	// This will help to reduce the number of page faults in the beginning of the processing.
	if err := unix.Madvise(ret, unix.MADV_WILLNEED); err != nil {
		unix.Munmap(ret)
		f.Close()
		return nil, nil, fmt.Errorf("failed to advise kernel for file %s: %w", filename, err)
	}

	return ret, f, err
}

func (h *Hashmap) doubleSlab() error {
	f := h.slabFILE
	f.Seek(2*h.slabSize-1, 0)
	f.Write([]byte("\x00"))
	f.Seek(0, 0)
	f.Sync()
	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	h.slabSize *= 2
	h.slabMap = m
	return nil
}
