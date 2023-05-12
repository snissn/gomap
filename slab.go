package gomap

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/go-errors/errors"
	"golang.org/x/sys/unix"
)

const SlabBufferSize = 4 * 256 * 4096

func (h *Hashmap) writeSlab(buf []byte) {
	// Append to slab buffer
	h.slabBuffer = append(h.slabBuffer, buf...)

	// If buffer size exceeds SlabBufferSize, flush the buffer
	if len(h.slabBuffer) >= SlabBufferSize {
		h.flushSlabBuffer()
	}
}

func (h *Hashmap) flushSlabBuffer() {
	if len(h.slabBuffer) == 0 {
		return
	}

	offset := *h.slabOffset
	slab := unsafe.Slice((*byte)(unsafe.Pointer(&h.slabMap[int(offset)-len(h.slabBuffer)])), len(h.slabBuffer))
	copy(slab, h.slabBuffer)

	// Reset slab buffer
	h.slabBuffer = []byte{}

	//actualTotalLength := len(h.slabBuffer)
	////*h.slabOffset += SlabOffset(actualTotalLength)
}

func (h *Hashmap) addSlab(item Item) Key {
	keyBytes := item.Key
	valueBytes := item.Value

	totalLength := len(keyBytes) + len(valueBytes) + 16 // 10 is the maximum length of LEB128 encoded uint64

	offset := *h.slabOffset

	// Make sure that offset + totalLength is within h.slabSize
	if uint64(offset)+uint64(totalLength) > uint64(h.slabSize) {
		err := h.doubleSlab()
		if err != nil {
			panic(err)
		}
	}

	slabData := []byte{}

	// Write key length
	slabData = append(slabData, encodeuint64(uint64(len(keyBytes)))...)
	slabData = append(slabData, encodeuint64(uint64(len(valueBytes)))...)
	slabData = append(slabData, keyBytes...)
	slabData = append(slabData, valueBytes...)
	h.writeSlab(slabData)

	actualTotalLength := 8 + 8 + len(keyBytes) + len(valueBytes)
	*h.slabOffset += SlabOffset(actualTotalLength)
	return Key(offset)
}

func (h *Hashmap) unmarshalItemFromSlab(slabValues Key) Item {
	var ret Item

	if slabValues+Key(16) > Key(*h.slabOffset)-Key(len(h.slabBuffer)) {
		h.flushSlabBuffer()
	}

	rawBytes := h.slabMap[slabValues:]

	keyLength, n := decodeuint64(rawBytes)
	valueLength, m := decodeuint64(rawBytes[n:])

	if slabValues+Key(16)+Key(keyLength)+Key(valueLength) > Key(*h.slabOffset)-Key(len(h.slabBuffer)) {
		h.flushSlabBuffer()
	}

	ret.Key = rawBytes[n+m : n+m+int(keyLength)]
	ret.Value = rawBytes[n+m+int(keyLength) : n+m+int(keyLength)+int(valueLength)]

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
