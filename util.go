package gomap

import (
	"encoding/binary"
	"fmt"

	"os"
	"runtime"
	"unsafe"

	"github.com/edsrzf/mmap-go"
)

func getCPUNumber() int {
	return runtime.NumCPU()
}

func handleError(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}
func doesFileExist(fileName string) bool {
	_, error := os.Stat(fileName)
	// check if error is "file not exists"
	if os.IsNotExist(error) {
		return false
	} else {
		return true
	}
}

func createByteSlice(byteArray []byte) []byte {
	sliceLength := 8
	if len(byteArray) >= sliceLength {
		return byteArray[:sliceLength]
	}
	sliceArray := make([]byte, sliceLength)
	copy(sliceArray, byteArray)
	return sliceArray
}

func hash(key []byte) Hash {
	return Hash(binary.LittleEndian.Uint64(createByteSlice(key)))
	//return Hash(fnv1.HashBytes64(key))
}

func NtoBytesHashmap(N uint64) int64 {
	i := Hash(0)
	return int64(unsafe.Sizeof(i)) * int64(N)
}

func NtoBytesHashmapOffsetIndex(N uint64) int64 {
	i := SlabOffset(0)

	return (int64(unsafe.Sizeof(i))) * int64(N)
}

func getSlabOffset(slabMap mmap.MMap) *SlabOffset {
	cap := (*SlabOffset)(unsafe.Pointer(&slabMap[0]))
	return cap
}

func getCount(slabMap mmap.MMap) *uint64 {
	return (*uint64)(unsafe.Pointer(&slabMap[8]))
}
