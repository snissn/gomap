package gomap

import (
	"fmt"

	"os"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/segmentio/fasthash/fnv1"
)

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

func hash(key string) uint64 {
	return fnv1.HashString64(key)
}

func NtoBytes(N uint64) int64 {
	return int64(size*3) * int64(2+N)
}

func getSlabOffset(slabMap mmap.MMap) *SlabOffset {
	cap := (*SlabOffset)(unsafe.Pointer(&slabMap[0]))
	return cap
}

func getCapacity(keyMap mmap.MMap) *uint64 {
	cap := (*uint64)(unsafe.Pointer(&keyMap[0]))
	if *cap == 0 {
		*cap = uint64(DEFAULTMAPSIZE)
	}
	return cap
}

func getCount(keyMap mmap.MMap) *uint64 {
	return (*uint64)(unsafe.Pointer(&keyMap[size]))
}
