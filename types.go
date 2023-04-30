package gomap

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

// todo consider chunking slabs so that uint32 is enough
type SlabOffset uint32

type Hash uint32

type Key SlabOffset

type Hashmap struct {
	Folder string

	hashMapFile *os.File
	hashMap     mmap.MMap
	slabFILE    *os.File
	slabMap     mmap.MMap

	slabSize int64

	Count    *uint64
	Capacity uint64

	Keys       *[]Key
	slabOffset *SlabOffset
}

type Item struct {
	Key   []byte
	Value []byte
}
