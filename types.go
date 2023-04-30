package gomap

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

<<<<<<< HEAD
type SlabOffset uint32
type SlabValueLength uint32
=======
// todo consider chunking slabs so that uint32 is enough
type SlabOffset uint32
>>>>>>> nostrings

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
