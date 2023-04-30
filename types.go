package gomap

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

type SlabOffset uint32

type Hash uint32

type Key struct {
	hash Hash
}

type SlabValues struct {
	slabOffset SlabOffset
}

type Hashmap struct {
	Folder string

	hashMapFile          *os.File
	hashMap              mmap.MMap
	hashMapSlabValueFile *os.File
	hashMapSlabValue     mmap.MMap
	slabFILE             *os.File
	slabMap              mmap.MMap

	slabSize int64

	Count    *uint64
	Capacity uint64

	Keys       *[]Key
	slabOffset *SlabOffset

	SlabValues *[]SlabValues
}

type Item struct {
	Key   string
	Value string
}
