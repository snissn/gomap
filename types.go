package gomap

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

type SlabOffset uint64
type SlabValueLength uint64

type Key struct {
	//todo make slab sizes their own array and index into it
	hash            uint64
	slabOffset      SlabOffset
	slabValueLength SlabValueLength
}
type Hashmap struct {
	Folder     string
	FILE       *os.File
	keyMap     mmap.MMap
	slabMap    mmap.MMap
	slabFILE   *os.File
	slabOffset *SlabOffset
	slabSize   int64
	Capacity   *uint64
	Count      *uint64
	Keys       *[]Key
}

type Item struct {
	Key   string
	Value string
}
