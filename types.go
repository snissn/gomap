package gomap

import (
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
)

// todo consider chunking slabs so that uint32 is enough
type SlabOffset uint64

type Hash uint64

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

	hashTime   time.Duration
	slabTime   time.Duration
	slabBuffer []byte
}

type Item struct {
	Key   []byte
	Value []byte
}
