package gomap

import (
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
)

// todo consider chunking slabs so that uint32 is enough
type SlabOffset uint64

const Tombstone SlabOffset = 0xFFFFFFFFFFFFFFFF

type Hash uint64

type Key struct {
	//todo try do tricks to make both 32 bit nums for speed
	slabOffset SlabOffset
	hash       Hash
}

type Hashmap struct {
	Folder string

	hashMapFile  *os.File
	hashMap      mmap.MMap
	metadataFile *os.File
	metadataMap  mmap.MMap

	realSlabFILE *os.File

	Count    *uint64
	Capacity uint64

	Keys       *[]Key
	slabOffset *SlabOffset

	hashTime   time.Duration
	resizeTime time.Duration
	slabTime   time.Duration

	slabData []byte
}

type Item struct {
	Key   []byte
	Value []byte
}
