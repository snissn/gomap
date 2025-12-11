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

	Count    *uint64
	Capacity uint64

	Keys       *[]Key
	slabOffset *SlabOffset

	hashTime   time.Duration
	resizeTime time.Duration
	slabTime   time.Duration

	slabData []byte
	
	slabFiles       map[uint16]*os.File
	activeSegmentId uint16
	
	CompressionEnabled bool
}

type Stats struct {
	KeyCount uint64
	Capacity uint64
	DataSize uint64
	Segments int
}

const (
	SegmentBits = 16
	OffsetBits  = 48
)

var MaxSegmentSize int64 = 64 * 1024 * 1024 // 64MB

type Item struct {
	Key   []byte
	Value []byte
}
