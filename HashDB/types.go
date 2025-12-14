package hashdb

import (
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
)

type SlabOffset uint64

const Tombstone SlabOffset = 0xFFFFFFFFFFFFFFFF

type Hash uint64

// DefaultCapacity is used when no capacity metadata exists on disk.
const DefaultCapacity uint64 = 32 * 1024

type Key struct {
	slabOffset SlabOffset
	hash       Hash
}

type DB struct {
	dir string

	hashMapFile  *os.File
	hashMap      mmap.MMap
	metadataFile *os.File
	metadataMap  mmap.MMap

	count    *uint64
	capacity uint64

	keys       []Key
	controls   []byte
	slabOffset *SlabOffset

	hashTime   time.Duration
	resizeTime time.Duration
	slabTime   time.Duration

	slabData []byte

	slabFiles       map[uint16]*os.File
	activeSegmentID uint16

	activeSegmentSize int64

	compressionEnabled bool

	// Incremental rehash state (per-shard, in-memory only).
	rehashInProgress  bool
	rehashOldMapFile  *os.File
	rehashOldMap      mmap.MMap
	rehashOldCapacity uint64
	rehashOldKeys     []Key
	rehashOldControls []byte
	rehashIdx         uint64

	resizeThreshold uint64
}

// Hashmap is kept as a compatibility alias for older code.
// New code should use DB.
type Hashmap = DB

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
