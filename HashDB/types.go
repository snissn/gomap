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

// DB is a single-shard HashDB instance.
//
// It is not safe for concurrent use; prefer HashDB for most applications.
type DB struct {
	dir string

	controlFile  *os.File
	controlMap   mmap.MMap
	keyFile      *os.File
	keyMap       mmap.MMap
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

	indexMemoryPolicy    IndexMemoryPolicy
	indexMemoryPolicySet bool
	controlsLocked       bool

	// Incremental rehash state (per-shard, in-memory only).
	rehashInProgress     bool
	rehashOldControlFile *os.File
	rehashOldControlMap  mmap.MMap
	rehashOldKeyFile     *os.File
	rehashOldKeyMap      mmap.MMap
	rehashOldCapacity    uint64
	rehashOldKeys        []Key
	rehashOldControls    []byte
	rehashIdx            uint64

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
