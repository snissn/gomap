package hashdb

import (
	"net"
	"os"
	"time"

	"github.com/edsrzf/mmap-go"
	"github.com/snissn/gomap/HashDB/internal/lockfile"
)

// SlabOffset encodes a segment ID and byte offset within a slab file.
type SlabOffset uint64

// Tombstone is a sentinel slab offset representing a deleted key.
const Tombstone SlabOffset = 0xFFFFFFFFFFFFFFFF

// Hash is the 64-bit hash value used for key placement.
type Hash uint64

// DefaultCapacity is used when no capacity metadata exists on disk.
const DefaultCapacity uint64 = 32 * 1024

// Key holds metadata for a stored key in the hash index.
type Key struct {
	slabOffset SlabOffset
	hash       Hash
}

// DB is a single-shard HashDB instance.
//
// It is not safe for concurrent use; prefer HashDB for most applications.
type DB struct {
	dir string

	lock *lockfile.Lock

	opened bool // set to true only after a successful Open

	batchSeq uint64 // monotonically increasing batch ID (process-local)

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

	slabData    []byte
	slabOffsets []Key
	slabHeaders []byte
	slabBuffers net.Buffers

	slabFiles       map[uint16]*os.File
	activeSegmentID uint16

	activeSegmentSize int64

	compressionEnabled bool

	// Read-only slab mmaps for sealed segments (segmentID < activeSegmentID).
	// These are an optimization to reduce syscalls for read-heavy workloads.
	slabROFiles map[uint16]*os.File
	slabROMaps  map[uint16]mmap.MMap

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

	resizeThreshold            uint64
	maxProbeGroupsBeforeResize uint64
}

// Hashmap is kept as a compatibility alias for older code.
// New code should use DB.
type Hashmap = DB

// Stats captures high-level storage metrics for a DB instance.
type Stats struct {
	KeyCount uint64
	Capacity uint64
	DataSize uint64
	Segments int
}

const (
	// SegmentBits encodes the slab segment ID width within a SlabOffset.
	SegmentBits = 16
	// OffsetBits encodes the byte-offset width within a SlabOffset.
	OffsetBits = 48
)

// MaxSegmentSize controls the maximum bytes per slab segment.
var MaxSegmentSize int64 = 64 * 1024 * 1024 // 64MB

// Item represents a key/value pair used in batch-style APIs.
type Item struct {
	Key   []byte
	Value []byte
}
