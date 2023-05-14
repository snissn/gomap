package gomap

import (
	"os"
	"sync"
	"time"

	"github.com/edsrzf/mmap-go"
)

// todo consider chunking slabs so that uint32 is enough
type SlabOffset uint64

type Hash uint64

type Key struct {
	//todo try do tricks to make both 32 bit nums for speed
	slabOffset SlabOffset
	hash       Hash
}

type Hashmap struct {
	Folder string

	hashMapFile *os.File
	hashMap     mmap.MMap
	slabFILE    *os.File

	resizeLk   sync.Mutex
	mapLk      sync.RWMutex
	slabMap    mmap.MMap
	slabMapOld mmap.MMap

	realSlabFILE *os.File

	slabSize int64

	Count       *uint64
	Capacity    uint64
	oldCapacity uint64

	Keys       *[]Key
	oldKeys    *[]Key
	slabOffset *SlabOffset

	hashTime   time.Duration
	resizeTime time.Duration
	slabTime   time.Duration
}

type Item struct {
	Key   []byte
	Value []byte
}
