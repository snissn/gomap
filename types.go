package gomap

import (
	"os"

	"github.com/edsrzf/mmap-go"
)

type Key struct {
	data uint64
	hash uint64
}
type Hashmap struct {
	Folder     string
	FILE       *os.File
	keyMap     mmap.MMap
	slabMap    mmap.MMap
	slabFILE   *os.File
	slabOffset *uint64
	slabSize   int64
	Capacity   *uint64
	Count      *uint64
	Keys       *[]Key
}

type Item struct {
	Key   string
	Value string
}
