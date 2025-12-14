package crc

import (
	"hash"
	"hash/crc32"
	"sync"
)

var table = crc32.MakeTable(crc32.Castagnoli)

func Checksum(data []byte) uint32 {
	return crc32.Checksum(data, table)
}

var digestPool = sync.Pool{
	New: func() any { return crc32.New(table) },
}

func ChecksumParts(parts ...[]byte) uint32 {
	d := digestPool.Get().(hash.Hash32)
	d.Reset()
	for _, p := range parts {
		if len(p) == 0 {
			continue
		}
		_, _ = d.Write(p)
	}
	sum := d.Sum32()
	digestPool.Put(d)
	return sum
}
