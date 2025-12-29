package compaction

import (
	"encoding/binary"
	"math/bits"

	"github.com/cespare/xxhash/v2"
	"github.com/snissn/gomap/TreeDB/page"
)

const (
	bloomBitsPerEntry = 10
	bloomHashCount    = 7
	bloomMinBits      = 1024
)

type bloomFilter struct {
	bits []uint64
	mask uint64
	k    uint8
}

func newBloomFilter(entries int) *bloomFilter {
	if entries <= 0 {
		entries = 1
	}
	bitCount := uint64(entries) * bloomBitsPerEntry
	if bitCount < bloomMinBits {
		bitCount = bloomMinBits
	}
	bitCount = nextPowerOfTwo(bitCount)
	words := (bitCount + 63) / 64
	return &bloomFilter{
		bits: make([]uint64, words),
		mask: bitCount - 1,
		k:    bloomHashCount,
	}
}

func (b *bloomFilter) add(ptr page.ValuePtr) {
	if b == nil || len(b.bits) == 0 {
		return
	}
	h1, h2 := bloomHashes(ptr)
	for i := uint8(0); i < b.k; i++ {
		idx := (h1 + uint64(i)*h2) & b.mask
		b.bits[idx>>6] |= 1 << (idx & 63)
	}
}

func (b *bloomFilter) mayContain(ptr page.ValuePtr) bool {
	if b == nil || len(b.bits) == 0 {
		return false
	}
	h1, h2 := bloomHashes(ptr)
	for i := uint8(0); i < b.k; i++ {
		idx := (h1 + uint64(i)*h2) & b.mask
		if b.bits[idx>>6]&(1<<(idx&63)) == 0 {
			return false
		}
	}
	return true
}

func bloomHashes(ptr page.ValuePtr) (uint64, uint64) {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], ptr.Offset)
	binary.LittleEndian.PutUint32(buf[8:12], ptr.Length)
	binary.LittleEndian.PutUint32(buf[12:16], ptr.FileID)
	h1 := xxhash.Sum64(buf[:])
	h2 := bits.RotateLeft64(h1, 17) ^ 0x9e3779b97f4a7c15
	if h2 == 0 {
		h2 = 0x9e3779b97f4a7c15
	}
	return h1, h2
}

func nextPowerOfTwo(v uint64) uint64 {
	if v <= 1 {
		return 1
	}
	return 1 << (64 - bits.LeadingZeros64(v-1))
}
