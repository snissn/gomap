package caching

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

type immutableBloomChecker interface {
	// MightContainHash returns false only if the key is definitely not present in
	// this immutable memtable. False positives are allowed.
	MightContainHash(keyHash uint64) bool
}

type immutableBloomMemtable struct {
	memtable.Table
	bloom *bloomFilter
}

func (m *immutableBloomMemtable) MightContainHash(keyHash uint64) bool {
	if m == nil || m.bloom == nil {
		return true
	}
	return m.bloom.mightContainHash(keyHash)
}

func newImmutableBloomMemtable(t memtable.Table) (*immutableBloomMemtable, error) {
	if t == nil {
		return nil, errors.New("cachingdb: nil immutable memtable")
	}
	n := t.Len()
	if n <= 0 {
		return &immutableBloomMemtable{Table: t}, nil
	}

	bloom := newBloomFilter(n)
	iter := t.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	for iter.Valid() {
		bloom.addHash(hashKey(iter.UnsafeKey()))
		iter.Next()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return &immutableBloomMemtable{Table: t, bloom: bloom}, nil
}

type bloomFilter struct {
	mask uint64
	bits []uint64
}

const (
	bloomBitsPerKey = 10
	bloomK          = 4
	bloomMinBits    = 1 << 10 // 1024 bits
	bloomMaxBits    = 1 << 28 // 256M bits (~32MiB)
)

func newBloomFilter(n int) *bloomFilter {
	if n <= 0 {
		n = 1
	}
	want := uint64(n) * bloomBitsPerKey
	if want < bloomMinBits {
		want = bloomMinBits
	}
	if want > bloomMaxBits {
		want = bloomMaxBits
	}
	m := nextPow2(want)
	words := (m + 63) / 64
	return &bloomFilter{
		mask: m - 1,
		bits: make([]uint64, words),
	}
}

func (b *bloomFilter) addHash(h uint64) {
	h1 := uint32(h)
	h2 := uint32(h>>32) | 1
	for i := uint32(0); i < bloomK; i++ {
		bit := uint64(h1+i*h2) & b.mask
		b.bits[bit>>6] |= 1 << (bit & 63)
	}
}

func (b *bloomFilter) mightContainHash(h uint64) bool {
	h1 := uint32(h)
	h2 := uint32(h>>32) | 1
	for i := uint32(0); i < bloomK; i++ {
		bit := uint64(h1+i*h2) & b.mask
		if (b.bits[bit>>6]>>(bit&63))&1 == 0 {
			return false
		}
	}
	return true
}

func nextPow2(v uint64) uint64 {
	if v <= 1 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}
