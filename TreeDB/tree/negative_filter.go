package tree

import (
	"hash/maphash"
	"sync/atomic"
)

// NegativeFilter is a fixed-size monotonic membership filter. The owner must
// prove complete key coverage for a root before attaching it to that Tree.
// Bits are never removed: concurrent inserts can only introduce false positives.
type NegativeFilter struct {
	seed  maphash.Seed
	words []atomic.Uint64
}

const NegativeFilterMaxKeyBytes = 1024

func NewNegativeFilter(bytes int) *NegativeFilter {
	if bytes < 8 || bytes > 64<<20 {
		return nil
	}
	return &NegativeFilter{seed: maphash.MakeSeed(), words: make([]atomic.Uint64, bytes/8)}
}

func (f *NegativeFilter) Add(key []byte) {
	if f == nil || len(key) > NegativeFilterMaxKeyBytes || len(f.words) == 0 {
		return
	}
	h := maphash.Bytes(f.seed, key)
	step := (h >> 32) | 1
	for i := 0; i < 7; i++ {
		bit := h % (uint64(len(f.words)) * 64)
		f.words[bit/64].Or(uint64(1) << (bit % 64))
		h += step
	}
}

func (f *NegativeFilter) DefinitelyAbsent(key []byte) bool {
	if f == nil || len(key) > NegativeFilterMaxKeyBytes || len(f.words) == 0 {
		return false
	}
	h := maphash.Bytes(f.seed, key)
	step := (h >> 32) | 1
	for i := 0; i < 7; i++ {
		bit := h % (uint64(len(f.words)) * 64)
		if f.words[bit/64].Load()&(uint64(1)<<(bit%64)) == 0 {
			return true
		}
		h += step
	}
	return false
}

// SetNegativeFilter attaches coverage for this exact root. Reset clears it.
func (t *Tree) SetNegativeFilter(f *NegativeFilter) { t.negativeFilter = f }

func (t *Tree) pointDefinitelyAbsent(key []byte) bool {
	absent := t.negativeFilter.DefinitelyAbsent(key)
	if treeHotReadStatsEnabled {
		if absent {
			treeNegativeRejectsTotal.Add(1)
		} else {
			treePointDescentsTotal.Add(1)
		}
	}
	return absent
}

// Bytes reports the fixed bit storage, excluding the small filter header.
func (f *NegativeFilter) Bytes() int {
	if f == nil {
		return 0
	}
	return len(f.words) * 8
}
