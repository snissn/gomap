package pager

import "math/bits"

// dirtyChunkBits is the pager's primary dirty membership. Its storage follows
// mapped chunks; low/high bound only the dirty word span, not all old mappings.
// Pager.mu protects both membership and its exact cardinality.
type dirtyChunkBits struct {
	words            []uint64
	revision         uint64
	count, low, high int
}

func (d *dirtyChunkBits) grow(chunks int) {
	n := (chunks + 63) / 64
	if n <= len(d.words) {
		return
	}
	next := make([]uint64, n)
	copy(next, d.words)
	d.words = next
	d.revision++
}
func (d *dirtyChunkBits) contains(chunk int) bool {
	return chunk >= 0 && chunk/64 < len(d.words) && d.words[chunk/64]&(uint64(1)<<uint(chunk%64)) != 0
}
func (d *dirtyChunkBits) set(chunk int) {
	word := chunk / 64
	mask := uint64(1) << uint(chunk%64)
	if d.words[word]&mask != 0 {
		return
	}
	d.words[word] |= mask
	if d.count == 0 || word < d.low {
		d.low = word
	}
	if d.count == 0 || word >= d.high {
		d.high = word + 1
	}
	d.count++
	d.revision++
}

// takeFrom atomically detaches the selected dirty snapshot. New marks after
// the caller unlocks remain separate; failure restores this snapshot by OR.
func (d *dirtyChunkBits) takeFrom(first int) []int {
	out := make([]int, 0, d.count)
	low, high := d.low, d.high
	beforeCount := d.count
	for word := low; word < high; word++ {
		selected := d.words[word]
		if word < first/64 {
			continue
		}
		if word == first/64 {
			selected &= ^uint64(0) << uint(first%64)
		}
		d.words[word] &^= selected
		d.count -= bits.OnesCount64(selected)
		for selected != 0 {
			bit := bits.TrailingZeros64(selected)
			out = append(out, word*64+bit)
			selected &= selected - 1
		}
	}
	if d.count == 0 {
		d.low = 0
		d.high = 0
	} else {
		for d.low < d.high && d.words[d.low] == 0 {
			d.low++
		}
		for d.high > d.low && d.words[d.high-1] == 0 {
			d.high--
		}
	}
	if d.count != beforeCount {
		d.revision++
	}
	return out
}
