package primaryarena

import (
	"github.com/snissn/gomap/TreeDB/page"
	"unsafe"
)

// ClaimMetadataRangeOrdinary reserves actual contiguous manifest banks for an
// ordinary publisher. Native bounded preparation must use its own charged
// cursor; this entry point never hides a scan behind a native work receipt.
func (a *Arena) ClaimMetadataRangeOrdinary(count uint32) ([]Ref, error) {
	if count == 0 {
		return nil, ErrFormat
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.recovered || a.metadataDecoder == nil {
		return nil, ErrUnrecovered
	}
	n := uint64(count)
	local := a.next
	if n > maxChunks*chunkBanks-2 {
		return nil, ErrFull
	}
	// Reuse a genuinely released contiguous generic range. Paired publications
	// remain indivisible until their own pair-release protocol completes.
	for start := uint64(2); start+n <= a.next; start++ {
		a.counters.EdgeReads++
		a.counters.Bytes += uint64(unsafe.Sizeof(bank{}))
		available := true
		for i := uint64(0); i < n; i++ {
			b := a.slot(Namespace + start + i)
			a.counters.EdgeReads++
			a.counters.Bytes += uint64(unsafe.Sizeof(bank{}))
			if b == nil || b.refs != 0 || !b.reusable || b.pair != 0 {
				available = false
				break
			}
		}
		if available {
			local = start
			break
		}
	}
	if local == a.next {
		if local+n > maxChunks*chunkBanks {
			return nil, ErrFull
		}
		if e := a.pager.GrowTo(local + n); e != nil {
			return nil, e
		}
		a.next += n
	}
	refs := make([]Ref, count)
	for i := uint64(0); i < n; i++ {
		id := local + i
		if a.chunks[id/chunkBanks] == nil {
			a.chunks[id/chunkBanks] = new(bankChunk)
			a.counters.Bytes += uint64(unsafe.Sizeof(bankChunk{}))
		}
		b := &a.chunks[id/chunkBanks][id%chunkBanks]
		inc, next, listed := b.incarnation+1, b.freeNext, b.genericListed
		*b = bank{incarnation: inc, refs: 1, class: Manifest, freeNext: next, genericListed: listed}
		refs[i] = Ref{Namespace + id, inc}
		a.counters.Claims++
		a.counters.Bytes += 2*uint64(unsafe.Sizeof(bank{})) + uint64(unsafe.Sizeof(Ref{}))
	}
	a.epoch++
	a.pager.SetPageCount(a.next)
	a.counters.Bytes += n * page.PageSize
	return refs, nil
}
