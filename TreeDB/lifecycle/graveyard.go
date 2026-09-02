package lifecycle

import (
	"sync"
)

// RetiredBatch represents a set of pages retired at a specific commit sequence.
// It is returned by ExtractBatchesUpTo so callers can reinsert pages on failure.
type RetiredBatch struct {
	Seq uint64
	IDs []uint64
}

type batch struct {
	seq uint64
	ids []uint64
}

type Graveyard struct {
	mu           sync.Mutex
	retiredPages []batch // Ordered by CommitSeq
}

type GraveyardStats struct {
	Batches uint64
	Pages   uint64
	MinSeq  uint64
	MaxSeq  uint64
}

func NewGraveyard() *Graveyard {
	return &Graveyard{
		retiredPages: make([]batch, 0, 64),
	}
}

// Add schedules pages for retirement at the given sequence.
func (g *Graveyard) Add(seq uint64, pages []uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(pages) == 0 {
		return
	}
	// Optimization: merge with last if same seq
	if len(g.retiredPages) > 0 {
		last := &g.retiredPages[len(g.retiredPages)-1]
		if last.seq == seq {
			last.ids = append(last.ids, pages...)
			return
		}
	}
	g.retiredPages = append(g.retiredPages, batch{seq: seq, ids: pages})
}

// Extract returns pages that are safe to free.
// Condition: seq < minPinnedSeq AND seq < (currentSeq - keepRecent).
func (g *Graveyard) Extract(minPinnedSeq, currentSeq, keepRecent uint64) []uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()

	var freed []uint64

	safeHistory := currentSeq - keepRecent
	if currentSeq < keepRecent {
		safeHistory = 0
	}

	limit := minPinnedSeq
	if safeHistory < limit {
		limit = safeHistory
	}

	cutIdx := 0
	for i := range g.retiredPages {
		b := &g.retiredPages[i]
		if b.seq < limit {
			freed = append(freed, b.ids...)
			cutIdx = i + 1
		} else {
			break
		}
	}

	if cutIdx > 0 {
		g.retiredPages = g.retiredPages[cutIdx:]
	}

	return freed
}

// ExtractBatchesUpTo returns up to maxIDs pages that are safe to free, grouped
// by retirement sequence so callers can reinsert on error.
//
// Safe-to-free condition is the same as Extract:
//   - retiredAtSeq < minPinnedSeq
//   - retiredAtSeq < (currentSeq - keepRecent)
//
// If maxIDs <= 0, all safe pages are returned.
func (g *Graveyard) ExtractBatchesUpTo(minPinnedSeq, currentSeq, keepRecent uint64, maxIDs int) []RetiredBatch {
	g.mu.Lock()
	defer g.mu.Unlock()

	if maxIDs <= 0 {
		maxIDs = int(^uint(0) >> 1)
	}

	safeHistory := currentSeq - keepRecent
	if currentSeq < keepRecent {
		safeHistory = 0
	}

	limit := minPinnedSeq
	if safeHistory < limit {
		limit = safeHistory
	}

	var out []RetiredBatch
	remaining := maxIDs
	cutIdx := 0

	for i := range g.retiredPages {
		if remaining <= 0 {
			break
		}
		b := &g.retiredPages[i]
		if b.seq >= limit {
			break
		}

		if len(b.ids) <= remaining {
			out = append(out, RetiredBatch{Seq: b.seq, IDs: b.ids})
			remaining -= len(b.ids)
			cutIdx = i + 1
			continue
		}

		ids := make([]uint64, remaining)
		copy(ids, b.ids[:remaining])
		out = append(out, RetiredBatch{Seq: b.seq, IDs: ids})
		b.ids = b.ids[remaining:]
		cutIdx = i
		remaining = 0
		break
	}

	if cutIdx > 0 {
		g.retiredPages = g.retiredPages[cutIdx:]
	}

	return out
}

// Reinsert adds pages back to the graveyard in order.
// Useful if freeing fails.
func (g *Graveyard) Reinsert(seq uint64, pages []uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(pages) == 0 {
		return
	}

	insertIdx := len(g.retiredPages)
	for i, b := range g.retiredPages {
		if b.seq > seq {
			insertIdx = i
			break
		}
	}

	val := batch{seq: seq, ids: pages}
	if insertIdx == len(g.retiredPages) {
		g.retiredPages = append(g.retiredPages, val)
	} else {
		g.retiredPages = append(g.retiredPages[:insertIdx+1], g.retiredPages[insertIdx:]...)
		g.retiredPages[insertIdx] = val
	}
}

func (g *Graveyard) Stats() GraveyardStats {
	g.mu.Lock()
	defer g.mu.Unlock()

	out := GraveyardStats{Batches: uint64(len(g.retiredPages))}
	for i, b := range g.retiredPages {
		if i == 0 {
			out.MinSeq = b.seq
		}
		out.MaxSeq = b.seq
		out.Pages += uint64(len(b.ids))
	}
	return out
}
