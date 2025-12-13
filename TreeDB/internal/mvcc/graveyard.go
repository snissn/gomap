package mvcc

import (
	"sync"

	"github.com/snissn/gomap/TreeDB/internal/page"
)

type batch struct {
	seq uint64
	ids []page.PageID
}

// Graveyard holds retired pages keyed by the commit sequence that retired them.
type Graveyard struct {
	mu      sync.Mutex
	retired []batch
}

func NewGraveyard() *Graveyard {
	return &Graveyard{
		retired: make([]batch, 0, 64),
	}
}

// Record appends retired page IDs for a commit sequence.
func (g *Graveyard) Record(seq uint64, ids []page.PageID) {
	if g == nil || len(ids) == 0 {
		return
	}
	cp := append([]page.PageID(nil), ids...)
	g.mu.Lock()
	defer g.mu.Unlock()

	// Optimization: if the last batch has the same sequence, merge it.
	// This happens if multiple Record calls occur for the same commit (e.g. partial updates).
	if len(g.retired) > 0 {
		last := &g.retired[len(g.retired)-1]
		if last.seq == seq {
			last.ids = append(last.ids, cp...)
			return
		}
	}
	g.retired = append(g.retired, batch{seq: seq, ids: cp})
}

func (g *Graveyard) takeEligible(minPinned, historyCutoff uint64) map[uint64][]page.PageID {
	out := make(map[uint64][]page.PageID)
	g.mu.Lock()
	defer g.mu.Unlock()

	cutIdx := 0
	for i := range g.retired {
		b := &g.retired[i]
		if b.seq < minPinned && b.seq < historyCutoff {
			out[b.seq] = b.ids
			cutIdx = i + 1
		} else {
			// Since retired is ordered by seq, once we find an ineligible batch,
			// all subsequent batches are also ineligible.
			break
		}
	}

	if cutIdx > 0 {
		// Remove eligible batches from the front.
		// To avoid memory leaks, we could nil out pointers if batch contained pointers,
		// but it contains []page.PageID (uint64), so it's fine.
		// However, the underlying array of the slice might grow large.
		// Ideally we slide or reallocate if waste is high.
		// For simplicity and performance in typical case (append/consume queue),
		// simple reslicing is standard, assuming capacity reuse or eventual GC copy.
		// If capacity grows too large relative to length, we compact.
		remaining := g.retired[cutIdx:]
		if len(remaining) == 0 {
			g.retired = g.retired[:0]
		} else {
			// If we removed a lot, maybe compact the backing array?
			// For now, standard slice mechanics:
			g.retired = remaining
		}
	}

	return out
}

// Reinsert puts pages back into the graveyard, maintaining seq order.
// Used when Prune fails to free pages.
func (g *Graveyard) Reinsert(seq uint64, ids []page.PageID) {
	if g == nil || len(ids) == 0 {
		return
	}
	cp := append([]page.PageID(nil), ids...)
	g.mu.Lock()
	defer g.mu.Unlock()

	// Find insertion point (linear scan is fine for error recovery).
	insertIdx := len(g.retired)
	for i, b := range g.retired {
		if b.seq > seq {
			insertIdx = i
			break
		}
	}

	if insertIdx == len(g.retired) {
		g.retired = append(g.retired, batch{seq: seq, ids: cp})
	} else {
		g.retired = append(g.retired[:insertIdx+1], g.retired[insertIdx:]...)
		g.retired[insertIdx] = batch{seq: seq, ids: cp}
	}
}
