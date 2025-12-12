package mvcc

import (
	"sync"

	"treedb/internal/page"
)

// Graveyard holds retired pages keyed by the commit sequence that retired them.
type Graveyard struct {
	mu      sync.Mutex
	retired map[uint64][]page.PageID
}

func NewGraveyard() *Graveyard {
	return &Graveyard{
		retired: make(map[uint64][]page.PageID),
	}
}

// Record appends retired page IDs for a commit sequence.
func (g *Graveyard) Record(seq uint64, ids []page.PageID) {
	if g == nil || len(ids) == 0 {
		return
	}
	cp := append([]page.PageID(nil), ids...)
	g.mu.Lock()
	g.retired[seq] = append(g.retired[seq], cp...)
	g.mu.Unlock()
}

func (g *Graveyard) takeEligible(minPinned, historyCutoff uint64) map[uint64][]page.PageID {
	out := make(map[uint64][]page.PageID)
	g.mu.Lock()
	for seq, ids := range g.retired {
		if seq < minPinned && seq < historyCutoff {
			out[seq] = ids
			delete(g.retired, seq)
		}
	}
	g.mu.Unlock()
	return out
}

