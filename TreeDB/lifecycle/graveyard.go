package lifecycle

import "sync"

type Graveyard struct {
	mu          sync.Mutex
	retiredPages map[uint64][]uint64 // CommitSeq -> []PageID
}

func NewGraveyard() *Graveyard {
	return &Graveyard{
		retiredPages: make(map[uint64][]uint64),
	}
}

// Add schedules pages for retirement at the given sequence.
func (g *Graveyard) Add(seq uint64, pages []uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	if len(pages) == 0 {
		return
	}
	g.retiredPages[seq] = append(g.retiredPages[seq], pages...)
}

// Extract returns pages that are safe to free.
// Condition: seq < minPinnedSeq AND seq < (currentSeq - keepRecent).
func (g *Graveyard) Extract(minPinnedSeq, currentSeq, keepRecent uint64) []uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	
	var freed []uint64
	
	// Threshold logic
	// Safe if seq < minPinnedSeq
	// AND seq < safeHistoryThreshold
	
	safeHistory := currentSeq - keepRecent
	if currentSeq < keepRecent {
		safeHistory = 0
	}
	
	limit := minPinnedSeq
	if safeHistory < limit {
		limit = safeHistory
	}
	
	for seq, pages := range g.retiredPages {
		if seq < limit {
			freed = append(freed, pages...)
			delete(g.retiredPages, seq)
		}
	}
	
	return freed
}
