package mvcc

import (
	"treedb/internal/page"
	"treedb/internal/pager"
)

// Pruner reclaims retired pages once safe.
type Pruner struct {
	pager      *pager.Pager
	graveyard  *Graveyard
	registry   *Registry
	keepRecent uint64
}

func NewPruner(p *pager.Pager, g *Graveyard, r *Registry, keepRecent uint64) *Pruner {
	return &Pruner{
		pager:      p,
		graveyard:  g,
		registry:   r,
		keepRecent: keepRecent,
	}
}

// Prune moves eligible retired pages to the on-disk freelist.
func (p *Pruner) Prune(currentSeq uint64) error {
	if p == nil || p.pager == nil || p.graveyard == nil || p.registry == nil {
		return nil
	}

	minPinned := p.registry.MinPinnedSeq()
	historyCutoff := uint64(0)
	if currentSeq > p.keepRecent {
		historyCutoff = currentSeq - p.keepRecent
	}

	eligible := p.graveyard.takeEligible(minPinned, historyCutoff)
	var pages []page.PageID
	for _, ids := range eligible {
		pages = append(pages, ids...)
	}
	if len(pages) == 0 {
		return nil
	}

	if err := p.pager.FreePages(pages); err != nil {
		// Reinsert on failure.
		p.graveyard.mu.Lock()
		for seq, ids := range eligible {
			p.graveyard.retired[seq] = append(p.graveyard.retired[seq], ids...)
		}
		p.graveyard.mu.Unlock()
		return err
	}
	return nil
}

