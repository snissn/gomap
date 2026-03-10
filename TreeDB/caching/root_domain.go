package caching

import (
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type rootDomainLookup interface {
	GetEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool)
}

// rootDomainState is the native cached state shape for one logical root-domain.
// It is intentionally small and read-centric in R1: later phases will add
// flush scheduling and grouped publication, but the authoritative state model
// starts here.
type rootDomainState struct {
	publishedRootID uint64
	published       rootDomainLookup
	mutable         memtable.Table
	immutables      []memtable.Table // oldest-to-newest
}

type rootDomainSnapshot struct {
	publishedRootID uint64
	published       rootDomainLookup
	mutable         memtable.Table
	immutables      []memtable.Table // oldest-to-newest
}

func rootDomainSnapshotFromMemtableView(view *memtableView, shardIdx int, includeMutable bool) rootDomainSnapshot {
	if view == nil {
		return rootDomainSnapshot{}
	}
	snap := rootDomainSnapshot{}
	if includeMutable && shardIdx >= 0 && shardIdx < len(view.mutables) {
		snap.mutable = view.mutables[shardIdx]
	}
	if len(view.queue) == 0 {
		return snap
	}
	snap.immutables = make([]memtable.Table, 0, len(view.queue))
	for idx, mt := range view.queue {
		if mt == nil {
			continue
		}
		if len(view.queueShardIDs) > idx && int(view.queueShardIDs[idx]) != shardIdx {
			continue
		}
		snap.immutables = append(snap.immutables, mt)
	}
	return snap
}

func (s *rootDomainState) snapshot() rootDomainSnapshot {
	if s == nil {
		return rootDomainSnapshot{}
	}
	snap := rootDomainSnapshot{
		publishedRootID: s.publishedRootID,
		published:       s.published,
		mutable:         s.mutable,
	}
	if len(s.immutables) > 0 {
		snap.immutables = append(make([]memtable.Table, 0, len(s.immutables)), s.immutables...)
	}
	return snap
}

func (s *rootDomainState) sealMutable(next memtable.Table) {
	if s == nil {
		return
	}
	if s.mutable != nil {
		s.mutable.Freeze()
		s.immutables = append(s.immutables, s.mutable)
	}
	s.mutable = next
}

func (s rootDomainSnapshot) getEntry(key []byte) (val []byte, ptr page.ValuePtr, flags byte, found bool) {
	if s.mutable != nil {
		if val, ptr, flags, found = s.mutable.GetEntry(key); found {
			return val, ptr, flags, true
		}
	}
	for idx := len(s.immutables) - 1; idx >= 0; idx-- {
		if val, ptr, flags, found = s.immutables[idx].GetEntry(key); found {
			return val, ptr, flags, true
		}
	}
	if s.published != nil {
		return s.published.GetEntry(key)
	}
	return nil, page.ValuePtr{}, 0, false
}

func (s rootDomainSnapshot) visibleValue(key []byte) ([]byte, bool) {
	val, _, flags, found := s.getEntry(key)
	if !found || flags&node.FlagTombstone != 0 {
		return nil, false
	}
	return val, true
}

func (s rootDomainSnapshot) hasVisibleKey(key []byte) bool {
	_, ok := s.visibleValue(key)
	return ok
}
