package db

import (
	"context"
	"errors"

	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// The first exact raw census covers only the ordinary DATA user-tree role.
// An empty pager-backed system leaf proves there are no descriptor attachments
// or system/user aliases. All richer root sets retain the full closure scanner.
func ordinaryRawRootEligible(idx *indexGen, user, system uint64) bool {
	if idx == nil || idx.pager == nil || idx.primary != nil || user == 0 || system == 0 || user == system {
		return false
	}
	image, err := idx.pager.Get(system)
	if err != nil {
		return false
	}
	n := node.NewNodeView(image)
	if n.Type() != page.PageTypeLeaf || n.Count() != 0 {
		return false
	}
	image, err = idx.pager.Get(user)
	if err != nil {
		return false
	}
	rootNode := node.NewNodeView(image)
	kind := rootNode.Type()
	return kind == page.PageTypeLeaf || kind == page.PageTypeInternal
}

func (db *DB) captureOrdinaryRawCensus(snapshot *Snapshot, candidate *candidateValueLogRefCountsV1) error {
	if !db.indexOuterLeavesInValueLog || !ordinaryRawRootEligible(snapshot.idx, snapshot.state.RootPageID, snapshot.state.SystemRootPageID) {
		return nil
	}
	counts := valueLogRefCounts{metadata: db.valueLogIdentityPins.MetadataOwner()}
	err := leafrefscan.Walk(context.Background(), snapshot.state.RootPageID, snapshot.idx.pager.Get, nil, func(ptr page.LeafLogPtr) error {
		id := ptr.ValueLogFileID()
		n := counts.get(id)
		if n == ^uint64(0) {
			return errors.New("raw leaf census overflow")
		}
		return counts.set(id, n+1)
	})
	if err != nil {
		counts.close()
		return err
	}
	candidate.rawCounts = counts.take()
	candidate.rawEligible = true
	return nil
}

func (db *DB) projectOrdinaryRawReferences(idx *indexGen, next page.MetaPageBody, delta *valueLogRefDelta, candidate *candidateValueLogRefCountsV1) (map[uint32]struct{}, bool, error) {
	if candidate == nil || delta == nil || !delta.rawExact || delta.rawIndexID != idx.id || delta.rawOutput != next.UserRootPageID || !ordinaryRawRootEligible(idx, next.UserRootPageID, next.SystemRootPageID) {
		return nil, false, nil
	}
	tracker := db.valueLogRefTracker
	if tracker == nil {
		return nil, false, nil
	}
	tracker.mu.RLock()
	incarnation := tracker.incarnation
	valid := tracker.valid && tracker.rawValid && tracker.commitSeq == next.CommitSeq-1 && tracker.rawCommitSeq == next.CommitSeq-1 && tracker.rawUserRoot == delta.rawInput && tracker.rawSystemRoot == next.SystemRootPageID && tracker.rawIncarnation == incarnation && incarnation.indexID == idx.id && incarnation.valid()
	if !valid {
		tracker.mu.RUnlock()
		return nil, false, nil
	}
	raw, err := tracker.rawCounts.clone()
	if err != nil {
		tracker.mu.RUnlock()
		return nil, false, err
	}
	counts, err := tracker.counts.clone()
	tracker.mu.RUnlock()
	defer raw.close()
	if err != nil {
		return nil, false, err
	}
	defer counts.close()
	if delta.rawChanges != nil {
		err = delta.rawChanges.forEachChange(func(id uint32, change int64) error {
			old := raw.get(id)
			if change > 0 {
				if uint64(change) > ^uint64(0)-old {
					return errors.New("raw leaf census overflow")
				}
				return raw.set(id, old+uint64(change))
			}
			if change < 0 {
				n := uint64(-change)
				if n > old {
					return errors.New("raw leaf census underflow")
				}
				return raw.set(id, old-n)
			}
			return nil
		})
		// Incomplete/invalid arithmetic evidence never authorizes resource omission.
		if err != nil {
			return nil, false, nil
		}
	}
	if err = applyProjectedReferenceChanges(&counts, delta); err != nil {
		return nil, false, nil
	}
	references := raw.referencedSet()
	for _, slot := range counts.slots {
		if slot.fileID != 0 {
			references[slot.fileID] = struct{}{}
		}
	}
	if err = db.requireDurableValueLogReferencesRegisteredV1(references); err != nil {
		return nil, false, err
	}
	*candidate = candidateValueLogRefCountsV1{idx: idx, incarnation: incarnation, commitSeq: next.CommitSeq, userRootID: next.UserRootPageID, systemRootID: next.SystemRootPageID, counts: counts.take(), rawCounts: raw.take(), rawEligible: true}
	return references, true, nil
}

func applyProjectedReferenceChanges(counts *valueLogRefCounts, delta *valueLogRefDelta) error {
	return delta.forEachChange(func(id uint32, change int64) error {
		old := counts.get(id)
		if change > 0 {
			if uint64(change) > ^uint64(0)-old {
				return errors.New("logical reference overflow")
			}
			return counts.set(id, old+uint64(change))
		}
		if change < 0 {
			n := uint64(-change)
			if n > old {
				return errors.New("logical reference underflow")
			}
			return counts.set(id, old-n)
		}
		return nil
	})
}
