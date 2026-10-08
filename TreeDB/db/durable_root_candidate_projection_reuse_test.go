package db

import (
	"context"
	"fmt"
	"maps"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/leafrefscan"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// The primary projection is needed before decoding pointer-backed descriptors.
// The resulting full closure must supply the count evidence without a third
// projection. Logical aliases count the same root once, while distinct pointer
// entries in that root retain their full multiplicity.
func TestDurableRootCandidateProjectionReusesFullClosure(t *testing.T) {
	for _, policy := range []OrderedRootStoragePolicy{OrderedRootStoragePagerLeaves, OrderedRootStorageValueLogLeaves} {
		for _, pointerDescriptor := range []bool{false, true} {
			t.Run(fmt.Sprintf("policy=%d/pointer-descriptor=%t", policy, pointerDescriptor), func(t *testing.T) {
				db, _ := openLeafGenerationGCTestDB(t)
				value := appendPointersInNewSegment(t, db.dir, 0, 1, 10000, 1, func(int) []byte { return []byte("shared value") })[0]
				if err := db.RefreshValueLogSet(); err != nil {
					t.Fatal(err)
				}
				b := db.NewBatch().(*Batch)
				if err := b.SetPointer([]byte("user"), value); err != nil {
					t.Fatal(err)
				}
				if err := b.WriteSync(); err != nil {
					t.Fatal(err)
				}
				closeNoErr(t, b)
				table := memtable.NewAppendOnlyWithEntryCapacity(2)
				table.SetEntry([]byte("doc/a"), nil, value, node.FlagPointer)
				table.SetEntry([]byte("doc/b"), nil, value, node.FlagPointer)
				table.Freeze()
				const alias = "collections/root/users/alias"
				_, roots, err := db.PublishOrderedRootGroupWithSystemBuilder([]OrderedRootPublishInput{{
					Iter: table.NewIterator(nil, nil), StoragePolicy: policy,
				}}, func(roots []uint64) (iterator.UnsafeIterator, error) {
					return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(roots[0]), alias, encodeMaintenanceRootID(roots[0])).NewIterator(nil, nil), nil
				})
				if err != nil {
					t.Fatal(err)
				}
				var descriptor page.ValuePtr
				if pointerDescriptor {
					descriptor = appendPointersInNewSegment(t, db.dir, 0, 2, 20000, 1, func(int) []byte { return encodeMaintenanceRootID(roots[0]) })[0]
					if err := db.RefreshValueLogSet(); err != nil {
						t.Fatal(err)
					}
					descriptors := memtable.NewAppendOnlyWithEntryCapacity(2)
					descriptors.SetEntry([]byte(maintenanceTestCollectionRootKey), nil, descriptor, node.FlagPointer)
					descriptors.SetEntry([]byte(alias), nil, descriptor, node.FlagPointer)
					descriptors.Freeze()
					if _, err := db.PublishSystemRootIterator(descriptors.NewIterator(nil, nil)); err != nil {
						t.Fatal(err)
					}
				}
				snap := db.AcquireSnapshot()
				if snap == nil {
					t.Fatal("missing snapshot")
				}
				defer closeNoErr(t, snap)
				ctx := context.Background()
				primary, err := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{
					Collectors:      maintenanceReachabilityValueLogRefCounts,
					ExplicitRootIDs: []uint64{snap.state.RootPageID, snap.state.SystemRootPageID},
				})
				if err != nil {
					t.Fatal(err)
				}
				full, err := db.maintenanceReachabilityScan(ctx, snap, maintenanceReachabilityScanOptions{Collectors: maintenanceReachabilityValueLogRefCounts})
				if err != nil {
					t.Fatal(err)
				}
				if full.valueLogRefCounts[value.FileID] != 3 || (pointerDescriptor && full.valueLogRefCounts[descriptor.FileID] != 2) {
					t.Fatalf("fixture lost root deduplication or pointer multiplicity: %v", full.valueLogRefCounts)
				}
				wantReferences := maps.Clone(full.valueLogReferencedSegments)
				closure, _, err := maintenanceReachabilityRoots(ctx, snap, nil, nil, false, false)
				if err != nil {
					t.Fatal(err)
				}
				if err := leafrefscan.WalkRoots(ctx, maintenanceRootIDs(closure), snap.idx.pager.Get, nil, func(ptr page.LeafLogPtr) error {
					wantReferences[ptr.ValueLogFileID()] = struct{}{}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				idx, sequence, user, system := snap.idx, snap.state.CommitSeq, snap.state.RootPageID, snap.state.SystemRootPageID
				before := db.durableRootCandidatePagesVisited.Load()
				var evidence candidateValueLogRefCountsV1
				references, err := db.scanCandidateExternalReferencesWithCountsAndLimitsV1(snap, &evidence, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !maps.Equal(evidence.counts, full.valueLogRefCounts) || !maps.Equal(references, wantReferences) {
					t.Fatalf("closure mismatch: counts=%v want=%v references=%v want=%v", evidence.counts, full.valueLogRefCounts, references, wantReferences)
				}
				if evidence.idx != idx || evidence.commitSeq != sequence || evidence.userRootID != user || evidence.systemRootID != system {
					t.Fatal("count evidence changed captured candidate coordinates")
				}
				if got, want := db.durableRootCandidatePagesVisited.Load()-before, primary.counters.PagesVisited+full.counters.PagesVisited; got != want {
					t.Fatalf("candidate projection pages=%d want primary+one full=%d (full=%d)", got, want, full.counters.PagesVisited)
				}
			})
		}
	}
}

func TestDurableRootCandidateProjectionEmptyCountsRemainPresent(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoErr(t, db)
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer closeNoErr(t, snap)
	var evidence candidateValueLogRefCountsV1
	if references, err := db.scanCandidateExternalReferencesWithCountsAndLimitsV1(snap, &evidence, nil); err != nil || len(references) != 0 {
		t.Fatalf("empty candidate references=%v error=%v", references, err)
	}
	if evidence.counts == nil || len(evidence.counts) != 0 {
		t.Fatalf("empty count evidence must retain presence: %v", evidence.counts)
	}
}

func TestDurableRootCandidateProjectionDescriptorFailureDoesNotStampCounts(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer closeNoErr(t, db)
	// Build an unattached root as ordinary data, then expose it only to this
	// private candidate as the system root. Published DB state stays unchanged.
	root, err := db.PublishOrderedRootIterator(0, mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, []byte("bad")).NewIterator(nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer closeNoErr(t, snap)
	state := *snap.state
	state.SystemRootPageID = root
	snap.state = &state
	before := snapshotCandidateTracker(db)
	var evidence candidateValueLogRefCountsV1
	if _, err := db.scanCandidateExternalReferencesWithCountsAndLimitsV1(snap, &evidence, nil); err == nil {
		t.Fatal("malformed descriptor accepted")
	}
	if evidence.counts != nil || evidence.idx != nil || evidence.commitSeq != 0 {
		t.Fatalf("failed projection stamped count evidence: %+v", evidence)
	}
	if got := snapshotCandidateTracker(db); !maps.Equal(got.counts, before.counts) || got.valid != before.valid || got.sequence != before.sequence || got.revision != before.revision {
		t.Fatal("failed projection changed the published reference tracker")
	}
}
