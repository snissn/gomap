package db

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/page"
)

type candidateTrackerSnapshot struct {
	valid    bool
	sequence uint64
	revision uint64
	counts   map[uint32]uint64
}

func snapshotCandidateTracker(db *DB) candidateTrackerSnapshot {
	tracker := db.valueLogRefTracker
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	return candidateTrackerSnapshot{tracker.valid, tracker.commitSeq, tracker.revision, maps.Clone(tracker.counts)}
}

func assertCandidateTrackerMatchesFullScan(t *testing.T, db *DB) {
	t.Helper()
	counts, sequence, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := snapshotCandidateTracker(db)
	if !got.valid || got.sequence != sequence || !maps.Equal(got.counts, counts) {
		t.Fatalf("tracker=%+v, full scan sequence=%d counts=%v", got, sequence, counts)
	}
}

func TestDurableRootCandidateScanRepairsReferenceTracker(t *testing.T) {
	for _, pointerBacked := range []bool{false, true} {
		name := "exact-empty"
		if pointerBacked {
			name = "pointer-counts"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var pointers []page.ValuePtr
			put := func(key string, index int) {
				t.Helper()
				if !pointerBacked {
					if err := db.SetSync([]byte(key), []byte("inline")); err != nil {
						t.Fatal(err)
					}
					return
				}
				batch := db.NewBatch().(*Batch)
				defer batch.Close()
				if err := batch.SetPointer([]byte(key), pointers[index]); err != nil {
					t.Fatal(err)
				}
				if err := batch.WriteSync(); err != nil {
					t.Fatal(err)
				}
			}
			if pointerBacked {
				pointers = appendPointersInNewSegment(t, dir, 0, 1, 10000, 2, func(int) []byte { return []byte("external value") })
				if err := db.RefreshValueLogSet(); err != nil {
					t.Fatal(err)
				}
			}
			put("seed", 0)
			db.valueLogRefTracker.invalidate()
			before := snapshotCandidateTracker(db)
			preparedUnchanged := false
			db.testDurableRootCandidatePreparedHook = func() {
				preparedUnchanged = reflect.DeepEqual(snapshotCandidateTracker(db), before)
			}
			scans := 0
			db.testScanCandidateExternalReferencesHook = func() { scans++ }
			put("next", 1)
			db.testDurableRootCandidatePreparedHook = nil
			if !preparedUnchanged || scans != 1 {
				t.Fatalf("before activation unchanged=%v scans=%d, want true/1", preparedUnchanged, scans)
			}
			assertCandidateTrackerMatchesFullScan(t, db)
			if got := snapshotCandidateTracker(db); got.counts == nil || (!pointerBacked && len(got.counts) != 0) {
				t.Fatalf("exact scan lost empty-map presence: %+v", got)
			}
			// Subsequent exact changes must apply once to the repaired counts.
			// In the pointer case these remove one reference at a time.
			if err := db.SetSync([]byte("seed"), []byte("replacement")); err != nil {
				t.Fatal(err)
			}
			assertCandidateTrackerMatchesFullScan(t, db)
			if err := db.DeleteSync([]byte("next")); err != nil {
				t.Fatal(err)
			}
			assertCandidateTrackerMatchesFullScan(t, db)
			db.testScanCandidateExternalReferencesHook = nil
			if scans != 1 {
				t.Fatalf("ordinary writes rescanned after repair: scans=%d", scans)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			assertCandidateTrackerMatchesFullScan(t, reopened)
			if got, err := reopened.Get([]byte("seed")); err != nil || string(got) != "replacement" {
				t.Fatalf("reopened seed=%q err=%v", got, err)
			}
			if got, err := reopened.Get([]byte("next")); err != nil || got != nil {
				t.Fatalf("reopened deleted next=%q err=%v", got, err)
			}
		})
	}
}

func TestDurableRootCandidateScanColdCollectionAttachmentRepairsTracker(t *testing.T) {
	dir := t.TempDir()
	enableCommandWALFormat(t, dir)
	db := openCommandWALDB(t, dir)
	defer db.Close()
	first := appendPointersInNewSegment(t, dir, 0, 1, 10000, 1, func(int) []byte { return []byte("first") })[0]
	second := appendPointersInNewSegment(t, dir, 0, 2, 20000, 1, func(int) []byte { return []byte("second") })[0]
	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatal(err)
	}
	publish := func(baseRoot uint64, ptr page.ValuePtr, commandKey string) uint64 {
		t.Helper()
		delta := mustFrozenSystemPointerMemtable(t, "document", ptr)
		_, roots, err := db.PublishOrderedRootDeltaGroupWithCommandWALContextAndSystemDeltaBuilder(
			[]OrderedRootDeltaPublishInput{{BaseRoot: baseRoot, Iter: delta.NewIterator(nil, nil)}},
			mustRawKVCommandWALIntent(t, db, commandKey, "1"),
			func(_ CommandWALPublishContext, roots []uint64) (iterator.UnsafeIterator, error) {
				return mustFrozenRawMemtable(t, maintenanceTestCollectionRootKey, encodeMaintenanceRootID(roots[0])).NewIterator(nil, nil), nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		return roots[0]
	}
	scans := 0
	db.testScanCandidateExternalReferencesHook = func() { scans++ }
	root := publish(0, first, "cmd/attach")
	if scans != 1 {
		t.Fatalf("cold descriptor attachment scans=%d, want 1", scans)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	_ = publish(root, second, "cmd/retarget")
	assertCandidateTrackerMatchesFullScan(t, db)
	db.testScanCandidateExternalReferencesHook = nil
	if scans != 1 {
		t.Fatalf("warm descriptor retarget rescanned after cold repair: scans=%d", scans)
	}
	got := snapshotCandidateTracker(db)
	if got.counts[first.FileID] != 0 || got.counts[second.FileID] != 1 {
		t.Fatalf("retarget counts=%v, old/new segment=%d/%d", got.counts, first.FileID, second.FileID)
	}
}

func TestDurableRootCandidateScanAbortPreservesReferenceTracker(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.valueLogRefTracker.invalidate()
	before := snapshotCandidateTracker(db)
	scans := 0
	db.testScanCandidateExternalReferencesHook = func() { scans++ }
	db.testFailDurableRootVisibleInstall.Store(true)
	err = db.SetSync([]byte("aborted"), []byte("unpublished"))
	db.testFailDurableRootVisibleInstall.Store(false)
	if !errors.Is(err, errTestDurableRootVisibleInstallFailpoint) {
		t.Fatalf("write error=%v, want visible-install failpoint", err)
	}
	if got := snapshotCandidateTracker(db); !reflect.DeepEqual(got, before) {
		t.Fatalf("aborted candidate changed tracker: before=%+v after=%+v", before, got)
	}
	if got := db.currentCommitSeq(); got != before.sequence {
		t.Fatalf("aborted candidate advanced visible sequence to %d", got)
	}
	if scans != 1 {
		t.Fatalf("aborted candidate scans=%d, want 1", scans)
	}
	if err := db.SetSync([]byte("accepted"), []byte("published")); err != nil {
		t.Fatal(err)
	}
	db.testScanCandidateExternalReferencesHook = nil
	if scans != 2 {
		t.Fatalf("successful retry scans=%d, want 2 including aborted candidate", scans)
	}
	assertCandidateTrackerMatchesFullScan(t, db)
	if got, err := db.Get([]byte("aborted")); err != nil || got != nil {
		t.Fatalf("aborted value=%q err=%v", got, err)
	}
}

func TestDurableRootCandidateScanRejectsMismatchedEvidence(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := snapshotCandidateTracker(db)
	next := db.meta
	next.CommitSeq++
	for _, name := range []string{"index", "sequence", "user-root", "system-root", "missing-counts", "stale-candidate", "allocator-abort"} {
		t.Run(name, func(t *testing.T) {
			evidence := &candidateValueLogRefCountsV1{idx: db.idx.Load(), commitSeq: next.CommitSeq,
				userRootID: next.UserRootPageID, systemRootID: next.SystemRootPageID, counts: map[uint32]uint64{}}
			install := &rootPublicationVisibleInstallV1{db: db, idx: db.idx.Load(), next: next, vlogRefCounts: evidence}
			switch name {
			case "index":
				evidence.idx = &indexGen{}
			case "sequence":
				evidence.commitSeq++
			case "user-root":
				evidence.userRootID++
			case "system-root":
				evidence.systemRootID++
			case "missing-counts":
				evidence.counts = nil
			case "stale-candidate":
				install.next.CommitSeq--
			}
			allocationCalled := false
			allocationAbort := errors.New("abort allocator activation")
			err := install.activate(func() error { allocationCalled = true; return allocationAbort })
			if err == nil || allocationCalled != (name == "allocator-abort") {
				t.Fatalf("activate error=%v allocator_called=%v", err, allocationCalled)
			}
			if name == "allocator-abort" && !errors.Is(err, allocationAbort) {
				t.Fatalf("allocator failure changed: %v", err)
			}
			if err := install.completeOrderedPostActivation(); err == nil {
				t.Fatal("completed tracker installation for unactivated candidate")
			}
			install.abort()
			if install.vlogRefCounts != nil {
				t.Fatal("aborted install retained candidate count evidence")
			}
			if got := snapshotCandidateTracker(db); !reflect.DeepEqual(got, before) {
				t.Fatalf("rejected evidence changed tracker: before=%+v after=%+v", before, got)
			}
		})
	}
}

func TestDurableRootCandidateScanDoesNotApplyDeltaTwice(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Unit-test the ordered post-activation precedence: a full scan describes
	// the final candidate, even if its builder also supplied an incremental delta.
	delta := newValueLogRefDelta()
	defer releaseValueLogRefDelta(delta)
	delta.add(7, 1)
	install := &rootPublicationVisibleInstallV1{db: db, activated: true,
		next: db.meta, post: finalizeCommitPost{commitSeq: db.meta.CommitSeq, vlogRefDelta: delta},
		vlogRefCounts: &candidateValueLogRefCountsV1{counts: map[uint32]uint64{7: 2}}}
	if err := install.completeOrderedPostActivation(); err != nil {
		t.Fatal(err)
	}
	if got := snapshotCandidateTracker(db); !got.valid || got.sequence != db.currentCommitSeq() || !maps.Equal(got.counts, map[uint32]uint64{7: 2}) {
		t.Fatalf("candidate counts were not installed exactly once: %+v", got)
	}
	if !install.post.vlogRefTrackerAdvanced || install.vlogRefCounts != nil {
		t.Fatal("post-work could reapply delta or retained scan evidence")
	}
	// Restore exact real counts before Close persists the optional tracker.
	counts, sequence, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	db.valueLogRefTracker.replace(counts, sequence, true)
}
