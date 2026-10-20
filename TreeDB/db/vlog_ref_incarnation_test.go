package db

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/crc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"maps"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// A same-sequence cache from a different real physical index pair must miss.
// Both counts were persisted by ordinary production publication, not fabricated.
func TestValueLogRefCacheRejectsOtherPhysicalIncarnation(t *testing.T) {
	t.Run("DATA", func(t *testing.T) {
		dirs := []string{t.TempDir(), t.TempDir()}
		dbs := make([]*DB, 2)
		for i, dir := range dirs {
			d, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			dbs[i] = d
			ptrs := appendPointersInNewSegment(t, dir, 0, uint32(i+1), uint64(1000+i*1000), 1, func(int) []byte { return []byte("persistent-pointer") })
			batch := d.NewBatch().(*Batch)
			if err = batch.SetPointer([]byte("key"), ptrs[0]); err != nil {
				t.Fatal(err)
			}
			if err = batch.Write(); err != nil {
				t.Fatal(err)
			}
			if err = batch.Close(); err != nil {
				t.Fatal(err)
			}
			if err = d.persistValueLogRefTracker(); err != nil {
				t.Fatal(err)
			}
		}
		if dbs[0].currentCommitSeq() != dbs[1].currentCommitSeq() {
			t.Fatal("fixture sequences differ")
		}
		var ids [2]rootpublication.StableIdentity
		for i, d := range dbs {
			if err := d.idx.Load().pager.WithStableResourceFile(func(f *os.File) error {
				var err error
				ids[i], err = rootpublication.StableIdentityFromFile(f)
				return err
			}); err != nil {
				t.Fatal(err)
			}
		}
		if rootpublication.SamePhysicalIdentity(ids[0], ids[1]) {
			t.Fatal("fixture physical DATA identities match")
		}

		oldCache, err := os.ReadFile(dbs[0].valueLogRefCountsPath())
		if err != nil {
			t.Fatal(err)
		}
		seq := dbs[1].currentCommitSeq()
		dbs[1].valueLogRefTracker.mu.RLock()
		want := dbs[1].valueLogRefTracker.counts.toMap()
		dbs[1].valueLogRefTracker.mu.RUnlock()
		for _, d := range dbs {
			if err = d.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err = os.WriteFile(dirs[1]+"/"+valueLogRefCountsFileName, oldCache, 0644); err != nil {
			t.Fatal(err)
		}
		var scans int
		restore := registerScanValueLogRefCountsHook(func() { scans++ })
		defer restore()
		reopened, err := Open(Options{Dir: dirs[1], DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := reopened.Close(); err != nil {
				t.Error(err)
			}
		}()
		if scans == 0 {
			t.Fatalf("same-sequence foreign cache accepted without original scan: seq=%d DATA=%+v versus %+v", seq, ids[0], ids[1])
		}
		reopened.valueLogRefTracker.mu.RLock()
		defer reopened.valueLogRefTracker.mu.RUnlock()
		got := reopened.valueLogRefTracker.counts.toMap()
		if len(got) != len(want) {
			t.Fatalf("rebuilt counts=%v want %v", got, want)
		}
		for id, n := range want {
			if got[id] != n {
				t.Fatalf("rebuilt counts=%v want %v", got, want)
			}
		}
	})
}

func TestValueLogRefTrackerSameSequencePhysicalReplacement(t *testing.T) {
	var databases [2]*DB
	var incarnations [2]valueLogRefIncarnation
	for n := range databases {
		d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
		if err != nil {
			t.Fatal(err)
		}
		databases[n] = d
		defer d.Close()
		incarnations[n], err = captureValueLogRefIncarnation(d.idx.Load())
		if err != nil {
			t.Fatal(err)
		}
	}
	if incarnations[0].samePhysical(incarnations[1]) {
		t.Fatal("fixture pair did not change")
	}
	tracker := newValueLogRefTracker()
	tracker.replace(map[uint32]uint64{11: 1}, 7, incarnations[0], true)
	seq, oldPair, observed, oldCounts, ok := tracker.dirtySnapshot()
	if !ok {
		t.Fatal("missing original persistence receipt")
	}
	tracker.replace(map[uint32]uint64{12: 2}, 7, incarnations[1], true)
	if tracker.replaceUnlessAdvanced(oldCounts, seq, oldPair, true, observed) {
		t.Fatal("old scan replaced same-sequence physical successor")
	}
	tracker.markClean(seq, oldPair, observed)
	_, nextPair, _, counts, dirty := tracker.dirtySnapshot()
	if !dirty || nextPair != incarnations[1] || !maps.Equal(counts, map[uint32]uint64{12: 2}) {
		t.Fatalf("old persistence completion consumed successor: dirty=%t counts=%v", dirty, counts)
	}
	if tracker.canTrackGeneration(7, incarnations[1].indexID+1) {
		t.Fatal("foreign original generation became eligible")
	}
	if err := tracker.applyDelta(8, incarnations[1].indexID+1, &valueLogRefDelta{}); err == nil {
		t.Fatal("delta crossed a physical generation without captured transition")
	}
	// A physically different audit pair must remain different even if all logical
	// fields and the Set identity in its key were copied from the current DB.
	in, err := databases[1].acquireCompactStorageAuditInputAttempt(CompactStorageOptions{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer in.close()
	key := in.key
	key.RefIncarnation = incarnations[0]
	if databases[1].compactStorageAuditCurrentMatches(in.snap.state, key) {
		t.Fatal("same-sequence audit accepted another physical pair")
	}
}

func TestValueLogRefCacheFormatBindsPhysicalPair(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	counts, seq, incarnation, err := d.scanValueLogRefCountsBound(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	blob, err := encodeValueLogRefCounts(seq, incarnation, counts)
	if err != nil {
		t.Fatal(err)
	}
	disk, err := decodeValueLogRefCounts(blob)
	if err != nil || disk.commitSeq != seq || !disk.incarnation.samePhysical(incarnation) || !maps.Equal(disk.counts, counts) {
		t.Fatalf("round trip: %+v %v", disk, err)
	}
	for _, offset := range []int{20, 24, 48, 72, 96, 120} {
		changed := append([]byte(nil), blob...)
		changed[offset] ^= 1
		if _, err := decodeValueLogRefCounts(changed); err == nil {
			t.Fatalf("physical field outside CRC at %d", offset)
		}
	}
	stale := append([]byte(nil), blob...)
	binary.LittleEndian.PutUint32(stale[8:12], 5)
	binary.LittleEndian.PutUint32(stale[len(stale)-4:], crc.Checksum(stale[:len(stale)-4]))
	if _, err := decodeValueLogRefCounts(stale); err == nil {
		t.Fatal("v5 lacks physical evidence but decoded")
	}
	invalid := append([]byte(nil), blob...)
	binary.LittleEndian.PutUint32(invalid[20:24], 2)
	binary.LittleEndian.PutUint32(invalid[len(invalid)-4:], crc.Checksum(invalid[:len(invalid)-4]))
	if _, err := decodeValueLogRefCounts(invalid); err == nil {
		t.Fatal("unknown physical form accepted")
	}
}

// DB-lifetime derived counts must survive the physical incarnation that
// originally populated them. Growth and a genuine closure scan use the same
// registry owner after the original DATA generation has actually closed.
func TestValueLogRefCountOwnerSurvivesDATAVacuum(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	original := d.idx.Load()
	if err = d.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d.idx.Load() == original {
		t.Fatal("physical pair did not cut over")
	}
	// Normal retirement enters the original ghost queue, whose production
	// scavenger deliberately waits five seconds. Do not replace that policy
	// with a direct Arena.Close or a scalar owner-state inference.
	deadline := time.Now().Add(15 * time.Second)
	for {
		original.wrapperMu.Lock()
		completed, physicalCompleted := original.closeCompleted, original.physicalCompleted
		var closeErr error
		if completed {
			closeErr = original.closeErr
		}
		original.wrapperMu.Unlock()
		if completed {
			if !physicalCompleted || closeErr != nil {
				t.Fatalf("genuine old DATA physical close failed: physicalCompleted=%t err=%v", physicalCompleted, closeErr)
			}
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("genuine old DATA physical close not reached: refs=%d minPinned=%d", original.refs.Load(), original.registry.MinPinnedSeq())
		}
		time.Sleep(20 * time.Millisecond)
	}
	tracker := d.valueLogRefTracker
	tracker.mu.Lock()
	records := tracker.counts.take()
	records.close()
	tracker.counts.metadata = tracker.metadata
	var growErr error
	for id := uint32(1); id <= 33; id++ {
		if growErr = tracker.counts.set(page.ValueLogFileID(id), uint64(id)); growErr != nil {
			break
		}
	}
	owner := tracker.metadata
	tracker.counts.close()
	tracker.valid, tracker.dirty = false, false
	tracker.mu.Unlock()
	if growErr != nil {
		t.Fatalf("actual post-cutover count growth: %v", growErr)
	}
	if owner != d.valueLogIdentityPins.MetadataOwner() {
		t.Fatal("derived storage retained retired arena owner")
	}
	snapshot := d.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("missing actual successor snapshot")
	}
	result, scanErr := d.maintenanceReachabilityScan(context.Background(), snapshot, maintenanceReachabilityScanOptions{
		Collectors: maintenanceReachabilityValueLogRefCounts, CountMetadata: owner,
	})
	result.countRecords.close()
	closeErr := snapshot.Close()
	if scanErr != nil || closeErr != nil {
		t.Fatal(scanErr, closeErr)
	}
}

// The real ghost queue removes discovery edges before physical disposal.
// A failed last Close must keep the exact generation externally rooted in the
// DB's original error receipt, alongside a later independent terminal failure.
func TestGhostGenerationActualCloseFailureRetainsOriginalDBReceipt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	d, err := Open(Options{Dir: t.TempDir(), CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	original := d.idx.Load()
	if err = d.VacuumIndexOnline(context.Background()); err != nil {
		t.Fatal(err)
	}
	successor := d.idx.Load()
	if successor == original {
		t.Fatal("no actual physical cutover")
	}
	if err = original.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for d.backgroundError() == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	first := d.backgroundError()
	if !errors.Is(first, os.ErrClosed) || !errors.Is(first, &original.closeFailure) || original.closeFailure.payload != original {
		t.Fatalf("lost genuine ghost physical refusal: %v", first)
	}
	d.idxMu.Lock()
	_, discovered := d.idxAll[original.id]
	d.idxMu.Unlock()
	d.ghostManager.mu.Lock()
	for _, ghost := range d.ghostManager.ghosts {
		if ghost.gen == original {
			discovered = true
		}
	}
	d.ghostManager.mu.Unlock()
	if discovered {
		t.Fatal("fixture did not leave both original discovery lists")
	}
	if original.metadata == nil || original.metadataCharge == 0 {
		t.Fatal("failed containing generation or physical owner was refunded")
	}
	// Concurrent repeat quiescence joins the same stopped worker; it neither
	// closes the arena nor retries physical cleanup.
	var stopped sync.WaitGroup
	for i := 0; i < 4; i++ {
		stopped.Add(1)
		go func() { defer stopped.Done(); original.pager.QuiesceBackgroundGrowth() }()
	}
	stopped.Wait()
	charged := original.metadata.Bytes()
	if err := original.close(); err != &original.closeFailure || original.metadata.Bytes() != charged {
		t.Fatal("repeated original Close replayed or refunded refusal")
	}
	if err = successor.pager.WithStableResourceFile(func(f *os.File) error { return f.Close() }); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); !errors.Is(err, &original.closeFailure) || !errors.Is(err, &successor.closeFailure) {
		t.Fatalf("DB teardown lost a sibling original failure: %v", err)
	}
	if successor.closeFailure.payload != successor || successor.metadataCharge == 0 || original.metadataCharge == 0 {
		t.Fatal("terminal sibling failed payload or charge lost")
	}
	if err = d.Close(); !errors.Is(err, &original.closeFailure) || !errors.Is(err, &successor.closeFailure) {
		t.Fatalf("repeated terminal Close lost first receipts: %v", err)
	}
}
