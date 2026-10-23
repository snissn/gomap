package caching

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWSuccessorBasisWorkspaceDoesNotWaitForSharedBlockDecoder(t *testing.T) {
	db, _, want := cowLeafCacheModeFixture(t, -1, true, "cow_ordered")
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	done := make(chan error, 1)
	// The basis is idle. Holding only the generic decoder lock must not make
	// its opportunistic borrower serialize formerly independent block reads.
	db.cow.physicalMu.Lock()
	go func() {
		key, got, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
		if err == nil && (!found || string(key) != "a000" || !bytes.Equal(got, want)) {
			err = fmt.Errorf("successor key=%q found=%t value=%q", key, found, got)
		}
		done <- err
	}()
	var result error
	timedOut := false
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		timedOut = true
	}
	db.cow.physicalMu.Unlock()
	if timedOut {
		cowWait(t, done)
		t.Fatal("successor block read waited for the generic shared decoder")
	}
	if result != nil {
		t.Fatal(result)
	}
	if s.cowReader != nil || s.cowCut.basis.reader == nil || len(s.cowCut.basis.reader.codecs) == 0 {
		t.Fatal("cold compressed successor did not use its admitted basis block decoder")
	}
}

func TestCOWSuccessorBasisWorkspaceReuseAndCutOwnership(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprintf("compressed=%t", compressed), func(t *testing.T) {
			// Disable the decoded-page cache so compressed reads exercise the
			// actual decoder, not only the shared traversal buffers.
			db, _, want := cowLeafCacheModeFixture(t, -1, compressed, "cow_ordered")
			old, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			basis := old.cowCut.basis
			if empty, err := basis.snapshot.OwnedUserRootEmpty(); err != nil || empty {
				t.Fatalf("fixture needs a nonempty retained physical root: empty=%t err=%v", empty, err)
			}
			seek := func(s *Snapshot, expected []byte) ([]byte, []byte) {
				t.Helper()
				key, got, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
				if err != nil || !found || string(key) != "a000" || !bytes.Equal(got, expected) {
					t.Fatalf("successor: key=%q value=%q found=%t err=%v", key, got, found, err)
				}
				return key, got
			}
			key, owned := seek(old, want)
			workspace := basis.reader
			if workspace == nil || workspace.point == nil || old.cowReader != nil {
				t.Fatal("uncontended inline successor did not use the original basis workspace")
			}
			point := workspace.point
			warm := db.cow.budget.Stats()
			for range 16 {
				cut, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				if cut.cowCut.basis != basis {
					t.Fatal("fixture changed basis without publication")
				}
				seek(cut, want)
				if cut.cowReader != nil {
					t.Fatal("fresh short-lived inline cut allocated traversal scratch")
				}
				if err := cut.Close(); err != nil {
					t.Fatal(err)
				}
			}
			after := db.cow.budget.Stats()
			if basis.reader != workspace || workspace.point != point || after.ExternalBytes != warm.ExternalBytes || after.ExternalLeases != warm.ExternalLeases || after.ReservedBytes != warm.ReservedBytes || after.Views != warm.Views {
				t.Fatalf("short-lived cuts did not reuse the admitted owner: before=%+v after=%+v", warm, after)
			}
			// A busy original owner must not delay a retained reader. The existing
			// all-three-reader progress test separately holds both shared locks.
			busy, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer busy.Close()
			basis.readMu.Lock()
			busyKey, busyValue, busyFound, busyErr := busy.SeekGEVersionRange([]byte("a000"), []byte("a001"))
			basis.readMu.Unlock()
			if busyErr != nil || !busyFound || string(busyKey) != "a000" || !bytes.Equal(busyValue, want) {
				t.Fatalf("busy successor: key=%q found=%t err=%v", busyKey, busyFound, busyErr)
			}
			if busy.cowReader == nil || busy.cowReader.point == nil || busy.cowReader.point == point {
				t.Fatal("busy successor did not retain its independent cut-local workspace")
			}
			if err := busy.Close(); err != nil {
				t.Fatal(err)
			}
			// Scratch may be overwritten immediately after copy-out; owned
			// results must also survive a new physical basis and old-basis close.
			key[0], owned[0] = '!', '!'
			seek(old, want)
			newValue := bytes.Repeat([]byte("new-inline"), 12)
			if err := db.Set([]byte("a000"), newValue); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			current, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer current.Close()
			if current.cowCut.basis == basis {
				t.Fatal("checkpoint did not replace the captured physical basis")
			}
			_, retained := seek(old, want)
			seek(current, newValue)
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if basis.reader != nil || workspace.point != nil || workspace.lease != nil || workspace.budget != nil || !bytes.Equal(retained, want) {
				t.Fatal("final basis close did not detach/refund scratch or changed owned output")
			}
			if _, _, _, err := old.SeekGEVersionRange(nil, nil); !errors.Is(err, backenddb.ErrClosed) {
				t.Fatalf("closed cut successor: %v", err)
			}
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if stats := db.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 || stats.Views != 0 {
				t.Fatalf("final successor owners not drained: %+v", stats)
			}
		})
	}
}

func TestCOWSuccessorBasisWorkspaceFiniteAdmission(t *testing.T) {
	db, _, want := cowLeafCacheModeFixture(t, -1, true, "cow_ordered")
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	budget := db.cow.budget
	before := budget.Stats()
	overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
	if err != nil {
		t.Fatal(err)
	}
	pressured := budget.Stats()
	_, _, found, readErr := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
	after := budget.Stats()
	pressure.Close()
	if !errors.Is(readErr, memtable.ErrCOWCapacity) || found || s.cowCut.basis.reader != nil || s.cowReader != nil {
		t.Fatalf("cold original scratch did not refuse before allocation: found=%t err=%v", found, readErr)
	}
	// The refusal neither loses a lock nor substitutes unadmitted scratch.
	if after.ExternalBytes != pressured.ExternalBytes || after.ExternalLeases != pressured.ExternalLeases || after.ReservedBytes != pressured.ReservedBytes {
		t.Fatalf("refused successor retained new admission: before=%+v after=%+v", pressured, after)
	}
	key, got, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
	if err != nil || !found || string(key) != "a000" || !bytes.Equal(got, want) {
		t.Fatalf("retry after pressure release: found=%t value=%q err=%v", found, got, err)
	}
}

func TestCOWSuccessorBasisWorkspaceKeepsAdmittedCutUnderPressure(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, shared := range []string{"cold", "workspace", "point"} {
			t.Run(fmt.Sprintf("compressed=%t/shared=%s", compressed, shared), func(t *testing.T) {
				db, _, want := cowLeafCacheModeFixture(t, -1, compressed, "cow_ordered")
				s, err := db.acquireCOWSnapshotWithError()
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				got, err := s.Get([]byte("a000"))
				if err != nil || !bytes.Equal(got, want) || s.cowReader == nil || s.cowReader.point == nil {
					t.Fatalf("physical Get did not warm cut storage: value=%q err=%v", got, err)
				}
				basis := s.cowCut.basis
				if basis.reader != nil {
					t.Fatal("point Get unexpectedly warmed shared successor storage")
				}
				if shared != "cold" {
					basis.readMu.Lock()
					w, err := basis.physicalWorkspaceLocked(db.cow.budget, db.cow.readLimits())
					if err == nil && shared == "point" {
						_, err = w.pointScratch()
					}
					basis.readMu.Unlock()
					if err != nil {
						t.Fatal(err)
					}
				}
				reader, point, sharedReader := s.cowReader, s.cowReader.point, basis.reader
				budget := db.cow.budget
				before := budget.Stats()
				overhead := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
				pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - overhead)
				if err != nil {
					t.Fatal(err)
				}
				defer pressure.Close()
				pressured := budget.Stats()
				key, value, found, readErr := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
				after := budget.Stats()
				pressure.Close()
				if readErr != nil || !found || string(key) != "a000" || !bytes.Equal(value, want) {
					t.Fatalf("admitted cut successor refused under pressure: key=%q found=%t value=%q err=%v", key, found, value, readErr)
				}
				if s.cowReader != reader || reader.point != point || basis.reader != sharedReader ||
					after.ExternalBytes != pressured.ExternalBytes || after.ExternalLeases != pressured.ExternalLeases ||
					after.ReservedBytes != pressured.ReservedBytes || after.Views != pressured.Views {
					t.Fatalf("warmed successor changed admission: before=%+v after=%+v", pressured, after)
				}
				key[0], value[0] = '!', '!'
				again, err := s.Get([]byte("a000"))
				if err != nil || !bytes.Equal(again, want) {
					t.Fatalf("successor result alias changed the retained value: value=%q err=%v", again, err)
				}
			})
		}
	}
}

// A cached pointer Get admits record input without point traversal. When a
// grouped physical leaf needs more new shared input than a local point costs,
// the existing local input can still serve SeekGE after shared admission fails.
func TestCOWSuccessorBasisCapacityFallsBackToAdmittedLocalInput(t *testing.T) {
	db, _, want := cowLeafCacheModeFixture(t, -1, false, "cow_ordered")
	// Remain in the cached root: reading this real uncompressed value pointer
	// must warm local input while never traversing the physical tree.
	warmValue := bytes.Repeat([]byte("warm-pointer-input"), 1<<10)
	if err := db.Set([]byte("zz-warm"), warmValue); err != nil {
		t.Fatal(err)
	}
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got, err := s.Get([]byte("zz-warm")); err != nil || !bytes.Equal(got, warmValue) {
		t.Fatalf("cached pointer Get: len=%d err=%v", len(got), err)
	}
	local, basis := s.cowReader, s.cowCut.basis
	if local == nil || local.point != nil || basis.reader != nil {
		t.Fatal("cached pointer Get must admit only local input, without point or basis scratch")
	}
	var keyScratch, leafScratch [page.PageSize]byte
	var ptr page.LeafLogPtr
	stop := errors.New("capture grouped leaf")
	_, err = basis.snapshot.GetEntryExactWithFixedScratch([]byte("a000"), keyScratch[:], leafScratch[:], func(p page.LeafLogPtr, _ []byte) ([]byte, error) {
		ptr = p
		return nil, stop
	})
	if !errors.Is(err, stop) || ptr.FileID == 0 {
		t.Fatalf("fixture did not traverse logged leaf: ptr=%+v err=%v", ptr, err)
	}
	f, _, err := basis.snapshot.PinnedValueLogFile(ptr.ValueLogFileID())
	if err != nil {
		t.Fatal(err)
	}
	shape, err := valuelog.InspectCOWLeafRecord(f, ptr.ValuePtr(), db.cow.readLimits())
	if err != nil {
		t.Fatal(err)
	}
	leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
	pointCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))) + leaseCharge
	inputCharge := memtable.COWAllocationCharge(cowReadCapacity(shape.PayloadBytes())) +
		memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadAllocation{}))) + leaseCharge
	if shape.Compressed || shape.RawBytes <= page.PageSize || inputCharge <= pointCharge || uint64(cap(local.input)) < shape.PayloadBytes() {
		t.Fatalf("need real grouped input larger than point: shape=%+v inputCharge=%d pointCharge=%d localInput=%d", shape, inputCharge, pointCharge, cap(local.input))
	}
	basis.readMu.Lock()
	shared, err := basis.physicalWorkspaceLocked(db.cow.budget, db.cow.readLimits())
	if err == nil {
		_, err = shared.pointScratch()
	}
	basis.readMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if cap(shared.input) != 0 {
		t.Fatal("basis input must still be cold")
	}
	budget := db.cow.budget
	before := budget.Stats()
	pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - pointCharge - leaseCharge)
	if err != nil {
		t.Fatal(err)
	}
	defer pressure.Close()
	pressured := budget.Stats()
	// Establish the actual shared leaf reader refuses with exactly this
	// remaining budget, without consuming any admission on the failed growth.
	basis.readMu.Lock()
	_, sharedErr := basis.readPhysicalLeafWithWorkspace(db.cow, shared, ptr, shared.point.leaf[:0], false, true)
	basis.readMu.Unlock()
	if !errors.Is(sharedErr, memtable.ErrCOWCapacity) || cap(shared.input) != 0 || budget.Stats() != pressured {
		t.Fatalf("shared input did not refuse cleanly: err=%v before=%+v after=%+v", sharedErr, pressured, budget.Stats())
	}
	key, value, found, err := s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
	if err != nil || !found || string(key) != "a000" || !bytes.Equal(value, want) {
		t.Fatalf("public successor did not fall back: key=%q found=%t len=%d err=%v", key, found, len(value), err)
	}
	after := budget.Stats()
	if s.cowReader != local || local.point == nil || basis.reader != shared || cap(shared.input) != 0 ||
		after.ExternalBytes != pressured.ExternalBytes+pointCharge || after.ReservedBytes != pressured.ReservedBytes+pointCharge ||
		after.ExternalLeases != pressured.ExternalLeases+1 || after.Views != pressured.Views {
		t.Fatalf("fallback must admit exactly one local point: before=%+v after=%+v", pressured, after)
	}
	key[0], value[0] = '!', '!'
	key, value, found, err = s.SeekGEVersionRange([]byte("a000"), []byte("a001"))
	if err != nil || !found || string(key) != "a000" || !bytes.Equal(value, want) || budget.Stats() != after {
		t.Fatalf("owned result/reuse: key=%q found=%t len=%d err=%v", key, found, len(value), err)
	}
	t.Logf("grouped payload=%d raw=%d localInput=%d sharedInputCharge=%d localPointCharge=%d", shape.PayloadBytes(), shape.RawBytes, cap(local.input), inputCharge, pointCharge)
}
