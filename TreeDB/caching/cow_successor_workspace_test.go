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
			for i := range basis.readSlots {
				basis.readSlots[i].mu.Lock()
			}
			busyKey, busyValue, busyFound, busyErr := busy.SeekGEVersionRange([]byte("a000"), []byte("a001"))
			for i := range basis.readSlots {
				basis.readSlots[i].mu.Unlock()
			}
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

func TestCOWSuccessorPhysicalPointerReusesBasisValueInput(t *testing.T) {
	db := cowPointerFixture(t)
	want := bytes.Repeat([]byte("physical-pointer"), 8)
	if err := db.Set([]byte("a"), want); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	var owned []byte
	var basis *cowBackendBasis
	for range 8 {
		s, err := db.acquireCOWSnapshotWithError()
		if err != nil {
			t.Fatal(err)
		}
		key, got, found, err := s.SeekGEVersionRange([]byte("a"), []byte("b"))
		if err != nil || !found || string(key) != "a" || !bytes.Equal(got, want) {
			_ = s.Close()
			t.Fatalf("physical pointer successor key=%q found=%t value=%q err=%v", key, found, got, err)
		}
		basis = s.cowCut.basis
		if s.cowReader != nil || basis.reader == nil || cap(basis.reader.output) < len(want) {
			_ = s.Close()
			t.Fatal("physical pointer winner allocated short-lived cut-local value input")
		}
		if owned != nil && !bytes.Equal(owned, want) {
			_ = s.Close()
			t.Fatal("basis scratch reuse changed a previous caller's output")
		}
		owned = got
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	// A newer queued pointer belongs to the complete cut, not the physical
	// basis's pinned files. It must retain its original local resolver.
	newValue := bytes.Repeat([]byte("queued-pointer"), 9)
	if err := db.Set([]byte("a"), newValue); err != nil {
		t.Fatal(err)
	}
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, got, found, err := s.SeekGEVersionRange([]byte("a"), []byte("b"))
	if err != nil || !found || !bytes.Equal(got, newValue) || s.cowReader == nil || !bytes.Equal(owned, want) {
		t.Fatalf("queued pointer lost its complete-cut resolver: found=%t err=%v", found, err)
	}
}

func cowSuccessorCompressedPointerFixture(t *testing.T) (*DB, []byte) {
	t.Helper()
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, backend, Options{
		DisableWAL: true, AllowUnsafe: true, MemtableMode: "cow_ordered", FlushThreshold: 1 << 30,
		ValueLogPointerThreshold: 1, ValueLogCompression: uint8(vlogCompressionBlock), ValueLogBlockCodec: 2,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	want := bytes.Repeat([]byte("compressed-pointer"), 1024)
	if err := db.Set([]byte("a"), want); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	return db, want
}

func TestCOWSuccessorCompressedInspectionHandoffUsesCutDecoder(t *testing.T) {
	db, want := cowSuccessorCompressedPointerFixture(t)
	s, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.cowReadMu.Lock()
	defer s.cowReadMu.Unlock()
	var inspected cowSuccessorInspection
	best, err := s.cowPhysicalSuccessorLocked([]byte("a"), []byte("b"), &inspected)
	if err != nil || !best.found || best.valueOwned || inspected.file == nil ||
		!inspected.shape.Compressed || inspected.shape.DictID != 0 || inspected.shape.Codec != valuelog.BlockCodecZSTD {
		t.Fatalf("compressed winner did not hand off its inspection: best=%+v shape=%+v err=%v", best, inspected.shape, err)
	}
	// The descriptor owns no scratch. Releasing the entire scratch owner must
	// leave its file/shape usable under the original cut's physical custody.
	basis := s.cowCut.basis
	basis.readMu.Lock()
	basis.reader.close()
	basis.reader = nil
	basis.readMu.Unlock()
	w, err := s.cowWorkspaceLocked()
	if err != nil {
		t.Fatal(err)
	}
	got, err := w.readWithInspection(inspected.file, best.ptr, inspected.shape, false, nil)
	if err != nil || !bytes.Equal(got, want) || len(w.codecs) != 0 {
		t.Fatalf("handoff lost complete-cut shared decoding: len=%d codecs=%d err=%v", len(got), len(w.codecs), err)
	}
	if shared := db.cow.physicalReader; shared == nil || len(shared.codecs) == 0 {
		t.Fatal("handoff did not preserve the original cache decoder owner")
	}
	// A reused shape must still fail closed if bound to another pointer.
	wrong := best.ptr
	wrong.Offset++
	if _, err := w.readWithInspection(inspected.file, wrong, inspected.shape, false, nil); err == nil {
		t.Fatal("inspected handoff accepted a different pointer")
	}
}

func TestCOWSuccessorFreshCompressedHandoffRefundsBeforeLocalAdmission(t *testing.T) {
	for _, busyPrimary := range []bool{false, true} {
		t.Run(fmt.Sprintf("busy-primary=%t", busyPrimary), func(t *testing.T) {
			db, want := cowSuccessorCompressedPointerFixture(t)
			warm, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			if _, got, found, err := warm.SeekGEVersionRange([]byte("a"), []byte("b")); err != nil || !found || !bytes.Equal(got, want) {
				_ = warm.Close()
				t.Fatalf("warm original shared decoder: found=%t err=%v", found, err)
			}
			if err := warm.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			basis, budget := s.cowCut.basis, db.cow.budget
			// Only the original shared codec is warm. Start either opportunistic
			// traversal owner cold so retaining it would duplicate local admission.
			basis.readMu.Lock()
			basis.reader.close()
			basis.reader = nil
			basis.readMu.Unlock()
			shared := db.cow.physicalReader
			if s.cowReader != nil || shared == nil || len(shared.codecs) == 0 || basis.reader != nil || basis.readSlots[0].reader != nil {
				t.Fatal("fixture needs a warm shared decoder and cold cut/primary/extra lane")
			}
			leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			workspaceCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge() + leaseCharge
			pointCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))) + leaseCharge
			outputCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadAllocation{}))) + leaseCharge +
				memtable.COWAllocationCharge(cowReadCapacity(uint64(len(want))))
			// The old independent cut can admit traversal plus output. Retaining a
			// fresh primary or extra lane would leave only outputCharge, stranding local admission.
			remaining := workspaceCharge + pointCharge + outputCharge
			before := budget.Stats()
			pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - remaining - leaseCharge)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			pressured := budget.Stats()
			if busyPrimary {
				basis.readMu.Lock()
				defer basis.readMu.Unlock()
			}
			key, got, found, err := s.SeekGEVersionRange([]byte("a"), []byte("b"))
			after := budget.Stats()
			if err != nil || !found || string(key) != "a" || !bytes.Equal(got, want) {
				t.Fatalf("compressed handoff stranded original local admission: found=%t err=%v", found, err)
			}
			if basis.reader != nil || basis.readSlots[0].reader != nil || s.cowReader == nil || s.cowReader.point != nil || len(s.cowReader.codecs) != 0 ||
				db.cow.physicalReader != shared || after.ExternalBytes != pressured.ExternalBytes+workspaceCharge+outputCharge ||
				after.ReservedBytes != pressured.ReservedBytes+workspaceCharge+outputCharge || after.ExternalLeases != pressured.ExternalLeases+2 {
				t.Fatalf("fresh compressed lane must refund before original cut admission: before=%+v after=%+v", pressured, after)
			}
		})
	}
}

func TestCOWSuccessorFreshValueGrowthRefusalRefundsBeforeLocalFallback(t *testing.T) {
	for _, busyPrimary := range []bool{false, true} {
		t.Run(fmt.Sprintf("busy-primary=%t", busyPrimary), func(t *testing.T) {
			db := cowPointerFixture(t)
			want := bytes.Repeat([]byte("large-physical-pointer"), 1024)
			if err := db.Set([]byte("a"), want); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			warm := bytes.Repeat([]byte("warm"), 256)
			if err := db.Set([]byte("zz-warm"), warm); err != nil {
				t.Fatal(err)
			}
			s, err := db.acquireCOWSnapshotWithError()
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if got, err := s.Get([]byte("zz-warm")); err != nil || !bytes.Equal(got, warm) {
				t.Fatalf("warm local pointer input: len=%d err=%v", len(got), err)
			}
			local, basis := s.cowReader, s.cowCut.basis
			if local == nil || local.point != nil || basis.reader != nil {
				t.Fatal("fixture must have partially warmed local storage and no traversal owner")
			}
			// Inspect the actual physical target with caller scratch, without
			// warming the local or opportunistic traversal/storage under test.
			var key, leaf [page.PageSize]byte
			entry, err := basis.snapshot.GetEntryExactWithFixedScratch([]byte("a"), key[:], leaf[:], nil)
			if err != nil {
				t.Fatal(err)
			}
			file, _, err := basis.snapshot.PinnedValueLogFile(entry.ValuePtr.FileID)
			if err != nil {
				t.Fatal(err)
			}
			shape, err := valuelog.InspectCOWRecord(file, entry.ValuePtr, db.cow.readLimits())
			if err != nil || shape.Compressed || shape.PayloadBytes() <= uint64(cap(local.input)) || shape.ValueBytes <= uint64(cap(local.output)) {
				t.Fatalf("fixture needs real larger uncompressed input and output: shape=%+v err=%v", shape, err)
			}
			budget := db.cow.budget
			leaseCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(memtable.COWExternalLease{})))
			growth := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadAllocation{}))) + leaseCharge +
				memtable.COWAllocationCharge(cowReadCapacity(shape.PayloadBytes())) + memtable.COWAllocationCharge(cowReadCapacity(shape.ValueBytes))
			pointCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowPointScratch{}))) + leaseCharge
			// The original local traversal plus its required growth fits. A new
			// workspace and point consume enough to refuse that same value grow.
			remaining := pointCharge + growth
			workspaceCharge := memtable.COWAllocationCharge(uint64(unsafe.Sizeof(cowReadWorkspace{}))) + cowReadWorkspaceInspectionCharge() + leaseCharge
			if workspaceCharge >= growth {
				t.Fatal("target growth must exceed the fresh workspace envelope")
			}
			before := budget.Stats()
			pressure, err := budget.AcquireExternal(budget.Limits().MaxInFlightBytes - before.ReservedBytes - remaining - leaseCharge)
			if err != nil {
				t.Fatal(err)
			}
			defer pressure.Close()
			pressured := budget.Stats()
			if busyPrimary {
				basis.readMu.Lock()
				defer basis.readMu.Unlock()
			}
			gotKey, got, found, err := s.SeekGEVersionRange([]byte("a"), []byte("b"))
			after := budget.Stats()
			if err != nil || !found || string(gotKey) != "a" || !bytes.Equal(got, want) {
				t.Fatalf("refused opportunistic value growth lost original local read: found=%t len=%d err=%v", found, len(got), err)
			}
			if basis.reader != nil || basis.readSlots[0].reader != nil || s.cowReader != local || local.point != nil ||
				after.ExternalBytes != pressured.ExternalBytes+growth || after.ReservedBytes != pressured.ReservedBytes+growth ||
				after.ExternalLeases != pressured.ExternalLeases+1 {
				t.Fatalf("fresh value refusal must refund before exactly one local grow: before=%+v after=%+v", pressured, after)
			}
		})
	}
}

func TestCOWSuccessorParallelBasisSlotsReuseAndRelease(t *testing.T) {
	db := cowPointerFixture(t)
	want := bytes.Repeat([]byte("parallel-pointer"), 8)
	if err := db.Set([]byte("a"), want); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	old, err := db.acquireCOWSnapshotWithError()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	basis := old.cowCut.basis
	var workspaces []*cowReadWorkspace
	var owned [][]byte
	basis.readMu.Lock()
	lockedSlots, locked := 0, true
	defer func() {
		if locked {
			for i := 0; i < lockedSlots; i++ {
				basis.readSlots[i].mu.Unlock()
			}
			basis.readMu.Unlock()
		}
	}()
	for i := range basis.readSlots {
		s, err := db.acquireCOWSnapshotWithError()
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		_, value, found, readErr := s.SeekGEVersionRange([]byte("a"), []byte("b"))
		if readErr != nil || !found || !bytes.Equal(value, want) || s.cowReader != nil || basis.readSlots[i].reader == nil {
			t.Fatalf("parallel slot %d did not reuse basis-owned value/traversal: found=%t err=%v", i, found, readErr)
		}
		owned = append(owned, value)
		workspaces = append(workspaces, basis.readSlots[i].reader)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		basis.readSlots[i].mu.Lock()
		lockedSlots++
	}
	for i := range basis.readSlots {
		basis.readSlots[i].mu.Unlock()
	}
	basis.readMu.Unlock()
	locked = false
	// Replace the root, then release the last original cut. Every slot must
	// detach/refund before the basis snapshot releases its physical custody.
	if err := db.Set([]byte("a"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	for i, w := range workspaces {
		if basis.readSlots[i].reader != nil || w.point != nil || w.lease != nil || w.budget != nil || !bytes.Equal(owned[i], want) {
			t.Fatalf("parallel slot %d retained scratch or changed owned output", i)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := db.COWMemoryStats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 || stats.Views != 0 {
		t.Fatalf("parallel basis owners did not refund: %+v", stats)
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
