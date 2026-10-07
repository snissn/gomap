package db

import (
	"context"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/allocatorownership"
	"io"
	"testing"
	"unsafe"
)

// This production Close/cut/FD check is ordinary. The composed finite DB
// credit witness remains required when finite staging is admitted and wired.
func TestPhysicalSnapshotCutAllocatorTerminalClose5105(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true, DisableSideStores: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.SetSync([]byte("key"), []byte("captured")); err != nil {
		t.Fatal(err)
	}
	if err = database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	index := database.idx.Load()
	allocator := index.allocator
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("missing logical snapshot")
	}
	defer snapshot.Close()
	token, bound := snapshot.StateToken()
	if !bound {
		t.Fatal("unbound snapshot")
	}
	roots, err := database.CaptureRecoverableRootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer roots.Release()
	cut, err := database.CapturePhysicalSnapshotCutV1(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	index.writerMu.RLock()
	detached := index.allocator == nil && index.zipper == nil && index.writerAuthority == nil
	index.writerMu.RUnlock()
	if !detached {
		t.Fatal("joined index retains allocator writer aliases")
	}
	if after, ok := snapshot.StateToken(); !ok || after != token || snapshot.idx != index || roots.idx != index {
		t.Fatal("logical/root-set basis changed at writer detach")
	}
	if profile := allocator.COWPrepareProfileV1(); profile.Valid {
		t.Fatal("DB Close retained current allocator transaction")
	}
	if profile := allocator.ResidentGenerationProfileV1(); profile.PhysicalCutLeases != 1 {
		t.Fatalf("held cut census: %+v", profile)
	}
	if err = cut.WriteToContext(context.Background(), io.Discard); err != nil {
		t.Fatalf("cut FD/generation invalid after DB close: %v", err)
	}
	if err = cut.Close(); err != nil {
		t.Fatal(err)
	}
	if profile := allocator.ResidentGenerationProfileV1(); profile.PhysicalCutLeases != 0 {
		t.Fatalf("released cut census: %+v", profile)
	}
	if err = cut.WriteToContext(context.Background(), io.Discard); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed cut output: %v", err)
	}
}

func TestDurableRootOrdinaryZipperExportRetainsWriter5105(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true, DisableSideStores: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	index := database.idx.Load()
	exported := database.Zipper()
	if exported == nil || database.Zipper() != exported {
		t.Fatal("ordinary zipper export changed")
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	index.writerMu.RLock()
	defer index.writerMu.RUnlock()
	if index.zipper != exported || index.allocator == nil || index.writerAuthority == nil {
		t.Fatal("ordinary escaped writer backing detached")
	}
	if database.Zipper() != nil {
		t.Fatal("closed DB exported new writer")
	}
}

func TestDurableRootManagedWriterRetirementPreservesPreparedDebt5105(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true, DisableSideStores: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	index := database.idx.Load()
	allocator, writer, authority := index.allocator, index.zipper, index.writerAuthority
	info, err := allocator.COWGenerationInfoV1()
	if err != nil {
		t.Fatal(err)
	}
	var id freelist.CandidateIDV1
	id[0] = 77
	prepared, err := allocator.PrepareOwnedCOWCandidateRetiringWithLimitsV1(info.GenerationID()+1, info.CommitSeq()+1, id, freelist.ReuseCapability{}, nil, 0, freelist.NewCandidatePageSinkV1(), nil)
	if err != nil {
		t.Fatal(err)
	}
	index.detachAllocatorWriterV1(false)
	if index.allocator != allocator || index.zipper != writer || index.writerAuthority != authority {
		t.Fatal("healthy retirement detached nonterminal publication debt")
	}
	if err = allocator.AbortCOWCandidateV1(prepared); err != nil {
		t.Fatal(err)
	}
	if err = prepared.ClearTerminalBackingV1(); err != nil {
		t.Fatal(err)
	}
}

func TestDurableRootManagedWriterControlClassWitness5105(t *testing.T) {
	t.Logf("indexGen_raw=%d managedWriter_raw=%d writerMutex_raw=%d", unsafe.Sizeof(indexGen{}), unsafe.Sizeof(allocatorownership.ManagedWriter{}), unsafe.Sizeof(indexGen{}.writerMu))
}
