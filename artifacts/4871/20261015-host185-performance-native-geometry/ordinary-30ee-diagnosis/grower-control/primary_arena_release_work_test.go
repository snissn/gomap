package db

import (
	"errors"
	"os"
	"path/filepath"
 "reflect"
 "runtime"
 "time"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
 "github.com/snissn/gomap/TreeDB/pager"
)

func TestPrimaryOwnerWorkActualArenaNamespaceAndExternalContainingReceipt(t *testing.T) {
	dir := t.TempDir()
	a, err := primaryarena.Open(filepath.Join(dir, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	if !a.Pager().SupportsBoundedPhysicalClose() {
		a.Close()
		t.Skip("bounded mapping unsupported")
	}
	owner, err := newPrimaryArenaOwnerV5(a)
	if err != nil {
		t.Fatal(err)
	}
	metadata := a.MetadataOwner()
	charge, err := rootpublication.StableFileMetadataCharge(uint64(len(dir)))
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	owner.namespaceParent, owner.namespaceParentCharge = parent, charge
	if err = a.Pager().WithStableResourceFile(func(file *os.File) error {
		owner.namespaceProof, err = rootpublication.NewOwnedStableNamespaceCreationProof(parent, file, "primary", metadata)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	budgets := make([]*memtable.COWBudget, 80)
	captures := make([]*retainedalloc.Enrollment, 80)
	for i := range budgets {
		budgets[i], err = memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		captures[i], err = metadata.Enroll(budgets[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	external, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admission, err := external.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(primaryArenaReleaseWork{}))))
	if err != nil {
		t.Fatal(err)
	}
	var receipt primaryArenaReleaseWork
 select {
 case observed := <-pager.DiagnosticOriginalGrowerEnteredForTest:
  if observed != a.Pager() { t.Fatal("gate did not withhold SAME original grower") }
 case <-time.After(5*time.Second): t.Fatal("original grower did not enter diagnostic gate")
 }
 originalDone := pager.DiagnosticOriginalGrowDoneForTest(a.Pager())
 releasedGrower := false
 defer func(){
  if !releasedGrower { close(pager.DiagnosticOriginalGrowerReleaseForTest) }
  select { case <-originalDone: case <-time.After(5*time.Second): t.Error("original grower not joined after gate release") }
 }()
	if ready, err := receipt.Bind(owner, &iterator.OrdinalScanWork{}); ready || err != nil || owner.refs != 1 {
		t.Fatal("refused bind changed actual edge", err)
	}
	if ready, err := receipt.Bind(owner, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
		t.Fatal(err)
	}
	held := owner
	owner = nil
	if consumed, debt, err := held.releaseWithOutcome(nil); consumed || debt || !errors.Is(err, ErrNativeMVCCSnapshotWork) {
		t.Fatal("ordinary last release stole original continuation", err)
	}
 // Negative: SAME original work/grants exhaust original4096 bound while
 // SAME producer grower is explicitly withheld. No second owner/worker.
 for negativeTurns := 0; negativeTurns <= 4096; negativeTurns++ {
  w := iterator.OrdinalScanWork{RecordLimit:32, ByteLimit:1<<20}
  ready,debt,e := receipt.ContinueWork(&w)
  if ready || !debt || e != nil || w.Records > 32 || w.Bytes > 1<<20 { t.Fatalf("withheld original progress=%v/%v/%v work=%+v",ready,debt,e,w) }
  if negativeTurns > 0 && (w.Records != 1 || w.Bytes != 128) { t.Fatalf("withheld growDone was not exact one-record/128-byte polling: %+v",w) }
 }
 arenaPhase := reflect.ValueOf(a).Elem().FieldByName("closeWork").FieldByName("phase").Int()
 pagerPhase := reflect.ValueOf(a.Pager()).Elem().FieldByName("closeWork").FieldByName("phase").Int()
 if receipt.phase != 0 || arenaPhase != 1 || pagerPhase != 1 || held.refs != 0 || receipt.owner != held || held.arena != a || held.namespaceProof == nil || held.namespaceParent == nil || held.metadataCharge == 0 || metadata.PhysicalClosed() { t.Fatalf("withheld custody/phase receipt=%d arena=%d pager=%d owner=%+v",receipt.phase,arenaPhase,pagerPhase,held) }
 select { case <-originalDone: t.Fatal("withheld original growDone closed"); default: }
 t.Logf("NEGATIVE matched original refusal after4097 calls: receiptPhase=%d ArenaPhase=%d PagerPhase=%d SAMEowner=true SAMEgrowDone=open; paused grant=1record/128bytes; owner=%d receipt=%d arena=%d pager=%d proof=%d token=%d namespace=%d bytes", receipt.phase,arenaPhase,pagerPhase,unsafe.Sizeof(*held),unsafe.Sizeof(receipt),unsafe.Sizeof(*a),unsafe.Sizeof(*a.Pager()),unsafe.Sizeof(*held.namespaceProof),unsafe.Sizeof(rootpublication.StableResourceToken{}),unsafe.Sizeof(rootpublication.StableNamespaceToken{}))
 close(pager.DiagnosticOriginalGrowerReleaseForTest)
 releasedGrower = true
 turns := 0
 for {
		w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, debt, err := receipt.ContinueWork(&w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("physical original cleanup", err)
		}
		if ready {
			if debt {
				t.Fatal("completed physical cleanup reports debt")
			}
			break
		}
		if !debt {
			t.Fatal("paused physical cleanup lost exact custody")
		}
		runtime.Gosched()
		turns++
		if turns > 4096 {
			t.Fatal("physical finish permanently refused")
		}
	}
 if originalDone != pager.DiagnosticOriginalGrowDoneForTest(a.Pager()) { t.Fatal("positive finish replaced original growDone") }
 select { case <-originalDone: default: t.Fatal("positive finish did not await SAME original grower") }
 t.Logf("POSITIVE same original owner/growDone completed after%d paused calls; grants32records/1MiB, original4096 bound unchanged", turns)
 if turns < 2 || receipt.owner != nil || held.arena != nil || held.namespaceParent != nil || held.namespaceProof != nil || held.metadataCharge != 0 || !metadata.PhysicalClosed() {
		t.Fatal("aliases/physical finish incomplete")
	}
	for i, capture := range captures {
		capture.Close()
		budgets[i].Close()
		if budgets[i].Stats().TotalBytes != 0 {
			t.Fatal("held capture metadata leaked")
		}
	}
	admission.Close()
	external.Close()
	if metadata.Bytes() != 0 || external.Stats().TotalBytes != 0 {
		t.Fatal("exact final containing refund incomplete")
	}
}

func TestPrimaryOwnerOrdinaryNonlastHasNoUnadmittedReceiptAllocation(t *testing.T) {
	a, err := primaryarena.Open(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newPrimaryArenaOwnerV5(a)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := owner.release(nil); err != nil {
			t.Fatal(err)
		}
	}()
	allocations := testing.AllocsPerRun(20, func() {
		if ready, err := owner.retain(nil); !ready || err != nil {
			panic("retain failed")
		}
		if consumed, debt, err := owner.releaseWithOutcome(nil); !consumed || debt || err != nil {
			panic("ordinary nonlast release failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("ordinary owner edge allocated unadmitted receipt: %g", allocations)
	}
}

func TestPrimaryOwnerOrdinaryNonlastPreservesStickySiblingError(t *testing.T) {
	a, err := primaryarena.Open(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := newPrimaryArenaOwnerV5(a)
	if err != nil {
		t.Fatal(err)
	}
	sticky := errors.New("earlier sibling failed")
	owner.cleanupErr = sticky
	if ready, e := owner.retain(nil); !ready || e != nil {
		t.Fatal(e)
	}
	if consumed, debt, e := owner.releaseWithOutcome(nil); !consumed || debt || !errors.Is(e, sticky) {
		t.Fatalf("nonlast consumed=%v debt=%v err=%v", consumed, debt, e)
	}
	if owner.refs != 1 || owner.arena != a || owner.closeWork.started {
		t.Fatal("nonlast edge started physical cleanup")
	}
	// The original terminal release still reports sticky failure and retains its
	// debt; the scalar fast path must neither clear nor relocate that authority.
	consumed, debt, e := owner.releaseWithOutcome(nil)
	if !consumed || !debt || !errors.Is(e, sticky) {
		t.Fatalf("last consumed=%v debt=%v err=%v", consumed, debt, e)
	}
	if owner.arena != a || owner.metadataCharge == 0 || !owner.closeWork.done {
		t.Fatal("original sticky cleanup debt was lost")
	}
}
