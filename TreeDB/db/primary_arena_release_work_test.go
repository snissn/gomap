package db

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
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
		// Physical close polls the original pager grower without waiting inside
		// its bounded turn. Give that worker a chance to observe its stop signal.
		runtime.Gosched()
		turns++
		if turns > 4096 {
			pagerState := reflect.ValueOf(a.Pager()).Elem()
			stack := make([]byte, 64<<10)
			stack = stack[:runtime.Stack(stack, true)]
			t.Fatalf("physical finish permanently refused: receiptPhase=%d arenaPhase=%d pagerPhase=%d growDone=%#x work=%+v ownerRefs=%d metadataBytes=%d physicalClosed=%t\n%s",
				receipt.phase, reflect.ValueOf(a).Elem().FieldByName("closeWork").FieldByName("phase").Int(),
				pagerState.FieldByName("closeWork").FieldByName("phase").Int(), pagerState.FieldByName("growDone").Pointer(),
				w, held.refs, metadata.Bytes(), metadata.PhysicalClosed(), stack)
		}
	}
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
