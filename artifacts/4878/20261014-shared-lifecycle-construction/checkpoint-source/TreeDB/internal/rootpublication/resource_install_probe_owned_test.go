//go:build linux

package rootpublication

import (
	"errors"
	"math"
	"os"
	"sync"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

type installProbeAdmissionBudget struct{ limit uint64 }
type installProbeAdmissionLease struct{ budget *installProbeAdmissionBudget }

func (b *installProbeAdmissionBudget) AcquireRetention(n uint64) (retainedalloc.Lease, error) {
	lease := &installProbeAdmissionLease{budget: b}
	return lease, lease.Resize(n)
}
func (l *installProbeAdmissionLease) Resize(n uint64) error {
	if n > l.budget.limit {
		return retainedalloc.ErrCapacity
	}
	return nil
}
func (*installProbeAdmissionLease) Close() {}

func TestOriginalInstallProbeAdmissionBeforeChildCreation(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableChildFileInstallProbe{})))
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	probe := new(StableChildFileInstallProbe)
	budget := &installProbeAdmissionBudget{limit: math.MaxUint64}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	before := owner.Bytes()
	budget.limit = before
	if err := probe.Run(parent, &owner); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("actual producer admission refusal=%v", err)
	}
	budget.limit = math.MaxUint64
	if got := owner.Bytes(); got != before {
		t.Fatalf("refusal changed original charge: %d -> %d", before, got)
	}
	if entries, err := os.ReadDir(parent.Name()); err != nil || len(entries) != 0 {
		t.Fatalf("refused child effects=%v err=%v", entries, err)
	}
	owner.RemovePending(charge)
}

func TestOriginalInstallProbePhysicalCloseFailureRetainsExactOperandsOnce(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	charge := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(StableChildFileInstallProbe{})))
	if err := owner.AddPending(charge); err != nil {
		t.Fatal(err)
	}
	probe := new(StableChildFileInstallProbe)
	before := owner.Bytes()
	cleanupCalls := 0
	closeFile := func(file *os.File) error {
		cleanupCalls++
		if err := file.Close(); err != nil {
			return err
		}
		return file.Close() // actual original last-Close refusal, no fake outcome
	}
	first := probe.run(parent, &owner, MoveStableChildFileNoReplace, RemoveStableChildFile, closeFile)
	if !errors.Is(first, os.ErrClosed) || probe.source == nil || probe.parent != parent || probe.CleanupError() == nil {
		t.Fatalf("original close operands/outcome missing: source=%p parent=%p err=%v", probe.source, probe.parent, first)
	}
	if owner.Bytes() <= before || probe.fileCharge == 0 || probe.nameCharge == 0 {
		t.Fatal("failed original physical operands refunded")
	}
	if entries, err := os.ReadDir(parent.Name()); err != nil || len(entries) != 0 {
		t.Fatalf("actual consumed namespace effects=%v err=%v", entries, err)
	}
	var callers sync.WaitGroup
	for i := 0; i < 8; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			if err := probe.Run(parent, &owner); err != first {
				t.Errorf("repeat replaced first outcome: %v", err)
			}
		}()
	}
	callers.Wait()
	if cleanupCalls != 1 {
		t.Fatalf("original physical cleanup replayed %d times", cleanupCalls)
	}
}

func TestOriginalInstallProbeSuccessScrubsBeforeRefund(t *testing.T) {
	parent, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	var owner retainedalloc.Owner
	owner.Initialize(0)
	var probe StableChildFileInstallProbe
	before := owner.Bytes()
	if err := probe.Run(parent, &owner); err != nil {
		t.Fatal(err)
	}
	if probe.parent != nil || probe.source != nil || probe.sourceName != "" || probe.destinationName != "" || probe.metadata != nil || owner.Bytes() != before {
		t.Fatal("successful original probe failed alias scrub/refund")
	}
}
