package rootpublication

import (
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/pager"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestPrimaryDependencyMetadataAdmissionAndLastRead(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "primary"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.GrowTo(3); err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	baseline := owner.Bytes()
	budget.cap = baseline
	var releases atomic.Int32
	release := func() (bool, error) { releases.Add(1); return true, nil }
	ref := DependencyDirectoryRefV2{RootPageID: page.PrimaryBankNamespace + 2}
	if _, err = NewOwnedLocalPrimaryDependencyDirectoryV5(p, ref, 3, &owner, release); !errors.Is(err, retainedalloc.ErrCapacity) {
		t.Fatalf("refusal=%v", err)
	}
	if owner.Bytes() != baseline || releases.Load() != 0 {
		t.Fatal("refusal consumed physical lease or leaked capacity")
	}
	budget.cap = 1 << 20
	directory, err := NewOwnedLocalPrimaryDependencyDirectoryV5(p, ref, 3, &owner, release)
	if err != nil {
		t.Fatal(err)
	}
	charged := owner.Bytes()
	if charged <= baseline || directory.tree == nil {
		t.Fatal("real tree allocation not admitted")
	}
	if err = directory.Retain(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { directory.Release(); close(done) }()
	<-done
	if releases.Load() != 0 || owner.Bytes() != charged {
		t.Fatal("physical caller release erased independent read custody")
	}
	directory.Release()
	if releases.Load() != 1 || owner.Bytes() != baseline || directory.tree != nil || directory.ownedRelease != nil {
		t.Fatal("last read did not clear actual tree/callback before refund")
	}
	directory.Release()
	if releases.Load() != 1 {
		t.Fatal("duplicate callback")
	}
	if err = directory.Retain(); !errors.Is(err, ErrResourceOwnership) {
		t.Fatal("closed proof reacquired", err)
	}
	enrollment.Close()
	if budget.bytes != 0 {
		t.Fatal("completed borrowed governor remains")
	}
}

func TestPrimaryDependencyMetadataFailedPhysicalReleaseRetainsDebt(t *testing.T) {
	p, err := pager.Open(filepath.Join(t.TempDir(), "primary"), 65536)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = p.GrowTo(3); err != nil {
		t.Fatal(err)
	}
	var owner retainedalloc.Owner
	owner.Initialize(0)
	budget := &registryBudget{cap: 1 << 20}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("physical lease remains")
	calls := 0
	directory, err := NewOwnedLocalPrimaryDependencyDirectoryV5(p, DependencyDirectoryRefV2{RootPageID: page.PrimaryBankNamespace + 2}, 3, &owner, func() (bool, error) { calls++; return false, failure })
	if err != nil {
		t.Fatal(err)
	}
	admitted := owner.Bytes()
	directory.Release()
	if calls != 1 || directory.tree == nil || directory.ownedRelease == nil || owner.Bytes() != admitted {
		t.Fatal("failed cleanup falsely disposed owned backing")
	}
	directory.Release()
	if calls != 1 {
		t.Fatal("ordinary release replayed failed physical effect")
	}
	enrollment.Close()
	if budget.bytes == 0 {
		t.Fatal("failed last capture erased governing debt")
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if budget.bytes == 0 || owner.Bytes() == 0 {
		t.Fatal("physical close erased admitted pending closure")
	}
}
