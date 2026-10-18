package db

import (
	"bytes"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestDATAActualPublicationUsesOriginalEnrolledOwner(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, Durability: DurabilityWALOffRelaxed, ResolvedProfile: ProfileNoWALFast, DisableBackgroundPrune: true, rootPublicationFixedDelay: 100 * time.Millisecond}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	idx, runtime := d.idx.Load(), d.rootPublication
	owner := d.StableResourceIdentityPinRegistry().MetadataOwner()
	if idx == nil || idx.primary != nil || runtime == nil || runtime.metadata != owner || d.StableResourceMetadataOwner() != owner {
		t.Fatal("not actual DATA route with original registry owner")
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	hold, err := d.acquireRootPublicationBuilderV1()
	if err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			hold.Release()
		}
	}()
	for _, key := range []string{"one", "two"} {
		if err = d.Set([]byte(key), []byte("value-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	runtime.mu.Lock()
	if len(runtime.visibleMembers)-runtime.visibleHead != 2 {
		runtime.mu.Unlock()
		t.Fatal("held original builder did not preserve two accepted DATA members")
	}
	for _, member := range runtime.visibleMembers[runtime.visibleHead:] {
		if member.primary != nil || member.transaction == nil || member.transaction.Owner() != rootpublication.ResourceOwnerCoordinator || member.resources.MetadataOwner() != owner || member.install.metadata != owner {
			runtime.mu.Unlock()
			t.Fatal("accepted DATA member/resource/terminal lost same owner")
		}
	}
	runtime.mu.Unlock()
	if budget.Stats().TotalBytes == 0 {
		t.Fatal("actual governing budget did not observe DATA charge")
	}
	hold.Release()
	held = false
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	idx = nil
	runtime = nil
	enrollment.Close()
	budget.Close()
	if owner.Bytes() != 0 || budget.Stats().TotalBytes != 0 {
		t.Fatalf("original DATA close retained owner/budget: %d %+v", owner.Bytes(), budget.Stats())
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, key := range []string{"one", "two"} {
		got, e := reopened.Get([]byte(key))
		if e != nil || !bytes.Equal(got, []byte("value-"+key)) {
			t.Fatalf("reopen %s=%q err=%v", key, got, e)
		}
	}
	if reopened.rootPublication.visibleResources.MetadataOwner() != reopened.StableResourceMetadataOwner() {
		t.Fatal("recovered DATA closure lost original owner")
	}
}

func TestDATAActualPublicationAdmissionRefusalPreservesVisibleRoot(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), Durability: DurabilityWALOffRelaxed, ResolvedProfile: ProfileNoWALFast, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	owner := d.StableResourceIdentityPinRegistry().MetadataOwner()
	budget := &originalClosureAdmissionBudget{limit: math.MaxUint64}
	enrollment, err := owner.Enroll(budget)
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	hold, err := d.acquireRootPublicationBuilderV1()
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Release()
	before, state := owner.Bytes(), d.state.Load()
	budget.setLimit(before)
	err = d.Set([]byte("denied"), []byte("value"))
	budget.setLimit(math.MaxUint64)
	if !errors.Is(err, retainedalloc.ErrCapacity) || CommitPublicationAccepted(err) || d.state.Load() != state || owner.Bytes() != before {
		t.Fatalf("DATA refusal changed original visible custody: err=%v accepted=%t bytes=%d before=%d", err, CommitPublicationAccepted(err), owner.Bytes(), before)
	}
	got, err := d.Get([]byte("denied"))
	if err != nil || got != nil {
		t.Fatalf("refused key visible: %q %v", got, err)
	}
	if err = d.Set([]byte("accepted"), []byte("value")); err != nil {
		t.Fatal(err)
	}
}
