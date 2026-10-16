package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
)

// Real ordinary publications retain independent logical roots through the
// original table's growth boundary. The Lease-only budget is intentional.
func TestOrdinaryPrimaryHeldRootsCrossNativeTableBoundaryV6(t *testing.T) {
	dir := t.TempDir()
	options := Options{Dir: dir, ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	d, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	if err = d.SetSync([]byte("key"), []byte("warm")); err != nil {
		t.Fatal(err)
	}
	a := d.idx.Load().primary
	enrollment, err := a.MetadataOwner().Enroll(&primaryReviewRefusalBudget{})
	if err != nil {
		t.Fatal(err)
	}
	defer enrollment.Close()
	var snapshots []*Snapshot
	defer func() {
		for _, snapshot := range snapshots {
			snapshot.Close()
		}
	}()
	crossed := false
	last := ""
	for i := 0; i < 1024; i++ {
		last = fmt.Sprintf("value-%04d", i)
		if err = d.SetSync([]byte("key"), []byte(last)); err != nil {
			t.Fatalf("ordinary publication %d: %v", i, err)
		}
		snapshot := d.AcquireSnapshot()
		snapshots = append(snapshots, snapshot)
		if snapshot.state.primaryRoot == nil {
			t.Fatal("ordinary PRIMARY root missing")
		}
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		geometry, ready := a.MaintenanceNativeBankGeometryForTestV6(snapshot.state.primaryRoot.ref, w)
		if !ready || !geometry.Valid || !geometry.TableApplicable || geometry.TableBackingOpaque {
			t.Fatal("protected table geometry unavailable", ready, geometry)
		}
		if geometry.PagerTable.TableNodeCount >= 512 && geometry.PagerTable.HeadCapacity >= 512 {
			crossed = true
			before, counters := a.MetadataOwner().Bytes(), a.Counters()
			ref, ready, e := a.PrepareNativeConstructedReadRootV6(nil, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			// The supported ordinary growth does not grant a finite native WorkLease.
			if ready || !errors.Is(e, retainedalloc.ErrWorkRequired) || ref.PageID != 0 || a.MetadataOwner().Bytes() != before || a.Counters() != counters {
				t.Fatal("native unsupported lease changed custody", ref, ready, e)
			}
			break
		}
	}
	if !crossed {
		t.Fatal("actual held-root table never crossed its 512-entry rehash boundary")
	}
	for i, snapshot := range snapshots {
		got, e := snapshot.Get([]byte("key"))
		if e != nil || string(got) != fmt.Sprintf("value-%04d", i) {
			t.Fatalf("held cut %d: %q %v", i, got, e)
		}
	}
	for _, snapshot := range snapshots {
		if err = snapshot.Close(); err != nil {
			t.Fatal("held snapshot close", err)
		}
	}
	snapshots = nil
	enrollment.Close()
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d = nil
	reopened, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, e := reopened.Get([]byte("key")); e != nil || string(got) != last {
		t.Fatal("reopen after held-root release", string(got), e)
	}
}
