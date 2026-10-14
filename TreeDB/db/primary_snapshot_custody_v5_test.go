package db

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestPrimaryPrivateSnapshotCustodyRetainsNonLeafDebt(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("capture")
	}
	// The registered terminal owner now completes all typed custody; no ordinary
	// harness finalization is counted as completion.
	if err = snapshot.beginRead(); err != nil {
		t.Fatal(err)
	}
	lease := &primaryBaseReadLeaseV5{snapshot: snapshot, pinned: true, wrapperGeneration: snapshot.generation.Load(), root: snapshot.primaryRoot}
	// The real private capture/producer is not connected yet. An ordinary public
	// capture is first refused; the harness then gives this exclusive capture the
	// internal eligibility marker, without changing any public fixture or option.
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, ready, err := claimPrimarySnapshotCustodyV5(lease, work); err == nil || ready || snapshot.closed.Load() {
		t.Fatal("public capture admitted")
	}
	snapshot.privatePrimaryOwnerV5 = true
	refs := database.leafGenerationPins.refsForGenerationIDs([]uint64{1001, 1002})
	set := newLeafGenerationPinSet(refs)
	set.markStale(&database.leafGenerationPins)
	if !set.retain(&database.leafGenerationPins) {
		t.Fatal("actual stale pin holder")
	}
	snapshot.leafGenerationPinSet = set
	// Retain a genuine transient read as well as the proof owner's read edge.
	if err = snapshot.beginRead(); err != nil {
		t.Fatal(err)
	}
	beforeID, beforeRoot := snapshot.registryID, snapshot.primaryRoot
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	custody, ready, err := claimPrimarySnapshotCustodyV5(lease, work)
	if err != nil || !ready {
		t.Fatalf("claim %v %v", ready, err)
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err = custody.ReleaseReadPinV5(work); err != nil || ready {
		t.Fatal("transient read ignored")
	}
	snapshot.endRead()
	if snapshot.registryID != beforeID || snapshot.primaryRoot != beforeRoot || snapshot.db != database {
		t.Fatal("public last-read cleanup stole debt")
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err = custody.ReleaseReadPinV5(work); err != nil || !ready {
		t.Fatalf("read release %v %v", ready, err)
	}
	// Two records detach the holder but cannot drop its first actual component
	// ref. Repinning between charged returns creates a second independent edge.
	work = &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
	done, progress, err := custody.ReleaseLeafPinsV5(work)
	if err != nil || done || !progress || refs[0].count.Load() != 1 || refs[1].count.Load() != 1 {
		t.Fatalf("detach %v %v %v", done, progress, err)
	}
	if !set.retain(&database.leafGenerationPins) {
		t.Fatal("new independent holder")
	}
	for turn := 0; turn < 10; turn++ {
		work = &iterator.OrdinalScanWork{RecordLimit: 2, ByteLimit: 1 << 20}
		done, _, err = custody.ReleaseLeafPinsV5(work)
		if err != nil {
			t.Fatal(err)
		}
		_ = snapshot.Close()
		if snapshot.registryID != beforeID || snapshot.primaryRoot != beforeRoot || snapshot.db != database {
			t.Fatal("partial leaf retirement released non-leaf custody")
		}
		if done {
			break
		}
	}
	if !done || refs[0].count.Load() != 1 || refs[1].count.Load() != 1 || set.holders != 1 || !set.pinned {
		t.Fatal("remaining holder lost or leaf cursor failed to finish")
	}
	set.release(&database.leafGenerationPins)
	if refs[0].count.Load() != 0 || refs[1].count.Load() != 0 {
		t.Fatal("holder leak")
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	if !custody.done || snapshot.db != nil || snapshot.registryID != 0 || database.primaryCustodyHead != nil {
		t.Fatal("terminal nonleaf completion failed")
	}
	t.Log("actual terminal owner completed primary/registry/DATA/vlog debt after charged leaf continuation")
}
