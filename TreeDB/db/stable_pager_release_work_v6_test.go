package db

import (
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"testing"
)

func TestNativeMaintenanceIndexTokenActualCompletionV6(t *testing.T) {
	d, err := Open(Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Establish the producer's existing namespace evidence exactly as ordinary
	// captures do; the native capture must borrow it rather than mint an authority.
	original := d.AcquireStableSnapshot()
	first, err := original.NewStableIndexResourceToken(rootpublication.StableResourceSpec{Kind: rootpublication.ResourceIndex, LogicalLane: "original", ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, ContentSynced: true}, NewStableDBResourceToken)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	idx := d.idx.Load()
	snapshot := d.acquireDurableCandidateStableIndexSnapshotV1(idx, false)
	if snapshot == nil {
		t.Fatal("actual maintenance capture")
	}
	var metadata retainedalloc.Owner
	token, err := snapshot.NewStableIndexResourceToken(rootpublication.StableResourceSpec{MetadataOwner: &metadata, Kind: rootpublication.ResourceIndex, LogicalLane: "native", ResourceID: "index", Reachability: rootpublication.ReachabilityIndexFile, ContentSynced: true}, NewStableDBResourceToken)
	if err != nil {
		snapshot.Close()
		t.Fatal(err)
	}
	operations := snapshot.stablePagerCompletion
	if operations == nil {
		t.Fatal("missing original callback")
	}
	ready, err := operations.SupportsReleaseWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if err != nil || !ready {
		t.Fatalf("actual producer unsupported ready=%v err=%v", ready, err)
	}
	var completion rootpublication.StableResourceTokenReleaseCompletionV6
	for calls := 0; calls < 64; calls++ {
		completion, ready, err = token.ReleaseWithWorkV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready || operations.snapshot != nil || operations.owner != nil || d.durableCandidateIndexCaptures.Load() != 0 {
		t.Fatal("original producer not physically completed")
	}
	token = nil
	completion.Complete()
	if metadata.Bytes() != 0 {
		t.Fatalf("retained metadata=%d", metadata.Bytes())
	}
}
