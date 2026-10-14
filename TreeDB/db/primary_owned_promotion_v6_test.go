package db

import (
	"bytes"
	"fmt"
	"github.com/snissn/gomap/TreeDB/batch"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/zipper"
	"testing"
)

func TestPrimaryOwnedPromotionV6ConstructorSurvivesVisibilityAndPromotion(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	idx := db.idx.Load()
	// Copy uses the SAME ordinary zipper constructor as put/materialization.
	id, err := idx.zipper.CopyPrimaryRoot(db.State().RootPageID)
	if err != nil {
		t.Fatal(err)
	}
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	owner, ready, err := idx.primary.ReadRootConstructionOwnerV6(id, work)
	if !ready || err != nil {
		t.Fatalf("constructor owner %v %v", ready, err)
	}
	txn, ok := owner.(*rootpublication.DurableRootTransaction)
	if !ok || txn.Owner() != rootpublication.ResourceOwnerBuilder || txn.Sequence() != 0 {
		t.Fatal("root constructed outside slot-independent transaction")
	}
	if err = txn.Abort(); err != nil {
		t.Fatal(err)
	}
	var owners []*rootpublication.DurableRootTransaction
	prior := db.durableRoot.record
	for _, key := range []string{"first", "second"} {
		if err = db.Set([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
		runtime := db.rootPublication
		runtime.mu.Lock()
		member := runtime.visibleMember(db.State().CommitSeq)
		runtime.mu.Unlock()
		if member == nil || member.transaction == nil {
			t.Fatal("visible member is not a transaction view")
		}
		owner, ready, err = idx.primary.ReadRootConstructionOwnerV6(member.next.UserRootPageID, work)
		if !ready || err != nil || owner != member.transaction {
			t.Fatal("visibility replaced constructor owner")
		}
		owners = append(owners, member.transaction)
		if member.transaction.PrimaryConstructionReferenceV6() != member.primary.Bundle().Directory {
			t.Fatal("logical root differs from constructor custody")
		}
	}
	if owners[0] == owners[1] || db.durableRoot.record != prior {
		t.Fatal("relaxed roots not independent before slot selection")
	}
	// Refused head-bound promotion cannot consume or replace the SAME visible
	// constructor. Missing-parent and pre-codec admission refusals are retryable
	// without touching its immutable root or dependency custody.
	latest := owners[len(owners)-1]
	beforeRef := latest.PrimaryConstructionReferenceV6()
	if _, ready, err := latest.PreparePrimaryPromotionV6(rootpublication.DurableRootRecordV1{}, rootpublication.PrimaryProjectionV5{}, nil, beforeRef, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || err == nil {
		t.Fatal("missing parent promotion accepted")
	}
	parentView := db.durableRoot.primary.capsuleViews[db.durableRoot.slot]
	refusedWork := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if _, ready, err := latest.PreparePrimaryPromotionV6(rootpublication.DurableRootRecordV1{}, rootpublication.PrimaryProjectionV5{}, &parentView, db.durableRoot.primary.records[db.durableRoot.slot], refusedWork); ready || err != nil {
		t.Fatalf("pre-codec admission refusal %v %v", ready, err)
	}
	if latest.PrimaryPromotionV6() != nil || latest.PrimaryConstructionReferenceV6() != beforeRef || latest.Owner() != rootpublication.ResourceOwnerCoordinator {
		t.Fatal("refusal changed immutable constructor custody")
	}
	snapshot := db.AcquireSnapshot()
	defer snapshot.Close()
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for _, txn := range owners {
		if txn.Owner() != rootpublication.ResourceOwnerReleased {
			t.Fatal("promotion did not finish same constructor transaction")
		}
	}
	if owners[1].PrimaryPromotionV6() == nil {
		t.Fatal("physical promotion owned by another lifecycle")
	}
	for _, key := range []string{"first", "second"} {
		got, err := snapshot.Get([]byte(key))
		if err != nil || !bytes.Equal(got, []byte(key)) {
			t.Fatalf("retained relaxed root %q %v", got, err)
		}
	}
}

// Retained and streamed/discarded non-exact routing spans must use identical
// bounds. Both still perform the same real recursive touched-node census.
func TestPrimaryOwnedPromotionV6RoutingEmission(t *testing.T) {
	db, e := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	initial := db.NewBatchWithSize(200).(*Batch)
	for i := 0; i < 200; i++ {
		if e = initial.SetWithRevision([]byte(fmt.Sprintf("base-%03d", i)), bytes.Repeat([]byte("v"), 128), page.EntryRevision(i+1)); e != nil {
			t.Fatal(e)
		}
	}
	if e = initial.WriteSync(); e != nil {
		t.Fatal(e)
	}
	initial.Close()
	delta := batch.New(nil, page.PageSize)
	defer delta.Close()
	if e = delta.SetOps([]batch.Entry{{Type: batch.OpPut, Key: []byte("future-a"), Value: []byte("a"), Revision: 301}, {Type: batch.OpPut, Key: []byte("future-b"), Value: []byte("b"), Revision: 302}}); e != nil {
		t.Fatal(e)
	}
	root := db.State().RootPageID
	var streamed []zipper.ReadOnlyLeafSpan
	retained, e := db.idx.Load().zipper.PrepareReadOnly(root, delta, zipper.ReadOnlyPrepareOptions{CountTouchedOldEntries: true, LeafSpanCallback: func(s zipper.ReadOnlyLeafSpan) { streamed = append(streamed, s) }})
	if e != nil || retained.ExactLeafSpans {
		t.Fatal("routing became exact", e)
	}
	if e = retained.ValidateLeafSpans(); e != nil {
		t.Fatal(e)
	}
	if len(streamed) != len(retained.LeafSpans) || len(streamed) != 2 {
		t.Fatal("emission count")
	}
	for i, s := range streamed {
		if !bytes.Equal(s.LowKey, retained.LeafSpans[i].LowKey) || !bytes.Equal(s.HighKey, retained.LeafSpans[i].HighKey) {
			t.Fatal("callback emitted different routing bounds")
		}
	}
	streamed = nil
	discarded, e := db.idx.Load().zipper.PrepareReadOnly(root, delta, zipper.ReadOnlyPrepareOptions{CountTouchedOldEntries: true, DiscardLeafSpans: true, LeafSpanCallback: func(s zipper.ReadOnlyLeafSpan) { streamed = append(streamed, s) }})
	if e != nil || discarded.ExactLeafSpans || len(discarded.LeafSpans) != 0 || len(streamed) != 2 {
		t.Fatal("discard changed routing census", e)
	}
	if discarded.TouchedOldPages != retained.TouchedOldPages || discarded.TouchedOldLeafEntries != retained.TouchedOldLeafEntries {
		t.Fatal("discard reduced actual reads")
	}
	for i, s := range streamed {
		if !bytes.Equal(s.LowKey, retained.LeafSpans[i].LowKey) || !bytes.Equal(s.HighKey, retained.LeafSpans[i].HighKey) {
			t.Fatal("discard emitted different routing bounds")
		}
	}
}
