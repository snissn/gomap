package db

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

// This connects genuine DB DATA generation/snapshot custody to private native
// base-absence output. Common Accepted/Finish/public wrappers remain separately
// required; this test makes no original native gate or whole-cost claim.
func TestPrimaryNativeBaseProofFreshInsertionAndClosedLease(t *testing.T) {
	database, e := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer database.Close()
	batch := database.NewBatchWithSize(200).(*Batch)
	for i := 0; i < 200; i++ {
		if e = batch.SetWithRevision([]byte(fmt.Sprintf("k%03d", i)), bytes.Repeat([]byte{'v'}, 128), page.EntryRevision(i+1)); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.WriteSync(); e != nil {
		t.Fatal(e)
	}
	_ = batch.Close()
	directory := primaryDBDirectory(t, database)
	base, _ := directory.Base()
	image, e := database.idx.Load().pager.Get(base.Ref.Page)
	if e != nil {
		t.Fatal(e)
	}
	if node.NewNode(image).Type() != page.PageTypeInternal {
		t.Fatal("fixture did not materialize a multi-page immutable DATA base")
	}
	snapshot := database.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("snapshot")
	}
	defer snapshot.Close()
	database.writeMu.Lock()
	database.durablePublishMu.Lock()
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	proof, lease, ready, e := database.preparePrimaryBaseKeyProofV5(snapshot, []byte("k003"), 4, work)
	database.durablePublishMu.Unlock()
	database.writeMu.Unlock()
	// Ordinary harness retirement; native Finish must charge this debt.
	defer func() {
		if released, retirement := lease.ReleasePinV5(nil); !released {
			t.Error("lease release")
		} else if retirement != nil {
			_ = retirement.finalizeCloseIfUnreferenced()
		}
	}()
	if e != nil || !ready {
		t.Fatalf("prepare base: %v %v work=%d/%d", ready, e, work.Records, work.Bytes)
	}
	records, byteCount := work.Records, work.Bytes
	for turn := 0; turn < 200; turn++ {
		work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ready, progress, e := proof.Step(work)
		records += work.Records
		byteCount += work.Bytes
		if e != nil || (!ready && !progress) {
			t.Fatalf("base proof turn%d ready=%v progress=%v error=%v", turn, ready, progress, e)
		}
		if ready {
			break
		}
		if turn == 199 {
			t.Fatal("base proof did not progress")
		}
	}
	t.Logf("actual generation-owned base preparation %d records %d bytes", records, byteCount)
	// A legal unrelated writer changes the directory while preserving the actual
	// immutable base. Only the current negative directory lookup may be consumed.
	if e = database.SetSync([]byte("above-floor"), []byte("writer")); e != nil {
		t.Fatal(e)
	}
	arena := database.idx.Load().primary
	claim, ready, e := arena.PrepareBundleClaim(nil)
	if !ready || e != nil {
		t.Fatalf("bundle %v %v", ready, e)
	}
	var bundle primaryarena.PublicationBundle
	for turn := 0; turn < 100; turn++ {
		bundle, ready, e = claim.Step(nil)
		if e != nil {
			t.Fatal(e)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("bundle claim")
	}
	defer func() { _, _ = arena.Drop(bundle.Record, nil); _, _ = arena.Drop(bundle.Directory, nil) }()
	componentClaim, ready, e := arena.PrepareClaim(primaryarena.Component, nil)
	if !ready || e != nil {
		t.Fatal(e)
	}
	var component primaryarena.Ref
	for turn := 0; turn < 100; turn++ {
		component, ready, e = componentClaim.Step(nil)
		if e != nil {
			t.Fatal(e)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("component claim")
	}
	defer func() { _, _ = arena.Drop(component, nil) }()
	componentImage := make([]byte, page.PageSize)
	builder := node.NewBuilderWithOptions(componentImage, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
	builder.SetPageID(component.PageID)
	if e = builder.AddLeafEntryWithRevision([]byte("k003"), nil, node.FlagTombstone, page.ValuePtr{}, 1000); e != nil {
		t.Fatal(e)
	}
	builder.FinishNoNode()
	if ready, e = arena.SealComponent(component, componentImage, nil); !ready || e != nil {
		t.Fatal(e)
	}
	current := database.durableRoot.primary.current.ref
	directory = primaryDBDirectory(t, database)
	entry := node.PrimaryDirectoryEntry{Key: []byte("k003"), Operand: node.PrimaryOperand{Ref: page.PageChildRef(component.PageID), Digest: sha256.Sum256(componentImage)}, Revision: 1000, Kind: node.PrimaryAbsence, ClassSlot: uint16(directory.Count())}
	scratch := make([]byte, page.PageSize)
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	ready, e = arena.SealBaseAbsence(bundle.Directory, current, entry, proof, scratch, work)
	if e != nil || !ready {
		t.Fatalf("fresh insertion ready=%v error=%v work=%d/%d", ready, e, work.Records, work.Bytes)
	}
	t.Logf("private base absence seal %d records %d bytes; whole native publication unqualified", work.Records, work.Bytes)
	result, e := arena.Get(bundle.Directory.PageID)
	if e != nil {
		t.Fatal(e)
	}
	sealed, e := node.DecodePrimaryDirectory(result)
	if e != nil {
		t.Fatal(e)
	}
	got, found := sealed.Search([]byte("k003"))
	if !found || got.Kind != node.PrimaryAbsence || sealed.Count() != directory.Count()+1 {
		t.Fatal("missing inserted exact absence")
	}
	writer, found := sealed.Search([]byte("above-floor"))
	if !found || writer.Kind != node.PrimaryPut {
		t.Fatal("fresh unrelated writer lost")
	}
	if value, e := snapshot.Get([]byte("k003")); e != nil || len(value) != 128 {
		t.Fatalf("held base %d %v", len(value), e)
	}

	// The same immutable DATA proof cannot mask an intervening replacement of
	// its key, even when that writer leaves the DATA base and proof lease intact.
	if e = database.SetSync([]byte("k003"), []byte("replacement")); e != nil {
		t.Fatal(e)
	}
	staleClaim, admitted, e := arena.PrepareBundleClaim(nil)
	if e != nil || !admitted {
		t.Fatal(e)
	}
	var staleBundle primaryarena.PublicationBundle
	for turn := 0; turn < 100; turn++ {
		staleBundle, admitted, e = staleClaim.Step(nil)
		if e != nil {
			t.Fatal(e)
		}
		if admitted {
			break
		}
	}
	if !admitted {
		t.Fatal("stale destination claim")
	}
	defer func() { _, _ = arena.Drop(staleBundle.Record, nil); _, _ = arena.Drop(staleBundle.Directory, nil) }()
	before, e := arena.Get(staleBundle.Directory.PageID)
	if e != nil {
		t.Fatal(e)
	}
	before = bytes.Clone(before)
	entry.ClassSlot = uint16(primaryDBDirectory(t, database).Count())
	scratch = bytes.Repeat([]byte{0xca}, page.PageSize)
	oldScratch := bytes.Clone(scratch)
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	admitted, e = arena.SealBaseAbsence(staleBundle.Directory, database.durableRoot.primary.current.ref, entry, proof, scratch, work)
	if admitted || e != node.ErrPrimaryDirectory {
		t.Fatalf("intervening exact replacement authorized: %v %v", admitted, e)
	}
	after, e := arena.Get(staleBundle.Directory.PageID)
	if e != nil || !bytes.Equal(before, after) || !bytes.Equal(scratch, oldScratch) {
		t.Fatal("stale negative lookup wrote private destination")
	}
	if value, e := database.Get([]byte("k003")); e != nil || string(value) != "replacement" {
		t.Fatalf("replacement lost %q %v", value, e)
	}
	if e = snapshot.Close(); e != nil {
		t.Fatal(e)
	}
	if _, _, e = proof.Step(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); e != primaryarena.ErrStale {
		t.Fatalf("closed/reused generation lease accepted %v", e)
	}
}

// Close may invalidate the proof concurrently, but cannot release the actual
// registry/DATA/primary custody or pool the wrapper while its read pin remains.
func TestPrimaryNativeBaseProofCloseReadPinRetirement(t *testing.T) {
	database, e := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if e != nil {
		t.Fatal(e)
	}
	defer database.Close()
	batch := database.NewBatchWithSize(200).(*Batch)
	for i := 0; i < 200; i++ {
		if e = batch.SetWithRevision([]byte(fmt.Sprintf("k%03d", i)), []byte("value"), page.EntryRevision(i+1)); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.WriteSync(); e != nil {
		t.Fatal(e)
	}
	_ = batch.Close()
	snapshot := database.AcquireSnapshot()
	database.writeMu.Lock()
	database.durablePublishMu.Lock()
	_, lease, ready, e := database.preparePrimaryBaseKeyProofV5(snapshot, []byte("k003"), 4, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	database.durablePublishMu.Unlock()
	database.writeMu.Unlock()
	if e != nil || !ready {
		t.Fatalf("prepare %v %v", ready, e)
	}
	var retirement *Snapshot
	defer func() {
		if retirement == nil {
			_, retirement = lease.ReleasePinV5(nil)
		}
		if retirement != nil {
			_ = retirement.finalizeCloseIfUnreferenced()
		}
	}()
	// An existing transient reader must never inherit last-read retirement.
	if e = snapshot.beginRead(); e != nil {
		t.Fatal(e)
	}
	closed := make(chan error, 1)
	go func() { closed <- snapshot.Close() }()
	for i := 0; i < 200; i++ {
		_ = lease.BaseReadAliveV5()
		_, e = lease.BaseReadPageV5(database.durableRoot.record.UserRootPageID, make([]byte, page.PageSize), &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
		if e != nil && e != primaryarena.ErrStale {
			t.Fatal(e)
		}
	}
	if e = <-closed; e != nil {
		t.Fatal(e)
	}
	if lease.BaseReadAliveV5() || snapshot.finalized.Load() || snapshot.registryID == 0 || snapshot.primaryRoot == nil {
		t.Fatal("Close lost pinned custody or kept proof live")
	}
	other := database.AcquireSnapshot()
	if other == snapshot {
		t.Fatal("pinned wrapper reused")
	}
	_ = other.Close()
	if ok, debt := lease.ReleasePinV5(&iterator.OrdinalScanWork{RecordLimit: 0, ByteLimit: 1 << 20}); ok || debt != nil {
		t.Fatal("unadmitted pin release")
	}
	work := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if ok, debt := lease.ReleasePinV5(work); ok || debt != nil {
		t.Fatal("transient reader inherited retirement")
	}
	snapshot.endRead()
	if snapshot.registryID == 0 || snapshot.primaryRoot == nil {
		t.Fatal("transient endRead retired native custody")
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	ok, debt := lease.ReleasePinV5(work)
	retirement = debt
	if !ok || debt != snapshot || work.Records != 1 || snapshot.finalized.Load() {
		t.Fatal("retirement was hidden or lost")
	}
	if lease.BaseReadAliveV5() {
		t.Fatal("released lease accepted")
	}
	if e = retirement.finalizeCloseIfUnreferenced(); e != nil {
		t.Fatal(e)
	}
	if snapshot.registryID != 0 || snapshot.idx != nil || snapshot.primaryRoot != nil {
		t.Fatal("explicit retirement failed")
	}
	retirement = nil
}
