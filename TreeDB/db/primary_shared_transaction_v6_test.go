package db

import (
	"bytes"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/page"
	"testing"
)

func TestPrimarySharedTransactionV6ReusesExactDataCertificate(t *testing.T) {
	opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.SetSync([]byte("first"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	old := db.rootPublication.primaryDataCertificateV6
	if old == nil {
		t.Fatal("ordinary selected producer did not construct DATA certificate")
	}
	snapshot := db.AcquireSnapshot()
	defer snapshot.Close()
	for _, key := range []string{"second", "third", "fourth"} {
		if err = db.SetSync([]byte(key), []byte(key)); err != nil {
			t.Fatal(err)
		}
		if db.rootPublication.primaryDataCertificateV6 != old {
			t.Fatal("directory-only ordinary publication rebuilt immutable DATA authority")
		}
	}
	// This real key cannot fit beside the current cells. The ordinary zipper
	// actually materializes DATA; no fixture changes or synthetic generation.
	largeKey := bytes.Repeat([]byte("x"), 3500)
	if err = db.SetSync(largeKey, []byte("materialized")); err != nil {
		t.Fatal(err)
	}
	if db.rootPublication.primaryDataCertificateV6 == old {
		t.Fatal("changed physical DATA reused old certificate")
	}
	if value, err := snapshot.Get([]byte("first")); err != nil || string(value) != "old" {
		t.Fatalf("old independent reader lost DATA custody: %q %v", value, err)
	}
	if value, err := db.Get(largeKey); err != nil || string(value) != "materialized" {
		t.Fatalf("materialized key unreadable: %q %v", value, err)
	}
}

func TestPrimarySharedTransactionV6FreshOwnedDirectoryRefusal(t *testing.T) {
	opts := Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true}
	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.SetSync([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	idx := db.idx.Load()
	id, err := idx.zipper.CopyPrimaryRoot(db.state.Load().RootPageID)
	if err != nil {
		t.Fatal(err)
	}
	image, err := idx.primary.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(image)
	image[len(image)-1] ^= 1
	owner, ready, err := idx.primary.ReadRootConstructionOwnerV6(id, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil {
		t.Fatal("ordinary copy lost actual constructor", err)
	}
	ref := owner.PrimaryConstructionReferenceV6()
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, ready, err := idx.primary.BorrowReadRootCertificateV6(ref, work); ready || err == nil {
		t.Fatalf("corrupted current directory accepted: ready=%v err=%v", ready, err)
	}
	copy(image, original)
	short := &iterator.OrdinalScanWork{RecordLimit: 1, ByteLimit: 1 << 20}
	if _, ready, err := idx.primary.BorrowReadRootCertificateV6(ref, short); ready || err != nil {
		t.Fatalf("under-admitted claim consumed ownership: %v %v", ready, err)
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	cert, ready, err := idx.primary.BorrowReadRootCertificateV6(ref, work)
	if !ready || err != nil {
		t.Fatalf("refused validation did not retain exact constructor custody: %v %v", ready, err)
	}
	if cert.Reference().PageID != id {
		t.Fatal("certificate identifies another root")
	}
	if err := owner.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := idx.primary.BorrowReadRootCertificateV6(ref, work); ready || err == nil {
		t.Fatal("aborted constructor supplied readable authority")
	}
}

// The same actual visible member supplies the typed codec and later ordinary
// durable publication. Refusal cannot modify destination or publication state.
func TestPrimarySharedTransactionV6TypedCodecOwnedOperandAndRefusal(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.SetSync([]byte("first"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	prior := db.durableRoot.record
	if err = db.Set([]byte("second"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	runtime := db.rootPublication
	runtime.mu.Lock()
	member := runtime.visibleMember(db.State().CommitSeq)
	runtime.mu.Unlock()
	if member == nil || member.primary == nil {
		t.Fatal("relaxed ordinary producer did not retain actual visible member")
	}
	if db.durableRoot.record != prior {
		t.Fatal("relaxed ACK unexpectedly crossed capsule durability boundary")
	}
	base := db.durableRoot
	target := uint64(1) - base.slot
	parent := base.primary.capsuleViews[base.slot]
	g, next := member.primary.Generation(), member.next
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: next.CommitSeq, DurableSeq: base.record.DurableSeq + 1, UserRootPageID: next.UserRootPageID, SystemRootPageID: next.SystemRootPageID, TotalPages: g.HighWater(),
		AppliedCommandLSN: next.AppliedCommandLSN, LastCommitHeight: next.LastCommitHeight, MaxEntryRevision: next.MaxEntryRevision,
		Freelist: g.GenerationRef(), FreelistFreeCount: g.FreeCount(), FreelistRetiredCount: g.RetiredCount(),
		Manifest: base.record.Manifest, Directory: base.record.Directory,
		MetaProjectionDigest: page.DurableMetaProjectionDigestV1(next.CommitSeq, base.record.DurableSeq+1, rootpublication.PrimaryCapsulePageV6(target)),
	}
	projection := member.primary.Projection()
	projection.ArenaHighWater = db.idx.Load().primary.Pager().PageCount()
	current := rootpublication.DurablePrimaryRootRecordV5{Record: record, Primary: projection}
	dst := bytes.Repeat([]byte{0xa5}, rootpublication.PrimaryCapsuleSizeV6)
	sentinel := bytes.Clone(dst)
	corrupt := parent.CopyOwned()
	corrupt.Image()[80] ^= 1
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, ready, err := rootpublication.ConstructPrimaryCapsuleV6(dst, target, current, member.primary, &corrupt, work); ready || err == nil || !bytes.Equal(dst, sentinel) {
		t.Fatalf("bad independent parent modified output: %v %v", ready, err)
	}
	wrong := current
	wrong.Record.MaxEntryRevision++
	guardWork := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if _, ready, err := rootpublication.ConstructPrimaryCapsuleV6(dst, target, wrong, member.primary, &parent, guardWork); ready || err == nil || !bytes.Equal(dst, sentinel) {
		t.Fatal("fresh record revision was not bound to actual member")
	}
	short := &iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	if _, ready, err := rootpublication.ConstructPrimaryCapsuleV6(dst, target, current, member.primary, &parent, short); ready || err != nil || !bytes.Equal(dst, sentinel) {
		t.Fatalf("unadmitted codec modified output: %v %v", ready, err)
	}
	work = &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	view, ready, err := rootpublication.ConstructPrimaryCapsuleV6(dst, target, current, member.primary, &parent, work)
	if !ready || err != nil {
		t.Fatalf("actual typed member refused: %v %v", ready, err)
	}
	decoded, err := rootpublication.DecodePrimaryCapsuleV6(dst, target, projection.ArenaUUID)
	if err != nil || decoded.Current() != view.Current() || decoded.Parent() != view.Parent() {
		t.Fatalf("shared physical core differs from recovery codec: %v", err)
	}
	if work.Records != 5 {
		t.Fatalf("distinct typed codec operands=%d, want5", work.Records)
	}
	t.Logf("typed codec %dR/%dB; actual ordinary relaxed member, private destination refusal", work.Records, work.Bytes)
	if err = db.SetSync([]byte("third"), []byte("three")); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get([]byte("second")); err != nil || string(got) != "two" {
		t.Fatalf("ordinary durable successor lost relaxed member: %q %v", got, err)
	}
}

// The selected ordinary route borrows candidate/current/runtime descriptors from
// the same constructor transaction. Coalescing may own an independent group,
// but an uncoalesced candidate cannot invent a second publication owner.
func TestPrimarySharedTransactionV6SinglePublicationOwner(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Set([]byte("first"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	current := db.state.Load()
	member := db.rootPublication.visibleMember(current.CommitSeq)
	if member == nil {
		t.Fatal("ordinary state has no actual publication owner")
	}
	transaction := member.transaction
	if transaction == nil || transaction.PublicationProjectionV6() != member {
		t.Fatal("runtime/current have independent publication truth")
	}
	candidate := transaction.PublicationCandidateV6()
	if candidate == nil || candidate.DurableRoot() != transaction || candidate.Resources() == nil {
		t.Fatal("candidate is not the transaction-owned descriptor")
	}
	if _, err := rootpublication.NewPreparedRootCandidate(rootpublication.CandidateSpec{Frontier: candidate.Frontier(), TotalPages: candidate.TotalPages(), ResourceSet: candidate.Resources(), DurableRoot: transaction}); err == nil {
		t.Fatal("already transferred owner produced another candidate")
	}
	snapshot := db.AcquireSnapshot()
	defer snapshot.Close()
	if err = db.Set([]byte("second"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	if db.rootPublication.visibleMember(db.State().CommitSeq).transaction == transaction {
		t.Fatal("new visibility reused mutable transaction")
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if value, err := snapshot.Get([]byte("first")); err != nil || string(value) != "one" {
		t.Fatalf("old owner custody lost: %q %v", value, err)
	}
	if transaction.PublicationCandidateV6() != nil || transaction.PublicationProjectionV6() != nil {
		t.Fatal("completed transaction retains candidate/runtime publication closure")
	}
	if candidate.DurableRoot() != transaction {
		t.Fatal("borrowed candidate changed transaction identity")
	}
}
