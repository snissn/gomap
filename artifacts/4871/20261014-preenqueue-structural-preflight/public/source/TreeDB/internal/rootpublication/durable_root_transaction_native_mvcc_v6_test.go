package rootpublication

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"path/filepath"
	"testing"
	"unsafe"
)

func nativeAbsenceConstructor(t *testing.T, a *primaryarena.Arena) *DurableRootTransaction {
	t.Helper()
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	owner, ready, err := NewPrimaryConstructionTransactionV6(a, w)
	if !ready || err != nil {
		t.Fatalf("constructor: ready=%t err=%v", ready, err)
	}
	txn := owner.(*DurableRootTransaction)
	t.Cleanup(func() {
		if err := txn.Abort(); err != nil {
			t.Error(err)
		}
	})
	return txn
}

// This controls current-directory composition and constructor custody only;
// it does not certify selected DATA traversal or native physical Finish.
func TestNativeMVCCBaseAbsenceCurrentConstructorV6(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	baseImage := primaryRootTestLeaf(2, "selected", "old", 1)
	base := node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256(baseImage)}
	makeCurrent := func(revision page.EntryRevision) *DurableRootTransaction {
		txn := nativeAbsenceConstructor(t, a)
		image := make([]byte, page.PageSize)
		if err := node.EncodePrimaryDirectory(image, txn.PrimaryConstructionReferenceV6().PageID, 7, base, []node.PrimaryDirectoryEntry{{Key: []byte("future"), Revision: revision, Kind: node.PrimaryAbsence}}); err != nil {
			t.Fatal(err)
		}
		w := &iterator.OrdinalScanWork{RecordLimit: 100, ByteLimit: 1 << 20}
		if ready, err := txn.SealPrimaryConstructionV6(image, [1]*primaryarena.Group{}, w); !ready || err != nil {
			t.Fatalf("ordinary source: %t %v", ready, err)
		}
		return txn
	}
	oldCurrent := makeCurrent(100)
	newCurrent := makeCurrent(101)
	txn := nativeAbsenceConstructor(t, a)
	var scratch [page.PageSize]byte
	entry := node.PrimaryDirectoryEntry{Key: []byte("selected"), Revision: 200, Kind: node.PrimaryAbsence}
	before := a.MetadataOwner().Bytes()
	counts := a.Counters()
	refuse := &iterator.OrdinalScanWork{RecordLimit: 4, ByteLimit: 1 << 20}
	if ready, err := txn.SealPrimaryBaseAbsenceV6(newCurrent.PrimaryConstructionReferenceV6(), entry, base, 7, scratch[:], refuse); ready || err != nil {
		t.Fatalf("refusal: %t %v", ready, err)
	}
	if a.MetadataOwner().Bytes() != before || a.Counters() != counts || txn.PrimaryConstructionCertificateV6() != nil {
		t.Fatal("refusal changed physical custody")
	}
	w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ready, err := txn.SealPrimaryBaseAbsenceV6(newCurrent.PrimaryConstructionReferenceV6(), entry, base, 7, scratch[:], w); !ready || err != nil {
		t.Fatalf("fresh composition: %t %v records=%d bytes=%d", ready, err, w.Records, w.Bytes)
	}
	cert := txn.PrimaryConstructionCertificateV6()
	if cert == nil || cert.Reference() != txn.PrimaryConstructionReferenceV6() {
		t.Fatal("output changed original constructor")
	}
	future, found := cert.Directory().Search([]byte("future"))
	if !found || future.Revision != 101 {
		t.Fatal("prepared output lost fresh future write")
	}
	selected, found := cert.Directory().Search(entry.Key)
	if !found || !selected.InlineAbsence() {
		t.Fatal("absence not appended")
	}
	old, _ := oldCurrent.PrimaryConstructionCertificateV6().Directory().Search([]byte("future"))
	if old.Revision != 100 {
		t.Fatal("old reader image mutated")
	}
	clear(scratch[:])
	if !page.VerifyChecksumNonMutating(cert.Image()) || cert.Digest() != sha256.Sum256(cert.Image()) {
		t.Fatal("certificate borrowed scratch")
	}
	owner, ready, err := a.ReadRootConstructionOwnerV6(cert.Reference().PageID, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil || owner != txn {
		t.Fatal("original bank lost exact constructor owner")
	}
	if bytes.Equal(cert.Image(), newCurrent.PrimaryConstructionCertificateV6().Image()) {
		t.Fatal("output did not encode absence")
	}
	badTxn := nativeAbsenceConstructor(t, a)
	bad := base
	bad.Digest[0] ^= 1
	if ready, err := badTxn.SealPrimaryBaseAbsenceV6(newCurrent.PrimaryConstructionReferenceV6(), entry, bad, 7, scratch[:], &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || !errors.Is(err, primaryarena.ErrStale) {
		t.Fatalf("wrong DATA base: %t %v", ready, err)
	}
	duplicate := nativeAbsenceConstructor(t, a)
	entry.Key = []byte("future")
	if ready, err := duplicate.SealPrimaryBaseAbsenceV6(newCurrent.PrimaryConstructionReferenceV6(), entry, base, 7, scratch[:], &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || !errors.Is(err, node.ErrPrimaryDirectory) {
		t.Fatalf("nonnegative current lookup: %t %v", ready, err)
	}
}

func TestNativeMVCCPrivateConstructorPhysicalAbortV6(t *testing.T) {
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	owner, ready, err := NewNativeMVCCPrimaryConstructionTransactionV6(a, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
	if !ready || err != nil {
		t.Fatalf("constructor %t %v", ready, err)
	}
	txn := owner.(*DurableRootTransaction)
	ref := txn.PrimaryConstructionReferenceV6()
	constructorBytes := a.MetadataOwner().Bytes()
	if ready, err := txn.PreparePrimaryPromotionStorageV6(&iterator.OrdinalScanWork{}); ready || err != nil || txn.PrimaryPromotionV6() != nil || a.MetadataOwner().Bytes() != constructorBytes {
		t.Fatalf("promotion storage pre-effect refusal: %t %v", ready, err)
	}
	if ready, err := txn.PreparePrimaryPromotionStorageV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
		t.Fatalf("private promotion storage: %t %v", ready, err)
	}
	promotion := txn.PrimaryPromotionV6()
	if promotion == nil || len(promotion.Image) != PrimaryCapsuleSizeV6 || promotion.Root != (primaryarena.Ref{}) || promotion.Parent != (primaryarena.Ref{}) || a.MetadataOwner().Bytes() <= constructorBytes {
		t.Fatal("private storage was not admitted or selected publication authority")
	}
	promotion = nil
	before := a.MetadataOwner().Bytes()
	if ready, err := txn.PreparePrimaryPromotionStorageV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil || a.MetadataOwner().Bytes() != before {
		t.Fatalf("private storage replay: %t %v", ready, err)
	}
	counts := a.Counters()
	var receipt PrimaryConstructionReleaseWork
	if ready, err := receipt.Bind(txn, &iterator.OrdinalScanWork{}); ready || err != nil || txn.constructionV6.cleanupPhase != 0 || a.Counters() != counts {
		t.Fatalf("before-effect refusal %t %v", ready, err)
	}
	if err := txn.Abort(); !errors.Is(err, ErrNativeMVCCRootWorkRequiredV6) || a.Counters() != counts {
		t.Fatalf("ordinary callback drain %v", err)
	}
	budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
	if err != nil {
		t.Fatal(err)
	}
	admission, err := budget.AcquireExternal(memtable.COWAllocationCharge(uint64(unsafe.Sizeof(receipt))))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { admission.Close(); budget.Close() }()
	if ready, err := receipt.Bind(txn, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); !ready || err != nil {
		t.Fatal("external receipt bind", err)
	}
	txn = nil
	// Admit exactly observation and root-drop. The actual physical bank and
	// containing transaction remain retained when the queue cannot progress.
	w := &iterator.OrdinalScanWork{RecordLimit: 3, ByteLimit: 1 << 20}
	if ready, err := receipt.ContinueWork(w); ready || err != nil || receipt.transaction.constructionV6 == nil || receipt.transaction.constructionV6.root != (primaryarena.Ref{}) {
		t.Fatalf("paused physical cleanup %t %v", ready, err)
	}
	if a.MetadataOwner().Bytes() != before || a.Counters().BanksFreed != counts.BanksFreed {
		t.Fatal("root consumption refunded unfinished physical custody")
	}
	if ready, err := a.ProgressOrdinaryRelease(32, &iterator.OrdinalScanWork{RecordLimit: 256, ByteLimit: 1 << 20}); !ready || err != nil || a.MetadataOwner().Bytes() != before || a.Counters().BanksFreed != counts.BanksFreed {
		t.Fatalf("ordinary release assisted native queue %t %v", ready, err)
	}
	completed := false
	for turn := 0; turn < 32 && !completed; turn++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		completed, err = receipt.ContinueWork(w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatalf("owned physical drain %t %v %+v", completed, err, w)
		}
	}
	if !completed || receipt.transaction != nil || a.MetadataOwner().Bytes() >= before || a.Counters().BanksFreed != counts.BanksFreed+1 {
		t.Fatal("native constructor did not complete actual physical closure")
	}
	if _, ready, err := a.ReadRootConstructionOwnerV6(ref.PageID, &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || err == nil {
		t.Fatal("freed physical constructor remained discoverable")
	}
	if ready, err := receipt.ContinueWork(&iterator.OrdinalScanWork{}); !ready || err != nil {
		t.Fatalf("terminal replay %t %v", ready, err)
	}
}
