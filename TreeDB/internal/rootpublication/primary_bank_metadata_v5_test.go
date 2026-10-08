package rootpublication

import (
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"path/filepath"
	"reflect"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func metadataClaim(t *testing.T, a *primaryarena.Arena, class primaryarena.Class) primaryarena.Ref {
	t.Helper()
	c, ok, e := a.PrepareClaim(class, nil)
	if !ok || e != nil {
		t.Fatalf("prepare %v %t", e, ok)
	}
	for i := 0; i < 100; i++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		r, ok, e := c.Step(w)
		if e != nil {
			t.Fatal(e)
		}
		if ok {
			return r
		}
	}
	t.Fatal("claim did not progress")
	return primaryarena.Ref{}
}
func metadataDrain(t *testing.T, a *primaryarena.Arena) {
	t.Helper()
	for i := 0; i < 100; i++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		ok, progress, e := a.ReleaseStep(w)
		if e != nil || !ok {
			t.Fatalf("release %v %t", e, ok)
		}
		if !progress {
			return
		}
	}
	t.Fatal("release did not progress")
}
func metadataSlot(t *testing.T, a *primaryarena.Arena, seq uint64) (primaryarena.Ref, []primaryarena.Ref) {
	return metadataSlotEntries(t, a, seq, nil, [1]*primaryarena.Group{})
}
func metadataSlotEntries(t *testing.T, a *primaryarena.Arena, seq uint64, entries []node.PrimaryDirectoryEntry, groups [1]*primaryarena.Group) (primaryarena.Ref, []primaryarena.Ref) {
	t.Helper()
	claim, ok, e := a.PrepareBundleClaim(nil)
	if !ok || e != nil {
		t.Fatalf("bundle prepare: %t %v", ok, e)
	}
	var bundle primaryarena.PublicationBundle
	for i := 0; i < 100; i++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		bundle, ok, e = claim.Step(w)
		if e != nil {
			t.Fatal(e)
		}
		if ok {
			break
		}
	}
	if !ok {
		t.Fatal("bundle claim did not progress")
	}
	dir := bundle.Directory
	base := primaryRootTestLeaf(2, "a", "base", 1)
	image := make([]byte, page.PageSize)
	if e := node.EncodePrimaryDirectory(image, dir.PageID, 1, node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256(base)}, entries); e != nil {
		t.Fatal(e)
	}
	manifest := metadataClaim(t, a, primaryarena.Manifest)
	m, e := NewDependencyManifestV1(nil)
	if e != nil {
		t.Fatal(e)
	}
	mr, e := m.Materialize(manifest.PageID, dependencyManifestSinkFunc(func(id uint64, image []byte) error {
		if id != manifest.PageID {
			t.Fatal("unexpected second manifest page")
		}
		ok, e := a.SealMetadata(manifest, image, nil)
		if !ok && e == nil {
			t.Fatal("manifest refused")
		}
		return e
	}))
	if e != nil {
		t.Fatal(e)
	}
	record := bundle.Record
	v := DurablePrimaryRootRecordV5{Record: DurableRootRecordV1{CommitSeq: seq, DurableSeq: seq, UserRootPageID: dir.PageID, SystemRootPageID: 3, TotalPages: 64, Freelist: freelist.GenerationRefV1{HeaderPageID: 4, GenerationID: 1, CommitSeq: 1, HighWater: 64, Digest: [32]byte{1}}, Manifest: mr, MetaProjectionDigest: [32]byte{1}}, Primary: PrimaryProjectionV5{ArenaUUID: a.UUID(), ArenaHighWater: a.Pager().PageCount(), DirectoryDigest: sha256.Sum256(image), BaseRootPageID: 2, BaseSequence: 1, DataCommitSeq: 1, BaseDigest: sha256.Sum256(base), SystemDigest: [32]byte{1}}}
	if ok, e := a.SealPublicationBundle(bundle, image, groups, func(digest [32]byte) ([]byte, error) {
		v.Primary.DirectoryDigest = digest
		recordImage, _, e := v.EncodePage(record.PageID)
		return recordImage, e
	}, nil); !ok || e != nil {
		t.Fatalf("bundle seal %t %v", ok, e)
	}
	clear(image)
	if _, e = a.Drop(dir, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(manifest, nil); e != nil {
		t.Fatal(e)
	}
	return record, []primaryarena.Ref{record, dir, manifest}
}
func TestPrimaryRootV5MetadataBanksRecoverBothIndependentSlotsAndRetainedCut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.db.primary")
	a, e := primaryarena.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.SetMetadataDecoder(PrimaryBankMetadataEdgesV5); e != nil {
		t.Fatal(e)
	}
	slotA, banksA := metadataSlot(t, a, 2)
	slotB, banksB := metadataSlot(t, a, 3)
	extent := a.Pager().PageCount()
	roots := make([]primaryarena.RecoveredRoot, 2)
	for i, r := range []primaryarena.Ref{slotA, slotB} {
		class, digest, ok, e := a.Identity(r, nil)
		if e != nil || !ok {
			t.Fatal(e)
		}
		roots[i] = primaryarena.RecoveredRoot{PageID: r.PageID, Digest: digest, Class: class}
	}
	if e = a.Pager().Sync(); e != nil {
		t.Fatal(e)
	}
	if e = a.Close(); e != nil {
		t.Fatal(e)
	}
	a, e = primaryarena.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	if e = a.SetMetadataDecoder(PrimaryBankMetadataEdgesV5); e != nil {
		t.Fatal(e)
	}
	recovered, e := a.Recover(extent, roots)
	if e != nil {
		t.Fatal(e)
	}
	// A retained physical cut independently owns the old complete record. A/B
	// slot overwrites may drop their handles while this old cut survives reuse.
	if _, e = a.Acquire(recovered[0], nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(recovered[0], nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(recovered[1], nil); e != nil {
		t.Fatal(e)
	}
	metadataDrain(t, a)
	for _, r := range banksA {
		if _, _, ok, e := a.Identity(r, nil); e != nil || !ok {
			t.Fatalf("retained cut lost bank %d: %v", r.PageID, e)
		}
	}
	for _, r := range banksB {
		if _, _, _, e := a.Identity(r, nil); e == nil {
			t.Fatalf("unowned independent slot bank %d retained", r.PageID)
		}
	}
	fresh := metadataClaim(t, a, primaryarena.Record)
	for _, r := range banksA {
		if fresh.PageID == r.PageID {
			t.Fatal("reused cut-owned metadata")
		}
	}
	if _, e = a.Drop(fresh, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(recovered[0], nil); e != nil {
		t.Fatal(e)
	}
	metadataDrain(t, a)
	for _, r := range banksA {
		if _, _, _, e := a.Identity(r, nil); e == nil {
			t.Fatalf("released cut bank %d still owned", r.PageID)
		}
	}
}

func TestPrimaryRootV5ExactReplacementBundleUsesRealCodecAndCustody(t *testing.T) {
	a, e := primaryarena.Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	if e = a.SetMetadataDecoder(PrimaryBankMetadataEdgesV5); e != nil {
		t.Fatal(e)
	}
	component := func(absent bool, rev page.EntryRevision) (primaryarena.Ref, node.PrimaryDirectoryEntry) {
		r := metadataClaim(t, a, primaryarena.Component)
		image := make([]byte, page.PageSize)
		b := node.NewBuilderWithOptions(image, page.PageTypeLeaf, node.BuilderOptions{EntryRevisions: true})
		b.SetPageID(r.PageID)
		flags := byte(0)
		value := []byte("old")
		kind := node.PrimaryPut
		if absent {
			flags = node.FlagTombstone
			value = nil
			kind = node.PrimaryAbsence
		}
		if e := b.AddLeafEntryWithRevision([]byte("a"), value, flags, page.ValuePtr{}, rev); e != nil {
			t.Fatal(e)
		}
		b.FinishNoNode()
		if ok, e := a.SealComponent(r, image, nil); !ok || e != nil {
			t.Fatal(e)
		}
		return r, node.PrimaryDirectoryEntry{Key: []byte("a"), Operand: node.PrimaryOperand{Ref: page.PageChildRef(r.PageID), Digest: sha256.Sum256(image)}, Revision: rev, Kind: kind}
	}
	original, entry := component(false, 1)
	var refs [49]primaryarena.Ref
	refs[0] = original
	g, ok, e := a.NewGroup(refs, nil)
	if !ok || e != nil {
		t.Fatal(e)
	}
	old, banks := metadataSlotEntries(t, a, 2, []node.PrimaryDirectoryEntry{entry}, [1]*primaryarena.Group{g})
	if _, e = a.DropGroup(g, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(original, nil); e != nil {
		t.Fatal(e)
	}
	oldRecord, e := a.Get(old.PageID)
	if e != nil {
		t.Fatal(e)
	}
	var oldDigest [32]byte
	copy(oldDigest[:], oldRecord[312:344])
	v, e := DecodeDurablePrimaryRootRecordV5(oldRecord, old.PageID, oldDigest)
	if e != nil {
		t.Fatal(e)
	}
	absence, replacement := component(true, 2)
	claim, ok, e := a.PrepareBundleClaim(nil)
	if !ok || e != nil {
		t.Fatal(e)
	}
	var bundle primaryarena.PublicationBundle
	for i := 0; i < 100; i++ {
		w := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		bundle, ok, e = claim.Step(w)
		if e != nil {
			t.Fatal(e)
		}
		if ok {
			break
		}
	}
	if !ok {
		t.Fatal("bundle claim stalled")
	}
	scratch := make([]byte, page.PageSize)
	encode := func(digest [32]byte) ([]byte, error) {
		v.Record.CommitSeq = 3
		v.Record.DurableSeq = 3
		v.Record.UserRootPageID = bundle.Directory.PageID
		v.Primary.DirectoryDigest = digest
		v.Primary.ArenaHighWater = a.Pager().PageCount()
		image, _, e := v.EncodePage(bundle.Record.PageID)
		return image, e
	}
	short := &iterator.OrdinalScanWork{RecordLimit: 16, ByteLimit: 1 << 20}
	if ok, e := a.SealExactReplacementBundle(bundle, banks[1], replacement, 1, scratch, encode, short); ok || e != nil {
		t.Fatalf("short admission %t %v", ok, e)
	}
	if _, _, _, e := a.Identity(bundle.Record, nil); e == nil {
		t.Fatal("underadmitted private record sealed")
	}
	work := &iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if ok, e := a.SealExactReplacementBundle(bundle, banks[1], replacement, 1, scratch, encode, work); !ok || e != nil {
		t.Fatalf("native bundle %t %v %+v", ok, e, work)
	}
	t.Logf("isolated actual Dir+V5 record bundle: %dR/%dB; cache/meta/fences/Accepted/Finish/releases excluded and whole-F UNQUALIFIED", work.Records, work.Bytes)
	clear(scratch)
	recordImage, e := a.Get(bundle.Record.PageID)
	if e != nil {
		t.Fatal(e)
	}
	var newDigest [32]byte
	copy(newDigest[:], recordImage[312:344])
	newRecord, e := DecodeDurablePrimaryRootRecordV5(recordImage, bundle.Record.PageID, newDigest)
	if e != nil {
		t.Fatal(e)
	}
	directoryImage, e := a.Get(bundle.Directory.PageID)
	if e != nil {
		t.Fatal(e)
	}
	if newRecord.Primary.DirectoryDigest != sha256.Sum256(directoryImage) {
		t.Fatal("record not bound to actual complete physical directory")
	}
	d, e := node.DecodePrimaryDirectory(directoryImage)
	if e != nil {
		t.Fatal(e)
	}
	selected, _ := d.Entry(0)
	if selected.Kind != node.PrimaryAbsence {
		t.Fatal("actual absence not installed")
	}
	if _, e = a.Drop(bundle.Directory, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(absence, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Drop(old, nil); e != nil {
		t.Fatal(e)
	}
	metadataDrain(t, a)
	if _, _, ok, e := a.Identity(bundle.Directory, nil); !ok || e != nil {
		t.Fatal("record did not independently own directory")
	}
	if _, e = a.Drop(bundle.Record, nil); e != nil {
		t.Fatal(e)
	}
	metadataDrain(t, a)
	if a.Counters().GroupsCreated != a.Counters().GroupsFreed {
		t.Fatalf("custody leaked %+v", a.Counters())
	}
}

func TestOwnedPrimaryBankMetadataScratchParityRefusalAndDisposal(t *testing.T) {
	a, err := primaryarena.Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.SetOwnedMetadataDecoder(PrimaryBankMetadataEdgesV5, PrimaryBankMetadataEdgesOwnedV5); err != nil {
		t.Fatal(err)
	}
	record, banks := metadataSlot(t, a, 2)
	owner := new(retainedalloc.Owner)
	owner.Initialize(0)
	for _, ref := range []primaryarena.Ref{record, banks[2]} {
		class, _, ok, e := a.Identity(ref, nil)
		if !ok || e != nil {
			t.Fatal(e)
		}
		image, e := a.Get(ref.PageID)
		if e != nil {
			t.Fatal(e)
		}
		generic, e := PrimaryBankMetadataEdgesV5(class, image)
		if e != nil {
			t.Fatal(e)
		}
		scratch, e := PrimaryBankMetadataEdgesOwnedV5(class, image, owner)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(generic, scratch.Edges()) || owner.Bytes() != retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryBankMetadataScratchV5{}))) {
			t.Fatal("decoder parity or exact scratch admission")
		}
		scratch.Close()
		scratch.Close()
		if owner.Bytes() != 0 {
			t.Fatal("decoder scratch retained after last use")
		}
		bad := append([]byte(nil), image...)
		bad[page.PageSize-1] ^= 1
		if _, e = PrimaryBankMetadataEdgesOwnedV5(class, bad, owner); e == nil || owner.Bytes() != 0 {
			t.Fatal("malformed decode leaked admission")
		}
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	image, err := a.Get(record.PageID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = PrimaryBankMetadataEdgesOwnedV5(primaryarena.Record, image, owner); !errors.Is(err, retainedalloc.ErrClosed) {
		t.Fatalf("closed admission %v", err)
	}
}
