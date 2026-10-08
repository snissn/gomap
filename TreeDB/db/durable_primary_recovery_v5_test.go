package db

import (
	"crypto/sha256"
	"errors"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"path/filepath"
	"testing"
)

func TestPrimaryV5SelectionIndependentSlotsActualDataAndParentFrontier(t *testing.T) {
	data := freelist.NewMemoryPageStoreV1()
	for _, id := range []uint64{2, 3} {
		image := make([]byte, page.PageSize)
		b := node.NewBuilder(image, page.PageTypeLeaf)
		b.SetPageID(id)
		b.FinishNoNode()
		data.Pages[id] = image
	}
	txn := freelist.NewFreelistTxn(freelist.MustNewFreelistGenerationV1(1, 64, nil, nil), freelist.NewReservationLedger())
	candidate, e := txn.MaterializeCandidate(7, 7, freelist.CandidateIDV1{7}, data)
	if e != nil {
		t.Fatal(e)
	}
	generation := candidate.Generation()
	a, e := primaryarena.Open(filepath.Join(t.TempDir(), "index.db.primary"))
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	if e = a.SetMetadataDecoder(rootpublication.PrimaryBankMetadataEdgesV5); e != nil {
		t.Fatal(e)
	}
	claim := func(class primaryarena.Class) primaryarena.Ref {
		c, ok, e := a.PrepareClaim(class, nil)
		if !ok || e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 100; i++ {
			r, ok, e := c.Step(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if e != nil {
				t.Fatal(e)
			}
			if ok {
				return r
			}
		}
		t.Fatal("claim failed bounded progress")
		return primaryarena.Ref{}
	}
	makeSlot := func(slot, seq, durable, lsn uint64, parent page.DurableMetaV1) (page.DurableMetaV1, primaryarena.PublicationBundle, rootpublication.DependencyManifestRefV1) {
		c, ok, e := a.PrepareBundleClaim(nil)
		if !ok || e != nil {
			t.Fatal(e)
		}
		var bundle primaryarena.PublicationBundle
		for i := 0; i < 100; i++ {
			bundle, ok, e = c.Step(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20})
			if e != nil {
				t.Fatal(e)
			}
			if ok {
				break
			}
		}
		if !ok {
			t.Fatal("bundle failed bounded progress")
		}
		image := make([]byte, page.PageSize)
		baseDigest := sha256.Sum256(data.Pages[2])
		if e = node.EncodePrimaryDirectory(image, bundle.Directory.PageID, 1, node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: baseDigest}, nil); e != nil {
			t.Fatal(e)
		}
		m, e := rootpublication.NewDependencyManifestV1(nil)
		if e != nil {
			t.Fatal(e)
		}
		manifest := claim(primaryarena.Manifest)
		mr, e := m.Materialize(manifest.PageID, primaryTestManifestSink(func(id uint64, image []byte) error {
			if id != manifest.PageID {
				t.Fatal("multi-page empty manifest")
			}
			ok, e := a.SealMetadata(manifest, image, nil)
			if !ok && e == nil {
				t.Fatal("metadata admission")
			}
			return e
		}))
		if e != nil {
			t.Fatal(e)
		}
		r := rootpublication.DurableRootRecordV1{CommitSeq: seq, DurableSeq: durable, UserRootPageID: bundle.Directory.PageID, SystemRootPageID: 3, TotalPages: generation.HighWater(), AppliedCommandLSN: lsn, Freelist: generation.GenerationRef(), FreelistFreeCount: generation.FreeCount(), FreelistRetiredCount: generation.RetiredCount(), Manifest: mr, MetaProjectionDigest: page.DurableMetaProjectionDigestV1(seq, durable, bundle.Record.PageID), ParentRecordPageID: parent.RootRecordPageID, ParentCommitSeq: parent.CommitSeq, ParentRecordDigest: parent.RootRecordDigest}
		v := rootpublication.DurablePrimaryRootRecordV5{Record: r, Primary: rootpublication.PrimaryProjectionV5{ArenaUUID: a.UUID(), ArenaHighWater: a.Pager().PageCount(), DirectoryDigest: sha256.Sum256(image), BaseRootPageID: 2, BaseSequence: 1, DataCommitSeq: generation.CommitSeq(), BaseDigest: baseDigest, SystemDigest: sha256.Sum256(data.Pages[3])}}
		var digest [32]byte
		ok, e = a.SealPublicationBundle(bundle, image, [1]*primaryarena.Group{}, func(dirDigest [32]byte) ([]byte, error) {
			v.Primary.DirectoryDigest = dirDigest
			image, d, e := v.EncodePage(bundle.Record.PageID)
			digest = d
			return image, e
		}, nil)
		if !ok || e != nil {
			t.Fatal(e)
		}
		meta, e := page.NewDurableMetaV1(seq, durable, bundle.Record.PageID, digest)
		if e != nil {
			t.Fatal(e)
		}
		metaImage := make([]byte, page.PageSize)
		header := page.PageHeader{PageID: slot, Flags: uint16(page.PageTypeMeta)}
		header.Encode(metaImage)
		if e = meta.Encode(metaImage[page.PageHeaderSize:]); e != nil {
			t.Fatal(e)
		}
		page.UpdateChecksum(metaImage)
		data.Pages[slot] = metaImage
		if _, e = a.Drop(bundle.Directory, nil); e != nil {
			t.Fatal(e)
		}
		if _, e = a.Drop(manifest, nil); e != nil {
			t.Fatal(e)
		}
		return meta, bundle, mr
	}
	old, _, _ := makeSlot(0, 9, 1, 7, page.DurableMetaV1{})
	_, _, newManifest := makeSlot(1, 10, 2, 8, old)
	selected, e := selectDurablePrimaryRootV5(data, a.Pager(), generation.HighWater(), a.Pager().PageCount(), a.UUID(), nil, nil)
	if e != nil || selected.Slot != 1 || selected.SlotCommits != [2]uint64{9, 10} || selected.Primary.DataCommitSeq != 7 {
		t.Fatalf("independent V5 selection: %+v %v", selected, e)
	}
	var metadata retainedalloc.Owner
	metadata.Initialize(0)
	visits := 0
	checkManifest := func(m *rootpublication.DependencyManifestV1) (*rootpublication.StableResourceSet, error) {
		visits++
		if m.MetadataOwnerV1() != &metadata || metadata.Bytes() == 0 || m.DiagnosticProjectionV1() != nil {
			t.Fatal("selected V5 manifest escaped admission")
		}
		return nil, m.WithEntriesV1(func(entries []rootpublication.DependencyManifestEntryV1) error {
			if len(entries) != 0 {
				t.Fatal("unexpected fixture obligations")
			}
			return nil
		})
	}
	owned, e := selectDurablePrimaryRootWithMetadataV5(data, a.Pager(), generation.HighWater(), a.Pager().PageCount(), a.UUID(), checkManifest, nil, &metadata)
	if e != nil || owned.SlotCommits != selected.SlotCommits || owned.Manifest != nil || metadata.Bytes() != 0 || visits != 3 {
		t.Fatalf("admitted V5 closure: commits=%v visits=%d remaining=%d err=%v", owned.SlotCommits, visits, metadata.Bytes(), e)
	}
	var fixed [4]primaryarena.RecoveredRoot
	if _, _, e = recoveredPrimaryRootsIntoV5(selected, fixed[:0:3]); !errors.Is(e, rootpublication.ErrResourceOwnership) {
		t.Fatal("refused root capacity", e)
	}
	extent, roots := recoveredPrimaryRootsV5(selected)
	if extent == 0 || len(roots) != 3 || roots[2].PageID != old.RootRecordPageID {
		t.Fatalf("finite independent proof custody: %d %+v", extent, roots)
	}
	// Parent validation is one hop only. The selected complete directory is never
	// reconstructed from a parent record or from current mutable arena state.
	if _, e = selectDurableRootV1(data, generation.HighWater(), nil); !errors.Is(e, ErrNoRecoverableMeta) {
		t.Fatal("strict V1 selector accepted namespaced V5 authority", e)
	}
	// Corrupt only the newest manifest physical bank; the older slot is complete
	// independently, including its own full manifest and actual DATA generation.
	corrupt, e := a.Pager().GetForWrite(primaryarena.Local(newManifest.FirstPageID))
	if e != nil {
		t.Fatal(e)
	}
	corrupt[80] ^= 1
	page.UpdateChecksum(corrupt)
	selected, e = selectDurablePrimaryRootV5(data, a.Pager(), generation.HighWater(), a.Pager().PageCount(), a.UUID(), nil, nil)
	if e != nil || selected.Slot != 0 || selected.SlotCommits != [2]uint64{9, 0} {
		t.Fatalf("newest corruption fallback: %+v %v", selected, e)
	}
	owned, e = selectDurablePrimaryRootWithMetadataV5(data, a.Pager(), generation.HighWater(), a.Pager().PageCount(), a.UUID(), checkManifest, nil, &metadata)
	if e != nil || owned.Slot != 0 || owned.SlotCommits != selected.SlotCommits || metadata.Bytes() != 0 {
		t.Fatalf("owned V5 fallback retained failed slot: %v %d %v", owned.SlotCommits, metadata.Bytes(), e)
	}
	if _, e = selectDurablePrimaryRootV5(data, a.Pager(), generation.HighWater(), a.Pager().PageCount(), [16]byte{42}, nil, nil); !errors.Is(e, ErrNoRecoverableMeta) {
		t.Fatal("foreign arena admitted", e)
	}
	data.Pages[2] = append([]byte(nil), data.Pages[2]...)
	data.Pages[2][200] ^= 1
	page.UpdateChecksum(data.Pages[2])
	if _, e = selectDurablePrimaryRootV5(data, a.Pager(), generation.HighWater(), a.Pager().PageCount(), a.UUID(), nil, nil); !errors.Is(e, ErrNoRecoverableMeta) {
		t.Fatal("changed actual DATA base admitted", e)
	}
}

type primaryTestManifestSink func(uint64, []byte) error

func (f primaryTestManifestSink) WritePage(id uint64, image []byte) error { return f(id, image) }
