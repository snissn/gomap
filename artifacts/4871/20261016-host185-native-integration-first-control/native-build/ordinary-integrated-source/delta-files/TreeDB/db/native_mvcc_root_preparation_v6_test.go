package db

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/primaryarena"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

// Tiny original primitive fixture: one real canonical source root, one empty
// group, and no DB/allocator/coordinator/publication. Ordinary fixture setup
// establishes the original held edge; every tested cleanup step is finite.
type nativeSourceWitnessFixtureV6 struct {
	arena *primaryarena.Arena
	root  *primaryStateRootV5 // owns the fixture's independent original edge
	prep  *nativeMVCCRootPreparationV6
}

func newNativeSourceWitnessFixtureV6(t *testing.T, storage ...*nativeMVCCRootPreparationV6) *nativeSourceWitnessFixtureV6 {
	t.Helper()
	a, err := primaryarena.OpenCapsule(filepath.Join(t.TempDir(), "primary"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	r, ready, err := a.PrepareReadRootV6(nil)
	if !ready || err != nil {
		t.Fatal("original root constructor", ready, err)
	}
	group, ready, err := a.NewGroup([node.PrimaryDirectoryMaxEntries]primaryarena.Ref{}, nil)
	if !ready || err != nil {
		t.Fatal("original group", ready, err)
	}
	var image [page.PageSize]byte
	base := node.PrimaryOperand{Ref: page.PageChildRef(2), Digest: sha256.Sum256([]byte("source DATA"))}
	if err := node.EncodePrimaryDirectory(image[:], r.PageID, 1, base, nil); err != nil {
		t.Fatal(err)
	}
	if ready, err := a.SealDirectory(r, image[:], [1]*primaryarena.Group{group}, nil); !ready || err != nil {
		t.Fatal("original canonical seal", ready, err)
	}
	cert, ready, err := a.TakeReadRootCertificateV6(r.PageID, nil)
	if !ready || err != nil {
		t.Fatal("original canonical certificate", ready, err)
	}
	if ready, err := a.Acquire(r, nativeSourceWorkV6(32)); !ready || err != nil {
		t.Fatal("SAME source edge", ready, err)
	}
	f := &nativeSourceWitnessFixtureV6{arena: a, root: &primaryStateRootV5{arena: a, ref: r, certificate: cert}, prep: new(nativeMVCCRootPreparationV6)}
	if len(storage) != 0 {
		f.prep = storage[0]
	}
	f.prep.arena, f.prep.source, f.prep.sourceCertificate = a, r, cert
	f.prep.sourceDirectory, f.prep.group, f.prep.phase = cert.Directory(), group, 5
	f.prep.entries[0] = node.PrimaryDirectoryEntry{Key: []byte("scratch"), Kind: node.PrimaryAbsence}
	f.prep.cells[0] = r
	f.prep.image[0] = 1
	a.InitializeReleaseQueue(&f.prep.queue)
	return f
}

func nativeSourceWorkV6(records uint64) *iterator.OrdinalScanWork {
	return &iterator.OrdinalScanWork{RecordLimit: records, ByteLimit: 1 << 20}
}

func nativeSourcePreparedV6(t *testing.T, f *nativeSourceWitnessFixtureV6) {
	t.Helper()
	for i := 0; i < 32; i++ {
		w := nativeSourceWorkV6(32)
		ready, err := f.prep.closeScratchStep(w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("scratch cleanup", ready, err, w)
		}
		if ready {
			if f.prep.phase != 7 || !f.prep.matchesCurrentRoot(f.root) {
				t.Fatal("prepared source witness was disposed")
			}
			return
		}
	}
	t.Fatal("tiny scratch cleanup did not progress")
}

func nativeSourceCloseV6(t *testing.T, s *nativeMVCCRootPreparationV6) {
	t.Helper()
	for i := 0; i < 128; i++ {
		w := nativeSourceWorkV6(32)
		ready, err := s.closeStep(w)
		if err != nil || w.Records > 32 || w.Bytes > 1<<20 {
			t.Fatal("source terminal", ready, err, w)
		}
		if ready {
			if s.phase != 0 && s.phase != 10 || s.arena != nil || s.source != (primaryarena.Ref{}) || s.sourceCertificate != nil || s.group != nil {
				t.Fatal("completed source retained an owning/borrowed alias")
			}
			return
		}
	}
	t.Fatal("tiny source terminal did not progress")
}

func (f *nativeSourceWitnessFixtureV6) dropHeldRoot(t *testing.T) {
	t.Helper()
	var q primaryarena.ReleaseQueue
	f.arena.InitializeReleaseQueue(&q)
	r := f.root.ref
	f.root = nil // End the fixture's canonical certificate borrow first.
	if ready, err := f.arena.DropToReleaseQueue(r, &q, nativeSourceWorkV6(32)); !ready || err != nil {
		t.Fatal("independent fixture edge", ready, err)
	}
	for i := 0; i < 128; i++ {
		ready, progress, err := f.arena.ReleaseQueueStep(&q, nativeSourceWorkV6(32))
		if err != nil {
			t.Fatal(err)
		}
		if ready && !progress {
			return
		}
	}
	t.Fatal("fixture edge did not drain")
}

func TestNativeRootPreparationSourceWitnessV6(t *testing.T) {
	f := newNativeSourceWitnessFixtureV6(t)
	before, charge := f.arena.Counters(), f.arena.MetadataOwner().Bytes()
	if ready, err := f.prep.closeScratchStep(nativeSourceWorkV6(0)); ready || err != nil || f.prep.phase != 5 || f.prep.sourceCertificate == nil {
		t.Fatal("scratch refusal changed witness", ready, err)
	}
	if ready, err := f.prep.closeScratchStep(nativeSourceWorkV6(1)); ready || err != nil || f.prep.phase != 6 || f.prep.group == nil || f.prep.sourceCertificate == nil {
		t.Fatal("partial scratch lost source/group", ready, err)
	}
	nativeSourcePreparedV6(t, f)
	if f.prep.entries[0].Key != nil || f.prep.cells[0] != (primaryarena.Ref{}) || f.prep.image[0] != 0 || f.prep.sourceDirectory.Count() != 0 || f.prep.group != nil || f.arena.Counters().RootDrops != before.RootDrops || f.arena.MetadataOwner().Bytes() != charge {
		t.Fatal("preparation dropped/refunded source or retained scratch")
	}
	// Exercise the actual shared predicate, including distinct nonnil operands.
	valid := *f.root
	if f.prep.matchesCurrentRoot(nil) {
		t.Fatal("nil current root accepted")
	}
	for _, change := range []func(*primaryStateRootV5){
		func(r *primaryStateRootV5) { r.arena = nil },
		func(r *primaryStateRootV5) { r.arena = new(primaryarena.Arena) },
		func(r *primaryStateRootV5) { r.ref.PageID++ },
		func(r *primaryStateRootV5) { r.ref.Incarnation++ },
		func(r *primaryStateRootV5) { r.certificate = nil },
		func(r *primaryStateRootV5) { r.certificate = new(primaryarena.ReadRootCertificateV6) },
	} {
		wrong := valid
		change(&wrong)
		if f.prep.matchesCurrentRoot(&wrong) {
			t.Fatal("source identity mismatch accepted")
		}
	}
	f.prep.phase = 6
	if f.prep.matchesCurrentRoot(f.root) {
		t.Fatal("unprepared witness accepted")
	}
	f.prep.phase = 7
	// Initial actual final-basis refusal includes the SAME comparison price.
	r, b := nativeRootSourceWitnessWorkV6()
	pr, pb := nativeCapsuleParentRecordWorkV6()
	if r != 2 || b != 2*uint64(unsafe.Sizeof(primaryStateRootV5{}))+2*uint64(unsafe.Sizeof(primaryarena.Ref{}))+32+1 {
		t.Fatal("source witness price differs", r, b)
	}
	c := NativeMVCCCurrentDeleteV6{}
	for _, w := range []iterator.OrdinalScanWork{{RecordLimit: 4 + pr + r - 1, ByteLimit: 1 << 20}, {RecordLimit: 32, ByteLimit: pb + b - 1}} {
		before := w
		if _, ready, err := c.prepareNativeSealInstallationV6(&w); ready || err != nil || w != before {
			t.Fatal("unadmitted comparison inspected empty producer", ready, err, w)
		}
	}
	valid.certificate = nil // End the copied test-only borrow before disposal.
	nativeSourceCloseV6(t, f.prep)
	if f.root.arena.Pager().PrimaryReadRootOwnerV6(primaryarena.Local(f.root.ref.PageID)) == nil {
		t.Fatal("source terminal consumed independent held root")
	}
	f.dropHeldRoot(t)
	t.Logf("source witness records=%d bytes=%d; comparison delta only, before adoption; original acquired edge retained", r, b)
}

func TestNativeRootPreparationSourceTerminalV6(t *testing.T) {
	t.Run("before-acquire", func(t *testing.T) {
		s := nativeMVCCRootPreparationV6{}
		nativeSourceCloseV6(t, &s)
	})
	for _, held := range []bool{false, true} {
		name := "last-owner"
		if held {
			name = "independent-held-owner"
		}
		t.Run(name, func(t *testing.T) {
			f := newNativeSourceWitnessFixtureV6(t)
			nativeSourcePreparedV6(t, f)
			if !held {
				f.dropHeldRoot(t)
			}
			charge, counters := f.arena.MetadataOwner().Bytes(), f.arena.Counters()
			ref := f.prep.source
			if ready, err := f.prep.closeStep(nativeSourceWorkV6(0)); ready || err != nil || f.prep.sourceCertificate == nil || f.prep.source != ref {
				t.Fatal("terminal refusal changed source", ready, err)
			}
			if ready, err := f.prep.closeStep(nativeSourceWorkV6(1)); ready || err != nil || f.prep.phase != 8 || f.prep.sourceCertificate != nil || f.prep.source != ref || f.arena.MetadataOwner().Bytes() != charge {
				t.Fatal("source alias scrub did not precede Drop", ready, err)
			}
			if ready, err := f.prep.closeStep(nativeSourceWorkV6(1)); ready || err != nil || f.prep.phase != 8 || f.prep.source != ref || f.arena.Counters().RootDrops != counters.RootDrops {
				t.Fatal("under-admitted original Drop changed custody", ready, err)
			}
			// Control(1), Drop(2), actual table header+one original link(2).
			if ready, err := f.prep.closeStep(nativeSourceWorkV6(5)); ready || err != nil || f.prep.phase != 9 || f.prep.source != (primaryarena.Ref{}) || f.arena.Counters().RootDrops != counters.RootDrops+1 || f.arena.MetadataOwner().Bytes() != charge {
				t.Fatal("post-Drop refusal lost SAME queue/charge", ready, err, f.prep.phase)
			}
			// An ordinary empty queue cannot assist this native source queue.
			freed := f.arena.Counters().BanksFreed
			if ready, progress, err := f.arena.ReleaseStep(nativeSourceWorkV6(32)); !ready || progress || err != nil || f.arena.Counters().BanksFreed != freed {
				t.Fatal("ordinary release stole native work", ready, progress, err)
			}
			if ready, err := f.prep.closeStep(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1}); ready || err != nil || f.prep.phase != 9 || f.arena.MetadataOwner().Bytes() != charge {
				t.Fatal("byte refusal advanced native queue", ready, err)
			}
			nativeSourceCloseV6(t, f.prep)
			if f.arena.Counters().RootDrops != counters.RootDrops+1 {
				t.Fatal("source Drop replayed")
			}
			after, bytes := f.arena.Counters(), f.arena.MetadataOwner().Bytes()
			if held && (after.BanksFreed != counters.BanksFreed || bytes != charge) || !held && (after.BanksFreed != counters.BanksFreed+1 || bytes >= charge) {
				t.Fatal("held/final owner release or refund differs", held, bytes, charge)
			}
			if ready, err := f.prep.closeStep(nativeSourceWorkV6(0)); !ready || err != nil || f.arena.Counters() != after || f.arena.MetadataOwner().Bytes() != bytes {
				t.Fatal("completed source disposal replayed", ready, err)
			}
			if held {
				f.dropHeldRoot(t)
			} else if _, _, err := f.arena.BorrowReadRootCertificateV6(ref, nativeSourceWorkV6(32)); !errors.Is(err, primaryarena.ErrStale) {
				t.Fatal("released source bank revived", err)
			}
		})
	}
	t.Run("partial-scratch-cancel-and-refund", func(t *testing.T) {
		f := newNativeSourceWitnessFixtureV6(t)
		budget, err := memtable.NewCOWBudget(memtable.DefaultCOWLimits())
		if err != nil {
			t.Fatal(err)
		}
		enrollment, err := f.arena.MetadataOwner().Enroll(budget)
		if err != nil {
			t.Fatal(err)
		}
		defer enrollment.Close() // ordinary fixture teardown, after tested native work
		f.dropHeldRoot(t)
		if ready, err := f.prep.closeScratchStep(nativeSourceWorkV6(1)); ready || err != nil || f.prep.phase != 6 {
			t.Fatal("partial scratch setup", ready, err)
		}
		for i := 0; i < 128 && f.arena.Counters().BanksFreed == 0; i++ {
			w := nativeSourceWorkV6(14)
			if ready, err := f.prep.closeStep(w); ready || err != nil {
				t.Fatal("bounded physical release/refund position", ready, err)
			}
		}
		if f.arena.Counters().BanksFreed != 1 || f.prep.phase != 9 || f.prep.arena == nil {
			t.Fatal("last-owner physical detach did not retain original refund frame")
		}
		charge, enrolled := f.arena.MetadataOwner().Bytes(), budget.Stats().ExternalBytes
		if ready, err := f.prep.closeStep(nativeSourceWorkV6(0)); ready || err != nil || f.arena.MetadataOwner().Bytes() != charge || budget.Stats().ExternalBytes != enrolled {
			t.Fatal("pending refund refusal lost charge/custody", ready, err)
		}
		nativeSourceCloseV6(t, f.prep)
		if f.arena.MetadataOwner().Bytes() >= charge || budget.Stats().ExternalBytes >= enrolled {
			t.Fatal("original typed last-owner refund did not complete")
		}
		if f.arena.Counters().BanksFreed != 1 || budget.Stats().ExternalBytes == 0 {
			t.Fatal("native last-owner accounting lost original owner baseline")
		}
	})
}

func TestNativePublishedRootPreparationDrainV6(t *testing.T) {
	c := NativeMVCCCurrentDeleteV6{phase: 8}
	// Construct the original source frame directly in its actual containing
	// producer. Its SAME initialized queue is never copied or replaced.
	f := newNativeSourceWitnessFixtureV6(t, &c.rootPreparation)
	nativeSourcePreparedV6(t, f)
	before, charge := f.arena.Counters(), f.arena.MetadataOwner().Bytes()
	if ready, err := c.closePublishedNativeStepV6(nativeSourceWorkV6(32)); ready || !errors.Is(err, rootpublication.ErrDurableRootOwnership) || c.phase != 8 || !c.rootPreparation.matchesCurrentRoot(f.root) || f.arena.Counters() != before || f.arena.MetadataOwner().Bytes() != charge {
		t.Fatal("actual receipt refusal disposed source/frame", ready, err)
	}
	// Explicitly synthetic post-receipt position: successful receipt authority
	// is NOT constructed/executed. Source transition 8->9 is reviewed separately.
	c.phase = 9
	if ready, err := c.closePublishedNativeStepV6(nativeSourceWorkV6(1)); ready || err != nil || c.phase != 9 || c.rootPreparation.phase != 8 || c.rootPreparation.source == (primaryarena.Ref{}) || f.arena.Counters().RootDrops != before.RootDrops {
		t.Fatal("phase9 source cleanup did not retain refusal cursor", ready, err)
	}
	nativeSourceCloseV6(t, &c.rootPreparation)
	if c.rootPreparation.arena != nil || c.rootPreparation.sourceCertificate != nil || f.arena.Counters().RootDrops != before.RootDrops+1 {
		t.Fatal("synthetic post-receipt source cleanup failed")
	}
	f.dropHeldRoot(t)
	t.Log("actual unfinished-receipt refusal preserves root; positive receipt-to-root ordering is source-reviewed only")
}
