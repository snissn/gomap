package db

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/pager"
)

func TestPrimaryDataParentCoverageV6ReceiptMismatchFallsBack(t *testing.T) {
	for _, name := range []string{"missing", "pager", "incarnation", "identity"} {
		t.Run(name, func(t *testing.T) {
			db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if db.durableRoot.primary.dataCompletion.pager != nil {
				t.Fatal("open minted DATA completion receipt")
			}
			if err = db.SetSync([]byte("seed"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			r := &db.durableRoot.primary.dataCompletion
			if r.pager == nil {
				t.Fatal("completed publication did not mint receipt")
			}
			switch name {
			case "missing":
				*r = primaryDataCompletionV6{}
			case "pager":
				r.pager = new(pager.Pager)
			case "incarnation":
				r.incarnation++
			case "identity":
				r.identity.ObjectID[0] ^= 1
			}
			data := 0
			restore := durabilitycut.Install(func(e durabilitycut.Event) error {
				if e.Point == durabilitycut.AfterIndexDataSync && filepath.Base(e.Path) == indexFileName {
					data++
				}
				return nil
			})
			defer restore()
			if err = db.SetSync([]byte("next"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if data != 1 {
				t.Fatalf("mismatched %s receipt DATA syncs=%d", name, data)
			}
		})
	}
}

func TestPrimaryDataParentCoverageV6ChargesActualContainingAllocations(t *testing.T) {
	if got, want := primaryRuntimeDescriptorChargeV5(), retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryDurableRuntimeV5{}))); got != want {
		t.Fatalf("runtime charge %d want %d", got, want)
	}
	want := retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(rootPublicationSealV1{}))) + retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(primaryPublicationSealV5{}))) + retainedalloc.AllocationCharge(0)
	if got := primarySealMetadataV6(0); got != want {
		t.Fatalf("seal charge %d want %d", got, want)
	}
}

func TestPrimaryDataParentCoverageV6FailureCannotMintReceipt(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := db.durableRoot.record
	injected := errors.New("DATA sync refused")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point == durabilitycut.BeforeIndexDataSync && filepath.Base(e.Path) == indexFileName {
			return injected
		}
		return nil
	})
	if err = db.SetSync([]byte("first"), []byte("value")); !errors.Is(err, injected) {
		t.Fatalf("DATA failure: %v", err)
	}
	restore()
	if db.durableRoot.record != before || db.durableRoot.primary.dataCompletion.pager != nil {
		t.Fatal("failed DATA sync minted durable authority")
	}
	if err = db.SetSync([]byte("retry"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	before = db.durableRoot.record
	completed := db.durableRoot.primary.dataCompletion
	db.testFailSyncMeta.Store(true)
	if err = db.SetSync(bytes.Repeat([]byte("z"), 3500), []byte("changed DATA")); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("ambiguous capsule failure: %v", err)
	}
	if db.durableRoot.record != before || db.durableRoot.primary.dataCompletion != completed || !db.publicationPoisoned.Load() {
		t.Fatal("ambiguous failure replaced completed parent or failed to poison")
	}
}

func TestPrimaryDataParentCoverageV6ExactOperands(t *testing.T) {
	db, err := Open(Options{Dir: t.TempDir(), ResolvedProfile: ProfileNoWALFast, IndexPrimaryDirectory: true, Durability: DurabilityWALOffRelaxed, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.SetSync([]byte("seed"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	checked := false
	stop := errors.New("stop after frozen proof inspection")
	restore := durabilitycut.Install(func(e durabilitycut.Event) error {
		if e.Point != durabilitycut.BeforeIndexDataSync || filepath.Base(e.Path) != primaryIndexFileName {
			return nil
		}
		r := db.rootPublication
		r.mu.Lock()
		defer r.mu.Unlock()
		s := r.activeSeal
		base := db.durableRoot
		if s == nil || s.primary == nil || !s.primary.dataParentCoverage.valid() || !r.primaryDataParentCoverageV6(s, base).valid() {
			return errors.New("actual bank-only seal did not capture exact parent proof")
		}
		tests := []struct {
			name   string
			change func(*rootPublicationSealV1, *durableRootRuntimeV1)
		}{
			{"system-root", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.record.SystemRootPageID++ }},
			{"generation", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.record.Freelist.GenerationID++ }},
			{"extent", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.record.TotalPages++ }},
			{"free", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.record.FreelistFreeCount++ }},
			{"retired", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.record.FreelistRetiredCount++ }},
			{"base-root", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.primary.projection.BaseRootPageID++ }},
			{"base-sequence", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.primary.projection.BaseSequence++ }},
			{"data-commit", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.primary.projection.DataCommitSeq++ }},
			{"base-digest", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.primary.projection.BaseDigest[0] ^= 1 }},
			{"system-digest", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) { s.primary.projection.SystemDigest[0] ^= 1 }},
			{"inherited-debt", func(s *rootPublicationSealV1, _ *durableRootRuntimeV1) {
				s.prefix = []*freelist.PreparedCOWCandidateV1{nil}
			}},
			{"pending-parent", func(_ *rootPublicationSealV1, b *durableRootRuntimeV1) {
				b.pending = new(durableRootPublishCandidateV1)
			}},
			{"ambiguous-parent", func(_ *rootPublicationSealV1, b *durableRootRuntimeV1) {
				b.ambiguous = []*durableRootPublishCandidateV1{nil}
			}},
			{"parent-meta", func(_ *rootPublicationSealV1, b *durableRootRuntimeV1) { b.meta.RootRecordDigest[0] ^= 1 }},
			{"parent-slot", func(_ *rootPublicationSealV1, b *durableRootRuntimeV1) { b.slot ^= 1 }},
		}
		for _, test := range tests {
			// Fresh test-only projections do not copy mutexes or acquire owners.
			copy := &rootPublicationSealV1{idx: s.idx, token: s.token, record: s.record, base: s.base,
				primary: &primaryPublicationSealV5{prepared: s.primary.prepared, projection: s.primary.projection, promotion: s.primary.promotion}}
			b := base
			test.change(copy, &b)
			if r.primaryDataParentCoverageV6(copy, b).valid() {
				return fmt.Errorf("%s mismatch accepted parent DATA coverage", test.name)
			}
		}
		checked = true
		return stop
	})
	defer restore()
	if err = db.SetSync([]byte("bank-only"), []byte("value")); !errors.Is(err, stop) {
		t.Fatalf("inspection stop: %v", err)
	}
	if !checked {
		t.Fatal("public publication did not reach proof inspection")
	}
}
