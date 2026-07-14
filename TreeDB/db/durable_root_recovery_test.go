package db

import (
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type durableRootFixtureV1 struct {
	store      *freelist.MemoryPageStoreV1
	generation *freelist.FreelistGenerationV1
	nextCommit uint64
}

func newDurableRootFixtureV1(t *testing.T) *durableRootFixtureV1 {
	t.Helper()
	store := freelist.NewMemoryPageStoreV1()
	for _, pageID := range []uint64{2, 3} {
		image := make([]byte, page.PageSize)
		builder := node.NewBuilder(image, page.PageTypeLeaf)
		builder.SetPageID(pageID)
		builder.Finish()
		if err := store.WritePage(pageID, image); err != nil {
			t.Fatal(err)
		}
	}
	return &durableRootFixtureV1{
		store:      store,
		generation: freelist.MustNewFreelistGenerationV1(1, 4, nil, nil),
		nextCommit: 1,
	}
}

func (fixture *durableRootFixtureV1) addCandidate(t *testing.T, slot uint64) durableRootSelectionV1 {
	t.Helper()
	commitSeq := fixture.nextCommit
	fixture.nextCommit++
	ledger := freelist.NewReservationLedger()
	txn, err := freelist.BeginCandidateV1(fixture.generation, fixture.generation.GenerationRef(), ledger)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := rootpublication.NewDependencyManifestV1(nil)
	if err != nil {
		t.Fatal(err)
	}
	manifestPageID, err := txn.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	recordPageID, err := txn.Allocate(0)
	if err != nil {
		t.Fatal(err)
	}
	var candidateID freelist.CandidateIDV1
	candidateID[0] = byte(commitSeq)
	candidate, err := txn.MaterializeCandidate(commitSeq, commitSeq, candidateID, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	manifestRef, err := manifest.Materialize(manifestPageID, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: commitSeq, DurableSeq: commitSeq,
		UserRootPageID: 2, SystemRootPageID: 3,
		TotalPages: candidate.Generation().HighWater(),
		Freelist:   candidate.GenerationRef(), FreelistFreeCount: candidate.Generation().FreeCount(),
		FreelistRetiredCount: candidate.Generation().RetiredCount(), Manifest: manifestRef,
		MetaProjectionDigest: page.DurableMetaProjectionDigestV1(commitSeq, commitSeq, recordPageID),
	}
	recordImage, recordDigest, err := record.EncodePage(recordPageID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.WritePage(recordPageID, recordImage); err != nil {
		t.Fatal(err)
	}
	meta, err := page.NewDurableMetaV1(commitSeq, commitSeq, recordPageID, recordDigest)
	if err != nil {
		t.Fatal(err)
	}
	metaImage := make([]byte, page.PageSize)
	if err := meta.Encode(metaImage[page.PageHeaderSize:]); err != nil {
		t.Fatal(err)
	}
	header := page.PageHeader{PageID: slot, Flags: uint16(page.PageTypeMeta)}
	header.Encode(metaImage)
	page.UpdateChecksum(metaImage)
	if err := fixture.store.WritePage(slot, metaImage); err != nil {
		t.Fatal(err)
	}
	fixture.generation = candidate.Generation()
	if err := ledger.MarkVisible(candidateID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Publish(candidateID); err != nil {
		t.Fatal(err)
	}
	return durableRootSelectionV1{Slot: slot, Meta: meta, Record: record, Freelist: candidate.Generation(), Manifest: manifest}
}

func TestSelectDurableRootV1FallsBackWhenNewestManifestIsMissing(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	older := fixture.addCandidate(t, MetaPage0ID)
	newer := fixture.addCandidate(t, MetaPage1ID)
	delete(fixture.store.Pages, newer.Record.Manifest.FirstPageID)

	selected, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Slot != older.Slot || selected.Meta.CommitSeq != older.Meta.CommitSeq {
		t.Fatalf("selected slot/commit=(%d,%d), want (%d,%d)", selected.Slot, selected.Meta.CommitSeq, older.Slot, older.Meta.CommitSeq)
	}
}

func TestSelectDurableRootV1ReportsStableReasonsWhenBothSlotsInvalid(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	older := fixture.addCandidate(t, MetaPage0ID)
	newer := fixture.addCandidate(t, MetaPage1ID)
	delete(fixture.store.Pages, older.Meta.RootRecordPageID)
	delete(fixture.store.Pages, newer.Meta.RootRecordPageID)

	_, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
	if !errors.Is(err, ErrNoRecoverableMeta) {
		t.Fatalf("error=%v, want ErrNoRecoverableMeta", err)
	}
	var detail *NoRecoverableMetaError
	if !errors.As(err, &detail) {
		t.Fatalf("error type=%T, want *NoRecoverableMetaError", err)
	}
	wantFragments := []string{"slot 0: root record", "slot 1: root record"}
	for _, fragment := range wantFragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("error %q missing %q", err, fragment)
		}
	}
}

func TestSelectDurableRootV1RejectsLegacySlotsWithRebuildDistinction(t *testing.T) {
	store := freelist.NewMemoryPageStoreV1()
	for slot := uint64(0); slot < 2; slot++ {
		image := make([]byte, page.PageSize)
		legacy := page.MetaPageBody{CommitSeq: slot + 1, UserRootPageID: 2, SystemRootPageID: 3, TotalPages: 4}
		legacy.Encode(image[page.PageHeaderSize:])
		header := page.PageHeader{PageID: slot, Flags: uint16(page.PageTypeMeta)}
		header.Encode(image)
		page.UpdateChecksum(image)
		if err := store.WritePage(slot, image); err != nil {
			t.Fatal(err)
		}
	}

	_, err := selectDurableRootV1(store, 4, nil)
	if !errors.Is(err, ErrNoRecoverableMeta) || !errors.Is(err, ErrLegacyFormatRebuildRequired) {
		t.Fatalf("error=%v, want no-recoverable plus legacy-rebuild distinction", err)
	}
}

func TestSelectDurableRootV1ReadsBoundedInventory(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	selected := fixture.addCandidate(t, MetaPage0ID)
	// The other slot is intentionally absent. Selection may inspect only the
	// two fixed metas plus the selected record, manifest, roots, and COW pages.
	fixture.store.Reads = 0
	got, err := selectDurableRootV1(fixture.store, selected.Record.TotalPages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Meta.CommitSeq != selected.Meta.CommitSeq {
		t.Fatalf("commit=%d want %d", got.Meta.CommitSeq, selected.Meta.CommitSeq)
	}
	if fixture.store.Reads > 16 {
		t.Fatalf("recovery reads=%d, want fixed bounded inventory", fixture.store.Reads)
	}
}
