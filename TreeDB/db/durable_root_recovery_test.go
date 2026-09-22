package db

import (
	"bytes"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestOpenDurableRootRecoveryDoesNotScanUnrelatedValueLogSegments(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
		ValueLog: ValueLogOptions{
			PointerThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	want := bytes.Repeat([]byte("bounded-recovery|"), 64)
	ptr := appendPointersInNewSegment(t, dir, 0, 1, 1, 1, func(int) []byte { return want })[0]
	if err := database.valueLogManager.RegisterSegment(valuelog.SegmentPath(ValueLogDirPath(dir), ptr.FileID), ptr.FileID); err != nil {
		_ = database.Close()
		t.Fatalf("register exact producer segment: %v", err)
	}
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("key"), ptr); err != nil {
		_ = batch.Close()
		_ = database.Close()
		t.Fatalf("set: %v", err)
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		_ = database.Close()
		t.Fatalf("write sync: %v", err)
	}
	if err := batch.Close(); err != nil {
		_ = database.Close()
		t.Fatalf("close batch: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for lane := uint32(1); lane <= 32; lane++ {
		fileID, err := valuelog.EncodeFileID(lane, 1)
		if err != nil {
			t.Fatalf("encode unrelated file id: %v", err)
		}
		path := valuelog.SegmentPath(ValueLogDirPath(dir), fileID)
		if err := os.WriteFile(path, []byte("not a value-log segment"), 0o600); err != nil {
			t.Fatalf("write unrelated segment: %v", err)
		}
	}

	reopened, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("bounded reopen: %v", err)
	}
	defer reopened.Close()
	if scans := reopened.valueLogManager.RefreshScanCount(); scans != 0 {
		t.Fatalf("recovery performed %d value-log directory scans, want 0", scans)
	}
	got, err := reopened.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("value differs after bounded reopen")
	}
}

func TestDurableRootPublicationRejectsUnregisteredCanonicalValueLogPath(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer database.Close()

	ptrs, _, _ := appendPointersInUnregisteredNewSegment(t, dir, 0, 1, 1, 1, func(int) []byte {
		return bytes.Repeat([]byte("producer-owned|"), 32)
	})
	ptr := ptrs[0]
	batch := database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("key"), ptr); err != nil {
		t.Fatalf("set pointer: %v", err)
	}
	if err := batch.WriteSync(); !errors.Is(err, rootpublication.ErrUnresolvedResource) {
		t.Fatalf("unregistered canonical path write error=%v want unresolved resource", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("close rejected batch: %v", err)
	}

	path := valuelog.SegmentPath(ValueLogDirPath(dir), ptr.FileID)
	if err := database.RegisterValueLogSegment(path, ptr.FileID); err != nil {
		t.Fatalf("register exact producer segment: %v", err)
	}
	batch = database.NewBatch().(*Batch)
	if err := batch.SetPointer([]byte("key"), ptr); err != nil {
		t.Fatalf("set registered pointer: %v", err)
	}
	if err := batch.WriteSync(); err != nil {
		t.Fatalf("write registered pointer: %v", err)
	}
	if err := batch.Close(); err != nil {
		t.Fatalf("close registered batch: %v", err)
	}
}

type durableRootFixtureV1 struct {
	store                *freelist.MemoryPageStoreV1
	generation           *freelist.FreelistGenerationV1
	nextCommit           uint64
	appliedCommandLSN    uint64
	lastRecordPageID     uint64
	lastRecordDigest     [32]byte
	lastRecordCommitSeq  uint64
	nextDurableSeq       uint64
	lastPublicationPages uint64
}

func newDurableRootFixtureV1(t testing.TB) *durableRootFixtureV1 {
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
		store:          store,
		generation:     freelist.MustNewFreelistGenerationV1(1, 4, nil, nil),
		nextCommit:     1,
		nextDurableSeq: 1,
	}
}

func (fixture *durableRootFixtureV1) addCandidate(t testing.TB, slot uint64) durableRootSelectionV1 {
	t.Helper()
	manifest, err := rootpublication.NewDependencyManifestV1(nil)
	if err != nil {
		t.Fatal(err)
	}
	return fixture.addCandidateWithManifest(t, slot, manifest)
}

func (fixture *durableRootFixtureV1) addCandidateWithManifest(t testing.TB, slot uint64, manifest *rootpublication.DependencyManifestV1) durableRootSelectionV1 {
	t.Helper()
	pagesBefore := len(fixture.store.Pages)
	commitSeq := fixture.nextCommit
	fixture.nextCommit++
	durableSeq := fixture.nextDurableSeq
	fixture.nextDurableSeq++
	ledger := freelist.NewReservationLedger()
	txn, err := freelist.BeginCandidateV1(fixture.generation, fixture.generation.GenerationRef(), ledger)
	if err != nil {
		t.Fatal(err)
	}
	rootIDs := make([]uint64, 2)
	for i := range rootIDs {
		rootIDs[i], err = txn.Allocate(0)
		if err != nil {
			t.Fatal(err)
		}
	}
	var manifestPageID uint64
	for pageIndex := uint32(0); pageIndex < manifest.PageCount(); pageIndex++ {
		pageID, err := txn.Allocate(0)
		if err != nil {
			t.Fatal(err)
		}
		if pageIndex == 0 {
			manifestPageID = pageID
		} else if pageID != manifestPageID+uint64(pageIndex) {
			t.Fatalf("manifest allocation page[%d]=%d want contiguous %d", pageIndex, pageID, manifestPageID+uint64(pageIndex))
		}
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
	for _, rootID := range rootIDs {
		image := make([]byte, page.PageSize)
		builder := node.NewBuilder(image, page.PageTypeLeaf)
		builder.SetPageID(rootID)
		builder.Finish()
		if err := fixture.store.WritePage(rootID, image); err != nil {
			t.Fatal(err)
		}
	}
	manifestRef, err := manifest.Materialize(manifestPageID, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	record := rootpublication.DurableRootRecordV1{
		CommitSeq: commitSeq, DurableSeq: durableSeq,
		UserRootPageID: rootIDs[0], SystemRootPageID: rootIDs[1],
		TotalPages: candidate.Generation().HighWater(), AppliedCommandLSN: fixture.appliedCommandLSN,
		Freelist: candidate.GenerationRef(), FreelistFreeCount: candidate.Generation().FreeCount(),
		FreelistRetiredCount: candidate.Generation().RetiredCount(), Manifest: manifestRef,
		ParentRecordPageID: fixture.lastRecordPageID, ParentCommitSeq: fixture.lastRecordCommitSeq,
		ParentRecordDigest:   fixture.lastRecordDigest,
		MetaProjectionDigest: page.DurableMetaProjectionDigestV1(commitSeq, durableSeq, recordPageID),
	}
	if fixture.lastRecordPageID == 0 {
		record.ParentCommitSeq = 0
	}
	recordImage, recordDigest, err := record.EncodePage(recordPageID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.WritePage(recordPageID, recordImage); err != nil {
		t.Fatal(err)
	}
	meta, err := page.NewDurableMetaV1(commitSeq, durableSeq, recordPageID, recordDigest)
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
	fixture.lastRecordPageID = recordPageID
	fixture.lastRecordDigest = recordDigest
	fixture.lastRecordCommitSeq = commitSeq
	if err := ledger.MarkVisible(candidateID); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Publish(candidateID); err != nil {
		t.Fatal(err)
	}
	fixture.lastPublicationPages = uint64(len(fixture.store.Pages) - pagesBefore)
	return durableRootSelectionV1{Slot: slot, Meta: meta, Record: record, Freelist: candidate.Generation(), Manifest: manifest}
}

func TestSelectDurableRootV1AcceptsGroupedCommitFrontierWithContiguousDurableLineage(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	older := fixture.addCandidate(t, MetaPage0ID)
	fixture.nextCommit = 5
	newer := fixture.addCandidate(t, MetaPage1ID)

	selected, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Slot != newer.Slot || selected.Meta.CommitSeq != 5 || selected.Meta.DurableSeq != older.Meta.DurableSeq+1 {
		t.Fatalf("selected slot/commit/durable=(%d,%d,%d), want (%d,5,%d)", selected.Slot, selected.Meta.CommitSeq, selected.Meta.DurableSeq, newer.Slot, older.Meta.DurableSeq+1)
	}
}

func TestSelectDurableRootV1RejectsDurableLineageGapAndFallsBack(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	older := fixture.addCandidate(t, MetaPage0ID)
	fixture.nextCommit = 5
	newer := fixture.addCandidate(t, MetaPage1ID)
	newer.Record.DurableSeq++
	newer.Record.MetaProjectionDigest = page.DurableMetaProjectionDigestV1(newer.Record.CommitSeq, newer.Record.DurableSeq, newer.Meta.RootRecordPageID)
	newer = rewriteDurableRootFixtureRecordV1(t, fixture, newer, newer.Record)

	selected, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Slot != older.Slot || selected.Meta.CommitSeq != older.Meta.CommitSeq {
		t.Fatalf("selected slot/commit=(%d,%d), want older (%d,%d)", selected.Slot, selected.Meta.CommitSeq, older.Slot, older.Meta.CommitSeq)
	}
}

func rewriteDurableRootFixtureRecordV1(t testing.TB, fixture *durableRootFixtureV1, selected durableRootSelectionV1, record rootpublication.DurableRootRecordV1) durableRootSelectionV1 {
	t.Helper()
	image, digest, err := record.EncodePage(selected.Meta.RootRecordPageID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.store.Pages[selected.Meta.RootRecordPageID] = append([]byte(nil), image...)
	meta, err := page.NewDurableMetaV1(record.CommitSeq, record.DurableSeq, selected.Meta.RootRecordPageID, digest)
	if err != nil {
		t.Fatal(err)
	}
	writeDurableRootFixtureMetaV1(t, fixture, selected.Slot, meta)
	selected.Meta = meta
	selected.Record = record
	return selected
}

func writeDurableRootFixtureMetaV1(t testing.TB, fixture *durableRootFixtureV1, slot uint64, meta page.DurableMetaV1) {
	t.Helper()
	metaImage := make([]byte, page.PageSize)
	if err := meta.Encode(metaImage[page.PageHeaderSize:]); err != nil {
		t.Fatal(err)
	}
	header := page.PageHeader{PageID: slot, Flags: uint16(page.PageTypeMeta)}
	header.Encode(metaImage)
	page.UpdateChecksum(metaImage)
	fixture.store.Pages[slot] = append([]byte(nil), metaImage...)
}

func TestSelectDurableRootV1RejectsNewestBrokenParentLineageAndFallsBack(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	older := fixture.addCandidate(t, MetaPage0ID)
	newer := fixture.addCandidate(t, MetaPage1ID)
	newer.Record.ParentRecordDigest[0] ^= 0xff
	newer = rewriteDurableRootFixtureRecordV1(t, fixture, newer, newer.Record)

	selected, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Slot != older.Slot || selected.Meta.CommitSeq != older.Meta.CommitSeq {
		t.Fatalf("selected slot/commit=(%d,%d), want older (%d,%d)", selected.Slot, selected.Meta.CommitSeq, older.Slot, older.Meta.CommitSeq)
	}
}

func TestSelectDurableRootV1RejectsNewestAppliedCommandWALRegressionAndFallsBack(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	fixture.appliedCommandLSN = 7
	older := fixture.addCandidate(t, MetaPage0ID)
	fixture.appliedCommandLSN = 6
	newer := fixture.addCandidate(t, MetaPage1ID)

	selected, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Slot != older.Slot || selected.Meta.CommitSeq != older.Meta.CommitSeq {
		t.Fatalf("selected slot/commit=(%d,%d), want older (%d,%d)", selected.Slot, selected.Meta.CommitSeq, older.Slot, older.Meta.CommitSeq)
	}
}

func TestSelectDurableRootV1TreatsMirroredMetasAsOneRecoveryGeneration(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	_ = fixture.addCandidate(t, MetaPage0ID)
	newer := fixture.addCandidate(t, MetaPage1ID)
	writeDurableRootFixtureMetaV1(t, fixture, MetaPage0ID, newer.Meta)

	selected, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Slot != MetaPage0ID || selected.SlotCommits != [2]uint64{newer.Meta.CommitSeq, 0} {
		t.Fatalf("mirrored selection slot/commits=(%d,%v), want (0,[%d 0])", selected.Slot, selected.SlotCommits, newer.Meta.CommitSeq)
	}
}

func TestSelectDurableRootV1RejectsConflictingRootsAtSameCommit(t *testing.T) {
	fixture := newDurableRootFixtureV1(t)
	older := fixture.addCandidate(t, MetaPage0ID)
	newer := fixture.addCandidate(t, MetaPage1ID)
	conflict, err := page.NewDurableMetaV1(older.Meta.CommitSeq, older.Meta.DurableSeq, newer.Meta.RootRecordPageID, newer.Meta.RootRecordDigest)
	if err != nil {
		t.Fatal(err)
	}
	writeDurableRootFixtureMetaV1(t, fixture, MetaPage1ID, conflict)

	_, err = selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
	if !errors.Is(err, ErrNoRecoverableMeta) || !strings.Contains(err.Error(), "conflicting recovery generation") {
		t.Fatalf("conflicting same-commit roots error=%v, want stable no-recoverable conflict", err)
	}
}

func TestSelectDurableRootV1ValidatesExactExternalRangesAndFallsBack(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	const (
		lane         = "events/column-assets"
		resourceID   = "1"
		resourceName = "segment-000001.tca"
	)
	relativeParent := filepath.ToSlash(filepath.Join("column_assets", lane, "assets", "segments"))
	parentPath := filepath.Join(dir, filepath.FromSlash(relativeParent))
	if err := os.MkdirAll(parentPath, 0o700); err != nil {
		t.Fatal(err)
	}
	parent, err := rootpublication.OpenStableParent(parentPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	resourcePath := filepath.Join(parentPath, resourceName)
	oldRange := []byte("older-slot-authoritative-range")
	newRange := []byte("newest-slot-only-range")
	payload := append(append([]byte(nil), oldRange...), newRange...)
	file, err := rootpublication.OpenStableChildFile(parent, resourceName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	identity, err := rootpublication.StableIdentityFromFile(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	identity.Generation = 1
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if rootpublication.StableRelativeNamespaceSupported() {
		if err := parent.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	// Windows' narrower create-only contract persists namespace metadata through
	// the exact child Sync above; full namespace platforms sync the parent.
	parentIdentity, err := rootpublication.StableIdentityFromFile(parent)
	if err != nil {
		t.Fatal(err)
	}
	parentIdentity.Generation = 1

	obligation := func(offset int64, raw []byte, partID uint64) rootpublication.StableLogicalObligation {
		entry := rootpublication.StableLogicalObligation{
			Class: "column-asset-ref-v1", Kind: "tcs1_part_image", Namespace: lane,
			Generation: 1, PartID: partID, FileID: 1, Offset: offset, Length: int64(len(raw)),
			Checksum: crc32.ChecksumIEEE(raw), Reachability: rootpublication.ReachabilityColumnManifest,
		}
		entry.Digest = durableColumnLogicalObligationDigestV1(entry)
		return entry
	}
	oldObligation := obligation(0, oldRange, 1)
	newObligation := obligation(int64(len(oldRange)), newRange, 2)
	manifest := func(frontier uint64, obligations ...rootpublication.StableLogicalObligation) *rootpublication.DependencyManifestV1 {
		result, err := rootpublication.NewDependencyManifestV1([]rootpublication.DependencyManifestEntryV1{{
			Kind: rootpublication.ResourceColumnAsset, LogicalLane: lane, ResourceID: resourceID,
			DiagnosticPath: filepath.ToSlash(filepath.Join(relativeParent, resourceName)),
			Identity:       identity, Generation: 1, Digest: durableColumnSegmentDigestV1(lane, 1),
			Frontier:           rootpublication.DurableFrontier{Bytes: frontier},
			Reachability:       []rootpublication.ReachabilityField{rootpublication.ReachabilityColumnManifest},
			LogicalObligations: obligations,
			Namespace: &rootpublication.DependencyManifestNamespaceV1{
				ParentIdentity: parentIdentity, Operation: rootpublication.NamespaceCreate,
				NewName: resourceName, DiagnosticPath: relativeParent,
			},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}

	fixture := newDurableRootFixtureV1(t)
	older := fixture.addCandidateWithManifest(t, MetaPage0ID, manifest(uint64(len(oldRange)), oldObligation))
	newer := fixture.addCandidateWithManifest(t, MetaPage1ID, manifest(uint64(len(payload)), oldObligation, newObligation))

	corrupt := func(offset int64) {
		f, err := os.OpenFile(resourcePath, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		byteAt := []byte{payload[offset] ^ 0xff}
		if _, err := f.WriteAt(byteAt, offset); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	corrupt(int64(len(oldRange)))
	selected, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, database.validateDurableDependencyManifestV1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, resources := range selected.SlotResources {
			resources.Release()
		}
	}()
	if selected.Slot != older.Slot || selected.Meta.CommitSeq != older.Meta.CommitSeq {
		t.Fatalf("selected slot/commit=(%d,%d), want older (%d,%d)", selected.Slot, selected.Meta.CommitSeq, older.Slot, older.Meta.CommitSeq)
	}
	if selected.SlotCommits != [2]uint64{older.Meta.CommitSeq, 0} {
		t.Fatalf("recoverable slot commits=%v want [%d 0]", selected.SlotCommits, older.Meta.CommitSeq)
	}

	corrupt(0)
	_, err = selectDurableRootV1(fixture.store, newer.Record.TotalPages, database.validateDurableDependencyManifestV1)
	if !errors.Is(err, ErrNoRecoverableMeta) {
		t.Fatalf("both dependency closures corrupt error=%v want ErrNoRecoverableMeta", err)
	}
	if !strings.Contains(err.Error(), "logical obligation checksum mismatch") {
		t.Fatalf("both dependency closures corrupt error=%v want stable checksum reason", err)
	}
}

func TestSelectDurableRootV1FallsBackForEveryNewestBoundedComponentFailure(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*durableRootFixtureV1, durableRootSelectionV1)
	}{
		{name: "root-record-missing", mutate: func(f *durableRootFixtureV1, newest durableRootSelectionV1) {
			delete(f.store.Pages, newest.Meta.RootRecordPageID)
		}},
		{name: "root-record-truncated", mutate: func(f *durableRootFixtureV1, newest durableRootSelectionV1) {
			f.store.Pages[newest.Meta.RootRecordPageID] = append([]byte(nil), f.store.Pages[newest.Meta.RootRecordPageID][:page.PageSize/2]...)
		}},
		{name: "manifest-missing", mutate: func(f *durableRootFixtureV1, newest durableRootSelectionV1) {
			delete(f.store.Pages, newest.Record.Manifest.FirstPageID)
		}},
		{name: "manifest-truncated", mutate: func(f *durableRootFixtureV1, newest durableRootSelectionV1) {
			f.store.Pages[newest.Record.Manifest.FirstPageID] = append([]byte(nil), f.store.Pages[newest.Record.Manifest.FirstPageID][:page.PageSize/2]...)
		}},
		{name: "root-page-missing", mutate: func(f *durableRootFixtureV1, newest durableRootSelectionV1) {
			delete(f.store.Pages, newest.Record.UserRootPageID)
		}},
		{name: "root-page-truncated", mutate: func(f *durableRootFixtureV1, newest durableRootSelectionV1) {
			f.store.Pages[newest.Record.UserRootPageID] = append([]byte(nil), f.store.Pages[newest.Record.UserRootPageID][:page.PageSize/2]...)
		}},
		{name: "freelist-generation-missing", mutate: func(f *durableRootFixtureV1, newest durableRootSelectionV1) {
			delete(f.store.Pages, newest.Record.Freelist.HeaderPageID)
		}},
		{name: "freelist-generation-truncated", mutate: func(f *durableRootFixtureV1, newest durableRootSelectionV1) {
			f.store.Pages[newest.Record.Freelist.HeaderPageID] = append([]byte(nil), f.store.Pages[newest.Record.Freelist.HeaderPageID][:page.PageSize/2]...)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDurableRootFixtureV1(t)
			older := fixture.addCandidate(t, MetaPage0ID)
			newer := fixture.addCandidate(t, MetaPage1ID)
			test.mutate(fixture, newer)

			selected, err := selectDurableRootV1(fixture.store, newer.Record.TotalPages, nil)
			if err != nil {
				t.Fatal(err)
			}
			if selected.Slot != older.Slot || selected.Meta.CommitSeq != older.Meta.CommitSeq {
				t.Fatalf("selected slot/commit=(%d,%d), want (%d,%d)", selected.Slot, selected.Meta.CommitSeq, older.Slot, older.Meta.CommitSeq)
			}
		})
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
