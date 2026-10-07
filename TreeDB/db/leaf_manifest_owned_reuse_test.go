package db

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func assertOwnedManifestCoversCurrentLeafFiles(t *testing.T, d *DB) {
	t.Helper()
	current, err := leafPageLogCurrentSegments(d.leafPageLog)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := loadOwnedLeafManifest(d.idx.Load().pager, d.meta.SystemRootPageID, d.idx.Load().pager.PageCount())
	if err != nil || !equalOwnedLeafManifest(captured, d.leafGenerationManifest) {
		t.Fatalf("same-ACK physical manifest mismatch: %v", err)
	}
	if len(current) == 0 {
		t.Fatal("fixture produced no current leaf file")
	}
	for _, segment := range current {
		raw := page.ValueLogSegmentID(segment.FileID)
		found := false
		for _, generation := range captured.Generations {
			if generation.State == leafGenerationStateDeleted {
				continue
			}
			for _, id := range generation.FileIDs {
				found = found || id == raw
			}
		}
		if !found {
			t.Fatalf("same ACK intrinsic revision omits live leaf file %d", raw)
		}
	}
}

func TestOwnedLeafManifestSameGenerationReuseAndRootEdits(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true}
	d, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{MaxSegmentBytes: 1 << 20, Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatal(err)
	}
	d.SetLeafPageLog(log)
	if err := d.SetSync([]byte("same/00"), []byte("complete row")); err != nil {
		t.Fatal(err)
	}
	assertOwnedManifestCoversCurrentLeafFiles(t, d)
	basis := d.leafGenerationManifest.clone()
	root := d.meta.SystemRootPageID
	held := d.AcquireSnapshot()
	for i := 1; i < 32; i++ {
		if err := d.SetSync([]byte(fmt.Sprintf("same/%02d", i)), []byte("complete row")); err != nil {
			t.Fatal(err)
		}
		if d.meta.SystemRootPageID != root || !equalOwnedLeafManifest(d.leafGenerationManifest, basis) {
			t.Fatalf("same active file rewrote intrinsic manifest at ACK %d root=%d/%d basis=%+v current=%+v", i, root, d.meta.SystemRootPageID, basis, d.leafGenerationManifest)
		}
	}
	if work := d.OwnedLeafManifestPublicationWork(); work.CanonicalObjects != 0 || work.PagerReads != 0 || work.LogicalComparisons < 32 {
		t.Fatalf("unchanged root recertified intrinsic bytes: %+v", work)
	}
	// Reading attribution is observational: it cannot consume credits or
	// mint authority, change the root, or invalidate exact object reuse.
	workBefore := d.OwnedLeafManifestPublicationWork()
	seqBefore := d.meta.CommitSeq
	for i := 0; i < 8; i++ {
		if got := d.OwnedLeafManifestPublicationWork(); got != workBefore {
			t.Fatal("attribution read changed work/credits")
		}
	}
	if d.meta.CommitSeq != seqBefore || d.meta.SystemRootPageID != root || !equalOwnedLeafManifest(d.leafGenerationManifest, basis) {
		t.Fatal("attribution read changed authority")
	}
	for _, record := range d.durableRoot.slotRecord {
		if !record.OwnedLeafManifest || record.SystemRootPageID != root {
			t.Fatal("dual slots did not share exact intrinsic root")
		}
	}
	// Already-known pending IDs must be consumed at visibility even when the
	// intrinsic object is reused. Capture their real encoded leaf identity.
	batch := d.NewPhysicalBatch().(*Batch)
	if err := batch.Set([]byte("same/known"), []byte("complete row")); err != nil {
		t.Fatal(err)
	}
	raw := basis.Generations[len(basis.Generations)-1].FileIDs[0]
	fileID := page.ValueLogFileID(raw)
	d.queueLeafGenerationWritableFileID(fileID)
	if err := batch.WriteSync(); err != nil {
		t.Fatal(err)
	}
	batch.Close()
	if len(d.snapshotLeafGenerationPendingFileIDs(0)) != 0 {
		t.Fatal("known pending ID not consumed at visible cut")
	}
	catalog := mustFrozenSystemMemtable(t, "catalog/unrelated", "complete descriptor")
	if _, err := d.PublishSystemRootIterator(catalog.NewIterator(nil, nil)); err != nil {
		t.Fatal(err)
	}
	copied, err := loadOwnedLeafManifest(d.idx.Load().pager, d.meta.SystemRootPageID, d.idx.Load().pager.PageCount())
	if err != nil || !equalOwnedLeafManifest(copied, basis) {
		t.Fatalf("catalog edit lost immutable basis: %v", err)
	}
	if work := d.OwnedLeafManifestPublicationWork(); work.CanonicalObjects == 0 || work.PagerReads == 0 || work.PageCredits == 0 || work.ByteCredits == 0 || work.EncodedObjects == 0 {
		t.Fatalf("catalog preservation/validation was uncharged: %+v", work)
	}
	// An attempted private namespace overwrite cannot be silently repaired.
	before := d.meta.CommitSeq
	bad := mustFrozenSystemMemtable(t, string(ownedLeafManifestHeaderKey), "invalid")
	if _, err := d.PublishSystemRootIterator(bad.NewIterator(nil, nil)); err == nil {
		t.Fatal("private intrinsic overwrite accepted")
	}
	if d.meta.CommitSeq != before {
		t.Fatal("rejected intrinsic edit became visible")
	}
	for i := 0; i < 8; i++ {
		rev, err := d.CheckpointOwnedLeafManifest()
		if err != nil {
			t.Fatal(err)
		}
		if rev != basis.ManifestRevision+uint64(i)+1 {
			t.Fatal("explicit checkpoint did not add exactly one revision")
		}
	}
	for i := 0; i < 32; i++ {
		if _, err := d.PruneOwnedLeafManifestStep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	old, err := loadOwnedLeafManifest(held.idx.pager, held.state.SystemRootPageID, held.idx.pager.PageCount())
	if err != nil || !equalOwnedLeafManifest(old, basis) {
		t.Fatalf("shared held intrinsic object changed: %v", err)
	}
	if got, err := held.Get([]byte("same/00")); err != nil || !bytes.Equal(got, []byte("complete row")) {
		t.Fatal("held public row lost")
	}
	held.Close()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < 32; i++ {
		if got, err := d.Get([]byte(fmt.Sprintf("same/%02d", i))); err != nil || !bytes.Equal(got, []byte("complete row")) {
			t.Fatalf("reopened row %d: %v", i, err)
		}
	}
	if d.leafGenerationManifest.ManifestRevision != basis.ManifestRevision+8 {
		t.Fatal("reopen lost explicit history")
	}
}

func TestOwnedLeafManifestActualRolloverAddsRevision(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{MaxSegmentBytes: 8192, Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	d.SetLeafPageLog(log)
	if err := d.SetSync([]byte("rollover/first"), bytes.Repeat([]byte("x"), 256)); err != nil {
		t.Fatal(err)
	}
	assertOwnedManifestCoversCurrentLeafFiles(t, d)
	first := d.leafGenerationManifest.clone()
	for i := 0; i < 32 && len(d.leafGenerationManifest.Generations) == len(first.Generations); i++ {
		if err := d.SetSync([]byte(fmt.Sprintf("rollover/%02d", i)), bytes.Repeat([]byte("y"), 256)); err != nil {
			t.Fatal(err)
		}
	}
	assertOwnedManifestCoversCurrentLeafFiles(t, d)
	last := d.leafGenerationManifest
	if len(last.Generations) <= len(first.Generations) || last.ManifestRevision <= first.ManifestRevision {
		t.Fatal("real segment rollover did not add intrinsic revision")
	}
	captured, err := loadOwnedLeafManifest(d.idx.Load().pager, d.meta.SystemRootPageID, d.idx.Load().pager.PageCount())
	if err != nil || !equalOwnedLeafManifest(captured, last) {
		t.Fatalf("rollover physical/logical manifest differs: %v", err)
	}
}

func TestOwnedLeafManifestFailedStageRetainsPendingAndQueuedReuse(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir, IndexOuterLeavesInValueLog: true, OwnedLeafManifests: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	log, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	d.SetLeafPageLog(log)
	basis, root, seq := d.leafGenerationManifest.clone(), d.meta.SystemRootPageID, d.meta.CommitSeq
	d.testFailFinalizeCommit.Store(true)
	if err := d.SetSync([]byte("failed/row"), []byte("complete row")); err == nil {
		t.Fatal("failed publication unexpectedly succeeded")
	}
	d.testFailFinalizeCommit.Store(false)
	if d.meta.CommitSeq != seq || d.meta.SystemRootPageID != root || !equalOwnedLeafManifest(basis, d.leafGenerationManifest) {
		t.Fatal("failed stage changed visible authority")
	}
	if len(d.snapshotLeafGenerationPendingFileIDs(0)) == 0 {
		t.Fatal("failed stage consumed uncommitted pending IDs")
	}
	if err := d.SetSync([]byte("retry/row"), []byte("complete row")); err != nil {
		t.Fatal(err)
	}
	if len(d.snapshotLeafGenerationPendingFileIDs(0)) != 0 {
		t.Fatal("retry did not atomically consume pending IDs")
	}
	basis, root, seq = d.leafGenerationManifest.clone(), d.meta.SystemRootPageID, d.meta.CommitSeq
	group, err := d.BeginRootPublicationBuildGroup()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		batch := d.NewPhysicalBatch().(*Batch)
		if err := batch.Set([]byte(fmt.Sprintf("reuse-group/%d", i)), []byte("complete row")); err != nil {
			t.Fatal(err)
		}
		if err := batch.SetRootPublicationBuildGroup(group, i == 1); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			err = batch.Write()
		} else {
			err = batch.WriteSync()
		}
		batch.Close()
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 && d.meta.CommitSeq != seq {
			t.Fatal("private group escaped visible cut")
		}
	}
	group.Close()
	if d.meta.CommitSeq != seq+1 || d.meta.SystemRootPageID != root || !equalOwnedLeafManifest(basis, d.leafGenerationManifest) {
		t.Fatal("same-generation group rewrote intrinsic authority")
	}
	if len(d.snapshotLeafGenerationPendingFileIDs(0)) != 0 {
		t.Fatal("queued reuse did not atomically consume pending IDs")
	}
}
