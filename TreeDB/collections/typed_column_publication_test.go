package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
)

func TestTypedColumnPublicationCheckpointReopen(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 1, 1)
	assertTypedColumnLatestRows1755(t, d, col, 1, []typedColumnExpectedRow1755{
		{PrimaryID: 0, Kind: "like", Score: 2.5, Flag: true},
		{PrimaryID: 1, Kind: "post", Score: 3.5, Flag: false},
	})
	got, err := col.Get([]byte("e1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`))

	updated, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`))
		return []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`), true, nil
	})
	if err != nil || !updated || !changed {
		_ = d.Close()
		t.Fatalf("Update e1 updated=%v changed=%v err=%v", updated, changed, err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 2, 2)
	got, err = col.Get([]byte("e1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get updated e1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`))
	got, err = col.Get([]byte("e2"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e2: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`))

	deleted, err := col.DeleteDocument([]byte("e2"))
	if err != nil || !deleted {
		_ = d.Close()
		t.Fatalf("DeleteDocument e2 deleted=%v err=%v", deleted, err)
	}
	if got, err := col.Get([]byte("e2")); err != nil || got != nil {
		_ = d.Close()
		t.Fatalf("Get deleted e2 got=%s err=%v, want missing", got, err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 3, 2)
	got, err = col.Get([]byte("e1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get e1 after delete: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`))

	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d = nil

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 3, 2)
	reopenedGot, err := reopenedCol.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("reopened Get e1: %v", err)
	}
	assertJSONEqualM13C(t, reopenedGot, []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`))
	if reopenedGot, err := reopenedCol.Get([]byte("e2")); err != nil || reopenedGot != nil {
		t.Fatalf("reopened Get deleted e2 got=%s err=%v, want missing", reopenedGot, err)
	}
}

func TestTypedColumnVectorDensePublicationCheckpointReopen1756(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnVectorPartCollection1756(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("v1"), []byte("v2")}, [][]byte{
		[]byte(`{"embedding":[1,0.5,-0.25],"payload":"alpha"}`),
		[]byte(`{"embedding":[2,3,4],"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 1, 1)
	got, err := col.Get([]byte("v1"))
	if err != nil {
		_ = d.Close()
		t.Fatalf("Get v1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"embedding":[1,0.5,-0.25],"payload":"alpha"}`))
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("vectors")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 1, 1)
	reopenedGot, err := reopenedCol.Get([]byte("v2"))
	if err != nil {
		t.Fatalf("reopened Get v2: %v", err)
	}
	assertJSONEqualM13C(t, reopenedGot, []byte(`{"embedding":[2,3,4],"payload":"beta"}`))
	reopenedGot, err = reopenedCol.Get([]byte("v1"))
	if err != nil {
		t.Fatalf("reopened Get v1: %v", err)
	}
	assertJSONEqualM13C(t, reopenedGot, []byte(`{"embedding":[1,0.5,-0.25],"payload":"alpha"}`))
}

func TestTypedColumnPublicationCommandWALReopenWithoutCheckpoint(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint baseline collection: %v", err)
	}

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`))
		return []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`), true, nil
	}); err != nil || !changed {
		_ = d.Close()
		t.Fatalf("Update changed=%v err=%v", changed, err)
	}
	assertTypedColumnManifestShape1755(t, d, col, 2, 2)
	preCloseRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(preCloseRefs) != 2 {
		_ = d.Close()
		t.Fatalf("pre-close typed refs=%+v want two", preCloseRefs)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close without checkpoint: %v", err)
	}
	d = nil

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	assertTypedColumnManifestShape1755(t, reopened, reopenedCol, 2, 2)
	got, err := reopenedCol.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("reopened Get e1: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":3,"kind":"share","score":6.5,"flag":false,"payload":"alpha2"}`))
	got, err = reopenedCol.Get([]byte("e2"))
	if err != nil {
		t.Fatalf("reopened Get e2: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`))
	reopenedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, reopened, reopenedCol))
	assertColumnAssetRefsEqual1755(t, preCloseRefs, reopenedRefs)
}

func TestTypedColumnReconstructionHybridOwners(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":7,"kind":"like","score":9.25,"flag":true,"payload":"hybrid"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	got, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":7,"kind":"like","score":9.25,"flag":true,"payload":"hybrid"}`))
	assertTypedColumnManifestShape1755(t, d, col, 1, 1)
}

func TestTypedColumnReconstructionScanHybridOwners(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`),
	}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	records, truncated, err := reopenedCol.ScanDocuments(10)
	if err != nil {
		t.Fatalf("ScanDocuments: %v", err)
	}
	if truncated || len(records) != 2 {
		t.Fatalf("ScanDocuments truncated=%v records=%d want 2", truncated, len(records))
	}
	assertJSONEqualM13C(t, records[0].Document, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true,"payload":"alpha"}`))
	assertJSONEqualM13C(t, records[1].Document, []byte(`{"time_us":2,"kind":"post","score":3.5,"flag":false,"payload":"beta"}`))
}

func TestTypedColumnPublicationRejectsOverlappingOwners(t *testing.T) {
	_, err := NormalizeTypedStorageLayout(TypedStorageLayout{
		Collection: "events",
		Fields: []TypedStorageField{
			{Name: "score_row", Path: "score", Owner: TypedStorageOwnerRowAsset, ValueType: ColumnStoreValueDouble},
			{Name: "score_column", Path: "score", Owner: TypedStorageOwnerColumnPart, ValueType: ColumnStoreValueDouble},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "overlapping authoritative typed-storage owners") {
		t.Fatalf("NormalizeTypedStorageLayout error=%v want overlapping owners rejection", err)
	}
}

func TestTypedColumnPublicationMissingAssetFailsClosed(t *testing.T) {
	d, col, typedRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	removeTypedColumnAssetPayload1755(t, d, col, typedRef)
	if got, err := col.Get([]byte("e1")); err == nil || got != nil || !strings.Contains(err.Error(), "typed-column reconstruction") {
		t.Fatalf("Get with missing typed-column asset got=%s err=%v, want typed-column fail-closed error", got, err)
	}
}

func TestTypedColumnPublicationCorruptAssetFailsClosed(t *testing.T) {
	d, col, typedRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	corruptTypedColumnAssetPayload1755(t, d, typedRef)
	if got, err := col.Get([]byte("e1")); err == nil || got != nil || !strings.Contains(err.Error(), "typed-column reconstruction") {
		t.Fatalf("Get with corrupt typed-column asset got=%s err=%v, want typed-column fail-closed error", got, err)
	}
}

func TestTypedColumnFailedExtractionLeavesDocumentAndManifestUnchanged(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	beforeIdentity, ok := col.ColumnStoreCacheIdentity()
	if !ok || beforeIdentity.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v want manifest", beforeIdentity, ok)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	beforeDoc, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get before: %v", err)
	}
	assertJSONEqualM13C(t, beforeDoc, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))

	cases := []struct {
		name string
		doc  []byte
	}{
		{name: "missing typed string", doc: []byte(`{"time_us":2,"score":4.5,"flag":false}`)},
		{name: "null typed double", doc: []byte(`{"time_us":2,"kind":"bad","score":null,"flag":false}`)},
		{name: "typed bool mismatch", doc: []byte(`{"time_us":2,"kind":"bad","score":4.5,"flag":"false"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
				assertJSONEqualM13C(t, current, beforeDoc)
				return tc.doc, true, nil
			})
			if !errors.Is(err, ErrColumnDeclaredValueUnsupported) {
				t.Fatalf("Update err=%v want ErrColumnDeclaredValueUnsupported", err)
			}
			afterIdentity, ok := col.ColumnStoreCacheIdentity()
			if !ok || afterIdentity != beforeIdentity {
				t.Fatalf("identity after failed update=%+v ok=%v want %+v", afterIdentity, ok, beforeIdentity)
			}
			afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
			assertColumnAssetRefsEqual1755(t, beforeRefs, afterRefs)
			afterDoc, err := col.Get([]byte("e1"))
			if err != nil {
				t.Fatalf("Get after failed update: %v", err)
			}
			assertJSONEqualM13C(t, afterDoc, beforeDoc)
		})
	}
}

func TestTypedColumnManifestRecoveryRefsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`)}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 {
		_ = d.Close()
		t.Fatalf("typed refs=%+v want one", typedRefs)
	}
	wantRef := typedRefs[0]
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedTypedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, reopened, reopenedCol))
	if len(reopenedTypedRefs) != 1 || reopenedTypedRefs[0] != wantRef {
		t.Fatalf("reopened typed refs=%+v want [%+v]", reopenedTypedRefs, wantRef)
	}
}

func TestTypedColumnManifestRejectsUnexpectedTypedPartID1755(t *testing.T) {
	asset := ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1TypedColumnPart,
			Namespace:  "events/column-assets",
			Generation: 1,
			PartID:     99,
			FileID:     1,
			Length:     64,
			Checksum:   7,
		},
		Rows:         1,
		Bytes:        64,
		PublishID:    1,
		GenerationID: 1,
		Reason:       string(ColumnPublishOperationInsert),
	}
	raw, err := encodeColumnManifestPartRecord(asset)
	if err != nil {
		t.Fatalf("encodeColumnManifestPartRecord: %v", err)
	}
	_, err = typedColumnPartRefsByGenerationFromManifestRecords([]columnManifestRecord{{
		key:   columnManifestPartRecordKey(asset.Ref.Generation, asset.Ref.PartID),
		value: raw,
	}}, asset.Ref.Namespace)
	if err == nil || !strings.Contains(err.Error(), "unexpected part_id=99") {
		t.Fatalf("typedColumnPartRefsByGenerationFromManifestRecords err=%v want unexpected part_id", err)
	}
}

func TestTypedColumnManifestSnapshotViewAcceptsTypedPartRefs1755(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v want manifest root", id, ok)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	records, err := loadColumnManifestRecordsFromRoot(snap, id.ManifestRoot)
	if err != nil {
		t.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	snapshot, refs, mutationParts, err := decodeColumnManifestSnapshotViewForScan(records, col.Meta().Options.ColumnStore.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("decodeColumnManifestSnapshotViewForScan: %v", err)
	}
	if snapshot.ExpectedParts != 2 || len(refs) != 1 || refs[0].Ref.Kind != ColumnAssetKindTCS1PartImage || mutationParts != 0 {
		t.Fatalf("snapshot expected_parts=%d refs=%+v mutationParts=%d; want one physical ref and typed part counted", snapshot.ExpectedParts, refs, mutationParts)
	}
}

func TestTypedColumnReachabilityRefsExposedForMaintenance(t *testing.T) {
	d, col, typedRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	for _, entry := range plan.Entries {
		if entry.Ref == typedRef {
			if entry.Status != ColumnAssetReachabilityProtected {
				t.Fatalf("typed ref reachability status=%s want protected entry=%+v", entry.Status, entry)
			}
			return
		}
	}
	t.Fatalf("typed ref %+v not exposed in reachability entries=%+v", typedRef, plan.Entries)
}

func TestTypedColumnSnapshotReadsOldRefsAndReachabilityPinsCandidates(t *testing.T) {
	d, col, firstRef := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	oldSnap := d.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = oldSnap.Close() }()
	oldDoc, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot did not find e1")
	}
	assertJSONEqualM13C(t, oldDoc, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))

	if _, changed, err := col.Update([]byte("e1"), func(current []byte) ([]byte, bool, error) {
		assertJSONEqualM13C(t, current, oldDoc)
		return []byte(`{"time_us":5,"kind":"share","score":7.5,"flag":false}`), true, nil
	}); err != nil || !changed {
		t.Fatalf("Update changed=%v err=%v", changed, err)
	}
	currentDoc, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("current Get e1: %v", err)
	}
	assertJSONEqualM13C(t, currentDoc, []byte(`{"time_us":5,"kind":"share","score":7.5,"flag":false}`))
	oldDocAgain, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot lost e1 after update")
	}
	assertJSONEqualM13C(t, oldDocAgain, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))

	candidate := writeTypedColumnAssetCandidate1755(t, d, col, 3, 99)
	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite with pinned old snapshot: %v", err)
	}
	if rewrite.RefsRemapped == 0 || len(rewrite.SupersededRefs) == 0 {
		t.Fatalf("rewrite stats=%+v want remapped/superseded refs", rewrite)
	}
	oldDocAfterRewrite, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot lost e1 after rewrite")
	}
	assertJSONEqualM13C(t, oldDocAfterRewrite, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
	if !columnAssetRefsContain1755(rewrite.SupersededRefs, firstRef) {
		t.Fatalf("superseded refs=%+v want initial typed ref %+v", rewrite.SupersededRefs, firstRef)
	}

	unpinned, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:      true,
		CandidateRefs: rewrite.SupersededRefs,
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability unpinned superseded: %v", err)
	}
	assertColumnAssetReachabilityEntry1755(t, unpinned, firstRef, ColumnAssetReachabilityReclaimable, false)

	pinned, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{
		Detailed:                              true,
		ProtectCandidateRefsForOlderSnapshots: true,
		CandidateRefs:                         rewrite.SupersededRefs,
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability pinned superseded: %v", err)
	}
	assertColumnAssetReachabilityEntry1755(t, pinned, firstRef, ColumnAssetReachabilityProtected, true)
}

func TestTypedColumnSnapshotReadsOldRefsAfterDelete(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	oldSnap := d.AcquireSnapshot()
	if oldSnap == nil {
		t.Fatalf("AcquireSnapshot returned nil")
	}
	defer func() { _ = oldSnap.Close() }()

	deleted, err := col.DeleteDocument([]byte("e1"))
	if err != nil || !deleted {
		t.Fatalf("DeleteDocument deleted=%v err=%v", deleted, err)
	}
	if got, err := col.Get([]byte("e1")); err != nil || got != nil {
		t.Fatalf("current Get after delete got=%s err=%v want missing", got, err)
	}
	oldDoc, found := typedColumnDocumentAtSnapshot1755(t, col, oldSnap, []byte("e1"))
	if !found {
		t.Fatalf("old snapshot lost e1 after delete")
	}
	assertJSONEqualM13C(t, oldDoc, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
}

func TestTypedColumnColumnAssetRewriteRoundTripMixedRefs(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	dir := d.Dir()
	dClosed := false
	defer func() {
		if !dClosed {
			_ = d.Close()
		}
	}()
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	beforePhysical := columnManifestPhysicalAssetRefsForTestM1634(beforeRefs)
	beforeTyped := typedColumnPartRefs1755(beforeRefs)
	if len(beforePhysical) != 1 || len(beforeTyped) != 1 {
		t.Fatalf("before physical=%+v typed=%+v want one each all=%+v", beforePhysical, beforeTyped, beforeRefs)
	}
	candidate := writeTypedColumnAssetCandidate1755(t, d, col, 2, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	oldSegmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if rewrite.SegmentsRewritten != 1 || rewrite.RefsRemapped != len(beforeRefs) {
		t.Fatalf("rewrite stats=%+v want one rewritten segment and %d remapped refs", rewrite, len(beforeRefs))
	}
	afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if afterPhysical := columnManifestPhysicalAssetRefsForTestM1634(afterRefs); len(afterPhysical) != 1 || afterPhysical[0] == beforePhysical[0] {
		t.Fatalf("after physical=%+v before=%+v want remapped physical ref", afterPhysical, beforePhysical)
	}
	if afterTyped := typedColumnPartRefs1755(afterRefs); len(afterTyped) != 1 || afterTyped[0] == beforeTyped[0] {
		t.Fatalf("after typed=%+v before=%+v want remapped typed ref", afterTyped, beforeTyped)
	}
	got, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get after rewrite: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}
	dClosed = true

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	reopenedGot, err := reopenedCol.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("reopened Get e1 after rewrite: %v", err)
	}
	assertJSONEqualM13C(t, reopenedGot, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
	gcStats, err := reopenedCol.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), rewrite.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC: %v", err)
	}
	if gcStats.SegmentsDeleted != 1 || gcStats.BytesDeleted == 0 {
		t.Fatalf("gc stats=%+v want old mixed segment deleted", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old mixed segment stat err=%v want not exist", err)
	}
	afterGC, err := reopenedCol.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get after GC: %v", err)
	}
	assertJSONEqualM13C(t, afterGC, []byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`))
}

func TestTypedColumnPhysicalQueryFailsClosedForColumnPartFields(t *testing.T) {
	d, col, _ := setupSingleTypedColumnPart1755(t)
	defer func() { _ = d.Close() }()
	if _, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}); !errors.Is(err, ErrColumnQueryPlanUnsupported) {
		t.Fatalf("RunColumnPhysicalQuery typed-column group err=%v want ErrColumnQueryPlanUnsupported", err)
	}
	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery row-owned time_us: %v", err)
	}
	if result.Diagnostics.ReduceRows != 1 || result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReconstructionRows != 0 {
		t.Fatalf("row-owned query diagnostics=%+v want one direct physical row and no reconstruction", result.Diagnostics)
	}
	count := 0
	for _, group := range result.Groups {
		count += group.Count
	}
	if count != 1 {
		t.Fatalf("row-owned query total count=%d groups=%+v want 1", count, result.Groups)
	}
}

func TestTypedColumnPublicationExistingTypedRowCompatibility(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"row"}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if typedRefs := typedColumnPartRefs1755(refs); len(typedRefs) != 0 {
		t.Fatalf("existing typed-row config published typed-column refs=%+v", typedRefs)
	}
	if physicalRefs := columnManifestPhysicalAssetRefsForTestM1634(refs); len(physicalRefs) != 1 {
		t.Fatalf("physical refs=%+v want one", physicalRefs)
	}
	got, err := col.Get([]byte("e1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	assertJSONEqualM13C(t, got, []byte(`{"time_us":1,"kind":"like","did":"d1","payload":"row"}`))
}

func TestTypedColumnPartMappedResourceReadUsesColumnPartClass1755(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 {
		t.Fatalf("typed refs=%+v want one", typedRefs)
	}
	typedRef := typedRefs[0]
	manager := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(d.ColumnAssetRootDir(), col.Meta().Options.ColumnStore.AssetManager.Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	scope := mappedresource.Scope{
		Kind:       mappedresource.ScopeColumnPartReader,
		ID:         "typed-column-part-1755",
		Namespace:  typedRef.Namespace,
		Collection: "events",
		Generation: typedRef.Generation,
		Reason:     "typed-column-publication-test",
	}
	if err := readCache.useMappedResourceManager(manager, scope, "typed-column-part-read"); err != nil {
		_ = readCache.close()
		t.Fatalf("useMappedResourceManager: %v", err)
	}
	if _, err := readCache.read(typedRef, nil); err != nil {
		_ = readCache.close()
		t.Fatalf("read typed part: %v", err)
	}
	pins := readCache.mappedResourcePins()
	if len(pins) != 1 {
		_ = readCache.close()
		t.Fatalf("pins=%d want 1", len(pins))
	}
	pin := pins[0]
	if pin.Key.Class != mappedresource.ClassTypedColumnAsset || pin.Scope.Kind != mappedresource.ScopeColumnPartReader || pin.Reason != "typed-column-part-read" {
		_ = readCache.close()
		t.Fatalf("unexpected mappedresource pin: %+v", pin)
	}
	if err := readCache.close(); err != nil {
		t.Fatalf("close read cache: %v", err)
	}
	if pins := manager.PinSummary(); len(pins) != 0 {
		t.Fatalf("pins after close=%d want 0", len(pins))
	}
}

func TestTypedColumnPublicationUnsupportedValueFailsClosed(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{
		Name:      "embedding_neighbors",
		Path:      "embedding_neighbors",
		ValueType: ColumnStoreValueAdjacencyList,
		Owner:     TypedStorageOwnerColumnPart,
	}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	_, err = col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"embedding_neighbors":[1,2,3]}`)})
	if !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("InsertBatch error=%v want ErrCommandWALRejected", err)
	}
}

func typedColumnDocumentAtSnapshot1755(t testing.TB, col *Collection, snap *backenddb.Snapshot, id []byte) ([]byte, bool) {
	t.Helper()
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	if catalog == nil {
		t.Fatalf("catalogForSnapshot returned nil")
	}
	retained, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionPrimaryRootName(catalog.meta.Name), id, nil)
	if err != nil {
		t.Fatalf("collectionGetAppendAtCatalogRoot(%q): %v", string(id), err)
	}
	if !found {
		return nil, false
	}
	if !columnStoreCanReconstructDocument(catalog.meta) {
		return bytes.Clone(retained), true
	}
	reconstructed, err := col.reconstructColumnDocumentAtSnapshot(snap, catalog, id, retained)
	if err != nil {
		t.Fatalf("reconstructColumnDocumentAtSnapshot(%q): %v", string(id), err)
	}
	return reconstructed, true
}

func assertColumnAssetReachabilityEntry1755(t testing.TB, plan ColumnAssetReachabilityPlan, ref ColumnAssetRef, status ColumnAssetReachabilityStatus, wantPinned bool) {
	t.Helper()
	for _, entry := range plan.Entries {
		if entry.Ref != ref {
			continue
		}
		if entry.Status != status {
			t.Fatalf("entry %+v status=%s want %s plan=%+v", entry.Ref, entry.Status, status, plan)
		}
		if wantPinned && !columnAssetReachabilitySourcesContain1755(entry.Sources, ColumnAssetReachabilitySourcePinnedSnapshot) {
			t.Fatalf("entry %+v sources=%v want pinned_snapshot", entry.Ref, entry.Sources)
		}
		return
	}
	t.Fatalf("ref %+v not found in reachability entries=%+v", ref, plan.Entries)
}

func columnAssetReachabilitySourcesContain1755(sources []ColumnAssetReachabilitySource, want ColumnAssetReachabilitySource) bool {
	for _, source := range sources {
		if source == want {
			return true
		}
	}
	return false
}

func writeTypedColumnAssetCandidate1755(t testing.TB, d *backenddb.DB, col *Collection, generation, partID uint64) ColumnAssetRef {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column store config: %+v", cfg)
	}
	payload := []byte("typed-column-candidate-1755")
	ref, err := writeColumnAssetToManager(d.ColumnAssetRootDir(), *cfg, payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID)
	if err != nil {
		t.Fatalf("writeColumnAssetToManager candidate: %v", err)
	}
	if ref.Length != int64(len(payload)) {
		t.Fatalf("candidate ref length=%d want %d", ref.Length, len(payload))
	}
	return ref
}

func columnAssetRefsContain1755(refs []ColumnAssetRef, want ColumnAssetRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

func assertColumnAssetRefsEqual1755(t testing.TB, want, got []ColumnAssetRef) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("refs=%+v want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("refs[%d]=%+v want %+v (all got %+v)", i, got[i], want[i], got)
		}
	}
}

func setupSingleTypedColumnPart1755(t testing.TB) (*backenddb.DB, *Collection, ColumnAssetRef) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	col := createTypedColumnPartCollection1755(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1")}, [][]byte{[]byte(`{"time_us":1,"kind":"like","score":2.5,"flag":true}`)}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	typedRefs := typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col))
	if len(typedRefs) != 1 {
		_ = d.Close()
		t.Fatalf("typed refs=%+v want one", typedRefs)
	}
	return d, col, typedRefs[0]
}

func removeTypedColumnAssetPayload1755(t testing.TB, d *backenddb.DB, col *Collection, typedRef ColumnAssetRef) {
	t.Helper()
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), typedRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	for _, physicalRef := range columnManifestPhysicalAssetRefsForTestM1634(columnManifestAssetRefsForCollectionM12A(t, d, col)) {
		if physicalRef.FileID == typedRef.FileID && physicalRef.Offset+physicalRef.Length > typedRef.Offset {
			t.Fatalf("cannot remove typed payload without damaging physical row asset: physical=%+v typed=%+v", physicalRef, typedRef)
		}
	}
	if err := os.Truncate(path, typedRef.Offset); err != nil {
		t.Fatalf("truncate typed-column asset payload: %v", err)
	}
}

func corruptTypedColumnAssetPayload1755(t testing.TB, d *backenddb.DB, typedRef ColumnAssetRef) {
	t.Helper()
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), typedRef)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open typed-column asset segment: %v", err)
	}
	defer func() { _ = f.Close() }()
	buf := []byte{0}
	if _, err := f.ReadAt(buf, typedRef.Offset); err != nil {
		t.Fatalf("read typed-column asset byte: %v", err)
	}
	buf[0] ^= 0xff
	if _, err := f.WriteAt(buf, typedRef.Offset); err != nil {
		t.Fatalf("corrupt typed-column asset byte: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync corrupt typed-column asset: %v", err)
	}
}

type typedColumnExpectedRow1755 struct {
	PrimaryID int64
	Kind      string
	Score     float64
	Flag      bool
}

func createTypedColumnPartCollection1755(t testing.TB, d *backenddb.DB) *Collection {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{
		{Name: "time_us", Path: "time_us", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
		{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart},
		{Name: "score", Path: "score", ValueType: ColumnStoreValueDouble, Owner: TypedStorageOwnerColumnPart},
		{Name: "flag", Path: "flag", ValueType: ColumnStoreValueBool, Owner: TypedStorageOwnerColumnPart},
	}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func createTypedColumnVectorPartCollection1756(t testing.TB, d *backenddb.DB) *Collection {
	t.Helper()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, Owner: TypedStorageOwnerColumnPart, VectorDims: 3}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "vectors", Options: CollectionOptions{ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("vectors")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func assertTypedColumnManifestShape1755(t testing.TB, d *backenddb.DB, col *Collection, wantGeneration uint64, wantTypedParts int) {
	t.Helper()
	id, ok := col.ColumnStoreCacheIdentity()
	if !ok || id.ManifestRoot == 0 || id.ManifestGeneration != wantGeneration {
		t.Fatalf("ColumnStoreCacheIdentity=%+v ok=%v want generation=%d manifest root", id, ok, wantGeneration)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	physicalRefs := columnManifestPhysicalAssetRefsForTestM1634(refs)
	typedRefs := typedColumnPartRefs1755(refs)
	if len(physicalRefs) != int(wantGeneration) {
		t.Fatalf("physical refs=%+v want %d", physicalRefs, wantGeneration)
	}
	if len(typedRefs) != wantTypedParts {
		t.Fatalf("typed refs=%+v want %d all refs=%+v", typedRefs, wantTypedParts, refs)
	}
}

func assertTypedColumnLatestRows1755(t testing.TB, d *backenddb.DB, col *Collection, generation uint64, want []typedColumnExpectedRow1755) {
	t.Helper()
	var ref ColumnAssetRef
	found := false
	for _, candidate := range typedColumnPartRefs1755(columnManifestAssetRefsForCollectionM12A(t, d, col)) {
		if candidate.Generation == generation {
			ref = candidate
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing typed-column part for generation=%d", generation)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref)
	if err != nil {
		t.Fatalf("read typed-column part: %v", err)
	}
	part, err := typedColumnAdapterPartFromBytes(typedColumnAdapterOptions{Fields: columnStoreTypedColumnPartFields(*col.Meta().Options.ColumnStore)}, raw)
	if err != nil {
		t.Fatalf("typedColumnAdapterPartFromBytes: %v", err)
	}
	rows, err := part.scanRows()
	if err != nil {
		t.Fatalf("scanRows: %v", err)
	}
	if len(rows) != len(want) {
		t.Fatalf("typed rows=%d want %d", len(rows), len(want))
	}
	for i, row := range rows {
		if row.PrimaryID != want[i].PrimaryID {
			t.Fatalf("row[%d] primary_id=%d want %d", i, row.PrimaryID, want[i].PrimaryID)
		}
		kind := row.Values["kind"]
		score := row.Values["score"]
		flag := row.Values["flag"]
		if kind.String != want[i].Kind || score.Double != want[i].Score || flag.Bool != want[i].Flag {
			t.Fatalf("row[%d] values kind=%+v score=%+v flag=%+v want %+v", i, kind, score, flag, want[i])
		}
	}
}

func typedColumnPartRefs1755(refs []ColumnAssetRef) []ColumnAssetRef {
	out := make([]ColumnAssetRef, 0, len(refs))
	for _, ref := range refs {
		if ref.Kind == ColumnAssetKindTCS1TypedColumnPart {
			out = append(out, ref)
		}
	}
	return out
}
