package treedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/transactionowner"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func compactStorageUsageBytes(usages []CompactStorageUsage, name string) int64 {
	for _, u := range usages {
		if u.Name == name {
			return u.Bytes
		}
	}
	return 0
}

func compactStoragePhaseByName(phases []CompactStoragePhaseStats, name string) (CompactStoragePhaseStats, bool) {
	for _, p := range phases {
		if p.Name == name {
			return p, true
		}
	}
	return CompactStoragePhaseStats{}, false
}

func makeStandaloneCOWBSONDoc(t testing.TB, id string, round, score int, payload string) []byte {
	t.Helper()
	raw, err := bson.Marshal(bson.D{
		{Key: "_id", Value: id},
		{Key: "email", Value: fmt.Sprintf("user-%s-r%d@example.com", id, round)},
		{Key: "score", Value: int64(score)},
		{Key: "round", Value: int64(round)},
		{Key: "payload", Value: payload},
	})
	if err != nil {
		t.Fatalf("bson.Marshal(%s): %v", id, err)
	}
	return raw
}

func openStandaloneCOWBSONFixtureWithLeafTarget(t testing.TB, profile Profile, dir string, leafSegmentTargetBytes int64, singleLane bool) (*DB, *treedbdb.DB, *collections.CollectionManager) {
	t.Helper()
	opts := OptionsForSemantic(profile, dir)
	opts.IndexOuterLeavesInValueLog = true
	opts.IndexInternalBaseDelta = false
	opts.ValueLog.Compression = ValueLogCompressionOff
	if singleLane {
		opts.JournalLanes = 1
		opts.MemtableShards = 1
	}
	if leafSegmentTargetBytes > 0 {
		opts.ValueLog.Generational.LeafSegmentTargetBytes = leafSegmentTargetBytes
	}
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", profile, err)
	}
	backend, err := database.NativeSemanticBackend()
	if err != nil {
		_ = database.Close()
		t.Fatalf("NativeSemanticBackend(%s): %v", profile, err)
	}
	manager := collections.NewCollectionManager(backend)
	return database, backend, manager
}

func openStandaloneCOWBSONFixture(t testing.TB, profile Profile, dir string) (*DB, *treedbdb.DB, *collections.CollectionManager) {
	return openStandaloneCOWBSONFixtureWithLeafTarget(t, profile, dir, 64<<10, false)
}

func createStandaloneCOWBSONCollection(t testing.TB, manager *collections.CollectionManager, name string) *collections.Collection {
	t.Helper()
	meta := &collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{
			DocumentFormat: collections.DocumentFormatBSON,
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("CreateCollection(%s): %v", name, err)
	}
	col, err := manager.OpenCollection(name)
	if err != nil {
		t.Fatalf("OpenCollection(%s): %v", name, err)
	}
	if _, err := col.CreateIndex(collections.IndexDefinition{
		Name:      "idx_email",
		Field:     "email",
		ValueType: collections.IndexValueString,
	}); err != nil {
		t.Fatalf("CreateIndex(idx_email): %v", err)
	}
	if _, err := col.CreateIndex(collections.IndexDefinition{
		Name:      "idx_score",
		Field:     "score",
		ValueType: collections.IndexValueInt64,
	}); err != nil {
		t.Fatalf("CreateIndex(idx_score): %v", err)
	}
	return col
}

func populateStandaloneCOWBSONChurn(t testing.TB, database *DB, col *collections.Collection, docCount, rounds int) map[string]int {
	t.Helper()
	const batchSize = 64
	payloadA := strings.Repeat("a", 512)
	payloadB := strings.Repeat("b", 512)
	scores := make(map[string]int, docCount)

	for start := 0; start < docCount; start += batchSize {
		end := min(start+batchSize, docCount)
		ids := make([][]byte, 0, end-start)
		docs := make([][]byte, 0, end-start)
		for i := start; i < end; i++ {
			id := fmt.Sprintf("doc-%05d", i)
			score := i * 10
			scores[id] = score
			ids = append(ids, []byte(id))
			docs = append(docs, makeStandaloneCOWBSONDoc(t, id, 0, score, payloadA))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatalf("InsertBatch[%d:%d]: %v", start, end, err)
		}
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(seed): %v", err)
	}

	for round := 1; round <= rounds; round++ {
		payload := payloadB
		if round%2 == 0 {
			payload = payloadA
		}
		for start := 0; start < docCount; start += batchSize {
			end := min(start+batchSize, docCount)
			items := make([]collections.UpdateBatchItem, 0, end-start)
			for i := start; i < end; i++ {
				id := fmt.Sprintf("doc-%05d", i)
				score := i*10 + round
				scores[id] = score
				doc := makeStandaloneCOWBSONDoc(t, id, round, score, payload)
				items = append(items, collections.UpdateBatchItem{
					DocumentID: []byte(id),
					Update: func([]byte) ([]byte, bool, error) {
						return doc, true, nil
					},
				})
			}
			if _, err := col.UpdateBatch(items); err != nil {
				t.Fatalf("UpdateBatch(round=%d [%d:%d]): %v", round, start, end, err)
			}
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(round=%d): %v", round, err)
		}
	}
	return scores
}

func verifyStandaloneCOWBSONState(t testing.TB, col *collections.Collection, docCount, lastRound int, scores map[string]int, extraID string) {
	t.Helper()
	for i := 0; i < docCount; i++ {
		id := fmt.Sprintf("doc-%05d", i)
		raw, err := col.Get([]byte(id))
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		var doc struct {
			ID    string `bson:"_id"`
			Email string `bson:"email"`
			Score int64  `bson:"score"`
			Round int64  `bson:"round"`
		}
		if err := bson.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("bson.Unmarshal(%s): %v", id, err)
		}
		wantEmail := fmt.Sprintf("user-%s-r%d@example.com", id, lastRound)
		if doc.ID != id || doc.Email != wantEmail || int(doc.Score) != scores[id] || int(doc.Round) != lastRound {
			t.Fatalf("doc %s mismatch: got %+v want email=%s score=%d round=%d", id, doc, wantEmail, scores[id], lastRound)
		}
	}
	// Verify equality secondary index lookup.
	probeID := fmt.Sprintf("doc-%05d", docCount/2)
	probeEmail := fmt.Sprintf("user-%s-r%d@example.com", probeID, lastRound)
	matched, err := col.FindByIndex("idx_email", probeEmail)
	if err != nil {
		t.Fatalf("FindByIndex(idx_email, %s): %v", probeEmail, err)
	}
	if len(matched) != 1 || string(matched[0]) != probeID {
		t.Fatalf("FindByIndex(idx_email, %s)=%q want [%s]", probeEmail, matched, probeID)
	}
	// Verify range secondary index lookup.
	lowScore := int64(10*10 + lastRound)
	highScore := int64(19*10 + lastRound)
	rangeIDs, _, err := col.FindByIndexRange("idx_score", collections.IndexRangeOptions{
		Lower: collections.IndexRangeBound{Value: lowScore, Inclusive: true},
		Upper: collections.IndexRangeBound{Value: highScore, Inclusive: true},
	})
	if err != nil {
		t.Fatalf("FindByIndexRange(idx_score): %v", err)
	}
	if len(rangeIDs) != 10 {
		t.Fatalf("FindByIndexRange(idx_score) returned %d ids want 10", len(rangeIDs))
	}
	if extraID != "" {
		raw, err := col.Get([]byte(extraID))
		if err != nil {
			t.Fatalf("Get(%s): %v", extraID, err)
		}
		if !bytes.Contains(raw, []byte(extraID)) {
			t.Fatalf("extra doc %s corrupt: %q", extraID, raw)
		}
		extraEmail := fmt.Sprintf("user-%s-r%d@example.com", extraID, lastRound+1)
		got, err := col.FindByIndex("idx_email", extraEmail)
		if err != nil || len(got) != 1 || string(got[0]) != extraID {
			t.Fatalf("FindByIndex(idx_email, %s)=%q err=%v want [%s]", extraEmail, got, err, extraID)
		}
	}
}

func TestCompactStorageFullStandaloneCOWReclaimsLeafVlogAndSurvivesReopen(t *testing.T) {
	profiles := []Profile{ProfileCommandWALRelaxed, ProfileNoWALFast}
	for _, profile := range profiles {
		t.Run(string(profile), func(t *testing.T) {
			t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
			dir := t.TempDir()
			// Use a 16 KiB single-lane leaf target with 32 documents overwritten
			// cleanly each round (with compression off) so multiple sealed
			// generations become wholly dead (100% reclaimable via
			// leaf-generation-gc + post-refresh settlement on every OS, including
			// platforms without renameat2(RENAME_NOREPLACE)).
			database, _, manager := openStandaloneCOWBSONFixtureWithLeafTarget(t, profile, dir, 16<<10, true)
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = manager.Close()
					_ = database.Close()
				}
			})

			col := createStandaloneCOWBSONCollection(t, manager, "users")
			const (
				docCount = 32
				rounds   = 24
			)
			scores := populateStandaloneCOWBSONChurn(t, database, col, docCount, rounds)

			ctx := context.Background()
			stats, err := database.CompactStorage(ctx, CompactStorageOptions{
				Mode:          CompactStorageFull,
				SyncEachPhase: true,
			})
			if err != nil {
				t.Fatalf("CompactStorage(Full) on standalone cow_btree (%s) failed: %v", profile, err)
			}

			leafBefore := compactStorageUsageBytes(stats.Before, "leaf_vlog")
			leafAfter := compactStorageUsageBytes(stats.After, "leaf_vlog")
			if leafBefore == 0 {
				t.Fatalf("expected non-zero leaf_vlog bytes before compaction, before=%+v", stats.Before)
			}
			if stats.LeafGenerationGC.FilesDeleted == 0 {
				t.Fatalf("expected LeafGenerationGC.FilesDeleted > 0, got %+v (packs=%+v phases=%+v)", stats.LeafGenerationGC, stats.LeafGenerationPacks, stats.Phases)
			}
			if leafAfter >= leafBefore {
				t.Fatalf("expected leaf_vlog bytes to shrink: before=%d after=%d", leafBefore, leafAfter)
			}
			if phase, ok := compactStoragePhaseByName(stats.Phases, "index-vacuum"); ok && phase.Required {
				if phase.Status != CompactStoragePhaseStatusUnsupported {
					t.Fatalf("index-vacuum phase=%+v want unsupported on standalone COW", phase)
				}
			}

			// Verify live reads, secondary indexes, and subsequent writes work cleanly.
			verifyStandaloneCOWBSONState(t, col, docCount, rounds, scores, "")
			extraID := "doc-post-compact"
			extraDoc := makeStandaloneCOWBSONDoc(t, extraID, rounds+1, 999999, strings.Repeat("z", 512))
			if _, err := col.Insert([]byte(extraID), extraDoc); err != nil {
				t.Fatalf("post-compact Insert: %v", err)
			}
			if err := database.Checkpoint(); err != nil {
				t.Fatalf("post-compact Checkpoint: %v", err)
			}
			verifyStandaloneCOWBSONState(t, col, docCount, rounds, scores, extraID)

			// Close and reopen to prove 100% pointer durability.
			if err := manager.Close(); err != nil {
				t.Fatalf("manager.Close: %v", err)
			}
			if err := database.Close(); err != nil {
				t.Fatalf("database.Close: %v", err)
			}
			closed = true

			reopenedDB, _, reopenedMgr := openStandaloneCOWBSONFixtureWithLeafTarget(t, profile, dir, 16<<10, true)
			defer func() {
				_ = reopenedMgr.Close()
				_ = reopenedDB.Close()
			}()
			reopenedCol, err := reopenedMgr.OpenCollection("users")
			if err != nil {
				t.Fatalf("reopened OpenCollection(users): %v", err)
			}
			verifyStandaloneCOWBSONState(t, reopenedCol, docCount, rounds, scores, extraID)
		})
	}
}

func TestCompactStorageFullStandaloneCOWLeafPackPromotionAndReopen(t *testing.T) {
	requireLeafGenerationPackPromotionSupport(t)
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	dir := t.TempDir()
	database, _, manager := openStandaloneCOWBSONFixture(t, ProfileCommandWALRelaxed, dir)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = manager.Close()
			_ = database.Close()
		}
	})

	col := createStandaloneCOWBSONCollection(t, manager, "users")
	const (
		docCount = 256
		rounds   = 6
	)
	scores := populateStandaloneCOWBSONChurn(t, database, col, docCount, rounds)

	stats, err := database.CompactStorage(context.Background(), CompactStorageOptions{
		Mode:          CompactStorageFull,
		SyncEachPhase: true,
	})
	if err != nil {
		t.Fatalf("CompactStorage(Full) with leaf-generation pack failed: %v", err)
	}
	var packRan bool
	for _, p := range stats.LeafGenerationPacks {
		if p.Ran && p.Pack.BytesCopied > 0 {
			packRan = true
			break
		}
	}
	if !packRan {
		t.Fatalf("expected at least one leaf-generation pack pass to run, packs=%+v", stats.LeafGenerationPacks)
	}
	leafBefore := compactStorageUsageBytes(stats.Before, "leaf_vlog")
	leafAfter := compactStorageUsageBytes(stats.After, "leaf_vlog")
	if stats.LeafGenerationGC.FilesDeleted == 0 || leafAfter >= leafBefore {
		t.Fatalf("expected packed leaf_vlog reclamation: files_deleted=%d before=%d after=%d", stats.LeafGenerationGC.FilesDeleted, leafBefore, leafAfter)
	}
	verifyStandaloneCOWBSONState(t, col, docCount, rounds, scores, "")

	if err := manager.Close(); err != nil {
		t.Fatalf("manager.Close: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("database.Close: %v", err)
	}
	closed = true

	reopenedDB, _, reopenedMgr := openStandaloneCOWBSONFixture(t, ProfileCommandWALRelaxed, dir)
	defer func() {
		_ = reopenedMgr.Close()
		_ = reopenedDB.Close()
	}()
	reopenedCol, err := reopenedMgr.OpenCollection("users")
	if err != nil {
		t.Fatalf("reopened OpenCollection(users): %v", err)
	}
	verifyStandaloneCOWBSONState(t, reopenedCol, docCount, rounds, scores, "")
}

func TestCompactStorageFullStandaloneCOWIndexVacuumRequiredDebtUnsupported(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	dir := t.TempDir()
	// Keep a large single-lane leaf segment target (256 MiB) so no
	// partially-live sealed leaf generation requires Linux pack promotion; this
	// test specifically exercises IndexVacuumRequired == true on both macOS and
	// Linux.
	database, _, manager := openStandaloneCOWBSONFixtureWithLeafTarget(t, ProfileCommandWALRelaxed, dir, 256<<20, true)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = manager.Close()
			_ = database.Close()
		}
	})

	// Create 4 indexed collections (12 internal B-tree roots) and churn across
	// checkpoints so CollectionRootPages >= 16 and CollectionRootSpanRatio >= 1.2x.
	const numCollections = 4
	const totalDocs = 2000
	const keepDocs = 1200
	payload := strings.Repeat("p", 384)
	cols := make([]*collections.Collection, 0, numCollections)
	for c := 0; c < numCollections; c++ {
		cols = append(cols, createStandaloneCOWBSONCollection(t, manager, fmt.Sprintf("churn_%d", c)))
	}
	for c, col := range cols {
		for start := 0; start < totalDocs; start += 200 {
			end := min(start+200, totalDocs)
			ids := make([][]byte, 0, end-start)
			docs := make([][]byte, 0, end-start)
			for i := start; i < end; i++ {
				id := fmt.Sprintf("c%d-k-%05d", c, i)
				ids = append(ids, []byte(id))
				docs = append(docs, makeStandaloneCOWBSONDoc(t, id, 0, i, payload))
			}
			if _, err := col.InsertBatch(ids, docs); err != nil {
				t.Fatalf("InsertBatch(c=%d [%d:%d]): %v", c, start, end, err)
			}
		}
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(insert): %v", err)
	}
	for c, col := range cols {
		for start := keepDocs; start < totalDocs; start += 200 {
			end := min(start+200, totalDocs)
			ids := make([][]byte, 0, end-start)
			for i := start; i < end; i++ {
				ids = append(ids, []byte(fmt.Sprintf("c%d-k-%05d", c, i)))
			}
			if _, err := col.DeleteBatch(ids); err != nil {
				t.Fatalf("DeleteBatch(c=%d [%d:%d]): %v", c, start, end, err)
			}
		}
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint(delete): %v", err)
	}
	for round := 1; round <= 2; round++ {
		for c, col := range cols {
			for start := 0; start < keepDocs; start += 200 {
				end := min(start+200, keepDocs)
				items := make([]collections.UpdateBatchItem, 0, end-start)
				for i := start; i < end; i++ {
					id := fmt.Sprintf("c%d-k-%05d", c, i)
					doc := makeStandaloneCOWBSONDoc(t, id, round, i*10+round, payload)
					items = append(items, collections.UpdateBatchItem{
						DocumentID: []byte(id),
						Update: func([]byte) ([]byte, bool, error) {
							return doc, true, nil
						},
					})
				}
				if _, err := col.UpdateBatch(items); err != nil {
					t.Fatalf("UpdateBatch(c=%d round=%d [%d:%d]): %v", c, round, start, end, err)
				}
			}
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint(round=%d): %v", round, err)
		}
	}
	col := cols[0]

	ctx := context.Background()
	plan, err := database.CompactStoragePlan(ctx, CompactStorageOptions{Mode: CompactStorageFull})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	if !plan.RemainingDebt.IndexVacuumRequired {
		t.Fatalf("expected IndexVacuumRequired==true debt before compaction, debt=%+v", plan.RemainingDebt)
	}

	compactOpts := CompactStorageOptions{
		Mode:          CompactStorageFull,
		SyncEachPhase: true,
	}
	if !rootpublication.StableRelativeNamespaceSupported() || !rootpublication.StableCrossParentMoveNoReplaceSupported() {
		// On platforms without renameat2(RENAME_NOREPLACE) leaf-pack promotion,
		// skip partially-live leaf packing so this test still exercises the real
		// IndexVacuumRequired == true -> ErrVacuumUnsupported path end-to-end.
		compactOpts.LeafPackMinExpectedReclaimBytes = 1 << 40
	}
	stats, err := database.CompactStorage(ctx, compactOpts)
	if err != nil {
		t.Fatalf("CompactStorage(Full) with IndexVacuumRequired debt failed: %v", err)
	}
	phase, ok := compactStoragePhaseByName(stats.Phases, "index-vacuum")
	if !ok {
		t.Fatalf("missing index-vacuum phase in report: %+v", stats.Phases)
	}
	if !phase.Required || phase.Status != CompactStoragePhaseStatusUnsupported || !phase.Skipped {
		t.Fatalf("index-vacuum phase=%+v want required+unsupported+skipped", phase)
	}
	if !strings.Contains(phase.Reason, treedbdb.ErrVacuumUnsupported.Error()) {
		t.Fatalf("index-vacuum reason=%q want %q", phase.Reason, treedbdb.ErrVacuumUnsupported)
	}

	// Prove db.acceptedPrefix and live publication remain intact after compaction.
	afterID := "k-after-vacuum-unsupported"
	afterDoc := makeStandaloneCOWBSONDoc(t, afterID, 1, 777777, payload)
	if _, err := col.Insert([]byte(afterID), afterDoc); err != nil {
		t.Fatalf("post-compact Insert: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("post-compact Checkpoint: %v", err)
	}
	if got, err := col.Get([]byte(afterID)); err != nil || !bytes.Contains(got, []byte(afterID)) {
		t.Fatalf("post-compact Get(%s): got=%q err=%v", afterID, got, err)
	}
	if got, err := col.Get([]byte("c0-k-00000")); err != nil || !bytes.Contains(got, []byte("c0-k-00000")) {
		t.Fatalf("surviving Get(c0-k-00000): got=%q err=%v", got, err)
	}

	if err := manager.Close(); err != nil {
		t.Fatalf("manager.Close: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("database.Close: %v", err)
	}
	closed = true

	reopenedDB, _, reopenedMgr := openStandaloneCOWBSONFixtureWithLeafTarget(t, ProfileCommandWALRelaxed, dir, 256<<20, true)
	defer func() {
		_ = reopenedMgr.Close()
		_ = reopenedDB.Close()
	}()
	reopenedCol, err := reopenedMgr.OpenCollection("churn_0")
	if err != nil {
		t.Fatalf("reopened OpenCollection(churn): %v", err)
	}
	if got, err := reopenedCol.Get([]byte(afterID)); err != nil || !bytes.Contains(got, []byte(afterID)) {
		t.Fatalf("reopened Get(%s): got=%q err=%v", afterID, got, err)
	}
}

func TestCompactStorageStandaloneCOWFailsClosedOnActiveTypedColumnGraph(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	opts := indexedSliceOptions(ProfileCommandWALDurable, t.TempDir())
	database, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		_ = database.Close()
		t.Fatalf("NativeSemanticBackend: %v", err)
	}
	manager := collections.NewCollectionManager(handle)
	t.Cleanup(func() {
		_ = manager.Close()
		_ = database.Close()
	})

	fixture := nativeColumnPendingCaseFor(t, "string")
	fixture.column.Owner = collections.TypedStorageOwnerRowAsset
	fixture.typedSeed = &collections.TypedColumnBatch{}
	if _, err := manager.CreateCollection(nativeColumnPendingMeta(fixture)); err != nil {
		t.Fatalf("CreateCollection(typed): %v", err)
	}
	col, err := manager.OpenCollection("events")
	if err != nil {
		t.Fatalf("OpenCollection(events): %v", err)
	}
	a, b, c := nativeTypedOpsRows("string")
	ids, retained, columns := nativeTypedOpsInput("string", a, b, c)
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatalf("InsertTypedBatchWithStats: %v", err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	beforeToken, live := handle.StateToken()
	if !live {
		t.Fatal("missing backend StateToken before CompactStorage")
	}

	_, err = database.CompactStorage(context.Background(), CompactStorageOptions{
		Mode: CompactStorageFull,
	})
	if !errors.Is(err, treedbdb.ErrNativeSemanticUnsupported) {
		t.Fatalf("CompactStorage on active typed column-graph collection err=%v want %v", err, treedbdb.ErrNativeSemanticUnsupported)
	}

	afterToken, live := handle.StateToken()
	if !live {
		t.Fatal("missing backend StateToken after refused CompactStorage")
	}
	if afterToken != beforeToken {
		t.Fatalf("refused CompactStorage mutated storage state: before=%+v after=%+v", beforeToken, afterToken)
	}

	// Verify DB is not poisoned and continues serving reads and writes.
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView after refusal: %v", err)
	}
	defer view.Close()
	got, err := view.FetchDocumentsByID(ids, collections.DocumentFetchOptions{})
	if err != nil || len(got.Results) != 3 {
		t.Fatalf("FetchDocumentsByID after refusal: results=%+v err=%v", got.Results, err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint after refusal: %v", err)
	}
}

func TestCompactStorageForPrivateCOWRejectsNilAndWrongOwner(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	dir := t.TempDir()
	database, backend, manager := openStandaloneCOWBSONFixture(t, ProfileCommandWALRelaxed, dir)
	defer func() {
		_ = manager.Close()
		_ = database.Close()
	}()

	ctx := context.Background()
	if _, err := backend.CompactStorageForPrivateCOW(nil, ctx, treedbdb.CompactStorageOptions{Mode: treedbdb.CompactStorageFull}); !errors.Is(err, treedbdb.ErrNativeSemanticUnsupported) {
		t.Fatalf("CompactStorageForPrivateCOW(nil) err=%v want %v", err, treedbdb.ErrNativeSemanticUnsupported)
	}
	if _, err := backend.CompactStorageForPrivateCOW(transactionowner.New(), ctx, treedbdb.CompactStorageOptions{Mode: treedbdb.CompactStorageFull}); !errors.Is(err, treedbdb.ErrNativeSemanticUnsupported) {
		t.Fatalf("CompactStorageForPrivateCOW(wrongOwner) err=%v want %v", err, treedbdb.ErrNativeSemanticUnsupported)
	}
}

func TestSettleCompactStorageAfterReadBasisRefreshRunsWhenLeafPackRan(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	dir := t.TempDir()
	database, backend, manager := openStandaloneCOWBSONFixtureWithLeafTarget(t, ProfileCommandWALRelaxed, dir, 16<<10, true)
	defer func() {
		_ = manager.Close()
		_ = database.Close()
	}()

	col := createStandaloneCOWBSONCollection(t, manager, "settle_pack")
	populateStandaloneCOWBSONChurn(t, database, col, 32, 24)

	// Simulate the state immediately after a Linux leaf-generation-pack where
	// in-loop LeafGenerationGC ran while the old COW cut / fallback still
	// protected the pre-pack generations (so GenerationsRetiring==0 and
	// BytesDeleted==0), while LeafGenerationPacks records a real pack pass.
	stats := treedbdb.CompactStorageStats{
		Mode: treedbdb.CompactStorageFull,
		LeafGenerationPacks: []treedbdb.LeafGenerationPackRunOnceStats{
			{
				Ran: true,
				Pack: treedbdb.LeafGenerationPackStats{
					BytesCopied:     4096,
					LeafPagesCopied: 1,
				},
			},
		},
	}
	ctx := context.Background()
	if err := backend.SettleCompactStorageAfterReadBasisRefresh(ctx, treedbdb.CompactStorageOptions{Mode: treedbdb.CompactStorageFull}, &stats); err != nil {
		t.Fatalf("SettleCompactStorageAfterReadBasisRefresh: %v", err)
	}
	if _, ok := compactStoragePhaseByName(stats.Phases, "leaf-generation-gc-after-read-basis-refresh"); !ok {
		t.Fatalf("expected leaf-generation-gc-after-read-basis-refresh phase when leaf pack ran, phases=%+v", stats.Phases)
	}
	if stats.LeafGenerationGC.BytesDeleted == 0 && stats.LeafGenerationGC.FilesDeleted == 0 {
		t.Fatalf("expected post-refresh settle GC to delete dead leaf generations, gc=%+v", stats.LeafGenerationGC)
	}
	if len(stats.After) == 0 {
		t.Fatalf("expected stats.After to be refreshed after settle, got %+v", stats.After)
	}
}

func BenchmarkCompactStorageFullStandaloneCOW(b *testing.B) {
	b.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	const (
		docCount = 32
		rounds   = 24
	)
	var (
		totalLeafBefore    int64
		totalLeafAfter     int64
		totalLeafReclaimed int64
		totalFilesDeleted  int64
	)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		database, _, manager := openStandaloneCOWBSONFixtureWithLeafTarget(b, ProfileCommandWALRelaxed, dir, 16<<10, true)
		col := createStandaloneCOWBSONCollection(b, manager, "bench_users")
		scores := populateStandaloneCOWBSONChurn(b, database, col, docCount, rounds)
		b.StartTimer()

		stats, err := database.CompactStorage(context.Background(), CompactStorageOptions{
			Mode:          CompactStorageFull,
			SyncEachPhase: true,
		})
		b.StopTimer()
		if err != nil {
			_ = manager.Close()
			_ = database.Close()
			b.Fatalf("CompactStorage(Full): %v", err)
		}
		leafBefore := compactStorageUsageBytes(stats.Before, "leaf_vlog")
		leafAfter := compactStorageUsageBytes(stats.After, "leaf_vlog")
		totalLeafBefore += leafBefore
		totalLeafAfter += leafAfter
		if leafBefore > leafAfter {
			totalLeafReclaimed += leafBefore - leafAfter
		}
		totalFilesDeleted += int64(stats.LeafGenerationGC.FilesDeleted)

		if err := manager.Close(); err != nil {
			_ = database.Close()
			b.Fatalf("manager.Close: %v", err)
		}
		if err := database.Close(); err != nil {
			b.Fatalf("database.Close: %v", err)
		}
		if i == b.N-1 {
			// Verify pointer durability across reopen on final iteration.
			reopenedDB, _, reopenedMgr := openStandaloneCOWBSONFixtureWithLeafTarget(b, ProfileCommandWALRelaxed, dir, 16<<10, true)
			reopenedCol, err := reopenedMgr.OpenCollection("bench_users")
			if err != nil {
				_ = reopenedMgr.Close()
				_ = reopenedDB.Close()
				b.Fatalf("reopened OpenCollection: %v", err)
			}
			verifyStandaloneCOWBSONState(b, reopenedCol, docCount, rounds, scores, "")
			if err := reopenedMgr.Close(); err != nil {
				_ = reopenedDB.Close()
				b.Fatalf("reopenedMgr.Close: %v", err)
			}
			if err := reopenedDB.Close(); err != nil {
				b.Fatalf("reopenedDB.Close: %v", err)
			}
		}
	}
	if b.N > 0 {
		b.ReportMetric(float64(totalLeafBefore)/float64(b.N), "leaf_before_bytes/op")
		b.ReportMetric(float64(totalLeafAfter)/float64(b.N), "leaf_after_bytes/op")
		b.ReportMetric(float64(totalLeafReclaimed)/float64(b.N), "leaf_reclaimed_bytes/op")
		b.ReportMetric(float64(totalFilesDeleted)/float64(b.N), "leaf_files_deleted/op")
	}
}
