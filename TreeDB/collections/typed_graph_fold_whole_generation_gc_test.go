package collections

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestTypedGraphPublicFoldReclaimsWholeGenerationsWithoutPack(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	ctx := context.Background()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{
		RequiredFeatures:           []string{backenddb.RequiredFeatureCommandWALV1},
		DurabilityProfile:          backenddb.ProfileCommandWALDurable,
		IndexOuterLeavesInValueLog: true,
	}); err != nil {
		t.Fatal(err)
	}
	open := func() (*backenddb.DB, backenddb.LeafPageLogCloser) {
		t.Helper()
		db, err := backenddb.Open(backenddb.Options{
			Dir: dir, DisableBackgroundPrune: true, CommandWAL: true,
			ResolvedProfile:            backenddb.ProfileCommandWALDurable,
			IndexOuterLeavesInValueLog: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		// Each nonempty append rotates. With this tiny collection, obsolete
		// leaves occupy wholly dead generations rather than mixed pack debt.
		log, err := backenddb.NewStandaloneLeafPageLog(dir, backenddb.StandaloneLeafPageLogOptions{
			MaxSegmentBytes: 1, Compression: backenddb.ValueLogCompressionOff,
		})
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		db.SetLeafPageLog(log)
		return db, log
	}
	db, log := open()
	defer func() {
		_ = db.Close()
		_ = log.Close()
	}()
	meta := typedMinimaCollectionMeta()
	manager := NewCollectionManager(db)
	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	const index = "embedding_graph"
	ids, retained := make([][]byte, 4), make([][]byte, 4)
	columns := []TypedColumnBatch{{Name: "embedding"}, {Name: "content"}, {Name: "user"}, {Name: "path"}}
	for i := range ids {
		ids[i] = []byte(fmt.Sprintf("row-%d", i))
		retained[i] = []byte(fmt.Sprintf(`{"id":%q}`, ids[i]))
		columns[0].Float32Vectors = append(columns[0].Float32Vectors, vectorBenchmarkEmbedding(i, 8))
		columns[1].Strings = append(columns[1].Strings, "original")
		columns[2].Strings = append(columns[2].Strings, "tenant")
		columns[3].Strings = append(columns[3].Strings, "old")
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex(index); err != nil {
		t.Fatal(err)
	}
	opts := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(ctx, index, opts); err != nil {
		t.Fatal(err)
	}
	changed := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: columns[0].Float32Vectors[:1]},
		{Name: "content", Strings: []string{"after-fold"}},
		{Name: "user", Strings: []string{"tenant"}},
		{Name: "path", Strings: []string{"new"}},
	}
	if _, err := col.ReplaceTypedBatch(ids[:1], retained[:1], changed); err != nil {
		t.Fatal(err)
	}
	limits := backenddb.LeafGenerationMaintenanceLimits{
		NativeEntries: opts.Maintenance.NativeEntries,
		NativeBytes:   opts.Maintenance.NativeBytes,
		PagerPages:    opts.Maintenance.PagerPages,
	}
	var deadPaths []string
	typedGraphPublicationAfterAcceptedHook.Lock()
	typedGraphPublicationAfterAcceptedHook.foldAfterInstall = func(c *Collection) {
		if c != col {
			return
		}
		// Observe the real post-publication plan after sealing, without
		// manufacturing a plan or reclaiming any files in the hook.
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		plan, err := db.LeafGenerationPlan(ctx, backenddb.LeafGenerationPlanOptions{MaintenanceLimits: limits})
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Candidates) != 0 || plan.Admission != "no_candidates" {
			t.Fatalf("fixture requires whole-generation-only debt: %+v", plan)
		}
		for _, generation := range plan.Generations {
			if generation.SkipReason != "whole_generation_gc_candidate" {
				continue
			}
			if generation.BytesLive != 0 || generation.BytesDead <= 0 {
				t.Fatalf("invalid whole-dead generation: %+v", generation)
			}
			for _, rawID := range generation.FileIDs {
				path := valuelog.SegmentPath(backenddb.LeafLogDirPath(dir), page.ValueLogFileID(rawID))
				if _, err := os.Stat(path); err != nil {
					t.Fatal(err)
				}
				deadPaths = append(deadPaths, path)
			}
		}
		if len(deadPaths) == 0 {
			t.Fatal("fixture produced no whole-dead native generations")
		}
		// The same bounded pack admission used by Fold is a no-op. GC must
		// not depend on Ran being true; no copy pass is necessary here.
		packed, err := db.LeafGenerationPackRunOnce(ctx, backenddb.LeafGenerationPackFromPlanOptions{
			Sync: true, MaxGenerations: opts.Maintenance.NativeEntries,
			MaxBytesToCopy: opts.Maintenance.NativeBytes, MaintenanceLimits: limits,
		})
		if err != nil || packed.Ran {
			t.Fatalf("whole-generation-only pack: ran=%v err=%v", packed.Ran, err)
		}
	}
	typedGraphPublicationAfterAcceptedHook.Unlock()
	defer func() {
		typedGraphPublicationAfterAcceptedHook.Lock()
		typedGraphPublicationAfterAcceptedHook.foldAfterInstall = nil
		typedGraphPublicationAfterAcceptedHook.Unlock()
	}()
	if err := col.FoldColumnGraphServing(ctx, index); err != nil {
		t.Fatal(err)
	}
	if len(deadPaths) == 0 {
		t.Fatal("missing post-install observation")
	}
	for _, path := range deadPaths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("whole-dead generation was not reclaimed: %s: %v", path, err)
		}
	}
	assertReadable := func(c *Collection) {
		t.Helper()
		var buffer VectorIndexSearchBuffer
		response, view, err := c.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
			IndexName: index, Query: columns[0].Float32Vectors[0], TopK: 1, EfSearch: 8,
			StatsMode:            VectorIndexSearchStatsModeMinimal,
			DeclaredScalarFilter: &HybridScalarFilter{IndexName: "path", Value: "new"},
		}, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		defer view.Close()
		docs, err := view.FetchDocumentsForVectorIndexSearchResults(response.Results, DocumentFetchOptions{})
		if err != nil || len(docs.Results) != 1 || !bytes.Equal(docs.Results[0].ID, ids[0]) || !bytes.Contains(docs.Results[0].Document, []byte("after-fold")) {
			t.Fatalf("current folded document: %+v err=%v", docs.Results, err)
		}
	}
	assertReadable(col)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	db, log = open()
	col, err = NewCollectionManager(db).OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(ctx, index, opts); err != nil {
		t.Fatal(err)
	}
	assertReadable(col)
}
