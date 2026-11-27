package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func scalarTextValidatedProjectionMeta(dims int, retainedPayload ColumnRetainedPayloadPolicy) CollectionMeta {
	return CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat:                   DocumentFormatJSON,
			AllowArrayValuesInIndex:          true,
			DisableBufferedIndexedAsyncFlush: true,
			ColumnStore: &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{
					Name:       "embedding",
					Path:       "embedding",
					Owner:      TypedStorageOwnerColumnPart,
					ValueType:  ColumnStoreValueFloat32Vector,
					VectorDims: dims,
				}},
				RetainedPayload:         retainedPayload,
				RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
			},
		},
		Indexes: []IndexDefinition{
			{Name: "kind", Field: "kind", ValueType: IndexValueString},
			{Name: "rank", Field: "rank", ValueType: IndexValueInt64},
			{Name: "active", Field: "active", ValueType: IndexValueBool},
			{Name: "tags", Field: "tags", ValueType: IndexValueString, MultiKey: true},
		},
		TextIndexes: []TextIndexDefinition{{
			Name:     "content_text",
			Fields:   []TextIndexField{{Field: "content"}},
			Analyzer: TextAnalyzerSimple,
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: dims,
			M:          4,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	}
}

func TestInsertBatchValidatedFloat32ProjectionWithScalarAndTextIndexes(t *testing.T) {
	openProfileDB := func(t *testing.T, dir string, durableProfile, initFormat bool) (*backenddb.DB, func() error) {
		t.Helper()
		if durableProfile {
			opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
			d, cleanup, err := treedb.OpenBackend(opts)
			if err != nil {
				t.Fatalf("OpenBackend(ProfileCommandWALDurable): %v", err)
			}
			return d, cleanup
		}
		if initFormat {
			if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{
				RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1},
			}); err != nil {
				t.Fatalf("SaveFormatConfig: %v", err)
			}
		}
		d := openCollectionCommandWALDB(t, dir)
		return d, d.Close
	}

	for _, tc := range []struct {
		name           string
		durableProfile bool
	}{
		{name: "default_command_wal", durableProfile: false},
		{name: "profile_command_wal_durable", durableProfile: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			d, cleanup := openProfileDB(t, dir, tc.durableProfile, true)
			closed := false
			defer func() {
				if !closed {
					_ = cleanup()
				}
			}()

			meta := scalarTextValidatedProjectionMeta(4, ColumnRetainedPayloadNonColumn)
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&meta); err != nil {
				t.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}

			// Snapshot base directory before inserts for CommandWAL crash-recovery replay check.
			if err := d.Checkpoint(); err != nil {
				t.Fatalf("pre-insert Checkpoint: %v", err)
			}
			replayDir := t.TempDir()
			if !tc.durableProfile {
				copyColumnStoreCommandWALReplayBenchmarkDirM10C(t, dir, replayDir)
			}

			ids := [][]byte{[]byte("doc-2"), []byte("doc-1"), []byte("doc-3")}
			retainedDocs := [][]byte{
				[]byte(`{"id":"doc-2","kind":"issue","rank":20,"active":false,"tags":["go","storage"],"content":"treedb wal durability recovery"}`),
				[]byte(`{"id":"doc-1","kind":"code","rank":10,"active":true,"tags":["go","vector"],"content":"fast vector projection scalar index"}`),
				[]byte(`{"id":"doc-3","kind":"code","rank":30,"active":true,"tags":["docs","vector"],"content":"hybrid text vector search guide"}`),
			}
			vecs := [][]float32{
				{0, 1, 0, 0},
				{1, 0, 0, 0},
				{0, 0, 1, 0},
			}

			gotIDs, stats, err := col.InsertBatchWithStatsValidatedFloat32Projection(
				ids, retainedDocs, "embedding", VectorMetricCosine, vecs, retainedDocs,
			)
			if err != nil {
				t.Fatalf("InsertBatchWithStatsValidatedFloat32Projection: %v", err)
			}
			if len(gotIDs) != len(ids) {
				t.Fatalf("gotIDs=%d want %d", len(gotIDs), len(ids))
			}
			if stats.ColumnPublishValidatedFloat32ProjectionRows != len(ids) || stats.ColumnPublishDocumentExtraction != 0 {
				t.Fatalf("unexpected insert stats: %+v", stats)
			}

			assertCollectionState := func(t *testing.T, c *Collection, phase string) {
				t.Helper()
				// Full Get reconstruction must include "embedding" with exact vector values.
				for i, id := range ids {
					got, err := c.Get(id)
					if err != nil {
						t.Fatalf("%s: Get(%q): %v", phase, id, err)
					}
					var obj map[string]any
					if err := json.Unmarshal(got, &obj); err != nil {
						t.Fatalf("%s: Get(%q) invalid JSON %s: %v", phase, id, got, err)
					}
					if obj["id"] != string(id) {
						t.Fatalf("%s: Get(%q) id=%v", phase, id, obj["id"])
					}
					embRaw, ok := obj["embedding"].([]any)
					if !ok || len(embRaw) != len(vecs[i]) {
						t.Fatalf("%s: Get(%q) missing/bad embedding in %s", phase, id, got)
					}
					for dIdx, wantVal := range vecs[i] {
						if gotVal, ok := embRaw[dIdx].(float64); !ok || float32(gotVal) != wantVal {
							t.Fatalf("%s: Get(%q) embedding[%d]=%v want %v", phase, id, dIdx, embRaw[dIdx], wantVal)
						}
					}
				}

				// Scalar index verification: string, int64, bool, array/MultiKey.
				codeIDs, err := c.FindByIndex("kind", "code")
				if err != nil || len(codeIDs) != 2 {
					t.Fatalf("%s: FindByIndex(kind=code)=%q err=%v", phase, codeIDs, err)
				}
				rankIDs, err := c.FindByIndexValue("rank", int64(10))
				if err != nil || len(rankIDs) != 1 || string(rankIDs[0]) != "doc-1" {
					t.Fatalf("%s: FindByIndexValue(rank=10)=%q err=%v", phase, rankIDs, err)
				}
				activeIDs, err := c.FindByIndexValue("active", true)
				if err != nil || len(activeIDs) != 2 {
					t.Fatalf("%s: FindByIndexValue(active=true)=%q err=%v", phase, activeIDs, err)
				}
				tagIDs, err := c.FindByIndex("tags", "vector")
				if err != nil || len(tagIDs) != 2 {
					t.Fatalf("%s: FindByIndex(tags=vector)=%q err=%v", phase, tagIDs, err)
				}

				if _, err := c.RebuildVectorIndex("embedding_graph"); err != nil {
					t.Fatalf("%s: RebuildVectorIndex: %v", phase, err)
				}

				preset, err := ProjectionOrientedVectorDocumentFetchPresetForField("embedding")
				if err != nil {
					t.Fatalf("%s: ProjectionOrientedVectorDocumentFetchPresetForField: %v", phase, err)
				}
				vecOpts := VectorIndexSearchOptions{
					IndexName: "embedding_graph",
					Query:     []float32{1, 0, 0, 0},
					TopK:      2,
					EfSearch:  8,
				}
				preset.ApplyToSearchOptions(&vecOpts)
				vecResp, err := c.SearchVectorIndex(vecOpts)
				if err != nil {
					t.Fatalf("%s: SearchVectorIndex: %v", phase, err)
				}
				if len(vecResp.Results) == 0 || string(vecResp.Results[0].ID) != "doc-1" {
					t.Fatalf("%s: SearchVectorIndex results=%+v want doc-1 first", phase, vecResp.Results)
				}
				for _, hit := range vecResp.Results {
					if bytes.Contains(hit.Document, []byte(`"embedding"`)) {
						t.Fatalf("%s: SearchVectorIndex hit %q still contains embedding: %s", phase, hit.ID, hit.Document)
					}
				}
				if vecResp.Stats.DocumentEmbeddingVectorReads != 0 || vecResp.Stats.DocumentEmbeddingOutputBytes != 0 {
					t.Fatalf("%s: SearchVectorIndex stats expected zero embedding reads/bytes, got reads=%d bytes=%d",
						phase, vecResp.Stats.DocumentEmbeddingVectorReads, vecResp.Stats.DocumentEmbeddingOutputBytes)
				}

				hybridResp, err := c.SearchHybrid(HybridSearchOptions{
					TopK: 2,
					Text: &HybridTextQuery{
						IndexName: "content_text",
						Query:     "vector projection",
					},
					Vector: &HybridVectorQuery{
						IndexName: "embedding_graph",
						Query:     []float32{1, 0, 0, 0},
						EfSearch:  8,
					},
					IncludeDocuments:     true,
					DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
				})
				if err != nil {
					t.Fatalf("%s: SearchHybrid: %v", phase, err)
				}
				if len(hybridResp.Results) == 0 || string(hybridResp.Results[0].ID) != "doc-1" {
					t.Fatalf("%s: SearchHybrid results=%+v want doc-1 first", phase, hybridResp.Results)
				}
				for _, hit := range hybridResp.Results {
					if bytes.Contains(hit.Document, []byte(`"embedding"`)) {
						t.Fatalf("%s: SearchHybrid hit %q still contains embedding: %s", phase, hit.ID, hit.Document)
					}
				}
				if hybridResp.Stats.EmbeddingOutputBytes != 0 {
					t.Fatalf("%s: SearchHybrid EmbeddingOutputBytes=%d want 0", phase, hybridResp.Stats.EmbeddingOutputBytes)
				}

				textResp, err := c.SearchText(TextSearchOptions{
					IndexName:            "content_text",
					Query:                "durability recovery",
					TopK:                 2,
					IncludeDocuments:     true,
					DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
				})
				if err != nil {
					t.Fatalf("%s: SearchText: %v", phase, err)
				}
				if len(textResp.Results) != 1 || string(textResp.Results[0].DocumentID) != "doc-2" {
					t.Fatalf("%s: SearchText results=%+v want doc-2", phase, textResp.Results)
				}
				if bytes.Contains(textResp.Results[0].Document, []byte(`"embedding"`)) {
					t.Fatalf("%s: SearchText hit still contains embedding: %s", phase, textResp.Results[0].Document)
				}
			}

			assertCollectionState(t, col, "immediate")

			// Verify CommandWAL crash-recovery replay on default_command_wal.
			if !tc.durableProfile {
				var replayedFrames int
				for _, frame := range collectionCommandWALFrames(t, dir) {
					if frame.Kind != commitlog.CommandKindCollectionInsertBatchByID {
						continue
					}
					writeCollectionCommandWALFrame(t, replayDir, frame.LSN, frame.Kind, frame.PayloadFormat, frame.Payload)
					replayedFrames++
				}
				if replayedFrames == 0 {
					t.Fatal("expected CommandWAL insert frame")
				}
				replayDB := openCollectionCommandWALDB(t, replayDir)
				defer replayDB.Close()
				replayCol, err := NewCollectionManager(replayDB).OpenCollection("docs")
				if err != nil {
					t.Fatalf("OpenCollection after WAL replay: %v", err)
				}
				assertCollectionState(t, replayCol, "command_wal_replay")
			}

			// Flush, checkpoint, close, and reopen to verify persisted state.
			if err := col.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := d.Checkpoint(); err != nil {
				t.Fatalf("Checkpoint: %v", err)
			}
			if err := cleanup(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			closed = true

			reopened, reopenedCleanup := openProfileDB(t, dir, tc.durableProfile, false)
			reopenedClosed := false
			defer func() {
				if !reopenedClosed {
					_ = reopenedCleanup()
				}
			}()
			reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection after reopen: %v", err)
			}
			assertCollectionState(t, reopenedCol, "checkpoint_reopen")

			// Verify nativeColumnNeedsReconstruct guards non-column scalar-indexed collections
			// even when nativeTypedMeta is false (never returning raw un-reconstructed JSON).
			reopenedMeta := reopenedCol.Meta()
			if nativeTypedMeta(reopenedMeta) {
				t.Fatalf("expected nativeTypedMeta=false for non-column scalar-indexed collection")
			}
			if !nativeColumnNeedsReconstruct(&reopenedMeta) {
				t.Fatalf("expected nativeColumnNeedsReconstruct=true for non-column scalar-indexed collection")
			}

			// On ProfileCommandWALDurable, also verify NativeSemanticBackend (c.nativeJSON != nil)
			// fails closed on unsupported catalog import (or reconstructs full JSON if opened),
			// never silently returning raw un-reconstructed non-column JSON.
			if tc.durableProfile {
				if err := reopenedCleanup(); err != nil {
					t.Fatalf("Close before native reopen: %v", err)
				}
				reopenedClosed = true

				nativeOpts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, dir)
				nativeOpts.MemtableMode = "cow_btree"
				owner, err := treedb.Open(nativeOpts)
				if err != nil {
					t.Fatalf("treedb.Open(cow_btree): %v", err)
				}
				defer owner.Close()
				nativeBackend, err := owner.NativeSemanticBackend()
				if err != nil {
					t.Fatalf("NativeSemanticBackend: %v", err)
				}
				nativeMgr := NewCollectionManager(nativeBackend)
				defer nativeMgr.Close()
				nativeCol, err := nativeMgr.OpenCollection("docs")
				if err != nil {
					if !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
						t.Fatalf("native OpenCollection unexpected err=%v want ErrNativeSemanticUnsupported", err)
					}
				} else {
					for i, id := range ids {
						got, err := nativeCol.Get(id)
						if err != nil {
							t.Fatalf("native Get(%q): %v", id, err)
						}
						var obj map[string]any
						if err := json.Unmarshal(got, &obj); err != nil {
							t.Fatalf("native Get(%q) invalid JSON %s: %v", id, got, err)
						}
						embRaw, ok := obj["embedding"].([]any)
						if !ok || len(embRaw) != len(vecs[i]) {
							t.Fatalf("native Get(%q) returned un-reconstructed non-column JSON (missing embedding): %s", id, got)
						}
					}
				}
			}
		})
	}
}

func TestInsertBatchValidatedFloat32ProjectionScalarOverlapAndContractRejections(t *testing.T) {
	t.Run("meta_scalar_overlap_rejects_and_non_overlap_allows", func(t *testing.T) {
		base := scalarTextValidatedProjectionMeta(3, ColumnRetainedPayloadNonColumn)
		proj, err := newTrustedFloat32Projection(
			[][]byte{[]byte("a")},
			[][]byte{[]byte(`{"id":"a","kind":"code"}`)},
			"embedding",
			VectorMetricCosine,
			[][]float32{{1, 0, 0}},
			[][]byte{[]byte(`{"id":"a","kind":"code"}`)},
		)
		if err != nil {
			t.Fatalf("newTrustedFloat32Projection: %v", err)
		}
		if err := validateTrustedFloat32ProjectionMeta(base, proj); err != nil {
			t.Fatalf("non-overlapping scalar indexes rejected: %v", err)
		}

		overlapCases := []struct {
			name string
			idx  IndexDefinition
		}{
			{name: "exact_field", idx: IndexDefinition{Name: "emb_idx", Field: "embedding", ValueType: IndexValueString}},
			{name: "prefix_field", idx: IndexDefinition{Name: "emb_sub", Field: "embedding.foo", ValueType: IndexValueString}},
			{name: "composite_exact", idx: IndexDefinition{
				Name:      "comp_emb",
				ValueType: IndexValueString,
				Components: []IndexComponent{
					{Field: "kind", Direction: IndexDirectionAscending},
					{Field: "embedding", Direction: IndexDirectionAscending},
				},
			}},
			{name: "composite_prefix", idx: IndexDefinition{
				Name:      "comp_emb_sub",
				ValueType: IndexValueString,
				Components: []IndexComponent{
					{Field: "kind", Direction: IndexDirectionAscending},
					{Field: "embedding.foo", Direction: IndexDirectionDescending},
				},
			}},
		}
		for _, tc := range overlapCases {
			t.Run(tc.name, func(t *testing.T) {
				meta := copyCollectionMeta(base)
				meta.Indexes = append(meta.Indexes, tc.idx)
				if err := validateTrustedFloat32ProjectionMeta(meta, proj); err == nil {
					t.Fatalf("expected scalar overlap %q to fail closed", tc.name)
				}
				// Also verify legacyTyped CommandWAL replay path fails closed before p.retainedJSON is set.
				payload := commitlog.CollectionTypedBatchPayload{
					LegacyProjection: true,
					Collection:       meta.Name,
					SchemaHash:       meta.Options.ColumnStore.SchemaHash,
					Columns: []commitlog.CollectionTypedColumn{{
						Name:       "embedding",
						Type:       commitlog.CollectionTypedFloat32Vector,
						Dimensions: 3,
					}},
					Documents: []commitlog.CollectionTypedDocument{{
						ID:       []byte("a"),
						Retained: []byte(`{"id":"a","kind":"code"}`),
						Values:   []commitlog.CollectionTypedValue{{Vector: []float32{1, 0, 0}}},
					}},
				}
				if _, _, _, err := typedProjectionFromPayload(meta, payload); err == nil {
					t.Fatalf("expected legacyTyped CommandWAL replay with scalar overlap %q to fail closed", tc.name)
				}
			})
		}
	})

	t.Run("legacy_replay_validates_non_column_retained_json", func(t *testing.T) {
		meta := scalarTextValidatedProjectionMeta(3, ColumnRetainedPayloadNonColumn)
		for _, tc := range []struct {
			name     string
			retained string
		}{
			{name: "missing_id", retained: `{"kind":"code"}`},
			{name: "contains_embedding", retained: `{"id":"a","embedding":[1,0,0]}`},
			{name: "malformed_json", retained: `{"id":"a"`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				payload := commitlog.CollectionTypedBatchPayload{
					LegacyProjection: true,
					Collection:       meta.Name,
					SchemaHash:       meta.Options.ColumnStore.SchemaHash,
					Columns: []commitlog.CollectionTypedColumn{{
						Name:       "embedding",
						Type:       commitlog.CollectionTypedFloat32Vector,
						Dimensions: 3,
					}},
					Documents: []commitlog.CollectionTypedDocument{{
						ID:       []byte("a"),
						Retained: []byte(tc.retained),
						Values:   []commitlog.CollectionTypedValue{{Vector: []float32{1, 0, 0}}},
					}},
				}
				if _, _, _, err := typedProjectionFromPayload(meta, payload); err == nil {
					t.Fatalf("expected invalid replayed retained JSON %q to fail closed", tc.retained)
				}
			})
		}
	})

	t.Run("insert_fails_closed_before_any_write_lands", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			extraIdx *IndexDefinition
			retained [][]byte
		}{
			{
				name:     "overlapping_scalar_exact",
				extraIdx: &IndexDefinition{Name: "emb_idx", Field: "embedding", ValueType: IndexValueString},
				retained: [][]byte{[]byte(`{"id":"bad","kind":"code","rank":1,"active":true,"tags":["go"],"content":"x"}`)},
			},
			{
				name:     "overlapping_scalar_prefix",
				extraIdx: &IndexDefinition{Name: "emb_sub", Field: "embedding.foo", ValueType: IndexValueString},
				retained: [][]byte{[]byte(`{"id":"bad","kind":"code","rank":1,"active":true,"tags":["go"],"content":"x"}`)},
			},
			{
				name:     "retained_missing_id",
				retained: [][]byte{[]byte(`{"kind":"code","rank":1,"active":true,"tags":["go"],"content":"x"}`)},
			},
			{
				name:     "retained_contains_embedding",
				retained: [][]byte{[]byte(`{"id":"bad","kind":"code","rank":1,"active":true,"tags":["go"],"content":"x","embedding":[1,0,0]}`)},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				dir := t.TempDir()
				if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{
					RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1},
				}); err != nil {
					t.Fatalf("SaveFormatConfig: %v", err)
				}
				d := openCollectionCommandWALDB(t, dir)
				defer d.Close()

				meta := scalarTextValidatedProjectionMeta(3, ColumnRetainedPayloadNonColumn)
				if tc.extraIdx != nil {
					meta.Indexes = append(meta.Indexes, *tc.extraIdx)
				}
				mgr := NewCollectionManager(d)
				if _, err := mgr.CreateCollection(&meta); err != nil {
					t.Fatalf("CreateCollection: %v", err)
				}
				col, err := mgr.OpenCollection("docs")
				if err != nil {
					t.Fatalf("OpenCollection: %v", err)
				}
				baseLSN := d.State().AppliedCommandLSN
				if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(
					[][]byte{[]byte("bad")}, tc.retained, "embedding", VectorMetricCosine, [][]float32{{1, 0, 0}}, tc.retained,
				); err == nil {
					t.Fatalf("expected insert %q to fail closed", tc.name)
				}
				if got := d.State().AppliedCommandLSN; got != baseLSN {
					t.Fatalf("AppliedCommandLSN=%d want unchanged %d", got, baseLSN)
				}
				if got, err := col.Get([]byte("bad")); err != nil || got != nil {
					t.Fatalf("rejected row persisted: doc=%s err=%v", got, err)
				}
			})
		}
	})
}

func BenchmarkInsertBatchValidatedFloat32ProjectionScalarText(b *testing.B) {
	const (
		rowsPerBatch = 32
		dims         = 768
	)
	contentBody := strings.Repeat("treedb fast vector projection scalar text hybrid search ", 8)

	for _, mode := range []struct {
		name     string
		fastPath bool
		retained ColumnRetainedPayloadPolicy
	}{
		{name: "naive_full_json", fastPath: false, retained: ColumnRetainedPayloadFull},
		{name: "validated_projection_noncolumn", fastPath: true, retained: ColumnRetainedPayloadNonColumn},
	} {
		b.Run(mode.name, func(b *testing.B) {
			dir := b.TempDir()
			if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{
				RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1},
			}); err != nil {
				b.Fatalf("SaveFormatConfig: %v", err)
			}
			d := openCollectionCommandWALDB(b, dir)
			defer d.Close()

			meta := scalarTextValidatedProjectionMeta(dims, mode.retained)
			meta.Options.BufferedIndexedWriteMaxDocuments = 1 << 20
			mgr := NewCollectionManager(d)
			if _, err := mgr.CreateCollection(&meta); err != nil {
				b.Fatalf("CreateCollection: %v", err)
			}
			col, err := mgr.OpenCollection("docs")
			if err != nil {
				b.Fatalf("OpenCollection: %v", err)
			}

			type benchBatch struct {
				ids      [][]byte
				fullDocs [][]byte
				retained [][]byte
				vecs     [][]float32
			}
			batches := make([]benchBatch, b.N)
			for i := range batches {
				batches[i].ids = make([][]byte, rowsPerBatch)
				batches[i].fullDocs = make([][]byte, rowsPerBatch)
				batches[i].retained = make([][]byte, rowsPerBatch)
				batches[i].vecs = make([][]float32, rowsPerBatch)
				for row := range rowsPerBatch {
					id := fmt.Sprintf("chunk-%06d-%02d", i, row)
					vec := make([]float32, dims)
					for dim := range dims {
						vec[dim] = float32(((i*rowsPerBatch+row+dim)%127)-63) / 64.0
					}
					vec[0] = 1.0
					batches[i].ids[row] = []byte(id)
					batches[i].vecs[row] = vec
					if mode.fastPath {
						raw, err := json.Marshal(map[string]any{
							"id":      id,
							"kind":    "code",
							"rank":    i*rowsPerBatch + row,
							"active":  row%2 == 0,
							"tags":    []string{"go", "treedb"},
							"content": contentBody,
						})
						if err != nil {
							b.Fatalf("marshal retained: %v", err)
						}
						batches[i].retained[row] = raw
					} else {
						raw, err := json.Marshal(map[string]any{
							"id":        id,
							"kind":      "code",
							"rank":      i*rowsPerBatch + row,
							"active":    row%2 == 0,
							"tags":      []string{"go", "treedb"},
							"content":   contentBody,
							"embedding": vec,
						})
						if err != nil {
							b.Fatalf("marshal full: %v", err)
						}
						batches[i].fullDocs[row] = raw
					}
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode.fastPath {
					if _, _, err := col.InsertBatchWithStatsValidatedFloat32Projection(
						batches[i].ids, batches[i].retained, "embedding", VectorMetricCosine, batches[i].vecs, batches[i].retained,
					); err != nil {
						b.Fatalf("InsertBatchWithStatsValidatedFloat32Projection: %v", err)
					}
				} else {
					if _, err := col.InsertBatch(batches[i].ids, batches[i].fullDocs); err != nil {
						b.Fatalf("InsertBatch: %v", err)
					}
				}
			}
			b.StopTimer()

			if b.N > 0 {
				if err := col.Flush(); err != nil {
					b.Fatalf("Flush: %v", err)
				}
				if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
					b.Fatalf("RebuildVectorIndex: %v", err)
				}
				queryVec := batches[0].vecs[0]
				vecResp, err := col.SearchVectorIndex(VectorIndexSearchOptions{
					IndexName:            "embedding_graph",
					Query:                queryVec,
					TopK:                 5,
					EfSearch:             16,
					IncludeDocuments:     true,
					DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
				})
				if err != nil {
					b.Fatalf("SearchVectorIndex: %v", err)
				}
				hybridResp, err := col.SearchHybrid(HybridSearchOptions{
					TopK: 5,
					Text: &HybridTextQuery{
						IndexName: "content_text",
						Query:     "vector projection",
					},
					Vector: &HybridVectorQuery{
						IndexName: "embedding_graph",
						Query:     queryVec,
						EfSearch:  16,
					},
					IncludeDocuments:     true,
					DocumentFetchOptions: DocumentFetchOptions{ExcludePaths: []string{"embedding"}},
				})
				if err != nil {
					b.Fatalf("SearchHybrid: %v", err)
				}
				b.ReportMetric(float64(vecResp.Stats.DocumentEmbeddingVectorReads), "vec_doc_emb_reads")
				b.ReportMetric(float64(hybridResp.Stats.EmbeddingOutputBytes), "hybrid_emb_output_B")
			}
		})
	}
}
