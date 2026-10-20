package treedb

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/sourcepartition"
	"github.com/snissn/gomap/TreeDB/internal/vectorpartition"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

// This is the existing source-only import contract, not ANN build/serving.
func TestNativeVectorPartitionSourceImportV2(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	runNativeVectorSourceImportV2(t, t.TempDir(), "", false)
}

// Match TestVectorPartitionSourceImportAtomicResumeAndReplayV2: transport
// actual producer envelopes onto a copied seed directory. Successful imports
// may already be physically published; this is not an unapplied-success claim.
func TestNativeVectorPartitionSourceImportTransportReplayV2(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	source, replay := t.TempDir(), t.TempDir()
	runNativeVectorSourceImportV2(t, source, replay, false)
	runNativeVectorSourceImportV2(t, replay, "", true)
}

func runNativeVectorSourceImportV2(t *testing.T, dir, replayDir string, replayed bool) {
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	beforeReplay := workstats.Read().Replay
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	afterReplay := workstats.Read().Replay
	var manager *collections.CollectionManager
	var held *collections.CollectionReadView
	t.Cleanup(func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
		}
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	handle, err := database.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = collections.NewCollectionManager(handle)
	meta := collections.CollectionMeta{Name: "source", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{
		Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
		Columns: []collections.ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, Owner: collections.TypedStorageOwnerColumnPart, VectorDims: 8},
			{Name: "content", Path: "content", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset},
		},
	}}, VectorIndexes: []collections.VectorIndexDefinition{{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 8, M: 2, Strategy: collections.VectorIndexStrategyColumnGraph}}}
	if !replayed {
		if _, err := manager.CreateCollection(&meta); err != nil {
			t.Fatalf("ordinary native source-only Create: %v", err)
		}
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if indexedCRUDAppliedLSN(t, handle) != 1 || handle.CommandWALNextLSN() != 2 {
			t.Fatal("source Create physical/canonical basis is not 1")
		}
	} else if afterReplay.FramesApplied-beforeReplay.FramesApplied != 2 || afterReplay.FrameErrors != beforeReplay.FrameErrors {
		t.Fatalf("actual format14 replay: %+v -> %+v", beforeReplay, afterReplay)
	}
	if replayDir != "" {
		// Copy a closed, checkpointed basis: no locked-file copying or synthetic
		// catalog/asset image. Reopen the source before executing real imports.
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		manager = nil
		cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		database = nil
		requirePublicCOWFinalZero(t, cache)
		if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("source seed resources: %+v", stats)
		}
		if err := os.CopyFS(replayDir, os.DirFS(dir)); err != nil {
			t.Fatal(err)
		}
		// Restore copied physical identities through the maintained snapshot
		// restore path; ordinary Open intentionally rejects stale source identities.
		layout, err := resolveOpenDirLayout(replayDir, opts.DisableSideStores)
		if err != nil {
			t.Fatal(err)
		}
		for _, side := range []string{layout.dictdbDir, layout.templatedbDir} {
			if side == "" {
				continue
			}
			if _, err := os.Stat(filepath.Join(side, "index.db")); os.IsNotExist(err) {
				continue
			} else if err != nil {
				t.Fatal(err)
			}
			if err := backenddb.RebindDurableRootSnapshotLayoutV1(side, ""); err != nil {
				t.Fatal(err)
			}
		}
		sideRoot := ""
		if !layout.disableSideStores {
			sideRoot = layout.rootDir
		}
		if err := backenddb.RebindDurableRootSnapshotLayoutV1(layout.mainDir, sideRoot); err != nil {
			t.Fatal(err)
		}
		database, err = Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		handle, err = database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = collections.NewCollectionManager(handle)
	}
	var observed []commitlog.CommandEnvelope
	col, err := manager.OpenCollection("source")
	if err != nil {
		t.Fatal(err)
	}
	sourceMap, err := sourcepartition.CanonicalSourceShardMapV2(sourcepartition.SourceShardMapV2{Format: sourcepartition.SourceShardMapFormatV2, Collection: sourcepartition.CollectionRefV2{Database: "default", Catalog: "default", Collection: "source"}, Epoch: 7, TokenAlgorithm: sourcepartition.DocumentIDTokenAlgorithmV2, Shards: []sourcepartition.SourceShardV2{{ShardID: "source-a", GroupID: "group-a", Start: 0, End: ^uint64(0)}}})
	if err != nil {
		t.Fatal(err)
	}
	ownership, err := sourcepartition.ValidateSourceShardMapV2(sourceMap)
	if err != nil {
		t.Fatal(err)
	}
	normalized := col.Meta()
	schema, err := collections.VectorPartitionSourceSchemaDigestV2(*normalized.Options.ColumnStore)
	if err != nil {
		t.Fatal(err)
	}
	var mapDigest, definitionDigest [sha256.Size]byte
	raw, err := hex.DecodeString(sourceMap.Digest)
	if err != nil {
		t.Fatal(err)
	}
	copy(mapDigest[:], raw)
	raw, err = hex.DecodeString(collections.VectorIndexDefinitionDigestV1(normalized.VectorIndexes[0]))
	if err != nil {
		t.Fatal(err)
	}
	copy(definitionDigest[:], raw)
	input := collections.VectorPartitionSourceImportChunkV2{Snapshot: collections.VectorPartitionSourceSnapshotV2{Version: 2, CollectionScope: "default/default/source", ShardID: "source-a", SnapshotRevision: 19, OrdinalNamespace: "source-a-v2", SourceMapEpoch: 7, SourceMapDigest: mapDigest, SchemaDigest: schema, IndexDefinitionDigest: definitionDigest, Encoding: vectorpartition.SourceSnapshotEncodingV2, Dimensions: 8, RowCount: 3, RowsPerChunk: 2}, IndexName: "embedding_graph", VectorColumn: "embedding", GroupID: "group-a", DocumentRevisions: []uint64{901, 42}}
	rows := func(names ...string) ([][]byte, [][]byte, []collections.TypedColumnBatch) {
		ids, residual := make([][]byte, len(names)), make([][]byte, len(names))
		columns := []collections.TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, len(names))}, {Name: "content", Strings: make([]string, len(names))}}
		for i, id := range names {
			ids[i] = []byte(id)
			residual[i] = []byte(fmt.Sprintf(`{"id":%q,"extra":"kept"}`, id))
			columns[0].Float32Vectors[i] = []float32{1, 0, 0, 0, 0, 0, 0, 0}
			columns[1].Strings[i] = "content-" + id
		}
		return ids, residual, columns
	}
	checkDocuments := func() {
		t.Helper()
		for _, id := range []string{"a", "b", "c"} {
			got, err := col.Get([]byte(id))
			if err != nil {
				t.Fatalf("source Get %s: %v", id, err)
			}
			want := []byte(fmt.Sprintf(`{"id":%q,"extra":"kept","content":%q,"embedding":[1,0,0,0,0,0,0,0]}`, id, "content-"+id))
			requireNativeColumnJSON(t, got, want)
			got[0] = '!'
			again, err := col.Get([]byte(id))
			if err != nil {
				t.Fatal(err)
			}
			requireNativeColumnJSON(t, again, want)
		}
	}
	firstIDs, firstResidual, firstColumns := rows("b", "a")
	last := input
	last.ChunkIndex = 1
	last.DocumentRevisions = []uint64{700}
	lastIDs, lastResidual, lastColumns := rows("c")
	if !replayed {
		first, stats, err := col.ImportVectorPartitionSourceChunkWithStatsV2(ownership, input, firstIDs, firstResidual, firstColumns)
		if err != nil || first.ImportedChunks != 1 || first.Complete || stats.SourceRows != 2 {
			t.Fatalf("first source chunk: %+v stats=%+v err=%v", first, stats, err)
		}
		if handle.CommandWALNextLSN() != 3 {
			t.Fatal("first source chunk did not append exactly one command")
		}
		if err := handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		observed = append(observed, checkNativeVectorSourceFrameV2(t, opts, 2))
		held, err = col.OpenCollectionReadView()
		if err != nil {
			t.Fatal(err)
		}
		if reader, err := col.OpenVectorPartitionSourceSnapshotV2(input.Snapshot, input.IndexName); err == nil {
			if reader != nil {
				_ = reader.Close()
			}
			t.Fatal("incomplete source admitted reader")
		}
		next := handle.CommandWALNextLSN()
		retry, err := col.ImportVectorPartitionSourceChunkV2(ownership, input, firstIDs, firstResidual, firstColumns)
		if err != nil || retry != first || handle.CommandWALNextLSN() != next {
			t.Fatalf("exact chunk retry: %+v err=%v", retry, err)
		}
		changed := append([]collections.TypedColumnBatch(nil), firstColumns...)
		changed[1].Strings = []string{"changed", "content-a"}
		if _, err := col.ImportVectorPartitionSourceChunkV2(ownership, input, firstIDs, firstResidual, changed); err == nil || handle.CommandWALNextLSN() != next {
			t.Fatal("changed retry accepted or appended")
		}
		wrong := last
		wrong.GroupID = "wrong-owner"
		if _, err := col.ImportVectorPartitionSourceChunkV2(ownership, wrong, lastIDs, lastResidual, lastColumns); err == nil || handle.CommandWALNextLSN() != next {
			t.Fatal("wrong owner accepted or appended")
		}
		gap := last
		gap.ChunkIndex = 2
		if _, err := col.ImportVectorPartitionSourceChunkV2(ownership, gap, lastIDs, lastResidual, lastColumns); err == nil || handle.CommandWALNextLSN() != next {
			t.Fatal("chunk gap accepted or appended")
		}
		// Caller input is not import storage or source-proof ownership.
		firstIDs[0][0] = '!'
		firstResidual[0][0] = '!'
		firstColumns[0].Float32Vectors[0][0] = 0
		firstColumns[1].Strings[0] = "changed"
	}
	complete, err := col.ImportVectorPartitionSourceChunkV2(ownership, last, lastIDs, lastResidual, lastColumns)
	if err != nil || !complete.Complete || complete.ImportedChunks != 2 || complete.Snapshot.Digest == ([sha256.Size]byte{}) || handle.CommandWALNextLSN() != 4 {
		t.Fatalf("complete/retry source: %+v err=%v next=%d", complete, err, handle.CommandWALNextLSN())
	}
	if !replayed {
		if err := handle.FlushCommandWAL(true); err != nil {
			t.Fatal(err)
		}
		observed = append(observed, checkNativeVectorSourceFrameV2(t, opts, 3))
	}
	checkDocuments()
	checkReader := func() {
		t.Helper()
		reader, err := col.OpenVectorPartitionSourceSnapshotV2(complete.Snapshot, input.IndexName)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := reader.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := reader.ValidateOwnerV2(ownership, "group-a"); err != nil {
			t.Fatal(err)
		}
		commitment := reader.Commitment()
		if commitment.Snapshot != complete.Snapshot || commitment.DirectoryGeneration == 0 || commitment.DirectoryDigest == ([sha256.Size]byte{}) {
			t.Fatalf("source completion binding: %+v", commitment)
		}
		for index, names := range [][]string{{"b", "a"}, {"c"}} {
			got, err := reader.ReadChunk(uint64(index))
			if err != nil || len(got.Chunk.Rows) != len(names) {
				t.Fatalf("read source chunk%d: rows=%d err=%v", index, len(got.Chunk.Rows), err)
			}
			for i, id := range names {
				revision := uint64(700)
				if index == 0 {
					revision = []uint64{901, 42}[i]
				}
				row := got.Chunk.Rows[i]
				if string(row.DocumentID) != id || row.LocalOrdinal != uint64(index*2+i) || row.DocumentRevision != revision || !reflect.DeepEqual(row.Values, []float32{1, 0, 0, 0, 0, 0, 0, 0}) {
					t.Fatalf("source row%d/%d: %+v", index, i, row)
				}
			}
			got.Chunk.Rows[0].DocumentID[0] = '!'
			got.Chunk.Rows[0].Values[0] = 0
			again, err := reader.ReadChunk(uint64(index))
			if err != nil || string(again.Chunk.Rows[0].DocumentID) != names[0] || again.Chunk.Rows[0].Values[0] != 1 {
				t.Fatal("source reader output aliases retained proof")
			}
		}
		if _, err := reader.ReadChunk(2); err == nil {
			t.Fatal("out-of-range source chunk accepted")
		}
	}
	checkReader()
	checkHeld := func() {
		t.Helper()
		if held == nil {
			return
		}
		got, err := held.FetchDocumentsByID([][]byte{[]byte("b"), []byte("c")}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 2 || !got.Results[0].Found || got.Results[1].Found {
			t.Fatalf("held partial-source cut: %+v err=%v", got, err)
		}
		requireNativeColumnJSON(t, got.Results[0].Document, []byte(`{"id":"b","extra":"kept","content":"content-b","embedding":[1,0,0,0,0,0,0,0]}`))
	}
	checkHeld()
	next := handle.CommandWALNextLSN()
	extraIDs, extraResidual, extraColumns := rows("d")
	if _, _, err := col.InsertTypedBatchWithStats(extraIDs, extraResidual, extraColumns); err == nil || handle.CommandWALNextLSN() != next {
		t.Fatal("ordinary typed mutation accepted imported cdir2 or appended")
	}
	if err := handle.FlushCommandWAL(true); err != nil {
		t.Fatal(err)
	}
	if replayDir != "" {
		layout, err := resolveOpenDirLayout(replayDir, opts.DisableSideStores)
		if err != nil {
			t.Fatal(err)
		}
		walDir := backenddb.WALDirPath(layout.mainDir)
		if err := os.MkdirAll(walDir, 0755); err != nil {
			t.Fatal(err)
		}
		writer, err := commitlog.NewWriter(filepath.Join(walDir, commitlog.CommandSegmentName(0, 1)))
		if err != nil {
			t.Fatal(err)
		}
		for _, frame := range observed {
			// All envelope fields and payload bytes came from the actual producer;
			// AppendCommand performs the same maintained framing as the original test.
			if err := writer.AppendCommand(frame); err != nil {
				_ = writer.Close()
				t.Fatal(err)
			}
		}
		if err := writer.Sync(); err != nil {
			_ = writer.Close()
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	}

	for reopen := 0; reopen < 2; reopen++ {
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if indexedCRUDAppliedLSN(t, handle) != 3 {
			t.Fatal("source checkpoint missed accepted import")
		}
		checkDocuments()
		checkReader()
		checkHeld()
		retry, err := col.ImportVectorPartitionSourceChunkV2(ownership, last, lastIDs, lastResidual, lastColumns)
		if err != nil || retry != complete || handle.CommandWALNextLSN() != 4 {
			t.Fatalf("sealed retry after checkpoint/reopen: %+v err=%v", retry, err)
		}
		firstIDs, firstResidual, firstColumns := rows("b", "a")
		retry, err = col.ImportVectorPartitionSourceChunkV2(ownership, input, firstIDs, firstResidual, firstColumns)
		if err != nil || retry != complete || handle.CommandWALNextLSN() != 4 {
			t.Fatalf("earlier sealed chunk retry after checkpoint/reopen: %+v err=%v", retry, err)
		}
		if held != nil {
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			held = nil
		}
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		manager = nil
		cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		database = nil
		requirePublicCOWFinalZero(t, cache)
		if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
			t.Fatalf("source import final resources: %+v", stats)
		}
		if reopen == 0 {
			before := workstats.Read().Replay
			database, err = Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			if after := workstats.Read().Replay; after.FramesApplied != before.FramesApplied || after.FrameErrors != before.FrameErrors {
				t.Fatal("checkpointed source replayed twice")
			}
			handle, err = database.NativeSemanticBackend()
			if err != nil {
				t.Fatal(err)
			}
			manager = collections.NewCollectionManager(handle)
			col, err = manager.OpenCollection("source")
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func checkNativeVectorSourceFrameV2(t *testing.T, opts Options, assigned uint64) commitlog.CommandEnvelope {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	var observed commitlog.CommandEnvelope
	found := false
	for _, path := range paths {
		reader, err := commitlog.NewReader(path)
		if err != nil {
			t.Fatal(err)
		}
		for {
			frame, err := reader.ReadCommandFrame()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				_ = reader.Close()
				t.Fatal(err)
			}
			if frame.LSN != assigned {
				continue
			}
			if found || frame.Kind != commitlog.CommandKindCollectionReplaceSourceByID || frame.PayloadFormat != commitlog.PayloadFormatCollectionSourceImportV2 {
				t.Fatalf("source canonical frame LSN%d kind%d format%d", frame.LSN, frame.Kind, frame.PayloadFormat)
			}
			payload, err := commitlog.DecodeCollectionSourceImportPayloadV2(frame.Payload)
			if err != nil {
				t.Fatal(err)
			}
			var metadata struct {
				Version           uint32
				OrderedIDs        [][]byte
				ChunkIndex        uint64
				DocumentRevisions []uint64
			}
			if err := json.Unmarshal(payload.Metadata, &metadata); err != nil {
				t.Fatal(err)
			}
			wantIDs, wantRevisions := [][]byte{[]byte("b"), []byte("a")}, []uint64{901, 42}
			if frame.LSN == 3 {
				wantIDs, wantRevisions = [][]byte{[]byte("c")}, []uint64{700}
			}
			if metadata.Version != 2 || metadata.ChunkIndex != frame.LSN-2 || !reflect.DeepEqual(metadata.OrderedIDs, wantIDs) || !reflect.DeepEqual(metadata.DocumentRevisions, wantRevisions) || payload.Inserted.Collection != "source" || len(payload.Inserted.Documents) != len(wantIDs) {
				t.Fatalf("source canonical payload LSN%d metadata=%+v rows=%d", frame.LSN, metadata, len(payload.Inserted.Documents))
			}
			wantCanonical := [][]byte{[]byte("a"), []byte("b")}
			if frame.LSN == 3 {
				wantCanonical = [][]byte{[]byte("c")}
			}
			for i, document := range payload.Inserted.Documents {
				if !bytes.Equal(document.ID, wantCanonical[i]) {
					t.Fatalf("source canonical document ordering LSN%d", frame.LSN)
				}
			}
			// Reader payload aliases are not retained across another read/Close.
			raw, err := commitlog.EncodeCommandFrameV2(frame)
			if err != nil {
				t.Fatal(err)
			}
			// This producer has no extensions or external references.
			if len(frame.ExternalRefs) != 0 || len(frame.Preconditions) != 0 || len(frame.ResultAssertions) != 0 {
				t.Fatal("unexpected source frame extensions")
			}
			frame.Payload = bytes.Clone(frame.Payload)
			check, err := commitlog.EncodeCommandFrameV2(frame)
			if err != nil || !bytes.Equal(raw, check) {
				t.Fatal("source envelope changed during owned copy")
			}
			observed, found = frame, true
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatalf("source canonical frame LSN%d missing at successful boundary", assigned)
	}
	return observed
}
