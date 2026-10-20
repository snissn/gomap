package treedb

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
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

// The source producer/schema is TestNativeVectorPartitionSourceImportV2's
// maintained three-row fixture. Rebuild/status expectations follow
// TestColumnGraphRebuildVectorIndexPublishesPhysicalManifestV2A. This test adds
// complete native ownership and actual kind103 transport; it claims no ANN query.
func TestNativeVectorGraphRebuildLifecycleAndTransport(t *testing.T) {
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	dir, replayDir := t.TempDir(), t.TempDir()
	opts := indexedSliceOptions(ProfileCommandWALDurable, dir)
	var database *DB
	var handle *backenddb.DB
	var manager *collections.CollectionManager
	var col *collections.Collection
	var held *collections.CollectionReadView
	var source *collections.VectorPartitionSourceReaderV2
	closeDB := func() {
		t.Helper()
		if source != nil {
			if err := source.Close(); err != nil {
				t.Error(err)
			}
			source = nil
		}
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
			held = nil
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if database != nil {
			cache, registry := database.cached, handle.ValueLogIdentityPinRegistry()
			err := database.Close()
			database = nil
			if err != nil {
				t.Fatal(err)
			}
			requirePublicCOWFinalZero(t, cache)
			if stats := registry.Stats(); stats.ActivePins != 0 || stats.ActiveIdentities != 0 {
				t.Fatalf("original graph/source resources remain: %+v", stats)
			}
		}
	}
	t.Cleanup(closeDB)
	openDB := func(options Options) {
		t.Helper()
		var err error
		database, err = Open(options)
		if err != nil {
			t.Fatal(err)
		}
		handle, err = database.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = collections.NewCollectionManager(handle)
	}
	openDB(opts)
	meta := collections.CollectionMeta{Name: "source", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{
		Enabled: true, RetainedPayload: collections.ColumnRetainedPayloadNonColumn, RetainedPayloadEncoding: collections.ColumnRetainedPayloadEncodingJSON,
		Columns: []collections.ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: collections.ColumnStoreValueFloat32Vector, Owner: collections.TypedStorageOwnerColumnPart, VectorDims: 8},
			{Name: "content", Path: "content", ValueType: collections.ColumnStoreValueString, Owner: collections.TypedStorageOwnerRowAsset},
		},
	}}, VectorIndexes: []collections.VectorIndexDefinition{{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 8, M: 2, Strategy: collections.VectorIndexStrategyColumnGraph}}}

	if _, err := manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	var err error
	col, err = manager.OpenCollection("source")
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

	firstIDs, firstResidual, firstColumns := rows("b", "a")
	if progress, err := col.ImportVectorPartitionSourceChunkV2(ownership, input, firstIDs, firstResidual, firstColumns); err != nil || progress.ImportedChunks != 1 || progress.Complete {
		s := database.Stats()
		t.Fatalf("first actual source import: %+v err=%v maintenance=%s/%s cow=%+v", progress, err, s["treedb.cache.cow.maintenance_used_bytes"], s["treedb.cache.cow.maintenance_capacity_bytes"], database.cached.COWMemoryStats())
	}
	last := input
	last.ChunkIndex, last.DocumentRevisions = 1, []uint64{700}
	lastIDs, lastResidual, lastColumns := rows("c")
	complete, err := col.ImportVectorPartitionSourceChunkV2(ownership, last, lastIDs, lastResidual, lastColumns)
	if err != nil || !complete.Complete || complete.ImportedChunks != 2 || complete.Snapshot.Digest == ([sha256.Size]byte{}) || handle.CommandWALNextLSN() != 4 {
		t.Fatalf("completed actual source import: %+v err=%v", complete, err)
	}
	checkDocuments := func() {
		t.Helper()
		for _, id := range []string{"a", "b", "c"} {
			got, err := col.Get([]byte(id))
			if err != nil {
				t.Fatal(err)
			}
			want := []byte(fmt.Sprintf(`{"id":%q,"extra":"kept","content":%q,"embedding":[1,0,0,0,0,0,0,0]}`, id, "content-"+id))
			requireNativeColumnJSON(t, got, want)
		}
	}
	checkSource := func(reader *collections.VectorPartitionSourceReaderV2) {
		t.Helper()
		if err := reader.ValidateOwnerV2(ownership, "group-a"); err != nil {
			t.Fatal(err)
		}
		if reader.Commitment().Snapshot != complete.Snapshot {
			t.Fatal("source snapshot changed across graph publication")
		}
		for index, names := range [][]string{{"b", "a"}, {"c"}} {
			got, err := reader.ReadChunk(uint64(index))
			if err != nil || len(got.Chunk.Rows) != len(names) {
				t.Fatalf("source chunk %d: %+v err=%v", index, got, err)
			}
			for i, id := range names {
				revision := uint64(700)
				if index == 0 {
					revision = []uint64{901, 42}[i]
				}
				row := got.Chunk.Rows[i]
				if string(row.DocumentID) != id || row.LocalOrdinal != uint64(index*2+i) || row.DocumentRevision != revision || !reflect.DeepEqual(row.Values, []float32{1, 0, 0, 0, 0, 0, 0, 0}) {
					t.Fatalf("source row %d/%d: %+v", index, i, row)
				}
			}
		}
	}
	checkCurrentSource := func() {
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
		checkSource(reader)
	}
	checkDocuments()
	checkCurrentSource()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if indexedCRUDAppliedLSN(t, handle) != 3 {
		t.Fatal("source seed physical LSN is not 3")
	}
	// Copy a CLOSED, checkpointed source basis; the replay arm receives only
	// the later real producer envelope, never a fabricated graph/catalog image.
	closeDB()
	if err := os.CopyFS(replayDir, os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
	rebindNativeGraphSeed(t, replayDir, opts.DisableSideStores)
	openDB(opts)
	col, err = manager.OpenCollection("source")
	if err != nil {
		t.Fatal(err)
	}
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	source, err = col.OpenVectorPartitionSourceSnapshotV2(complete.Snapshot, input.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	commitment := source.Commitment()
	checkHeld := func() {
		t.Helper()
		got, err := held.FetchDocumentsByID([][]byte{[]byte("a"), []byte("c")}, collections.DocumentFetchOptions{})
		if err != nil || len(got.Results) != 2 || !got.Results[0].Found || !got.Results[1].Found {
			t.Fatalf("held source cut: %+v err=%v", got, err)
		}
		for i, id := range []string{"a", "c"} {
			requireNativeColumnJSON(t, got.Results[i].Document, []byte(fmt.Sprintf(`{"id":%q,"extra":"kept","content":%q,"embedding":[1,0,0,0,0,0,0,0]}`, id, "content-"+id)))
		}
		checkSource(source)
		if source.Commitment() != commitment {
			t.Fatal("held source commitment changed")
		}
	}
	sequence := func() uint64 {
		t.Helper()
		capture, err := handle.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		result := capture.LogicalSequence()
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	beforeNext, beforePhysical, beforeSequence := handle.CommandWALNextLSN(), indexedCRUDAppliedLSN(t, handle), sequence()
	beforeToken, ok := handle.StateToken()
	if !ok {
		t.Fatal("source basis has no physical state token")
	}
	beforeMeta, beforeAssets := col.Meta(), nativeGraphAssetContents(t, handle.ColumnAssetRootDir())
	status, rebuildErr := col.RebuildVectorIndex(input.IndexName)
	if rebuildErr != nil {
		// errCollectionDBNil is private. Check its exact existing public error
		// text, then prove the call refused without assignment/publication/assets.
		if rebuildErr.Error() != "collections: db is nil" {
			t.Fatalf("unexpected rebuild boundary: %v", rebuildErr)
		}
		afterToken, ok := handle.StateToken()
		if !ok || afterToken != beforeToken || handle.CommandWALNextLSN() != beforeNext || indexedCRUDAppliedLSN(t, handle) != beforePhysical || sequence() != beforeSequence || !reflect.DeepEqual(status, collections.VectorIndexStatus{}) || !reflect.DeepEqual(col.Meta(), beforeMeta) || !reflect.DeepEqual(nativeGraphAssetContents(t, handle.ColumnAssetRootDir()), beforeAssets) {
			t.Fatal("refused rebuild changed LSN/frontier/metadata/assets/status")
		}
		checkDocuments()
		checkCurrentSource()
		checkHeld()
		if err := database.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if indexedCRUDAppliedLSN(t, handle) != 3 {
			t.Fatal("refused rebuild published a graph command")
		}
		closeDB()
		openDB(opts)
		col, err = manager.OpenCollection("source")
		if err != nil {
			t.Fatal(err)
		}
		checkDocuments()
		checkCurrentSource()
		closeDB()
		t.Fatalf("native graph producer unavailable after verified side-effect-free refusal: %v", rebuildErr)
	}
	checkGraph := func(got collections.VectorIndexStatus) {
		t.Helper()
		if got.Name != input.IndexName || got.Strategy != collections.VectorIndexStrategyColumnGraph || got.State != collections.VectorIndexStateColumnGraphLoaded || !got.Loaded || got.RebuildNeeded || got.Stats.Nodes != 3 || got.Stats.LiveDocs != 3 || got.Stats.BytesDisk <= 0 {
			t.Fatalf("native physical three-row graph status: %+v", got)
		}
	}
	checkGraph(status)
	if handle.CommandWALNextLSN() != beforeNext+1 || sequence() != beforeSequence+1 {
		t.Fatal("rebuild did not publish exactly one native command")
	}
	checkDocuments()
	checkCurrentSource()
	checkHeld()
	if err := handle.FlushCommandWAL(true); err != nil {
		t.Fatal(err)
	}
	frame := readNativeGraphRebuildFrame(t, opts, beforeNext)
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if indexedCRUDAppliedLSN(t, handle) != beforeNext {
		t.Fatal("checkpoint missed rebuilt graph")
	}
	gotStatus, err := col.VectorIndexStatus(input.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	checkGraph(gotStatus)
	checkCurrentSource()
	checkHeld()
	closeDB()
	beforeReplay := workstats.Read().Replay
	openDB(opts)
	if after := workstats.Read().Replay; after.FramesApplied != beforeReplay.FramesApplied || after.FrameErrors != beforeReplay.FrameErrors {
		t.Fatal("checkpointed rebuild replayed twice")
	}
	col, err = manager.OpenCollection("source")
	if err != nil {
		t.Fatal(err)
	}
	gotStatus, err = col.VectorIndexStatus(input.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	checkGraph(gotStatus)
	checkDocuments()
	checkCurrentSource()
	closeDB()

	replayOpts := indexedSliceOptions(ProfileCommandWALDurable, replayDir)
	layout, err := resolveOpenDirLayout(replayDir, replayOpts.DisableSideStores)
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
	// Every field, including extensions/references, is copied from the actual
	// successful producer using the original command-frame codec below.
	appendErr := writer.AppendCommand(frame)
	if appendErr == nil {
		appendErr = writer.Sync()
	}
	if err := errors.Join(appendErr, writer.Close()); err != nil {
		t.Fatal(err)
	}
	beforeReplay = workstats.Read().Replay
	openDB(replayOpts)
	if after := workstats.Read().Replay; after.FramesApplied-beforeReplay.FramesApplied != 1 || after.FrameErrors != beforeReplay.FrameErrors {
		t.Fatal("actual graph envelope replay did not apply exactly once")
	}
	col, err = manager.OpenCollection("source")
	if err != nil {
		t.Fatal(err)
	}
	gotStatus, err = col.VectorIndexStatus(input.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	checkGraph(gotStatus)
	checkDocuments()
	checkCurrentSource()
	if handle.CommandWALNextLSN() != beforeNext+1 {
		t.Fatal("transport altered assigned graph LSN")
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if indexedCRUDAppliedLSN(t, handle) != beforeNext {
		t.Fatal("transport checkpoint missed actual graph command")
	}
	closeDB()
	beforeReplay = workstats.Read().Replay
	openDB(replayOpts)
	if after := workstats.Read().Replay; after.FramesApplied != beforeReplay.FramesApplied || after.FrameErrors != beforeReplay.FrameErrors {
		t.Fatal("checkpointed transported graph replayed twice")
	}
	col, err = manager.OpenCollection("source")
	if err != nil {
		t.Fatal(err)
	}
	gotStatus, err = col.VectorIndexStatus(input.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	checkGraph(gotStatus)
	checkDocuments()
	checkCurrentSource()
	// Append through the same public source producer after the real graph has
	// survived transport and reopen. Its final closure must retain and decode
	// both graph/TVIS records while the new source generation stales the graph.
	held, err = col.OpenCollectionReadView()
	if err != nil {
		t.Fatal(err)
	}
	source, err = col.OpenVectorPartitionSourceSnapshotV2(complete.Snapshot, input.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	appendInput := input
	appendInput.Snapshot.SnapshotRevision = 20
	appendInput.Snapshot.OrdinalNamespace = "source-a-v2-next"
	appendInput.Snapshot.RowCount, appendInput.Snapshot.RowsPerChunk = 1, 1
	appendInput.DocumentRevisions = []uint64{800}
	appendIDs, appendResidual, appendColumns := rows("d")
	appendNext := handle.CommandWALNextLSN()
	appended, err := col.ImportVectorPartitionSourceChunkV2(ownership, appendInput, appendIDs, appendResidual, appendColumns)
	if err != nil || !appended.Complete || appended.ImportedChunks != 1 || appended.Snapshot.Digest == ([sha256.Size]byte{}) || handle.CommandWALNextLSN() != appendNext+1 {
		t.Fatalf("source append after graph: %+v err=%v", appended, err)
	}
	checkAppended := func() {
		t.Helper()
		checkDocuments()
		checkCurrentSource()
		got, err := col.Get([]byte("d"))
		if err != nil {
			t.Fatal(err)
		}
		requireNativeColumnJSON(t, got, []byte(`{"id":"d","extra":"kept","content":"content-d","embedding":[1,0,0,0,0,0,0,0]}`))
		reader, err := col.OpenVectorPartitionSourceSnapshotV2(appended.Snapshot, input.IndexName)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := reader.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := reader.ValidateOwnerV2(ownership, "group-a"); err != nil || reader.Commitment().Snapshot != appended.Snapshot {
			t.Fatalf("appended source owner/commitment: %v", err)
		}
		chunk, err := reader.ReadChunk(0)
		if err != nil || len(chunk.Chunk.Rows) != 1 {
			t.Fatalf("appended source chunk: %+v err=%v", chunk, err)
		}
		row := chunk.Chunk.Rows[0]
		if string(row.DocumentID) != "d" || row.LocalOrdinal != 0 || row.DocumentRevision != 800 || !reflect.DeepEqual(row.Values, []float32{1, 0, 0, 0, 0, 0, 0, 0}) {
			t.Fatalf("appended source row: %+v", row)
		}
		status, err := col.VectorIndexStatus(input.IndexName)
		if err != nil || status.Loaded || !status.RebuildNeeded || status.State != collections.VectorIndexStateColumnGraphRebuildNeeded {
			t.Fatalf("graph must stale after source append: %+v err=%v", status, err)
		}
	}
	checkAppended()
	checkHeld()
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if indexedCRUDAppliedLSN(t, handle) != appendNext {
		t.Fatal("checkpoint missed source append after graph")
	}
	checkAppended()
	checkHeld()
	heldAppend, err := held.FetchDocumentsByID([][]byte{[]byte("d")}, collections.DocumentFetchOptions{})
	if err != nil || len(heldAppend.Results) != 1 || heldAppend.Results[0].Found {
		t.Fatalf("held graph cut observed later source append: %+v err=%v", heldAppend, err)
	}
	closeDB()
	openDB(replayOpts)
	col, err = manager.OpenCollection("source")
	if err != nil {
		t.Fatal(err)
	}
	checkAppended()
	closeDB()
}

// Match the maintained source-import fixture's original snapshot-restore path.
func rebindNativeGraphSeed(t *testing.T, dir string, disableSideStores bool) {
	t.Helper()
	layout, err := resolveOpenDirLayout(dir, disableSideStores)
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
}

func nativeGraphAssetContents(t *testing.T, dir string) map[string][sha256.Size]byte {
	t.Helper()
	result := make(map[string][sha256.Size]byte)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == dir {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		result[relative] = sha256.Sum256(raw)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func readNativeGraphRebuildFrame(t *testing.T, opts Options, assigned uint64) commitlog.CommandEnvelope {
	t.Helper()
	layout, err := resolveOpenDirLayout(opts.Dir, opts.DisableSideStores)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(backenddb.WALDirPath(layout.mainDir), "commit-l0-*.log"))
	if err != nil {
		t.Fatal(err)
	}
	var result commitlog.CommandEnvelope
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
			if found || frame.Kind != commitlog.CommandKindCollectionRebuildVectorIndex {
				t.Fatal("missing/duplicate/wrong graph producer kind")
			}
			payload, err := commitlog.DecodeCollectionRebuildVectorIndexPayload(frame.Payload)
			if err != nil || payload.Collection != "source" || payload.IndexName != "embedding_graph" {
				t.Fatalf("graph producer payload: %+v err=%v", payload, err)
			}
			raw, err := commitlog.EncodeCommandFrameV2(frame)
			if err != nil {
				t.Fatal(err)
			}
			result, err = commitlog.DecodeCommandFrameV2(bytes.Clone(raw))
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := commitlog.EncodeCommandFrameV2(result)
			if err != nil || !bytes.Equal(raw, encoded) {
				t.Fatal("owned graph envelope changed")
			}
			found = true
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !found {
		t.Fatal("successful graph producer has no actual canonical envelope")
	}
	return result
}
