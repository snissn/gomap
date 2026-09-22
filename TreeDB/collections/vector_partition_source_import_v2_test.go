package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/sourcepartition"
	"github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

func sourceImportFixtureV2(t *testing.T, c *Collection, rows uint64) (sourcepartition.ResolvedSourceShardMapV2, VectorPartitionSourceImportChunkV2) {
	t.Helper()
	m, err := sourcepartition.CanonicalSourceShardMapV2(sourcepartition.SourceShardMapV2{Format: sourcepartition.SourceShardMapFormatV2, Collection: sourcepartition.CollectionRefV2{Database: "default", Catalog: "default", Collection: c.Meta().Name}, Epoch: 7, TokenAlgorithm: sourcepartition.DocumentIDTokenAlgorithmV2, Shards: []sourcepartition.SourceShardV2{{ShardID: "source-a", GroupID: "group-a", Start: 0, End: ^uint64(0)}}})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := sourcepartition.ValidateSourceShardMapV2(m)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := VectorPartitionSourceSchemaDigestV2(*c.Meta().Options.ColumnStore)
	if err != nil {
		t.Fatal(err)
	}
	var mapDigest, definitionDigest [sha256.Size]byte
	decoded, err := hex.DecodeString(m.Digest)
	if err != nil {
		t.Fatal(err)
	}
	copy(mapDigest[:], decoded)
	meta := c.Meta()
	decoded, err = hex.DecodeString(VectorIndexDefinitionDigestV1(meta.VectorIndexes[0]))
	if err != nil {
		t.Fatal(err)
	}
	copy(definitionDigest[:], decoded)
	input := VectorPartitionSourceImportChunkV2{Snapshot: VectorPartitionSourceSnapshotV2{Version: 2, CollectionScope: "default/default/" + meta.Name, ShardID: "source-a", SnapshotRevision: 19, OrdinalNamespace: "source-a-v2", SourceMapEpoch: 7, SourceMapDigest: mapDigest, SchemaDigest: schema, IndexDefinitionDigest: definitionDigest, Encoding: vectorpartition.SourceSnapshotEncodingV2, Dimensions: 8, RowCount: rows, RowsPerChunk: 2}, IndexName: "embedding_graph", VectorColumn: "embedding", GroupID: "group-a"}
	return resolved, input
}

func sourceImportRowsV2(names ...string) ([][]byte, [][]byte, []TypedColumnBatch) {
	ids, retained := make([][]byte, len(names)), make([][]byte, len(names))
	columns := []TypedColumnBatch{{Name: "embedding", Float32Vectors: make([][]float32, len(names))}, {Name: "content", Strings: make([]string, len(names))}, {Name: "user", Strings: make([]string, len(names))}, {Name: "path", Strings: make([]string, len(names))}}
	for i, name := range names {
		ids[i] = []byte(name)
		retained[i] = []byte(fmt.Sprintf(`{"id":%q,"extra":"kept"}`, name))
		columns[0].Float32Vectors[i] = []float32{1, 0, 0, 0, 0, 0, 0, 0}
		columns[1].Strings[i] = "content-" + name
		columns[2].Strings[i] = "user-" + name
		columns[3].Strings[i] = "path-" + name
	}
	return ids, retained, columns
}

func TestVectorPartitionSourceImportAtomicResumeAndReplayV2(t *testing.T) {
	dir, d, c := openTypedMinimaCollection(t)
	defer d.Close()
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	replayDir := t.TempDir()
	copyColumnStoreCommandWALReplayBenchmarkDirM10C(t, dir, replayDir)
	before := collectionCommandWALFrames(t, dir)
	baseLSN := before[len(before)-1].LSN
	ownership, input := sourceImportFixtureV2(t, c, 3)
	input.DocumentRevisions = []uint64{901, 42}
	ids, retained, columns := sourceImportRowsV2("b", "a") // WAL sorts IDs; source ordinals must preserve this order.
	first, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns)
	if err != nil || first.ImportedChunks != 1 || first.Complete {
		t.Fatalf("first range: %+v %v", first, err)
	}
	binding := sourceImportBindingV2{Snapshot: input.Snapshot, IndexName: input.IndexName, VectorColumn: input.VectorColumn, GroupID: input.GroupID, Start: 0, End: ^uint64(0)}
	progressKey, _ := sourceImportKeysV2(c.Meta().Name, binding, 0)
	snap := d.AcquireSnapshot()
	progressRaw, progressPresent, progressErr := getSystemValue(snap, progressKey)
	bindingRaw, bindingPresent, bindingErr := getSystemValue(snap, progressKey+":binding")
	_ = snap.Close()
	if progressErr != nil || bindingErr != nil || !progressPresent || !bindingPresent || len(progressRaw) > 3000 || len(bindingRaw) > 3000 {
		t.Fatalf("unbounded or missing durable source progress: %d %d %v %v", len(progressRaw), len(bindingRaw), progressErr, bindingErr)
	}
	after := collectionCommandWALFrames(t, dir)
	if len(after) != len(before)+1 || after[len(after)-1].PayloadFormat != commitlog.PayloadFormatCollectionSourceImportV2 {
		t.Fatal("range did not use exactly one format-14 WAL command")
	}
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if len(collectionCommandWALFrames(t, dir)) != len(after) {
		t.Fatal("exact retry appended WAL")
	}
	changedColumns := append([]TypedColumnBatch(nil), columns...)
	changedColumns[1].Strings = []string{"changed", "content-a"}
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, changedColumns); err == nil {
		t.Fatal("changed non-vector typed row retry accepted")
	}
	changed := input
	changed.Snapshot.RowCount++
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, changed, ids, retained, columns); err == nil {
		t.Fatal("same revision rebound to changed immutable input")
	}
	for _, id := range ids {
		row, err := c.Get(id)
		if err != nil || !bytes.Contains(row, []byte("content-"+string(id))) {
			t.Fatalf("committed row %s: %s %v", id, row, err)
		}
	}
	last := input
	last.ChunkIndex = 1
	last.DocumentRevisions = []uint64{700}
	lastIDs, lastRetained, lastColumns := sourceImportRowsV2("c")
	complete, err := c.ImportVectorPartitionSourceChunkV2(ownership, last, lastIDs, lastRetained, lastColumns)
	if err != nil || !complete.Complete || complete.ImportedChunks != 2 || complete.Snapshot.Digest == ([sha256.Size]byte{}) {
		t.Fatalf("completion: %+v %v", complete, err)
	}
	for _, frame := range collectionCommandWALFrames(t, dir) {
		if frame.LSN > baseLSN {
			writeCollectionCommandWALFrame(t, replayDir, frame.LSN, frame.Kind, frame.PayloadFormat, frame.Payload)
		}
	}
	replayedDB := openTypedMinimaDB(t, replayDir)
	defer replayedDB.Close()
	replayed, err := NewCollectionManager(replayedDB).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	got, err := replayed.ImportVectorPartitionSourceChunkV2(ownership, last, lastIDs, lastRetained, lastColumns)
	if err != nil || got != complete {
		t.Fatalf("WAL replay changed source order/revisions/root: %+v want %+v err %v", got, complete, err)
	}
	if err := replayedDB.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := replayedDB.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedDB := openTypedMinimaDB(t, replayDir)
	defer reopenedDB.Close()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	got, err = reopened.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns)
	if err != nil || got != complete {
		t.Fatalf("checkpoint resume: %+v %v", got, err)
	}
}

func TestVectorPartitionSourceImportRejectsGapDuplicateAndWrongOwnerV2(t *testing.T) {
	dir, d, c := openTypedMinimaCollection(t)
	defer d.Close()
	ownership, input := sourceImportFixtureV2(t, c, 3)
	input.DocumentRevisions = []uint64{1, 2}
	ids, retained, columns := sourceImportRowsV2("b", "a")
	before := len(collectionCommandWALFrames(t, dir))
	bad := input
	bad.GroupID = "group-b"
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, bad, ids, retained, columns); err == nil {
		t.Fatal("wrong group accepted")
	}
	bad = input
	bad.Snapshot.ShardID = "source-b"
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, bad, ids, retained, columns); err == nil {
		t.Fatal("nonowner shard accepted")
	}
	bad = input
	bad.ChunkIndex = 1
	bad.DocumentRevisions = []uint64{3}
	oneID, oneRetained, oneColumns := sourceImportRowsV2("c")
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, bad, oneID, oneRetained, oneColumns); err == nil {
		t.Fatal("missing preceding range accepted")
	}
	if len(collectionCommandWALFrames(t, dir)) != before {
		t.Fatal("refusal appended WAL")
	}
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	duplicateID, duplicateRetained, duplicateColumns := sourceImportRowsV2("b")
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, bad, duplicateID, duplicateRetained, duplicateColumns); err == nil {
		t.Fatal("duplicate exact ID across ranges accepted")
	}
	progress, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns)
	if err != nil || progress.ImportedChunks != 1 || progress.Complete {
		t.Fatalf("refused range changed progress: %+v %v", progress, err)
	}
}

func TestVectorPartitionSourceImportPublicationBoundaryV2(t *testing.T) {
	_, d, c := openTypedMinimaCollection(t)
	defer d.Close()
	ownership, input := sourceImportFixtureV2(t, c, 2)
	input.DocumentRevisions = []uint64{1, 2}
	ids, retained, columns := sourceImportRowsV2("b", "a")
	command := sourceImportCommandMetadataV2{Version: 2, OrderedIDs: ids, Binding: sourceImportBindingV2{Snapshot: input.Snapshot, IndexName: input.IndexName, VectorColumn: input.VectorColumn, GroupID: input.GroupID, Start: 0, End: ^uint64(0)}, ChunkIndex: 0, DocumentRevisions: input.DocumentRevisions}
	injected := errors.New("before atomic source publication")
	call := func(hooks *sourcePublicationHooks) (VectorPartitionSourceImportProgressV2, error) {
		unlockSchema := c.lockCollectionSchemaRead()
		defer unlockSchema()
		unlockCoverage := c.lockVectorIndexCoverageMutation()
		defer unlockCoverage()
		projection, err := newTrustedTypedProjection(c.Meta(), ids, retained, columns)
		if err != nil {
			return VectorPartitionSourceImportProgressV2{}, err
		}
		return c.importSourceChunkSchemaLockedV2(command, ids, retained, projection, nil, hooks)
	}
	seq, root := dbCommitSeqAndSystemRoot(d)
	if _, err := call(&sourcePublicationHooks{beforePublish: func() error { return injected }}); !errors.Is(err, injected) {
		t.Fatalf("prepublication failure: %v", err)
	}
	if afterSeq, afterRoot := dbCommitSeqAndSystemRoot(d); afterSeq != seq || afterRoot != root {
		t.Fatal("failed range changed rows or progress root")
	}
	progressKey, receiptKey := sourceImportKeysV2(c.Meta().Name, command.Binding, 0)
	snap := d.AcquireSnapshot()
	_, progressPresent, err := getSystemValue(snap, progressKey)
	_, receiptPresent, receiptErr := getSystemValue(snap, receiptKey)
	_ = snap.Close()
	if err != nil || receiptErr != nil || progressPresent || receiptPresent {
		t.Fatalf("failed range retained progress/receipt: %t %t %v %v", progressPresent, receiptPresent, err, receiptErr)
	}
	got, err := call(&sourcePublicationHooks{afterPublish: func() error { return injected }})
	if !errors.Is(err, ErrCommitAmbiguous) || got.Complete {
		t.Fatalf("accepted ambiguous range: %+v %v", got, err)
	}
	retry, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns)
	if err != nil || !retry.Complete || retry.ImportedChunks != 1 {
		t.Fatalf("ambiguous exact retry: %+v %v", retry, err)
	}
}

// The explicit V2 source path must select an incremental authenticated
// directory. A bounded input batch alone is insufficient: the legacy TCS1
// publisher scans and re-encodes every prior manifest record on each import.
func TestVectorPartitionSourceImportUsesIncrementalDirectoryV2(t *testing.T) {
	_, d, c := openTypedMinimaCollection(t)
	defer d.Close()
	ownership, input := sourceImportFixtureV2(t, c, 2)
	input.DocumentRevisions = []uint64{1, 2}
	ids, retained, columns := sourceImportRowsV2("b", "a")
	if _, err := c.ImportVectorPartitionSourceChunkV2(ownership, input, ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	identity := c.Meta().Options.ColumnStore.ActiveManifest
	if identity == nil || identity.Format != "tcd2" || identity.Version != 2 {
		t.Fatalf("source import retained full TCS1 manifest instead of V2 incremental directory: %+v", identity)
	}
}
