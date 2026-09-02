package collections

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

func TestColumnAssetRewriteEligibleRefsAreDeterministicM15C(t *testing.T) {
	older := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  "ns",
		FileID:     2,
		Offset:     4,
		Length:     8,
		Generation: 1,
		PartID:     1,
	}
	newer := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  "ns",
		FileID:     7,
		Offset:     4,
		Length:     8,
		Generation: 1,
		PartID:     1,
	}
	refs := columnAssetRewriteEligibleRefs(
		ColumnAssetReachabilityPlan{Namespace: "ns"},
		map[ColumnAssetRef]columnAssetReachabilitySourceMask{
			newer: columnAssetReachabilitySourceActiveManifestMask,
			older: columnAssetReachabilitySourceActiveManifestMask,
		},
		map[uint32]ColumnAssetReachabilitySegmentEntry{
			newer.FileID: {},
			older.FileID: {},
		},
	)
	if len(refs) != 2 {
		t.Fatalf("eligible refs len=%d want 2", len(refs))
	}
	if compareColumnAssetRefs(refs[0], older) != 0 || compareColumnAssetRefs(refs[1], newer) != 0 {
		t.Fatalf("eligible refs order=%+v want [%+v %+v]", refs, older, newer)
	}
}

func TestPatchColumnAssetRewriteManifestRecordsRemapsOnlyRefFieldsM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	state, err := col.loadColumnAssetRewriteManifestState()
	if err != nil {
		t.Fatalf("loadColumnAssetRewriteManifestState: %v", err)
	}
	originalRecords := cloneColumnManifestRecords(state.records)
	var oldPart columnManifestPartSnapshot
	var oldRecordIdx int
	found := false
	for i, record := range state.records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			t.Fatalf("decodeColumnManifestPartRecord: %v", err)
		}
		oldPart = part
		oldRecordIdx = i
		found = true
		break
	}
	if !found {
		t.Fatal("no manifest part record found")
	}
	newRef := oldPart.AssetRef
	newRef.FileID += 17
	newRef.Offset += oldPart.AssetRef.Length + 11

	patched, count, err := patchColumnAssetRewriteManifestRecords(
		state.records,
		map[ColumnAssetRef]ColumnAssetRef{oldPart.AssetRef: newRef},
		state.cfg.AssetManager.Namespace,
	)
	if err != nil {
		t.Fatalf("patchColumnAssetRewriteManifestRecords: %v", err)
	}
	if count != 1 {
		t.Fatalf("patch count=%d want 1", count)
	}
	if len(patched) != len(state.records) {
		t.Fatalf("patched records=%d want %d", len(patched), len(state.records))
	}
	for i := range patched {
		if !bytes.Equal(patched[i].key, state.records[i].key) {
			t.Fatalf("patched key[%d]=%x want %x", i, patched[i].key, state.records[i].key)
		}
		if !bytes.Equal(state.records[i].key, originalRecords[i].key) || !bytes.Equal(state.records[i].value, originalRecords[i].value) {
			t.Fatalf("input record[%d] mutated", i)
		}
	}

	patchedPart, err := decodeColumnManifestPartRecord(patched[oldRecordIdx].value)
	if err != nil {
		t.Fatalf("decode patched part: %v", err)
	}
	if patchedPart.AssetRef != newRef {
		t.Fatalf("patched ref=%+v want %+v", patchedPart.AssetRef, newRef)
	}
	if patchedPart.Bytes != oldPart.Bytes || patchedPart.PublishID != oldPart.PublishID ||
		patchedPart.GenerationID != oldPart.GenerationID || patchedPart.Reason != oldPart.Reason {
		t.Fatalf("patched metadata=%+v want original metadata=%+v", patchedPart, oldPart)
	}
}

func TestPatchColumnAssetRewriteManifestRecordsRemapsVectorGraphRefsV2A(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	state, err := col.loadColumnAssetRewriteManifestState()
	if err != nil {
		t.Fatalf("loadColumnAssetRewriteManifestState: %v", err)
	}
	var oldPart columnManifestPartSnapshot
	for _, record := range state.records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			t.Fatalf("decodeColumnManifestPartRecord: %v", err)
		}
		oldPart = part
		break
	}
	if oldPart.AssetRef == (ColumnAssetRef{}) {
		t.Fatal("no manifest part record found")
	}
	oldGraphRef := oldPart.AssetRef
	oldGraphRef.PartID += 100
	oldGraphRef.Offset += oldPart.AssetRef.Length + 37
	def := testColumnGraphVectorIndexDefinitionV2A()
	oldGraphRecord := testColumnVectorGraphManifestRecordV2A(t, &state.cfg, def, *state.cfg.ActiveManifest, oldGraphRef)
	sourceCfg, _, err := columnVectorGraphLayer0AdjacencySourceColumnStoreConfig("events", state.cfg, def)
	if err != nil {
		t.Fatalf("columnVectorGraphLayer0AdjacencySourceColumnStoreConfig: %v", err)
	}
	sourceCfgLayer1, _, err := columnVectorGraphAdjacencySourceColumnStoreConfig("events", state.cfg, def, 1)
	if err != nil {
		t.Fatalf("columnVectorGraphAdjacencySourceColumnStoreConfig layer 1: %v", err)
	}
	oldSourceRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: oldGraphRef.Namespace, Generation: oldGraphRef.Generation, PartID: oldGraphRef.PartID + 1, FileID: oldGraphRef.FileID + 101, Offset: oldGraphRef.Offset + oldGraphRef.Length + 5, Length: 512, Checksum: 0x19181918}
	oldLayer1SourceRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: oldGraphRef.Namespace, Generation: oldGraphRef.Generation, PartID: oldGraphRef.PartID + 2, FileID: oldGraphRef.FileID + 102, Offset: oldSourceRef.Offset + oldSourceRef.Length + 7, Length: 640, Checksum: 0x19201920}
	oldGraph, err := decodeColumnVectorGraphManifestRecord(oldGraphRecord.value)
	if err != nil {
		t.Fatalf("decode old graph record: %v", err)
	}
	oldGraph.Layer0AdjacencySource = columnVectorGraphLayer0AdjacencySourceSnapshot{Present: true, Schema: columnVectorGraphLayer0AdjacencySourceSchema, ColumnName: columnVectorGraphLayer0AdjacencySourceColumnName, ValueType: string(ColumnStoreValueAdjacencyList), Encoding: "raw_uint32_offsets_list", Layer: 0, SourceSchemaHash: sourceCfg.SchemaHash, RowCount: oldGraph.RowCount, ValuesCount: 2, OffsetsBytes: int64(oldGraph.RowCount+1) * 8, ValuesBytes: 8, PaddingBytes: 0, Ref: oldSourceRef, AssetBytes: oldSourceRef.Length, BaseManifestGeneration: oldGraph.BaseManifestGeneration, BaseManifestChecksum: oldGraph.BaseManifestChecksum, BaseSchemaHash: oldGraph.BaseSchemaHash, GraphSchemaHash: oldGraph.GraphSchemaHash, GraphAssetGeneration: oldGraph.AssetRef.Generation, GraphAssetPartID: oldGraph.AssetRef.PartID, GraphAssetFileID: oldGraph.AssetRef.FileID, GraphAssetOffset: oldGraph.AssetRef.Offset, GraphAssetLength: oldGraph.AssetRef.Length, GraphAssetChecksum: oldGraph.AssetRef.Checksum}
	layer1Source := columnVectorGraphLayer0AdjacencySourceSnapshot{Present: true, Schema: columnVectorGraphAdjacencySourceSchema(1), ColumnName: columnVectorGraphAdjacencySourceColumnName(1), ValueType: string(ColumnStoreValueAdjacencyList), Encoding: "raw_uint32_offsets_list", Layer: 1, SourceSchemaHash: sourceCfgLayer1.SchemaHash, RowCount: oldGraph.RowCount, ValuesCount: 1, OffsetsBytes: int64(oldGraph.RowCount+1) * 8, ValuesBytes: 4, PaddingBytes: 0, Ref: oldLayer1SourceRef, AssetBytes: oldLayer1SourceRef.Length, BaseManifestGeneration: oldGraph.BaseManifestGeneration, BaseManifestChecksum: oldGraph.BaseManifestChecksum, BaseSchemaHash: oldGraph.BaseSchemaHash, GraphSchemaHash: oldGraph.GraphSchemaHash, GraphAssetGeneration: oldGraph.AssetRef.Generation, GraphAssetPartID: oldGraph.AssetRef.PartID, GraphAssetFileID: oldGraph.AssetRef.FileID, GraphAssetOffset: oldGraph.AssetRef.Offset, GraphAssetLength: oldGraph.AssetRef.Length, GraphAssetChecksum: oldGraph.AssetRef.Checksum}
	oldGraph.AdjacencyLayerCount = 2
	oldGraph.AdjacencyLayerSources = []columnVectorGraphLayer0AdjacencySourceSnapshot{oldGraph.Layer0AdjacencySource, layer1Source}
	oldGraphRecord.value, err = encodeColumnVectorGraphManifestRecord(oldGraph)
	if err != nil {
		t.Fatalf("encode old graph record with source: %v", err)
	}
	records := append(cloneColumnManifestRecords(state.records), oldGraphRecord)
	sortColumnManifestRecords(records)

	newPartRef := oldPart.AssetRef
	newPartRef.FileID += 17
	newPartRef.Offset += oldPart.AssetRef.Length + 11
	newGraphRef := oldGraphRef
	newGraphRef.FileID += 19
	newGraphRef.Offset += oldGraphRef.Length + 13
	newSourceRef := oldSourceRef
	newSourceRef.FileID += 23
	newSourceRef.Offset += oldSourceRef.Length + 17
	newLayer1SourceRef := oldLayer1SourceRef
	newLayer1SourceRef.FileID += 29
	newLayer1SourceRef.Offset += oldLayer1SourceRef.Length + 19
	patched, count, err := patchColumnAssetRewriteManifestRecords(
		records,
		map[ColumnAssetRef]ColumnAssetRef{
			oldPart.AssetRef:   newPartRef,
			oldGraphRef:        newGraphRef,
			oldSourceRef:       newSourceRef,
			oldLayer1SourceRef: newLayer1SourceRef,
		},
		state.cfg.AssetManager.Namespace,
	)
	if err != nil {
		t.Fatalf("patchColumnAssetRewriteManifestRecords: %v", err)
	}
	if count != 4 {
		t.Fatalf("patch count=%d want 4", count)
	}
	graphRecord, ok := findColumnVectorGraphManifestRecord(patched, testColumnGraphVectorIndexDefinitionV2A().Name)
	if !ok {
		t.Fatal("patched graph record missing")
	}
	graph, err := decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		t.Fatalf("decode patched graph record: %v", err)
	}
	if graph.AssetRef != newGraphRef {
		t.Fatalf("patched graph ref=%+v want %+v", graph.AssetRef, newGraphRef)
	}
	if graph.Layer0AdjacencySource.Ref != newSourceRef {
		t.Fatalf("patched graph source ref=%+v want %+v", graph.Layer0AdjacencySource.Ref, newSourceRef)
	}
	if graph.Layer0AdjacencySource.GraphAssetFileID != newGraphRef.FileID || graph.Layer0AdjacencySource.GraphAssetOffset != newGraphRef.Offset || graph.Layer0AdjacencySource.GraphAssetLength != newGraphRef.Length || graph.Layer0AdjacencySource.GraphAssetChecksum != newGraphRef.Checksum {
		t.Fatalf("patched graph source identity=%+v does not track graph ref=%+v", graph.Layer0AdjacencySource, newGraphRef)
	}
	if len(graph.AdjacencyLayerSources) != 2 || graph.AdjacencyLayerSources[1].Ref != newLayer1SourceRef {
		t.Fatalf("patched graph all-layer sources=%+v want layer-1 ref %+v", graph.AdjacencyLayerSources, newLayer1SourceRef)
	}
	if graph.AdjacencyLayerSources[1].GraphAssetFileID != newGraphRef.FileID || graph.AdjacencyLayerSources[1].GraphAssetOffset != newGraphRef.Offset || graph.AdjacencyLayerSources[1].GraphAssetLength != newGraphRef.Length || graph.AdjacencyLayerSources[1].GraphAssetChecksum != newGraphRef.Checksum {
		t.Fatalf("patched graph layer-1 source identity=%+v does not track graph ref=%+v", graph.AdjacencyLayerSources[1], newGraphRef)
	}

	inPlaceRecords := cloneColumnManifestRecords(records)
	inPlacePatched, inPlaceCount, err := patchColumnAssetRewriteManifestRecordsInPlace(
		inPlaceRecords,
		map[ColumnAssetRef]ColumnAssetRef{
			oldPart.AssetRef:   newPartRef,
			oldGraphRef:        newGraphRef,
			oldSourceRef:       newSourceRef,
			oldLayer1SourceRef: newLayer1SourceRef,
		},
		state.cfg.AssetManager.Namespace,
	)
	if err != nil {
		t.Fatalf("patchColumnAssetRewriteManifestRecordsInPlace: %v", err)
	}
	if inPlaceCount != 4 {
		t.Fatalf("in-place patch count=%d want 4", inPlaceCount)
	}
	if len(inPlacePatched) != 0 && &inPlacePatched[0] != &inPlaceRecords[0] {
		t.Fatal("in-place vector graph patch returned a copied record slice")
	}
	graphRecord, ok = findColumnVectorGraphManifestRecord(inPlacePatched, testColumnGraphVectorIndexDefinitionV2A().Name)
	if !ok {
		t.Fatal("in-place patched graph record missing")
	}
	graph, err = decodeColumnVectorGraphManifestRecord(graphRecord.value)
	if err != nil {
		t.Fatalf("decode in-place patched graph record: %v", err)
	}
	if graph.AssetRef != newGraphRef {
		t.Fatalf("in-place patched graph ref=%+v want %+v", graph.AssetRef, newGraphRef)
	}
	if graph.Layer0AdjacencySource.Ref != newSourceRef {
		t.Fatalf("in-place patched graph source ref=%+v want %+v", graph.Layer0AdjacencySource.Ref, newSourceRef)
	}
	if graph.Layer0AdjacencySource.GraphAssetFileID != newGraphRef.FileID || graph.Layer0AdjacencySource.GraphAssetOffset != newGraphRef.Offset || graph.Layer0AdjacencySource.GraphAssetLength != newGraphRef.Length || graph.Layer0AdjacencySource.GraphAssetChecksum != newGraphRef.Checksum {
		t.Fatalf("in-place patched graph source identity=%+v does not track graph ref=%+v", graph.Layer0AdjacencySource, newGraphRef)
	}
	if len(graph.AdjacencyLayerSources) != 2 || graph.AdjacencyLayerSources[1].Ref != newLayer1SourceRef {
		t.Fatalf("in-place patched graph all-layer sources=%+v want layer-1 ref %+v", graph.AdjacencyLayerSources, newLayer1SourceRef)
	}
	if graph.AdjacencyLayerSources[1].GraphAssetFileID != newGraphRef.FileID || graph.AdjacencyLayerSources[1].GraphAssetOffset != newGraphRef.Offset || graph.AdjacencyLayerSources[1].GraphAssetLength != newGraphRef.Length || graph.AdjacencyLayerSources[1].GraphAssetChecksum != newGraphRef.Checksum {
		t.Fatalf("in-place patched graph layer-1 source identity=%+v does not track graph ref=%+v", graph.AdjacencyLayerSources[1], newGraphRef)
	}
}

func TestPatchColumnAssetRewriteManifestRecordsRemapsVectorIndexStateRefs1986(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	state, err := col.loadColumnAssetRewriteManifestState()
	if err != nil {
		t.Fatalf("loadColumnAssetRewriteManifestState: %v", err)
	}
	if state.cfg.ActiveManifest == nil {
		t.Fatal("test collection missing active manifest")
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	oldGraphRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: state.cfg.AssetManager.Namespace, Generation: state.cfg.ActiveManifest.Generation, PartID: 90, FileID: 7, Offset: 128, Length: 2048, Checksum: 0x19860001}
	oldGraphRecord := testColumnVectorGraphManifestRecordV2A(t, &state.cfg, def, *state.cfg.ActiveManifest, oldGraphRef)
	oldGraph, err := decodeColumnVectorGraphManifestRecord(oldGraphRecord.value)
	if err != nil {
		t.Fatalf("decode old graph record: %v", err)
	}
	oldAdjacencyRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: oldGraphRef.Namespace, Generation: oldGraph.BaseManifestGeneration, PartID: oldGraphRef.PartID + 1, FileID: oldGraphRef.FileID + 11, Offset: oldGraphRef.Offset + oldGraphRef.Length + 64, Length: 512, Checksum: 0x19860002}
	oldNormRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1TypedColumnPart, Namespace: oldGraphRef.Namespace, Generation: oldGraph.BaseManifestGeneration, PartID: oldGraphRef.PartID + 2, FileID: oldGraphRef.FileID + 12, Offset: oldAdjacencyRef.Offset + oldAdjacencyRef.Length + 64, Length: 256, Checksum: 0x19860003}
	oldPackRef := ColumnAssetRef{Kind: ColumnAssetKindTCS1HNSWSearchPack, Namespace: oldGraphRef.Namespace, Generation: oldGraph.BaseManifestGeneration, PartID: oldGraphRef.PartID + 3, FileID: oldGraphRef.FileID + 13, Offset: oldNormRef.Offset + oldNormRef.Length + 64, Length: 768, Checksum: 0x19860004}
	indexState := columnVectorIndexStateSnapshotFromGraph(oldGraph)
	indexState.Assets = []columnVectorIndexStateAssetSnapshot{
		columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleAdjacency, "hnsw/layer/0", oldAdjacencyRef, indexState.RowCount, state.cfg.SchemaHash+1),
		columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleInverseNorm, "inv_norm_by_ordinal", oldNormRef, indexState.RowCount, state.cfg.SchemaHash+2),
		columnVectorIndexStateAssetSnapshotForTest(columnVectorIndexStateAssetRoleHNSWSearchPack, columnVectorIndexStateHNSWSearchPackAssetID, oldPackRef, indexState.RowCount, state.cfg.SchemaHash+3),
	}
	stateRaw, err := encodeColumnVectorIndexStateRecord(indexState)
	if err != nil {
		t.Fatalf("encodeColumnVectorIndexStateRecord: %v", err)
	}
	records := append(cloneColumnManifestRecords(state.records), oldGraphRecord, columnManifestRecord{key: columnVectorIndexStateRecordKey(def.Name), value: stateRaw})
	sortColumnManifestRecords(records)

	newAdjacencyRef := oldAdjacencyRef
	newAdjacencyRef.FileID += 101
	newAdjacencyRef.Offset += oldAdjacencyRef.Length + 17
	newNormRef := oldNormRef
	newNormRef.FileID += 102
	newNormRef.Offset += oldNormRef.Length + 19
	newPackRef := oldPackRef
	newPackRef.FileID += 103
	newPackRef.Offset += oldPackRef.Length + 23
	patched, count, err := patchColumnAssetRewriteManifestRecords(
		records,
		map[ColumnAssetRef]ColumnAssetRef{
			oldAdjacencyRef: newAdjacencyRef,
			oldNormRef:      newNormRef,
			oldPackRef:      newPackRef,
		},
		state.cfg.AssetManager.Namespace,
	)
	if err != nil {
		t.Fatalf("patchColumnAssetRewriteManifestRecords: %v", err)
	}
	if count != 3 {
		t.Fatalf("patch count=%d want 3", count)
	}
	stateRecord, ok := findColumnVectorIndexStateRecord(patched, def.Name)
	if !ok {
		t.Fatal("patched vector-index state record missing")
	}
	patchedState, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		t.Fatalf("decode patched vector-index state record: %v", err)
	}
	if len(patchedState.Assets) != 3 {
		t.Fatalf("patched state assets=%d want 3", len(patchedState.Assets))
	}
	if patchedState.Assets[0].Ref != newAdjacencyRef || patchedState.Assets[0].AssetBytes != newAdjacencyRef.Length {
		t.Fatalf("patched adjacency asset=%+v want ref=%+v bytes=%d", patchedState.Assets[0], newAdjacencyRef, newAdjacencyRef.Length)
	}
	if patchedState.Assets[1].Ref != newNormRef || patchedState.Assets[1].AssetBytes != newNormRef.Length {
		t.Fatalf("patched norm asset=%+v want ref=%+v bytes=%d", patchedState.Assets[1], newNormRef, newNormRef.Length)
	}
	if patchedState.Assets[2].Ref != newPackRef || patchedState.Assets[2].AssetBytes != newPackRef.Length {
		t.Fatalf("patched hnsw search pack asset=%+v want ref=%+v bytes=%d", patchedState.Assets[2], newPackRef, newPackRef.Length)
	}

	inPlaceRecords := cloneColumnManifestRecords(records)
	inPlacePatched, inPlaceCount, err := patchColumnAssetRewriteManifestRecordsInPlace(
		inPlaceRecords,
		map[ColumnAssetRef]ColumnAssetRef{
			oldAdjacencyRef: newAdjacencyRef,
			oldNormRef:      newNormRef,
			oldPackRef:      newPackRef,
		},
		state.cfg.AssetManager.Namespace,
	)
	if err != nil {
		t.Fatalf("patchColumnAssetRewriteManifestRecordsInPlace: %v", err)
	}
	if inPlaceCount != 3 {
		t.Fatalf("in-place patch count=%d want 3", inPlaceCount)
	}
	if len(inPlacePatched) != 0 && &inPlacePatched[0] != &inPlaceRecords[0] {
		t.Fatal("in-place vector-index state patch returned a copied record slice")
	}
	stateRecord, ok = findColumnVectorIndexStateRecord(inPlacePatched, def.Name)
	if !ok {
		t.Fatal("in-place patched vector-index state record missing")
	}
	patchedState, err = decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		t.Fatalf("decode in-place patched vector-index state record: %v", err)
	}
	if patchedState.Assets[0].Ref != newAdjacencyRef || patchedState.Assets[1].Ref != newNormRef || patchedState.Assets[2].Ref != newPackRef {
		t.Fatalf("in-place patched state assets=%+v", patchedState.Assets)
	}
}

func TestPatchColumnAssetRewriteManifestRecordsInPlaceM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	state, err := col.loadColumnAssetRewriteManifestState()
	if err != nil {
		t.Fatalf("loadColumnAssetRewriteManifestState: %v", err)
	}
	var oldPart columnManifestPartSnapshot
	oldRecordIdx := -1
	for i, record := range state.records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		part, err := decodeColumnManifestPartRecord(record.value)
		if err != nil {
			t.Fatalf("decodeColumnManifestPartRecord: %v", err)
		}
		oldPart = part
		oldRecordIdx = i
		break
	}
	if oldRecordIdx < 0 {
		t.Fatal("no manifest part record found")
	}
	newRef := oldPart.AssetRef
	newRef.FileID += 23
	newRef.Offset += oldPart.AssetRef.Length + 7

	patched, count, err := patchColumnAssetRewriteManifestRecordsInPlace(
		state.records,
		map[ColumnAssetRef]ColumnAssetRef{oldPart.AssetRef: newRef},
		state.cfg.AssetManager.Namespace,
	)
	if err != nil {
		t.Fatalf("patchColumnAssetRewriteManifestRecordsInPlace: %v", err)
	}
	if count != 1 {
		t.Fatalf("patch count=%d want 1", count)
	}
	if len(patched) != len(state.records) {
		t.Fatalf("patched records=%d want %d", len(patched), len(state.records))
	}
	if len(patched) != 0 && &patched[0] != &state.records[0] {
		t.Fatal("in-place patch returned a copied record slice")
	}
	patchedPart, err := decodeColumnManifestPartRecord(state.records[oldRecordIdx].value)
	if err != nil {
		t.Fatalf("decode patched part: %v", err)
	}
	if patchedPart.AssetRef != newRef {
		t.Fatalf("patched ref=%+v want %+v", patchedPart.AssetRef, newRef)
	}
	if patchedPart.Bytes != oldPart.Bytes || patchedPart.PublishID != oldPart.PublishID ||
		patchedPart.GenerationID != oldPart.GenerationID || patchedPart.Reason != oldPart.Reason {
		t.Fatalf("patched metadata=%+v want original metadata=%+v", patchedPart, oldPart)
	}
}

func TestColumnAssetRewriteCopyStableAuthorityExactSyncCounts(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(refs) == 0 {
		t.Fatal("manifest refs empty, test requires stable rewrite inputs")
	}
	cfg := *col.Meta().Options.ColumnStore
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	const cycles = 16
	for cycle := 0; cycle < cycles; cycle++ {
		func() {
			remap, err := col.copyColumnAssetRewriteRefs(context.Background(), cfg, refs)
			if err != nil {
				t.Fatalf("copy cycle %d: %v", cycle, err)
			}
			defer remap.releaseStableResources()
			if remap.stableSegments != 1 || remap.stableDescriptors != 2 || remap.stableContentSyncs != 1 || remap.stableNamespaceSyncs != 1 || remap.stablePinHighWater != remap.stableDescriptors {
				t.Fatalf("copy cycle %d stable counters segments=%d descriptors=%d content_syncs=%d namespace_syncs=%d pin_high_water=%d want 1,2,1,1,2", cycle, remap.stableSegments, remap.stableDescriptors, remap.stableContentSyncs, remap.stableNamespaceSyncs, remap.stablePinHighWater)
			}
			if got := len(remap.stableResources.Descriptors()); got != 2 {
				t.Fatalf("copy cycle %d descriptors=%d want 2", cycle, got)
			}
			if got := registry.ActivePins(); got != baselinePins+2 {
				t.Fatalf("copy cycle %d active pins=%d want %d", cycle, got, baselinePins+2)
			}
			if got := registry.ActiveIdentities(); got != baselineIdentities+1 {
				t.Fatalf("copy cycle %d active identities=%d want %d", cycle, got, baselineIdentities+1)
			}
		}()
		if got := registry.ActivePins(); got != baselinePins {
			t.Fatalf("copy cycle %d released pins=%d want baseline %d", cycle, got, baselinePins)
		}
		if got := registry.ActiveIdentities(); got != baselineIdentities {
			t.Fatalf("copy cycle %d released identities=%d want baseline %d", cycle, got, baselineIdentities)
		}
	}
	t.Logf("stable rewrite copy: cycles=%d segments_per_cycle=1 descriptors_per_cycle=2 content_syncs_per_cycle=1 namespace_syncs_per_cycle=1 descriptor_pin_high_water=2", cycles)
}

func TestColumnAssetRewritePreservesScalarU8Alignment4234(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	cfg := *col.Meta().Options.ColumnStore

	appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(d.ColumnAssetRootDir(), cfg, d.StableResourceIdentityPinRegistry())
	if err != nil {
		t.Fatalf("new source appender: %v", err)
	}
	seed, err := appender.appendKindWithAlignment([]byte("seed-payload!"), ColumnAssetKindTCS1PartImage, 7, 1, 0)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append seed: %v", err)
	}
	aligned, err := appender.appendKindWithAlignment([]byte("scalar-u8-codes"), ColumnAssetKindTCS1TypedColumnPart, 7, 2, columnVectorGraphScalarU8CodesAlignment)
	if err != nil {
		_ = appender.abort()
		t.Fatalf("append aligned ref: %v", err)
	}
	if err := appender.close(); err != nil {
		t.Fatalf("close source appender: %v", err)
	}
	if appender.stableResources == nil {
		t.Fatal("source appender returned no stable authority")
	}
	appender.stableResources.Release()
	appender.stableResources = nil
	if aligned.Offset%columnVectorGraphScalarU8CodesAlignment != 0 {
		t.Fatalf("source offset=%d want %d-byte alignment", aligned.Offset, columnVectorGraphScalarU8CodesAlignment)
	}

	remap, err := col.copyColumnAssetRewriteRefs(context.Background(), cfg, []ColumnAssetRef{seed, aligned})
	if err != nil {
		t.Fatalf("copyColumnAssetRewriteRefs: %v", err)
	}
	defer remap.releaseStableResources()
	rewritten, ok := remap.byOldRef[aligned]
	if !ok {
		t.Fatalf("aligned ref missing from remap: %+v", remap.byOldRef)
	}
	if rewritten.Offset%columnVectorGraphScalarU8CodesAlignment != 0 {
		t.Fatalf("rewritten offset=%d want preserved %d-byte alignment", rewritten.Offset, columnVectorGraphScalarU8CodesAlignment)
	}
}

func TestColumnAssetRewriteRemapsManifestRefsOutOfMixedSegmentM15C(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	dClosed := false
	defer func() {
		if !dClosed {
			_ = d.Close()
		}
	}()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) == 0 {
		t.Fatal("manifest refs empty, test requires live physical assets")
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	oldSegmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	dry, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		DryRun:        true,
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite dry-run: %v", err)
	}
	if !dry.DryRun || dry.SegmentsEligible != 1 || dry.SegmentsRewritten != 0 || dry.RefsEligible != len(beforeRefs) {
		t.Fatalf("dry-run stats=%+v want one eligible mixed segment and %d eligible refs", dry, len(beforeRefs))
	}
	if dry.Plan.Segments.Mixed != 1 || dry.Plan.RewriteDebtBytes != candidate.Length {
		t.Fatalf("dry-run plan segments=%+v debt=%d want one mixed segment with candidate debt %d", dry.Plan.Segments, dry.Plan.RewriteDebtBytes, candidate.Length)
	}
	if len(dry.Plan.Entries) == 0 {
		t.Fatal("detailed dry-run rewrite plan omitted ref entries")
	}
	for i, entry := range dry.Plan.Entries {
		if len(entry.Sources) == 0 {
			t.Fatalf("detailed dry-run entry[%d] ref=%+v omitted sources", i, entry.Ref)
		}
		if i > 0 && compareColumnAssetRefs(dry.Plan.Entries[i-1].Ref, entry.Ref) > 0 {
			t.Fatalf("detailed dry-run entries are not sorted at %d: prev=%+v current=%+v", i, dry.Plan.Entries[i-1].Ref, entry.Ref)
		}
	}

	stats, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if stats.DryRun || stats.SegmentsEligible != 1 || stats.SegmentsRewritten != 1 || stats.RefsRemapped != len(beforeRefs) {
		t.Fatalf("stats=%+v want one rewritten segment and %d remapped refs", stats, len(beforeRefs))
	}
	if stats.BytesCopied <= 0 || stats.BytesReclaimable < candidate.Length {
		t.Fatalf("stats=%+v want copied live bytes and reclaimable candidate bytes >= %d", stats, candidate.Length)
	}
	if len(stats.SupersededRefs) != len(beforeRefs) || len(stats.RemappedRefs) != len(beforeRefs) {
		t.Fatalf("stats superseded=%d remapped=%d want %d", len(stats.SupersededRefs), len(stats.RemappedRefs), len(beforeRefs))
	}

	afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, afterRefs)
	for _, ref := range afterRefs {
		if _, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("remapped ref %+v unreadable: %v", ref, err)
		}
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("rewrite removed old mixed segment before GC: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}
	dClosed = true
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	reopenRefs := columnManifestAssetRefsForCollectionM12A(t, reopen, reopened)
	assertColumnAssetRefsEqualM15C(t, afterRefs, reopenRefs)
	diag, err := reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{})
	if err != nil {
		t.Fatalf("scanColumnPhysicalRows after reopen: %v", err)
	}
	afterPhysicalRefs := columnManifestPhysicalAssetRefsForTestM1634(afterRefs)
	if diag.RowsScanned != 2 || diag.AssetRefs != len(afterPhysicalRefs) {
		t.Fatalf("diag=%+v want 2 rows and %d remapped physical refs", diag, len(afterPhysicalRefs))
	}
	gcStats, err := reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), stats.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after rewrite: %v", err)
	}
	if gcStats.SegmentsEligible != 1 || gcStats.SegmentsDeleted != 0 {
		t.Fatalf("first gc stats=%+v want old mixed segment retained by fallback durable generation", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("first GC removed fallback generation segment: %v", err)
	}
	advanceColumnAssetDurableFallbackM15C(t, reopen)
	gcStats, err = reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), stats.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after fallback advance: %v", err)
	}
	if gcStats.SegmentsDeleted != 1 || gcStats.BytesDeleted == 0 {
		t.Fatalf("gc stats=%+v want old mixed segment deleted after fallback advance", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old segment still exists or unexpected stat error: %v", err)
	}
	for _, ref := range reopenRefs {
		if _, err := readColumnPhysicalAssetFromManager(reopen.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("live remapped ref %+v unreadable after GC: %v", ref, err)
		}
	}
}

func TestColumnAssetRewriteSkipsSegmentWhenManifestRefAlsoProtectedByPinnedSourceM15C(t *testing.T) {
	namespace := "collections/events/column-assets"
	segmentDir := t.TempDir()
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  namespace,
		Generation: 7,
		PartID:     3,
		FileID:     2,
		Offset:     0,
		Length:     10,
		Checksum:   99,
	}
	plan := ColumnAssetReachabilityPlan{
		Namespace: namespace,
		Segments:  ColumnAssetReachabilitySegmentStats{Mixed: 1},
		SegmentEntries: []ColumnAssetReachabilitySegmentEntry{{
			Namespace:        namespace,
			FileID:           ref.FileID,
			Path:             filepath.Join(segmentDir, columnAssetSegmentFileName(ref.FileID)),
			Bytes:            20,
			Status:           ColumnAssetReachabilitySegmentMixed,
			ProtectedBytes:   ref.Length,
			ReclaimableBytes: 10,
			RefCount:         2,
		}},
	}
	sourceMasks := map[ColumnAssetRef]columnAssetReachabilitySourceMask{
		ref: columnAssetReachabilitySourceActiveManifestMask | columnAssetReachabilitySourcePinnedSnapshotMask,
	}

	segments := columnAssetRewriteEligibleSegments(segmentDir, plan, sourceMasks)
	if len(segments) != 0 {
		t.Fatalf("eligible segments=%v want none when manifest ref is also pinned", segments)
	}
	refs := columnAssetRewriteEligibleRefs(plan, sourceMasks, segments)
	if len(refs) != 0 {
		t.Fatalf("eligible refs=%v want none when manifest ref is also pinned", refs)
	}
}

func TestColumnAssetRewriteSkipsPreparedRunnerLifecyclePin1954(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) == 0 {
		t.Fatal("manifest refs empty, test requires live physical assets")
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	runner, err := col.PrepareColumnPhysicalQuery(ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"})
	if err != nil {
		t.Fatalf("PrepareColumnPhysicalQuery: %v", err)
	}
	pinned, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		_ = runner.Close()
		t.Fatalf("ColumnAssetRewrite while prepared runner lifecycle pin active: %v", err)
	}
	if pinned.SegmentsEligible != 0 || pinned.SegmentsRewritten != 0 || pinned.RefsEligible != 0 || pinned.RefsRemapped != 0 {
		_ = runner.Close()
		t.Fatalf("pinned rewrite stats=%+v want prepared-runner segment skipped", pinned)
	}
	if pinned.Plan.Sources.PreparedQueryRefs != len(columnPhysicalScanSnapshotViewAssetRefs(runner.view)) {
		_ = runner.Close()
		t.Fatalf("prepared-query refs=%d want runner refs; plan=%+v", pinned.Plan.Sources.PreparedQueryRefs, pinned.Plan.Sources)
	}
	assertColumnAssetRefsEqualM15C(t, beforeRefs, columnManifestAssetRefsForCollectionM12A(t, d, col))
	if err := runner.Close(); err != nil {
		t.Fatalf("runner close: %v", err)
	}

	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{CandidateRefs: []ColumnAssetRef{candidate}})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite after prepared runner close: %v", err)
	}
	if rewrite.SegmentsRewritten != 1 || rewrite.RefsRemapped != len(beforeRefs) {
		t.Fatalf("rewrite stats=%+v want one rewritten segment and %d refs", rewrite, len(beforeRefs))
	}
}

func TestColumnAssetRewriteAutomaticMappedResourcePinSkipsSegment1788(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) == 0 {
		t.Fatal("manifest refs empty, test requires live physical assets")
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}

	mgr := mappedresource.NewManager()
	readCache, err := newColumnPhysicalAssetReadCacheWithIntegrity(d.ColumnAssetRootDir(), beforeRefs[0].Namespace, ColumnAssetReadIntegrityVerify)
	if err != nil {
		t.Fatalf("new read cache: %v", err)
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: "auto-rewrite-pin-1788", Namespace: beforeRefs[0].Namespace, Collection: "events", Generation: beforeRefs[0].Generation, Reason: "rewrite auto pin test"}
	if err := readCache.useMappedResourceManager(mgr, scope, "rewrite-auto-pin"); err != nil {
		_ = readCache.close()
		t.Fatalf("useMappedResourceManager: %v", err)
	}
	if _, err := readCache.read(beforeRefs[0], nil); err != nil {
		_ = readCache.close()
		t.Fatalf("read live ref for rewrite pin: %v", err)
	}

	pinned, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		_ = readCache.close()
		t.Fatalf("ColumnAssetRewrite while mappedresource pin active: %v", err)
	}
	if pinned.SegmentsEligible != 0 || pinned.SegmentsRewritten != 0 || pinned.RefsEligible != 0 || pinned.RefsRemapped != 0 {
		_ = readCache.close()
		t.Fatalf("pinned rewrite stats=%+v want segment skipped", pinned)
	}
	if pinned.Plan.MappedResources.ActiveHandles == 0 || pinned.Plan.MappedResources.PinnedRefs == 0 || pinned.Plan.Sources.MappedResourcePins == 0 {
		_ = readCache.close()
		t.Fatalf("mappedresource stats=%+v sources=%+v want active pin", pinned.Plan.MappedResources, pinned.Plan.Sources)
	}
	assertColumnAssetRefsEqualM15C(t, beforeRefs, columnManifestAssetRefsForCollectionM12A(t, d, col))
	if err := readCache.close(); err != nil {
		t.Fatalf("close read cache: %v", err)
	}

	rewrite, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite after pin release: %v", err)
	}
	if rewrite.SegmentsRewritten != 1 || rewrite.RefsRemapped != len(beforeRefs) {
		t.Fatalf("rewrite stats=%+v want one rewritten segment and %d refs", rewrite, len(beforeRefs))
	}
}

func TestColumnAssetRewriteLifecycleSmokeWithMutationsM15C(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	dClosed := false
	defer func() {
		if !dClosed {
			_ = d.Close()
		}
	}()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1","payload":"alpha"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2","payload":"beta"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	if _, modified, err := col.Update([]byte("e1"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"time_us":3,"kind":"share","did":"d1","payload":"alpha2"}`), true, nil
	}); err != nil || !modified {
		t.Fatalf("Update modified=%t err=%v, want modified update", modified, err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("e2")}); err != nil || deleted != 1 {
		t.Fatalf("DeleteBatch deleted=%d err=%v, want one delete", deleted, err)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) < 3 {
		t.Fatalf("manifest refs=%d want insert/update/delete assets", len(beforeRefs))
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 4, 99)
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
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, afterRefs)
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("rewrite removed old mixed segment before GC: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}
	dClosed = true
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	liveEvents := []columnPhysicalQueryEventM13B{{
		ID:     "e1",
		TimeUS: 3,
		Kind:   "share",
		Did:    "d1",
	}}
	assertLifecycleQueries := func(label string) {
		t.Helper()
		tests := []struct {
			name     string
			hashName string
			req      ColumnPhysicalQueryRequest
		}{
			{name: "q1", hashName: "q1", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "kind"}},
			{name: "q2", hashName: "q2", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupCountDistinct, GroupColumn: "kind", DistinctColumn: "did"}},
			{name: "q3", hashName: "q3", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryHourCount, ValueColumn: "time_us"}},
			{name: "q4a", hashName: "q4a", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMinInt64, GroupColumn: "did", ValueColumn: "time_us"}},
			{name: "q4b", hashName: "q4b", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupMaxInt64, GroupColumn: "did", ValueColumn: "time_us"}},
			{name: "q5", hashName: "q5", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"}},
			{name: "q5_metadata", hashName: "q5", req: ColumnPhysicalQueryRequest{Kind: ColumnPhysicalQueryGroupInt64Span, GroupColumn: "did", ValueColumn: "time_us"}},
		}
		for _, tc := range tests {
			result, err := reopened.RunColumnPhysicalQuery(tc.req)
			if err != nil {
				t.Fatalf("%s RunColumnPhysicalQuery(%s): %v", label, tc.name, err)
			}
			got := columnPhysicalQueryHashLinesM13B(columnPhysicalQueryLinesM13B(tc.name, result.Groups))
			want := columnPhysicalQueryReferenceHashM13B(tc.hashName, liveEvents)
			if got != want {
				t.Fatalf("%s %s hash=%016x want %016x lines=%v diagnostics=%+v", label, tc.name, got, want, columnPhysicalQueryLinesM13B(tc.name, result.Groups), result.Diagnostics)
			}
			if result.Diagnostics.RowMaterializations != 0 || result.Diagnostics.ReconstructionRows != 0 {
				t.Fatalf("%s %s diagnostics materialized/reconstructed rows: %+v", label, tc.name, result.Diagnostics)
			}
			if result.Diagnostics.ReduceRows != 1 || result.Diagnostics.DeletedRows != 1 {
				t.Fatalf("%s %s visibility diagnostics=%+v want one reduced live row and one tombstone", label, tc.name, result.Diagnostics)
			}
		}
	}
	assertLifecycleQueries("after reopen")

	gcStats, err := reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), rewrite.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after lifecycle rewrite: %v", err)
	}
	if gcStats.SegmentsEligible != 1 || gcStats.SegmentsDeleted != 0 {
		t.Fatalf("first gc stats=%+v want old mixed segment retained by fallback durable generation", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("first GC removed fallback generation segment: %v", err)
	}
	advanceColumnAssetDurableFallbackM15C(t, reopen)
	gcStats, err = reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), rewrite.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after fallback advance: %v", err)
	}
	if gcStats.SegmentsDeleted != 1 || gcStats.BytesDeleted == 0 {
		t.Fatalf("gc stats=%+v want old mixed segment deleted after fallback advance", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old segment still exists or unexpected stat error: %v", err)
	}
	assertLifecycleQueries("after GC")
}

func TestColumnAssetRewriteFailClosedOnIncompletePlanM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	cfg := col.Meta().Options.ColumnStore
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(namespace.SegmentDir, "segment-unknown.tca"), []byte("unknown-bytes"), 0o600); err != nil {
		t.Fatalf("WriteFile unknown segment: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)

	dry, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		DryRun:        true,
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite dry-run: %v", err)
	}
	if !dry.DryRun || dry.Plan.Complete || dry.Plan.Segments.Unknown == 0 || dry.SegmentsRewritten != 0 {
		t.Fatalf("dry-run stats=%+v want incomplete plan and no rewrite", dry)
	}

	stats, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, ErrColumnAssetReachabilityIncomplete) {
		t.Fatalf("ColumnAssetRewrite error=%v want ErrColumnAssetReachabilityIncomplete", err)
	}
	if stats.SegmentsRewritten != 0 || stats.RefsRemapped != 0 || len(stats.SupersededRefs) != 0 {
		t.Fatalf("incomplete plan rewrote stats=%+v", stats)
	}
	refs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	for _, ref := range refs {
		if ref.FileID != columnAssetM12ASegmentFileID {
			t.Fatalf("manifest ref %+v changed despite incomplete rewrite", ref)
		}
	}
}

func TestColumnAssetRewriteRetainsCopiedOrphanOnStalePublishPreflightM15C(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	beforeSegments := columnAssetSegmentNamesM15C(t, d, col)
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	replacement := []byte("rebound-rewrite-replacement")
	var copiedPath, displacedPath string
	var pinsAfterStalePublish uint64
	var rewriteOwnedPins uint64

	stats, err := col.columnAssetRewriteWithOptions(context.Background(), columnAssetRewriteOptions{
		ColumnAssetRewriteOptions: ColumnAssetRewriteOptions{
			CandidateRefs: []ColumnAssetRef{candidate},
		},
		afterCopyHookForTest: func() error {
			if got := registry.ActivePins(); got <= baselinePins {
				t.Fatalf("rewrite pins before backend publication=%d want > baseline %d", got, baselinePins)
			} else {
				rewriteOwnedPins = got - baselinePins
			}
			duringSegments := columnAssetSegmentNamesM15C(t, d, col)
			var copiedName string
			for _, name := range duringSegments {
				if !slices.Contains(beforeSegments, name) {
					if copiedName != "" {
						t.Fatalf("rewrite created multiple copied segments: %v", duringSegments)
					}
					copiedName = name
				}
			}
			if copiedName == "" {
				t.Fatalf("rewrite created no copied segment: before=%v during=%v", beforeSegments, duringSegments)
			}
			cfg := col.Meta().Options.ColumnStore
			namespace, namespaceErr := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
			if namespaceErr != nil {
				t.Fatal(namespaceErr)
			}
			copiedPath = filepath.Join(namespace.SegmentDir, copiedName)
			displacedPath = copiedPath + ".displaced"
			if renameErr := os.Rename(copiedPath, displacedPath); renameErr != nil {
				t.Fatalf("displace copied segment: %v", renameErr)
			}
			if writeErr := os.WriteFile(copiedPath, replacement, 0o600); writeErr != nil {
				t.Fatalf("write replacement segment: %v", writeErr)
			}
			staleErr := staleColumnAssetRewriteManifestRootM15C(d)
			if staleErr == nil {
				pinsAfterStalePublish = registry.ActivePins()
			}
			return staleErr
		},
	})
	if err == nil {
		t.Fatal("ColumnAssetRewrite stale publish unexpectedly succeeded")
	}
	if stats.SegmentsRewritten != 0 || stats.RefsRemapped != 0 || len(stats.SupersededRefs) != 0 {
		t.Fatalf("stale publish reported successful rewrite stats=%+v", stats)
	}
	if stats.BytesCopied != 0 {
		t.Fatalf("stale publish reported copied bytes stats=%+v", stats)
	}
	if pinsAfterStalePublish <= baselinePins {
		t.Fatalf("stale publication pins=%d want more than baseline %d for retained fallback generation", pinsAfterStalePublish, baselinePins)
	}
	wantPins := pinsAfterStalePublish - rewriteOwnedPins
	if got := registry.ActivePins(); got != wantPins {
		t.Fatalf("rewrite stale preflight pins=%d want %d after releasing %d rewrite-owned pins from post-stale-publication %d", got, wantPins, rewriteOwnedPins, pinsAfterStalePublish)
	}
	afterSegments := columnAssetSegmentNamesM15C(t, d, col)
	if len(afterSegments) != len(beforeSegments)+2 {
		t.Fatalf("rebound persistent orphan segments after=%v want replacement and displaced inode beyond before=%v", afterSegments, beforeSegments)
	}
	for _, before := range beforeSegments {
		if !slices.Contains(afterSegments, before) {
			t.Fatalf("pre-existing segment %q removed after stale preflight: %v", before, afterSegments)
		}
	}
	if got, readErr := os.ReadFile(copiedPath); readErr != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("rewrite replacement=%q err=%v want %q", got, readErr, replacement)
	}
	if info, statErr := os.Stat(displacedPath); statErr != nil || info.Size() == 0 {
		t.Fatalf("displaced exact copied orphan info=%v err=%v want non-empty", info, statErr)
	}
}

func TestColumnAssetRewriteRetainsCopiedOrphanOnPublishPreflightRaceM15C(t *testing.T) {
	requireStandaloneColumnProductionAuthorityTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	col := openColumnStoreCollectionM10B(t, d, mgr)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	beforeSegments := columnAssetSegmentNamesM15C(t, d, col)

	stats, err := col.columnAssetRewriteWithOptions(context.Background(), columnAssetRewriteOptions{
		ColumnAssetRewriteOptions: ColumnAssetRewriteOptions{
			CandidateRefs: []ColumnAssetRef{candidate},
		},
		afterPrePublishHookForTest: func() error {
			return staleColumnAssetRewriteManifestRootM15C(d)
		},
	})
	if err == nil {
		t.Fatal("ColumnAssetRewrite stale publish unexpectedly succeeded")
	}
	if stats.SegmentsRewritten != 0 || stats.RefsRemapped != 0 || len(stats.SupersededRefs) != 0 {
		t.Fatalf("stale publish reported successful rewrite stats=%+v", stats)
	}
	if stats.BytesCopied != 0 {
		t.Fatalf("stale publish reported copied bytes stats=%+v", stats)
	}
	afterSegments := columnAssetSegmentNamesM15C(t, d, col)
	if len(afterSegments) != len(beforeSegments)+1 {
		t.Fatalf("persistent orphan segments after=%v want one more than before=%v", afterSegments, beforeSegments)
	}
	for _, before := range beforeSegments {
		if !slices.Contains(afterSegments, before) {
			t.Fatalf("pre-existing segment %q removed after publish race: %v", before, afterSegments)
		}
	}
}

func TestColumnAssetRewriteRecognizesBackendPreApplyFailureM15C(t *testing.T) {
	err := errors.Join(backenddb.ErrStorageMaintenancePublishPreApplyFailed, backenddb.ErrRecoveryRequired)
	if !columnAssetRewritePublishFailedBeforeApply(err) {
		t.Fatalf("columnAssetRewritePublishFailedBeforeApply(%v)=false, want true", err)
	}
	ambiguousErr := backenddb.ErrRecoveryRequired
	if columnAssetRewritePublishFailedBeforeApply(ambiguousErr) {
		t.Fatalf("columnAssetRewritePublishFailedBeforeApply(%v)=true, want false", ambiguousErr)
	}
}

func TestColumnAssetRewriteRegistersAmbiguousPublishQuarantine1954(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)

	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	copied := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 96, []byte("ambiguous-publish-copied-segment"))
	if err := col.columnAssetRewriteRegisterAmbiguousPublishQuarantine([]ColumnAssetRef{copied}, errors.New("ambiguous publish test")); err != nil {
		t.Fatalf("columnAssetRewriteRegisterAmbiguousPublishQuarantine: %v", err)
	}
	report, err := col.PlanColumnAssetLifecycle(context.Background(), ColumnAssetLifecycleOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{copied},
	})
	if err != nil {
		t.Fatalf("PlanColumnAssetLifecycle: %v", err)
	}
	if report.Quarantine.OpenRecords != 1 || report.Quarantine.Refs != 1 || report.Roots.QuarantineRefs != 1 || report.Reachability.Sources.QuarantineRefs != 1 {
		t.Fatalf("quarantine report=%+v roots=%+v sources=%+v", report.Quarantine, report.Roots, report.Reachability.Sources)
	}
	if !columnAssetLifecycleRegistrySourcesContain(report.Quarantine.Sources, "ambiguous_publish", 1, copied.Length, 0, 0) {
		t.Fatalf("quarantine sources=%+v missing ambiguous_publish", report.Quarantine.Sources)
	}
	entry, ok := columnAssetLifecycleFindEntry(report.Reachability.Entries, copied)
	if !ok || entry.Status != ColumnAssetReachabilityProtected || !slices.Contains(entry.Sources, ColumnAssetReachabilitySourceQuarantine) {
		t.Fatalf("ambiguous publish entry=%+v ok=%t", entry, ok)
	}
}

func TestColumnAssetRewriteManifestRootStoragePolicyM15C(t *testing.T) {
	if _, err := columnAssetRewriteManifestRootStoragePolicy(columnAssetRewriteManifestState{}); err == nil {
		t.Fatal("columnAssetRewriteManifestRootStoragePolicy missing manifest root err=nil, want error")
	}
	if _, err := columnAssetRewriteManifestRootStoragePolicy(columnAssetRewriteManifestState{
		cfg: ColumnStoreConfig{
			ManifestRoot: &ColumnManifestRootDescriptor{StoragePolicy: RootStoragePolicy("unsupported")},
		},
	}); err == nil {
		t.Fatal("columnAssetRewriteManifestRootStoragePolicy unsupported storage policy err=nil, want error")
	}
	policy, err := columnAssetRewriteManifestRootStoragePolicy(columnAssetRewriteManifestState{
		cfg: ColumnStoreConfig{
			ManifestRoot: &ColumnManifestRootDescriptor{StoragePolicy: RootStorageCompressed},
		},
	})
	if err != nil {
		t.Fatalf("columnAssetRewriteManifestRootStoragePolicy compressed: %v", err)
	}
	if policy != backenddb.OrderedRootStorageValueLogLeaves {
		t.Fatalf("policy=%v want OrderedRootStorageValueLogLeaves", policy)
	}
}

func staleColumnAssetRewriteManifestRootM15C(d *backenddb.DB) error {
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:    commitlog.RawKVOpSet,
		Key:   []byte("column/rewrite/stale-publish-test"),
		Value: []byte("1"),
	}})
	if err != nil {
		return err
	}
	intent, err := d.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		return err
	}
	_, _, err = d.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, nil, intent, func(_ backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
		current := d.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		return buildSystemTargetIterator(current, map[string][]byte{
			systemCollectionRootKey(collectionColumnManifestRootName("events")): encodeRootID(0),
		})
	})
	return err
}

func advanceColumnAssetDurableFallbackM15C(t *testing.T, d *backenddb.DB) {
	t.Helper()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle current durable root before fallback advance: %v", err)
	}
	payload, err := commitlog.EncodeRawKVBatchPayload([]commitlog.RawKVOperation{{
		Op:    commitlog.RawKVOpSet,
		Key:   []byte("column/rewrite/durable-fallback-advance"),
		Value: []byte("1"),
	}})
	if err != nil {
		t.Fatalf("EncodeRawKVBatchPayload fallback advance: %v", err)
	}
	intent, err := d.NewCommandWALIntent(commitlog.CommandKindRawKVBatch, commitlog.CommandScopeRawKV, commitlog.PayloadFormatRawKVBatchV1, payload)
	if err != nil {
		t.Fatalf("NewCommandWALIntent fallback advance: %v", err)
	}
	emptySystemDelta, err := memtable.NewWithCapacityMode(1, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new empty system delta fallback advance: %v", err)
	}
	emptySystemDelta.Freeze()
	if _, _, err := d.PublishOrderedRootDeltaGroupWithPreflightCommandWALContextAndSystemDeltaBuilder(nil, nil, intent, func(_ backenddb.CommandWALPublishContext, _ []uint64) (iterator.UnsafeIterator, error) {
		return emptySystemDelta.NewIterator(nil, nil), nil
	}); err != nil {
		t.Fatalf("publish fallback advance: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("wait durable fallback advance: %v", err)
	}
}

func TestColumnAssetRewriteRejectsReadOnlyM15C(t *testing.T) {
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.Insert([]byte("e1"), []byte(`{"time_us":1,"kind":"like","did":"d1"}`)); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	readonly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open readonly: %v", err)
	}
	defer func() { _ = readonly.Close() }()
	readonlyCol := openColumnStoreCollectionM10B(t, readonly)
	stats, err := readonlyCol.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("ColumnAssetRewrite read-only error=%v want ErrReadOnly", err)
	}
	if stats.SegmentsRewritten != 0 || stats.RefsRemapped != 0 {
		t.Fatalf("read-only rewrite stats=%+v", stats)
	}
	afterRefs := columnManifestAssetRefsForCollectionM12A(t, readonly, readonlyCol)
	assertColumnAssetRefsEqualM15C(t, beforeRefs, afterRefs)
}

func TestColumnAssetRewriteRejectsUnsupportedRefKindM15C(t *testing.T) {
	err := validateColumnAssetRewriteRefKinds([]ColumnAssetRef{{
		Kind: ColumnAssetKind("future-kind"),
	}})
	if err == nil || !strings.Contains(err.Error(), "supports only") {
		t.Fatalf("validateColumnAssetRewriteRefKinds error=%v want unsupported kind", err)
	}
}

func columnAssetSegmentNamesM15C(t testing.TB, d *backenddb.DB, col *Collection) []string {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column-store asset manager config")
	}
	namespace, err := columnAssetManagerNamespaceForRoot(d.ColumnAssetRootDir(), cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatalf("columnAssetManagerNamespaceForRoot: %v", err)
	}
	entries, err := os.ReadDir(namespace.SegmentDir)
	if err != nil {
		t.Fatalf("ReadDir segment dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func assertStringSlicesEqualM15C(t testing.TB, before, after []string) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("slice length after=%d want %d: after=%v before=%v", len(after), len(before), after, before)
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("slice[%d]=%q want %q: after=%v before=%v", i, after[i], before[i], after, before)
		}
	}
}

func assertColumnAssetRefsRemappedM15C(t testing.TB, before, after []ColumnAssetRef) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("ref count after=%d want before=%d", len(after), len(before))
	}
	matched := make([]bool, len(after))
	for _, oldRef := range before {
		found := false
		for i, newRef := range after {
			if matched[i] || !columnAssetRefsSameLogicalAssetM15C(oldRef, newRef) {
				continue
			}
			if oldRef.FileID == newRef.FileID && oldRef.Offset == newRef.Offset {
				t.Fatalf("ref %+v was not remapped", oldRef)
			}
			matched[i] = true
			found = true
			break
		}
		if !found {
			t.Fatalf("old ref %+v has no logically equivalent remapped ref in %+v", oldRef, after)
		}
	}
}

func assertColumnAssetRefsEqualM15C(t testing.TB, before, after []ColumnAssetRef) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("ref count after=%d want before=%d", len(after), len(before))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("ref[%d]=%+v want %+v", i, after[i], before[i])
		}
	}
}

func columnAssetRefsSameLogicalAssetM15C(left, right ColumnAssetRef) bool {
	return left.Kind == right.Kind &&
		left.Namespace == right.Namespace &&
		left.Generation == right.Generation &&
		left.PartID == right.PartID &&
		left.Length == right.Length &&
		left.Checksum == right.Checksum
}

func BenchmarkColumnAssetRewriteMixedSegmentM15C(b *testing.B) {
	for _, refs := range []int{1, 128} {
		b.Run(fmt.Sprintf("refs_%d", refs), func(b *testing.B) {
			bytesCopiedPerRun := columnAssetRewriteBenchmarkBytesPerRunM15C(b, refs)
			b.SetBytes(bytesCopiedPerRun)
			b.ReportAllocs()
			ctx := context.Background()
			b.ResetTimer()
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				tc := prepareColumnAssetRewriteBenchmarkCaseM15C(b, refs)
				if tc.bytesCopied != bytesCopiedPerRun {
					if err := closeColumnAssetRewriteBenchmarkCaseM15C(tc); err != nil {
						b.Fatalf("close benchmark case after unstable bytes: %v", err)
					}
					b.Fatalf("bytesCopied=%d want stable benchmark bytes=%d", tc.bytesCopied, bytesCopiedPerRun)
				}
				b.StartTimer()
				stats, err := tc.col.ColumnAssetRewrite(ctx, ColumnAssetRewriteOptions{
					CandidateRefs: []ColumnAssetRef{tc.candidate},
				})
				b.StopTimer()
				if err != nil {
					b.Fatalf("ColumnAssetRewrite refs=%d: %v", refs, err)
				}
				if stats.RefsRemapped != len(tc.liveRefs) || stats.SegmentsRewritten != 1 || stats.BytesCopied != bytesCopiedPerRun {
					b.Fatalf("stats=%+v liveRefs=%d bytesCopied=%d", stats, len(tc.liveRefs), bytesCopiedPerRun)
				}
				if err := closeColumnAssetRewriteBenchmarkCaseM15C(tc); err != nil {
					b.Fatalf("close benchmark case: %v", err)
				}
			}
		})
	}
}

type columnAssetRewriteBenchmarkCaseM15C struct {
	d           *backenddb.DB
	col         *Collection
	candidate   ColumnAssetRef
	liveRefs    []ColumnAssetRef
	bytesCopied int64
}

func columnAssetRewriteBenchmarkBytesPerRunM15C(b *testing.B, refs int) int64 {
	b.Helper()
	tc := prepareColumnAssetRewriteBenchmarkCaseM15C(b, refs)
	defer func() {
		if err := closeColumnAssetRewriteBenchmarkCaseM15C(tc); err != nil {
			b.Fatalf("close benchmark probe: %v", err)
		}
	}()
	return tc.bytesCopied
}

func prepareColumnAssetRewriteBenchmarkCaseM15C(b testing.TB, refs int) columnAssetRewriteBenchmarkCaseM15C {
	b.Helper()
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(b)
	d := openCollectionCommandWALDB(b, dir)
	col := openColumnStoreCollectionM10B(b, d)
	for refIdx := 0; refIdx < refs; refIdx++ {
		id := []byte(fmt.Sprintf("e%06d", refIdx))
		doc := []byte(fmt.Sprintf(`{"time_us":%d,"kind":"like","did":"d%d"}`, refIdx, refIdx))
		if _, err := col.Insert(id, doc); err != nil {
			_ = d.Close()
			b.Fatalf("Insert ref=%d: %v", refIdx, err)
		}
	}
	liveRefs := columnManifestAssetRefsForCollectionM12A(b, d, col)
	candidate := writeColumnAssetReachabilityCandidateM15A(b, d, col, uint64(refs+2), 99)
	var bytesCopied int64
	for _, ref := range liveRefs {
		bytesCopied += ref.Length
	}
	if bytesCopied <= 0 {
		_ = d.Close()
		b.Fatalf("benchmark bytesCopied=%d for refs=%d", bytesCopied, refs)
	}
	return columnAssetRewriteBenchmarkCaseM15C{
		d:           d,
		col:         col,
		candidate:   candidate,
		liveRefs:    liveRefs,
		bytesCopied: bytesCopied,
	}
}

func closeColumnAssetRewriteBenchmarkCaseM15C(tc columnAssetRewriteBenchmarkCaseM15C) error {
	if tc.d == nil {
		return nil
	}
	return tc.d.Close()
}
