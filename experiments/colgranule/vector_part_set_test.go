package colgranule

import (
	"slices"
	"testing"
)

func TestColumnPartSetVectorAdjacencyVisibilityAndCompaction(t *testing.T) {
	opts := vectorPartTestOptions()
	workspace, err := OpenColumnWorkspace(t.TempDir(), ColumnWorkspaceOptions{Collection: "vectors"})
	if err != nil {
		t.Fatalf("OpenColumnWorkspace: %v", err)
	}
	defer workspace.Close()

	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:        "vectors",
		StoreOptions:      opts,
		InitialPartID:     100,
		InitialGeneration: 1,
	})
	if err != nil {
		t.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	if _, err := adapter.PublishBaseBatch(vectorPartTestBatch(), ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: 5}); err != nil {
		t.Fatalf("PublishBaseBatch: %v", err)
	}
	_, err = adapter.Apply(ColumnMutationBatch{
		Inserts: ColumnBatch{
			Rows: 1,
			Columns: map[string][]int64{
				"id": {6},
			},
			Float32Vectors: map[string]Float32VectorColumn{
				"embedding": {Dims: 3, Values: []float32{60, 61, 62}},
			},
			AdjacencyLists: map[string]AdjacencyListColumn{
				"neighbors": {Offsets: []uint32{0, 3}, Values: []int64{61, 62, 63}},
			},
		},
		Updates: ColumnBatch{
			Rows: 1,
			Columns: map[string][]int64{
				"id": {2},
			},
			Float32Vectors: map[string]Float32VectorColumn{
				"embedding": {Dims: 3, Values: []float32{200, 201, 202}},
			},
			AdjacencyLists: map[string]AdjacencyListColumn{
				"neighbors": {Offsets: []uint32{0, 2}, Values: []int64{2001, 2002}},
			},
		},
		Deletes:                 []int64{3},
		SourceRowRootGeneration: 1,
		SourceRowVersionLower:   5,
		SourceRowVersionUpper:   7,
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	if stats := reader.VisibilityStats(); stats.VisibleRows != 5 || stats.SupersededRows != 1 || stats.DeletedRows != 1 {
		t.Fatalf("visibility stats=%+v want visible=5 superseded=1 deleted=1", stats)
	}
	assertVectorPartSetSnapshot(t, reader,
		[]int64{1, 4, 5, 2, 6},
		[]float32{
			10, 11, 12,
			40, 41, 42,
			50, 51, 52,
			200, 201, 202,
			60, 61, 62,
		},
		[]uint32{0, 1, 3, 3, 5, 8},
		[]int64{11, 41, 42, 2001, 2002, 61, 62, 63},
	)
	vector, ok, err := reader.Float32VectorAtLatest(2, "embedding", nil)
	if err != nil {
		t.Fatalf("Float32VectorAtLatest: %v", err)
	}
	if !ok {
		t.Fatal("updated id 2 missing latest vector")
	}
	assertFloat32s(t, "updated vector", vector, []float32{200, 201, 202})
	neighbors, ok, err := reader.AdjacencyListAtLatest(6, "neighbors", nil)
	if err != nil {
		t.Fatalf("AdjacencyListAtLatest: %v", err)
	}
	if !ok {
		t.Fatal("inserted id 6 missing latest adjacency")
	}
	assertInt64s(t, "inserted adjacency", neighbors, []int64{61, 62, 63})
	if _, ok, err := reader.ScanFloat32VectorAtLatest(3, "embedding", nil); err != nil || ok {
		t.Fatalf("deleted id 3 latest vector ok=%v err=%v", ok, err)
	}

	compacted, err := CompactColumnPartSet(workspace, reader, opts, nil, 200)
	if err != nil {
		t.Fatalf("CompactColumnPartSet: %v", err)
	}
	if compacted.VisibleRows != 5 || compacted.DroppedRows != 2 {
		t.Fatalf("compaction rows visible=%d dropped=%d want visible=5 dropped=2", compacted.VisibleRows, compacted.DroppedRows)
	}
	if err := workspace.SaveCollectionManifest(compacted.Manifest); err != nil {
		t.Fatalf("SaveCollectionManifest(compacted): %v", err)
	}
	reopenedManifest, err := workspace.LoadCollectionManifest()
	if err != nil {
		t.Fatalf("LoadCollectionManifest(compacted): %v", err)
	}
	compactedReader, err := OpenColumnPartSetReader(workspace, reopenedManifest, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("OpenColumnPartSetReader(compacted): %v", err)
	}
	if stats := compactedReader.VisibilityStats(); stats.VisibleRows != 5 || stats.SupersededRows != 0 || stats.DeletedRows != 0 {
		t.Fatalf("compacted visibility stats=%+v want visible=5 no hidden rows", stats)
	}
	assertVectorPartSetSnapshot(t, compactedReader,
		[]int64{1, 2, 4, 5, 6},
		[]float32{
			10, 11, 12,
			200, 201, 202,
			40, 41, 42,
			50, 51, 52,
			60, 61, 62,
		},
		[]uint32{0, 1, 3, 5, 5, 8},
		[]int64{11, 2001, 2002, 41, 42, 61, 62, 63},
	)
}

func assertVectorPartSetSnapshot(t *testing.T, reader *ColumnPartSetReader, wantIDs []int64, wantVectors []float32, wantNeighborOffsets []uint32, wantNeighbors []int64) {
	t.Helper()
	ids, err := reader.ScanProjected([]string{"id"})
	if err != nil {
		t.Fatalf("ScanProjected(id): %v", err)
	}
	assertInt64s(t, "part-set ids", ids.Columns["id"], wantIDs)
	vectors, err := reader.ScanFloat32VectorsInto("embedding", nil)
	if err != nil {
		t.Fatalf("ScanFloat32VectorsInto: %v", err)
	}
	if vectors.Rows != len(wantIDs) || vectors.Dims != 3 {
		t.Fatalf("vector rows/dims=(%d,%d) want (%d,3)", vectors.Rows, vectors.Dims, len(wantIDs))
	}
	assertFloat32s(t, "part-set vectors", vectors.Values, wantVectors)
	neighbors, err := reader.ScanAdjacencyListsInto("neighbors", nil, nil)
	if err != nil {
		t.Fatalf("ScanAdjacencyListsInto: %v", err)
	}
	if neighbors.Rows != len(wantIDs) {
		t.Fatalf("adjacency rows=%d want %d", neighbors.Rows, len(wantIDs))
	}
	if !slices.Equal(neighbors.Offsets, wantNeighborOffsets) {
		t.Fatalf("adjacency offsets=%v want %v", neighbors.Offsets, wantNeighborOffsets)
	}
	assertInt64s(t, "part-set neighbors", neighbors.Values, wantNeighbors)
}

func BenchmarkColumnPartSetVectorScan(b *testing.B) {
	reader := benchmarkVectorPartSetReader(b, 8192, 128, 16, 128)
	warm, err := reader.ScanFloat32VectorsInto("embedding", nil)
	if err != nil {
		b.Fatalf("ScanFloat32VectorsInto: %v", err)
	}
	scratch := warm.Values
	b.ReportAllocs()
	b.SetBytes(int64(len(scratch) * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan, err := reader.ScanFloat32VectorsInto("embedding", scratch)
		if err != nil {
			b.Fatalf("ScanFloat32VectorsInto: %v", err)
		}
		scratch = scan.Values
		benchSink += int64(scan.Values[0])
	}
}

func BenchmarkColumnPartSetAdjacencyScan(b *testing.B) {
	reader := benchmarkVectorPartSetReader(b, 8192, 128, 16, 128)
	warm, err := reader.ScanAdjacencyListsInto("neighbors", nil, nil)
	if err != nil {
		b.Fatalf("ScanAdjacencyListsInto: %v", err)
	}
	offsets := warm.Offsets
	values := warm.Values
	b.ReportAllocs()
	b.SetBytes(int64(len(values) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan, err := reader.ScanAdjacencyListsInto("neighbors", offsets, values)
		if err != nil {
			b.Fatalf("ScanAdjacencyListsInto: %v", err)
		}
		offsets = scan.Offsets
		values = scan.Values
		benchSink += int64(scan.Offsets[len(scan.Offsets)-1])
	}
}

func BenchmarkColumnPartSetVectorPointLookup(b *testing.B) {
	reader := benchmarkVectorPartSetReader(b, 8192, 128, 16, 128)
	var lookupScratch ColumnPartSetPointLookupScratch
	warm, ok, err := reader.Float32VectorAtLatestWithScratch(4096, "embedding", nil, &lookupScratch)
	if err != nil {
		b.Fatalf("Float32VectorAtLatestWithScratch: %v", err)
	}
	if !ok {
		b.Fatal("missing vector for id 4096")
	}
	scratch := warm
	b.ReportAllocs()
	b.SetBytes(int64(len(scratch) * 4))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vector, ok, err := reader.Float32VectorAtLatestWithScratch(4096, "embedding", scratch, &lookupScratch)
		if err != nil {
			b.Fatalf("Float32VectorAtLatestWithScratch: %v", err)
		}
		if !ok {
			b.Fatal("missing vector for id 4096")
		}
		scratch = vector
		benchSink += int64(vector[0])
	}
}

func BenchmarkColumnPartSetAdjacencyPointLookup(b *testing.B) {
	reader := benchmarkVectorPartSetReader(b, 8192, 128, 16, 128)
	var lookupScratch ColumnPartSetPointLookupScratch
	warm, ok, err := reader.AdjacencyListAtLatestWithScratch(4096, "neighbors", nil, &lookupScratch)
	if err != nil {
		b.Fatalf("AdjacencyListAtLatestWithScratch: %v", err)
	}
	if !ok {
		b.Fatal("missing adjacency for id 4096")
	}
	scratch := warm
	b.ReportAllocs()
	b.SetBytes(int64(len(scratch) * 8))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		neighbors, ok, err := reader.AdjacencyListAtLatestWithScratch(4096, "neighbors", scratch, &lookupScratch)
		if err != nil {
			b.Fatalf("AdjacencyListAtLatestWithScratch: %v", err)
		}
		if !ok {
			b.Fatal("missing adjacency for id 4096")
		}
		scratch = neighbors
		benchSink += neighbors[0]
	}
}

func benchmarkVectorPartSetReader(b *testing.B, rows int, dims int, degree int, mutationStep int) *ColumnPartSetReader {
	b.Helper()
	opts := vectorPartBenchmarkOptions(rows, dims)
	workspace, err := OpenColumnWorkspace(b.TempDir(), ColumnWorkspaceOptions{Collection: "vectors"})
	if err != nil {
		b.Fatalf("OpenColumnWorkspace: %v", err)
	}
	b.Cleanup(func() {
		if err := workspace.Close(); err != nil {
			b.Fatalf("Close workspace: %v", err)
		}
	})
	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:        "vectors",
		StoreOptions:      opts,
		InitialPartID:     1,
		InitialGeneration: 1,
	})
	if err != nil {
		b.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	if _, err := adapter.PublishBaseBatch(vectorPartBenchmarkBatch(rows, dims, degree), ColumnPartCoverageOptions{SourceRowRootGeneration: 1, SourceRowVersionUpper: uint64(rows)}); err != nil {
		b.Fatalf("PublishBaseBatch: %v", err)
	}
	updates, deletes := vectorPartSetBenchmarkMutations(rows, dims, degree, mutationStep)
	if _, err := adapter.Apply(ColumnMutationBatch{
		Updates:                 updates,
		Deletes:                 deletes,
		SourceRowRootGeneration: 1,
		SourceRowVersionLower:   uint64(rows),
		SourceRowVersionUpper:   uint64(rows + updates.Rows + len(deletes)),
	}); err != nil {
		b.Fatalf("Apply: %v", err)
	}
	reader, err := adapter.Reader(ColumnPartImageReadOptions{})
	if err != nil {
		b.Fatalf("Reader: %v", err)
	}
	return reader
}

func vectorPartSetBenchmarkMutations(rows int, dims int, degree int, step int) (ColumnBatch, []int64) {
	if step <= 0 {
		step = rows
	}
	updateRows := 0
	for id := 0; id < rows; id += step {
		updateRows++
	}
	ids := make([]int64, updateRows)
	vectors := make([]float32, updateRows*dims)
	offsets := make([]uint32, updateRows+1)
	neighbors := make([]int64, updateRows*degree)
	deletes := make([]int64, 0, updateRows)
	row := 0
	for id := 0; id < rows; id += step {
		ids[row] = int64(id)
		for dim := 0; dim < dims; dim++ {
			vectors[row*dims+dim] = float32((id+dim+17)%2048) / 2048
		}
		offsets[row] = uint32(row * degree)
		for edge := 0; edge < degree; edge++ {
			neighbors[row*degree+edge] = int64((id + edge + 17) % rows)
		}
		if id+1 < rows {
			deletes = append(deletes, int64(id+1))
		}
		row++
	}
	offsets[updateRows] = uint32(len(neighbors))
	return ColumnBatch{
		Rows: updateRows,
		Columns: map[string][]int64{
			"id": ids,
		},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding": {Dims: dims, Values: vectors},
		},
		AdjacencyLists: map[string]AdjacencyListColumn{
			"neighbors": {Offsets: offsets, Values: neighbors},
		},
	}, deletes
}
