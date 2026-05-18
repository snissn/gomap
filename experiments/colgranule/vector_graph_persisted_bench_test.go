package colgranule

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type columnVectorGraphPersistedOrder string

const (
	columnVectorGraphPersistedOrderLocal    columnVectorGraphPersistedOrder = "neighborhood_local"
	columnVectorGraphPersistedOrderShuffled columnVectorGraphPersistedOrder = "shuffled"
)

type columnVectorGraphPersistedFixture struct {
	graph       *ColumnVectorGraph
	query       []float32
	opts        ColumnVectorGraphSearchOptions
	loadStats   ColumnVectorGraphLoadStats
	buildNanos  int64
	openNanos   int64
	decodeNanos int64
	accounting  columnVectorGraphPersistedAccounting
	cleanupOnce sync.Once
	cleanup     func() error
	cleanupErr  error
}

type columnVectorGraphPersistedAccounting struct {
	rows                       int
	encodedRawBytes            int
	declaredColumnStoredBytes  int
	assetBytes                 int
	imageBytes                 int
	settledDiskBytes           int64
	vectorRawBytes             int
	vectorStoredBytes          int
	invNormRawBytes            int
	invNormStoredBytes         int
	adjacencyRawBytes          int
	adjacencyStoredBytes       int
	sourceDocumentVectorBytes  int
	actualCompressionBlocks    map[string]int
	compressionFallbackReasons map[string]int
}

func (f *columnVectorGraphPersistedFixture) Close() error {
	if f == nil {
		return nil
	}
	f.cleanupOnce.Do(func() {
		if f.cleanup != nil {
			f.cleanupErr = f.cleanup()
		}
	})
	return f.cleanupErr
}

func registerColumnVectorGraphPersistedFixtureCleanup(tb testing.TB, fixture *columnVectorGraphPersistedFixture) {
	tb.Helper()
	tb.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			tb.Fatalf("Close persisted vector graph fixture: %v", err)
		}
	})
}

func TestColumnVectorGraphPersistedReopenPath(t *testing.T) {
	fixture := buildColumnVectorGraphPersistedFixture(t, 1024, 32, 8, 256, CompressionZSTD, columnVectorGraphPersistedOrderLocal)
	registerColumnVectorGraphPersistedFixtureCleanup(t, fixture)
	if fixture.graph.Rows() != 1024 || fixture.graph.Dims() != 32 {
		t.Fatalf("graph shape rows=%d dims=%d want rows=1024 dims=32", fixture.graph.Rows(), fixture.graph.Dims())
	}
	if fixture.loadStats.Rows != 1024 || fixture.loadStats.Edges != 1024*8 {
		t.Fatalf("load stats=%+v want rows=1024 edges=%d", fixture.loadStats, 1024*8)
	}
	if fixture.accounting.sourceDocumentVectorBytes != 0 {
		t.Fatalf("source document vector bytes=%d want 0 for column-only fixture", fixture.accounting.sourceDocumentVectorBytes)
	}
	var scratch ColumnVectorGraphSearchScratch
	results, stats, err := fixture.graph.SearchCosine(fixture.query, fixture.opts, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) != fixture.opts.TopK {
		t.Fatalf("results=%d want %d", len(results), fixture.opts.TopK)
	}
	if stats.CandidatesExamined == 0 || stats.EdgesVisited == 0 {
		t.Fatalf("search stats=%+v want non-zero traversal", stats)
	}
}

func BenchmarkColumnVectorGraphPersistedSearchCosine(b *testing.B) {
	shapes := []struct {
		name string
		rows int
	}{
		{name: "100k", rows: 100_000},
		{name: "1m", rows: 1_000_000},
	}
	compressions := []Compression{CompressionNone, CompressionSnappy, CompressionLZ4, CompressionZSTD}
	orders := []columnVectorGraphPersistedOrder{columnVectorGraphPersistedOrderLocal, columnVectorGraphPersistedOrderShuffled}
	for _, shape := range shapes {
		shape := shape
		b.Run(shape.name, func(b *testing.B) {
			if shape.rows == 1_000_000 && os.Getenv("COLUMN_VECTOR_PERSISTED_1M") != "1" {
				b.Skip("set COLUMN_VECTOR_PERSISTED_1M=1 to run the 1M persisted-column shape")
			}
			for _, order := range orders {
				order := order
				b.Run(string(order), func(b *testing.B) {
					for _, compression := range compressions {
						compression := compression
						b.Run(compression.String(), func(b *testing.B) {
							fixture := buildColumnVectorGraphPersistedFixture(b, shape.rows, 128, 16, 8192, compression, order)
							registerColumnVectorGraphPersistedFixtureCleanup(b, fixture)
							b.Run("serial", func(b *testing.B) {
								benchmarkColumnVectorGraphPersistedSearchSerial(b, fixture)
							})
							b.Run("parallel", func(b *testing.B) {
								benchmarkColumnVectorGraphPersistedSearchParallel(b, fixture)
							})
						})
					}
				})
			}
		})
	}
}

func benchmarkColumnVectorGraphPersistedSearchSerial(b *testing.B, fixture *columnVectorGraphPersistedFixture) {
	var scratch ColumnVectorGraphSearchScratch
	warm, warmStats, err := fixture.graph.SearchCosine(fixture.query, fixture.opts, &scratch)
	if err != nil {
		b.Fatalf("warm SearchCosine: %v", err)
	}
	if len(warm) != fixture.opts.TopK {
		b.Fatalf("warm results=%d want %d", len(warm), fixture.opts.TopK)
	}
	b.ReportAllocs()
	b.SetBytes(int64(warmStats.CandidatesExamined * fixture.graph.Dims() * 4))
	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		results, stats, err := fixture.graph.SearchCosine(fixture.query, fixture.opts, &scratch)
		if err != nil {
			b.Fatalf("SearchCosine: %v", err)
		}
		if len(results) != fixture.opts.TopK {
			b.Fatalf("results=%d want %d", len(results), fixture.opts.TopK)
		}
		benchSink += int64(results[0].Ordinal + stats.CandidatesExamined + stats.EdgesVisited)
	}
	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "searches/s")
	}
	reportColumnVectorGraphPersistedMetrics(b, fixture, warmStats)
}

func benchmarkColumnVectorGraphPersistedSearchParallel(b *testing.B, fixture *columnVectorGraphPersistedFixture) {
	workers := runtime.GOMAXPROCS(0)
	scratches := make([]*ColumnVectorGraphSearchScratch, workers)
	var warmStats ColumnVectorGraphSearchStats
	for i := 0; i < workers; i++ {
		scratch := new(ColumnVectorGraphSearchScratch)
		warm, stats, err := fixture.graph.SearchCosine(fixture.query, fixture.opts, scratch)
		if err != nil {
			b.Fatalf("warm SearchCosine worker %d: %v", i, err)
		}
		if len(warm) != fixture.opts.TopK {
			b.Fatalf("warm worker %d results=%d want %d", i, len(warm), fixture.opts.TopK)
		}
		warmStats = stats
		scratches[i] = scratch
	}
	b.ReportAllocs()
	b.SetParallelism(1)
	b.SetBytes(int64(warmStats.CandidatesExamined * fixture.graph.Dims() * 4))
	b.ResetTimer()
	start := time.Now()
	var nextWorker uint64
	b.RunParallel(func(pb *testing.PB) {
		workerID := int(atomic.AddUint64(&nextWorker, 1)) - 1
		if workerID >= len(scratches) {
			b.Errorf("RunParallel spawned worker %d, but only %d scratches were prewarmed", workerID+1, len(scratches))
			return
		}
		scratch := scratches[workerID]
		var localSink int64
		for pb.Next() {
			results, stats, err := fixture.graph.SearchCosine(fixture.query, fixture.opts, scratch)
			if err != nil {
				panic(err)
			}
			if len(results) != fixture.opts.TopK {
				panic("unexpected column vector graph result count")
			}
			localSink += int64(results[0].Ordinal + stats.CandidatesExamined + stats.EdgesVisited)
		}
		atomic.AddInt64(&benchSink, localSink)
	})
	elapsed := time.Since(start)
	b.StopTimer()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "searches/s")
	}
	reportColumnVectorGraphPersistedMetrics(b, fixture, warmStats)
}

func reportColumnVectorGraphPersistedMetrics(b *testing.B, fixture *columnVectorGraphPersistedFixture, searchStats ColumnVectorGraphSearchStats) {
	b.Helper()
	accounting := fixture.accounting
	rows := float64(accounting.rows)
	if rows == 0 {
		rows = 1
	}
	b.ReportMetric(float64(fixture.buildNanos)/1e6, "build_ms")
	b.ReportMetric(float64(fixture.openNanos)/1e6, "open_ms")
	b.ReportMetric(float64(fixture.decodeNanos)/1e6, "decode_ms")
	b.ReportMetric(float64(accounting.encodedRawBytes)/rows, "encoded_raw_B/entry")
	b.ReportMetric(float64(accounting.declaredColumnStoredBytes)/rows, "stored_B/entry")
	b.ReportMetric(float64(accounting.assetBytes)/rows, "asset_B/entry")
	b.ReportMetric(float64(accounting.imageBytes)/rows, "image_B/entry")
	// Colgranule has no row-store checkpoint/GC/rewrite hook. This is the
	// settled namespace size after part publish, manifest save, close, and reopen.
	b.ReportMetric(float64(accounting.settledDiskBytes)/rows, "settled_disk_B/entry")
	b.ReportMetric(float64(accounting.vectorRawBytes)/rows, "vector_raw_B/entry")
	b.ReportMetric(float64(accounting.vectorStoredBytes)/rows, "vector_stored_B/entry")
	b.ReportMetric(float64(accounting.invNormRawBytes)/rows, "invnorm_raw_B/entry")
	b.ReportMetric(float64(accounting.invNormStoredBytes)/rows, "invnorm_stored_B/entry")
	b.ReportMetric(float64(accounting.adjacencyRawBytes)/rows, "adjacency_raw_B/entry")
	b.ReportMetric(float64(accounting.adjacencyStoredBytes)/rows, "adjacency_stored_B/entry")
	b.ReportMetric(float64(accounting.sourceDocumentVectorBytes)/rows, "source_doc_vector_B/entry")
	b.ReportMetric(float64(accounting.sourceDocumentVectorBytes+accounting.vectorRawBytes)/rows, "source_plus_column_vector_B/entry")
	for compression, blocks := range accounting.actualCompressionBlocks {
		b.ReportMetric(float64(blocks), "actual_"+metricToken(compression)+"_blocks")
	}
	for reason, blocks := range accounting.compressionFallbackReasons {
		b.ReportMetric(float64(blocks), "fallback_"+metricToken(reason)+"_blocks")
	}
	b.ReportMetric(float64(fixture.loadStats.Edges)/float64(fixture.loadStats.Rows), "edges/node")
	b.ReportMetric(float64(searchStats.CandidatesExamined), "candidates/search")
	b.ReportMetric(float64(searchStats.EdgesVisited), "edges/search")
}

func buildColumnVectorGraphPersistedFixture(tb testing.TB, rows int, dims int, degree int, blockRows int, compression Compression, order columnVectorGraphPersistedOrder) *columnVectorGraphPersistedFixture {
	tb.Helper()
	dir, err := os.MkdirTemp("", "colgranule-vector-graph-*")
	if err != nil {
		tb.Fatalf("MkdirTemp: %v", err)
	}
	var reopened *ColumnWorkspace
	cleanup := func() error {
		var cleanupErr error
		if reopened != nil {
			if err := reopened.Close(); err != nil {
				cleanupErr = err
			}
			reopened = nil
		}
		if err := os.RemoveAll(dir); err != nil && cleanupErr == nil {
			cleanupErr = err
		}
		return cleanupErr
	}
	success := false
	defer func() {
		if !success {
			_ = cleanup()
		}
	}()
	opts := columnVectorGraphPersistedOptions(rows, dims, blockRows, compression)
	rowsPerPart := columnVectorGraphPersistedRowsPerPart(rows)
	ordinalBySource := columnVectorGraphPersistedOrdinalBySource(rows, order)

	buildStart := time.Now()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "persisted_vector_graph"})
	if err != nil {
		tb.Fatalf("OpenColumnWorkspace(build): %v", err)
	}
	workspaceOpen := true
	defer func() {
		if workspaceOpen {
			_ = workspace.Close()
		}
	}()
	adapter, err := NewColumnMutationAdapter(workspace, ColumnMutationAdapterOptions{
		Collection:        "persisted_vector_graph",
		StoreOptions:      opts,
		InitialPartID:     1,
		InitialGeneration: 1,
	})
	if err != nil {
		tb.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	for start := 0; start < rows; start += rowsPerPart {
		end := min(start+rowsPerPart, rows)
		batch := columnVectorGraphPersistedBatchRange(rows, dims, degree, start, end, order, ordinalBySource)
		_, err := adapter.PublishBaseBatch(batch, ColumnPartCoverageOptions{
			SourceRowRootGeneration: 1,
			SourceRowVersionLower:   uint64(start),
			SourceRowVersionUpper:   uint64(end),
		})
		if err != nil {
			tb.Fatalf("PublishBaseBatch rows [%d,%d): %v", start, end, err)
		}
	}
	closeErr := workspace.Close()
	workspaceOpen = false
	if closeErr != nil {
		tb.Fatalf("Close build workspace: %v", closeErr)
	}
	buildNanos := time.Since(buildStart).Nanoseconds()

	openStart := time.Now()
	reopened, err = OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "persisted_vector_graph"})
	if err != nil {
		tb.Fatalf("OpenColumnWorkspace(reopen): %v", err)
	}
	manifest, err := reopened.LoadCollectionManifest()
	if err != nil {
		tb.Fatalf("LoadCollectionManifest: %v", err)
	}
	reader, err := OpenColumnPartSetReader(reopened, manifest, ColumnPartImageReadOptions{})
	if err != nil {
		tb.Fatalf("OpenColumnPartSetReader: %v", err)
	}
	openNanos := time.Since(openStart).Nanoseconds()

	decodeStart := time.Now()
	graph, loadStats, err := NewColumnVectorGraphFromPartSet(reader, ColumnVectorGraphOptions{})
	if err != nil {
		tb.Fatalf("NewColumnVectorGraphFromPartSet: %v", err)
	}
	decodeNanos := time.Since(decodeStart).Nanoseconds()

	queryOrdinal := rows / 2
	query, ok := graph.VectorAt(nil, queryOrdinal)
	if !ok {
		tb.Fatalf("missing query vector ordinal %d", queryOrdinal)
	}
	query = append([]float32(nil), query...)
	accounting := columnVectorGraphPersistedByteAccounting(tb, reader)
	settledDiskBytes, err := columnVectorGraphPersistedDirBytes(dir)
	if err != nil {
		tb.Fatalf("settled dir bytes: %v", err)
	}
	accounting.settledDiskBytes = settledDiskBytes
	fixture := &columnVectorGraphPersistedFixture{
		graph:       graph,
		query:       query,
		opts:        ColumnVectorGraphSearchOptions{TopK: 10, EfSearch: 128},
		loadStats:   loadStats,
		buildNanos:  buildNanos,
		openNanos:   openNanos,
		decodeNanos: decodeNanos,
		accounting:  accounting,
		cleanup:     cleanup,
	}
	success = true
	return fixture
}

func columnVectorGraphPersistedOptions(rows int, dims int, blockRows int, vectorCompression Compression) ColumnStoreOptions {
	if blockRows <= 0 {
		blockRows = DefaultRowsPerGranule
	}
	return ColumnStoreOptions{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CodecBlockRows: blockRows},
			{Name: "embedding", Type: ColumnTypeFloat32Vector, VectorDims: dims, Compression: vectorCompression, CodecBlockRows: blockRows},
			{Name: "embedding_inv_norm", Type: ColumnTypeFloat32Vector, VectorDims: 1, Compression: vectorCompression, CodecBlockRows: blockRows},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Compression: CompressionNone, CodecBlockRows: blockRows},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy: ColumnPartPolicy{
			RowsPerGranule:        blockRows,
			DefaultCodecBlockRows: blockRows,
		},
	}
}

func columnVectorGraphPersistedRowsPerPart(rows int) int {
	const maxRowsPerPart = 100_000
	if rows > maxRowsPerPart {
		return maxRowsPerPart
	}
	return rows
}

func columnVectorGraphPersistedOrdinalBySource(rows int, order columnVectorGraphPersistedOrder) []int {
	if order != columnVectorGraphPersistedOrderShuffled {
		return nil
	}
	ordinalBySource := make([]int, rows)
	for ordinal := 0; ordinal < rows; ordinal++ {
		source := columnVectorGraphPersistedSourceForOrdinal(ordinal, rows, order)
		ordinalBySource[source] = ordinal
	}
	return ordinalBySource
}

func columnVectorGraphPersistedSourceForOrdinal(ordinal int, rows int, order columnVectorGraphPersistedOrder) int {
	if order == columnVectorGraphPersistedOrderShuffled {
		return (ordinal*1009 + 9173) % rows
	}
	return ordinal
}

func columnVectorGraphPersistedOrdinalForSource(source int, order columnVectorGraphPersistedOrder, ordinalBySource []int) int {
	if order == columnVectorGraphPersistedOrderShuffled {
		return ordinalBySource[source]
	}
	return source
}

func columnVectorGraphPersistedBatchRange(rows int, dims int, degree int, start int, end int, order columnVectorGraphPersistedOrder, ordinalBySource []int) ColumnBatch {
	chunkRows := end - start
	ids := make([]int64, chunkRows)
	vectors := make([]float32, chunkRows*dims)
	invNorms := make([]float32, chunkRows)
	offsets := make([]uint32, chunkRows+1)
	neighbors := make([]uint32, 0, chunkRows*degree)
	for localRow := 0; localRow < chunkRows; localRow++ {
		ordinal := start + localRow
		source := columnVectorGraphPersistedSourceForOrdinal(ordinal, rows, order)
		ids[localRow] = int64(ordinal)
		var normSquared float64
		for dim := 0; dim < dims; dim++ {
			value := columnVectorGraphPersistedVectorValue(source, dim)
			vectors[localRow*dims+dim] = value
			normSquared += float64(value) * float64(value)
		}
		invNorms[localRow] = float32(1 / math.Sqrt(normSquared))
		offsets[localRow] = uint32(len(neighbors))
		for edge := 0; edge < degree; edge++ {
			step := edge/2 + 1
			neighborSource := source + step
			if edge%2 == 1 {
				neighborSource = source - step
			}
			neighborSource %= rows
			if neighborSource < 0 {
				neighborSource += rows
			}
			neighborOrdinal := columnVectorGraphPersistedOrdinalForSource(neighborSource, order, ordinalBySource)
			neighbors = append(neighbors, uint32(neighborOrdinal))
		}
	}
	offsets[chunkRows] = uint32(len(neighbors))
	return ColumnBatch{
		Rows: chunkRows,
		Columns: map[string][]int64{
			"id": ids,
		},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding":          {Dims: dims, Values: vectors},
			"embedding_inv_norm": {Dims: 1, Values: invNorms},
		},
		AdjacencyLists: map[string]AdjacencyListColumn{
			"neighbors": {Offsets: offsets, Values: neighbors},
		},
	}
}

func columnVectorGraphPersistedVectorValue(source int, dim int) float32 {
	cluster := source / 32
	return float32(((cluster+1)*(dim+17)%251)+1) / 251
}

func columnVectorGraphPersistedByteAccounting(tb testing.TB, reader *ColumnPartSetReader) columnVectorGraphPersistedAccounting {
	tb.Helper()
	out := columnVectorGraphPersistedAccounting{
		actualCompressionBlocks:    make(map[string]int),
		compressionFallbackReasons: make(map[string]int),
	}
	if reader == nil {
		return out
	}
	for _, loaded := range reader.parts {
		partAccounting := loaded.Part.ByteAccounting()
		out.rows += partAccounting.Rows
		out.encodedRawBytes += partAccounting.EncodedRawBytes
		out.declaredColumnStoredBytes += partAccounting.DeclaredColumnStoredBytes
		out.assetBytes += loaded.Ref.Part.AssetBytes
		out.imageBytes += loaded.Ref.Part.ImageBytes
		for _, detail := range partAccounting.ColumnsDetail {
			switch detail.Column {
			case "embedding":
				out.vectorRawBytes += detail.EncodedRawBytes
				out.vectorStoredBytes += detail.StoredBytes
			case "embedding_inv_norm":
				out.invNormRawBytes += detail.EncodedRawBytes
				out.invNormStoredBytes += detail.StoredBytes
			case "neighbors":
				out.adjacencyRawBytes += detail.EncodedRawBytes
				out.adjacencyStoredBytes += detail.StoredBytes
			}
		}
		for _, compression := range partAccounting.CompressionDetail {
			out.actualCompressionBlocks[compression.ActualCompression.String()] += compression.Blocks
			if compression.FallbackReason != "" {
				out.compressionFallbackReasons[compression.FallbackReason] += compression.Blocks
			}
		}
	}
	return out
}

func columnVectorGraphPersistedDirBytes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	return total, err
}

func metricToken(value string) string {
	value = strings.ReplaceAll(value, "/", "_")
	value = strings.ReplaceAll(value, " ", "_")
	value = strings.ReplaceAll(value, "-", "_")
	if value == "" {
		return "empty"
	}
	return value
}

func BenchmarkColumnVectorGraphPersistedBuildOpenDecode(b *testing.B) {
	for _, compression := range []Compression{CompressionNone, CompressionSnappy, CompressionLZ4, CompressionZSTD} {
		compression := compression
		b.Run(fmt.Sprintf("100k/%s/%s", columnVectorGraphPersistedOrderLocal, compression), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				fixture := buildColumnVectorGraphPersistedFixture(b, 100_000, 128, 16, 8192, compression, columnVectorGraphPersistedOrderLocal)
				benchSink += int64(fixture.graph.Rows() + fixture.loadStats.Edges)
				b.StopTimer()
				if err := fixture.Close(); err != nil {
					b.Fatalf("Close persisted vector graph fixture: %v", err)
				}
				b.StartTimer()
			}
		})
	}
}
