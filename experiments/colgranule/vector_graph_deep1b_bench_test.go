package colgranule

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	columnVectorGraphDeep1BBase10MURL = "https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/base.10M.fbin"
	columnVectorGraphDeep1BQueryURL   = "https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin"
	columnVectorGraphDeep1BDims       = 96
	columnVectorGraphDeep1BDegree     = 16
	columnVectorGraphDeep1BBlockRows  = 8192
)

type columnVectorGraphDeep1BDataPaths struct {
	basePath      string
	queryPath     string
	baseHeader    columnVectorGraphDeep1BFbinHeader
	queryHeader   columnVectorGraphDeep1BFbinHeader
	sourceRows    int
	sourceBytes   int64
	cacheBytes    int64
	queryBytes    int64
	subsetFrom10M bool
}

type columnVectorGraphDeep1BFbinHeader struct {
	Rows int
	Dims int
	Size int64
}

type columnVectorGraphDeep1BFixture struct {
	*columnVectorGraphPersistedFixture
	sourceReadNanos  int64
	sourceFileBytes  int64
	sourceCacheBytes int64
	queryFileBytes   int64
	sourceRows       int
	subsetFrom10M    bool
}

func TestColumnVectorGraphDeep1BFbinReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tiny.fbin")
	values := []float32{
		1, 2, 3,
		4, 5, 6,
		7, 8, 9,
	}
	if err := writeColumnVectorGraphDeep1BTestFbin(path, 3, 3, values); err != nil {
		t.Fatalf("write test fbin: %v", err)
	}
	header, err := columnVectorGraphDeep1BValidateFbin(path, 2, 3)
	if err != nil {
		t.Fatalf("validate fbin: %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer file.Close()
	var raw []byte
	var out []float32
	raw, out, err = columnVectorGraphDeep1BReadFbinVectorsAt(file, header, 1, 2, raw, out)
	if err != nil {
		t.Fatalf("read fbin vectors: %v", err)
	}
	if len(raw) != 2*3*4 {
		t.Fatalf("raw bytes=%d want %d", len(raw), 2*3*4)
	}
	want := []float32{4, 5, 6, 7, 8, 9}
	for i, got := range out {
		if got != want[i] {
			t.Fatalf("out[%d]=%v want %v", i, got, want[i])
		}
	}
}

func BenchmarkColumnVectorGraphDeep1BPersistedSearchCosine(b *testing.B) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B") != "1" {
		b.Skip("set COLUMN_VECTOR_DEEP1B=1 to run the opt-in Deep1B persisted vector graph benchmark")
	}
	for _, shape := range columnVectorGraphDeep1BShapes() {
		shape := shape
		b.Run(shape.name, func(b *testing.B) {
			if shape.rows == 10_000_000 && os.Getenv("COLUMN_VECTOR_DEEP1B_10M") != "1" {
				b.Skip("set COLUMN_VECTOR_DEEP1B_10M=1 to run the 10M Deep1B shape")
			}
			data := columnVectorGraphDeep1BEnsureData(b, shape.rows)
			for _, compression := range columnVectorGraphDeep1BCompressions(b) {
				compression := compression
				b.Run(compression.String(), func(b *testing.B) {
					fixture := buildColumnVectorGraphDeep1BFixture(b, data, shape.rows, compression)
					registerColumnVectorGraphPersistedFixtureCleanup(b, fixture.columnVectorGraphPersistedFixture)
					b.Run("serial", func(b *testing.B) {
						benchmarkColumnVectorGraphPersistedSearchSerial(b, fixture.columnVectorGraphPersistedFixture)
						reportColumnVectorGraphDeep1BProductMetrics(b, fixture)
					})
					b.Run("parallel", func(b *testing.B) {
						benchmarkColumnVectorGraphPersistedSearchParallel(b, fixture.columnVectorGraphPersistedFixture)
						reportColumnVectorGraphDeep1BProductMetrics(b, fixture)
					})
				})
			}
		})
	}
}

func BenchmarkColumnVectorGraphDeep1BPersistedBuildOpenDecode(b *testing.B) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_BUILD_OPEN_DECODE") != "1" {
		b.Skip("set COLUMN_VECTOR_DEEP1B_BUILD_OPEN_DECODE=1 to run the opt-in Deep1B build/open/decode benchmark")
	}
	for _, shape := range columnVectorGraphDeep1BShapes() {
		shape := shape
		b.Run(shape.name, func(b *testing.B) {
			if shape.rows == 10_000_000 && os.Getenv("COLUMN_VECTOR_DEEP1B_10M") != "1" {
				b.Skip("set COLUMN_VECTOR_DEEP1B_10M=1 to run the 10M Deep1B shape")
			}
			data := columnVectorGraphDeep1BEnsureData(b, shape.rows)
			for _, compression := range columnVectorGraphDeep1BCompressions(b) {
				compression := compression
				b.Run(compression.String(), func(b *testing.B) {
					var totalBuildNanos int64
					var totalOpenNanos int64
					var totalDecodeNanos int64
					var totalSourceReadNanos int64
					var lastAccounting columnVectorGraphPersistedAccounting
					var lastLoadStats ColumnVectorGraphLoadStats
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						fixture := buildColumnVectorGraphDeep1BFixtureWithPhaseTimer(b, b, data, shape.rows, compression)
						totalBuildNanos += fixture.buildNanos
						totalOpenNanos += fixture.openNanos
						totalDecodeNanos += fixture.decodeNanos
						totalSourceReadNanos += fixture.sourceReadNanos
						lastAccounting = fixture.accounting
						lastLoadStats = fixture.loadStats
						benchSink += int64(fixture.graph.Rows() + fixture.loadStats.Edges)
						b.StopTimer()
						if err := fixture.Close(); err != nil {
							b.Fatalf("Close Deep1B persisted vector graph fixture: %v", err)
						}
					}
					if b.N > 0 {
						avgPersisted := &columnVectorGraphPersistedFixture{
							buildNanos:  totalBuildNanos / int64(b.N),
							openNanos:   totalOpenNanos / int64(b.N),
							decodeNanos: totalDecodeNanos / int64(b.N),
							accounting:  lastAccounting,
						}
						avgDeep := &columnVectorGraphDeep1BFixture{
							columnVectorGraphPersistedFixture: avgPersisted,
							sourceReadNanos:                   totalSourceReadNanos / int64(b.N),
							sourceFileBytes:                   data.sourceBytes,
							sourceCacheBytes:                  data.cacheBytes,
							queryFileBytes:                    data.queryBytes,
							sourceRows:                        data.sourceRows,
							subsetFrom10M:                     data.subsetFrom10M,
						}
						reportColumnVectorGraphPersistedProductMetrics(b, avgPersisted)
						reportColumnVectorGraphDeep1BProductMetrics(b, avgDeep)
						b.ReportMetric(float64(lastLoadStats.Edges)/float64(lastLoadStats.Rows), "edges/node")
					}
				})
			}
		})
	}
}

func columnVectorGraphDeep1BShapes() []struct {
	name string
	rows int
} {
	return []struct {
		name string
		rows int
	}{
		{name: "1m", rows: 1_000_000},
		{name: "10m", rows: 10_000_000},
	}
}

func columnVectorGraphDeep1BCompressions(tb testing.TB) []Compression {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_COMPRESSIONS"))
	if raw == "" {
		return []Compression{CompressionNone, CompressionZSTD}
	}
	if strings.EqualFold(raw, "all") {
		return []Compression{CompressionNone, CompressionSnappy, CompressionLZ4, CompressionZSTD}
	}
	parts := strings.Split(raw, ",")
	out := make([]Compression, 0, len(parts))
	for _, part := range parts {
		switch strings.ToLower(strings.TrimSpace(part)) {
		case "", "skip":
			continue
		case "none":
			out = append(out, CompressionNone)
		case "snappy":
			out = append(out, CompressionSnappy)
		case "lz4":
			out = append(out, CompressionLZ4)
		case "zstd":
			out = append(out, CompressionZSTD)
		default:
			tb.Fatalf("unknown COLUMN_VECTOR_DEEP1B_COMPRESSIONS value %q", part)
		}
	}
	if len(out) == 0 {
		tb.Fatalf("COLUMN_VECTOR_DEEP1B_COMPRESSIONS selected no compression variants")
	}
	return out
}

func buildColumnVectorGraphDeep1BFixture(tb testing.TB, data columnVectorGraphDeep1BDataPaths, rows int, compression Compression) *columnVectorGraphDeep1BFixture {
	return buildColumnVectorGraphDeep1BFixtureWithPhaseTimer(tb, nil, data, rows, compression)
}

func buildColumnVectorGraphDeep1BFixtureWithPhaseTimer(tb testing.TB, phaseTimer *testing.B, data columnVectorGraphDeep1BDataPaths, rows int, compression Compression) *columnVectorGraphDeep1BFixture {
	tb.Helper()
	dir, err := os.MkdirTemp("", "colgranule-deep1b-vector-graph-*")
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

	baseFile, err := os.Open(data.basePath)
	if err != nil {
		tb.Fatalf("Open Deep1B base: %v", err)
	}
	defer baseFile.Close()
	opts := columnVectorGraphPersistedOptions(rows, columnVectorGraphDeep1BDims, columnVectorGraphDeep1BBlockRows, compression)
	rowsPerPart := columnVectorGraphPersistedRowsPerPart(rows)

	if phaseTimer != nil {
		phaseTimer.StartTimer()
	}
	buildStart := time.Now()
	workspace, err := OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "deep1b_vector_graph"})
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
		Collection:        "deep1b_vector_graph",
		StoreOptions:      opts,
		InitialPartID:     1,
		InitialGeneration: 1,
	})
	if err != nil {
		tb.Fatalf("NewColumnMutationAdapter: %v", err)
	}
	var sourceReadNanos int64
	var rawScratch []byte
	var vectorScratch []float32
	for start := 0; start < rows; start += rowsPerPart {
		end := min(start+rowsPerPart, rows)
		readStart := time.Now()
		rawScratch, vectorScratch, err = columnVectorGraphDeep1BReadFbinVectorsAt(baseFile, data.baseHeader, start, end-start, rawScratch, vectorScratch)
		if err != nil {
			tb.Fatalf("read Deep1B base rows [%d,%d): %v", start, end, err)
		}
		sourceReadNanos += time.Since(readStart).Nanoseconds()
		batch := columnVectorGraphDeep1BBatchRange(rows, start, vectorScratch)
		_, err := adapter.PublishBaseBatch(batch, ColumnPartCoverageOptions{
			SourceRowRootGeneration: 1,
			SourceRowVersionLower:   uint64(start),
			SourceRowVersionUpper:   uint64(end),
		})
		if err != nil {
			tb.Fatalf("PublishBaseBatch Deep1B rows [%d,%d): %v", start, end, err)
		}
	}
	closeErr := workspace.Close()
	workspaceOpen = false
	if closeErr != nil {
		tb.Fatalf("Close build workspace: %v", closeErr)
	}
	buildNanos := time.Since(buildStart).Nanoseconds()
	if phaseTimer != nil {
		phaseTimer.StopTimer()
	}

	if phaseTimer != nil {
		phaseTimer.StartTimer()
	}
	openStart := time.Now()
	reopened, err = OpenColumnWorkspace(dir, ColumnWorkspaceOptions{Collection: "deep1b_vector_graph"})
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
	if phaseTimer != nil {
		phaseTimer.StopTimer()
	}

	if phaseTimer != nil {
		phaseTimer.StartTimer()
	}
	decodeStart := time.Now()
	graph, loadStats, err := NewColumnVectorGraphFromPartSet(reader, ColumnVectorGraphOptions{})
	if err != nil {
		tb.Fatalf("NewColumnVectorGraphFromPartSet: %v", err)
	}
	decodeNanos := time.Since(decodeStart).Nanoseconds()
	if phaseTimer != nil {
		phaseTimer.StopTimer()
	}

	query := columnVectorGraphDeep1BReadQuery(tb, data.queryPath, data.queryHeader, 0)
	accounting := columnVectorGraphPersistedByteAccounting(tb, reader)
	settledDiskBytes, err := columnVectorGraphPersistedDirBytes(dir)
	if err != nil {
		tb.Fatalf("settled dir bytes: %v", err)
	}
	accounting.settledDiskBytes = settledDiskBytes
	persisted := &columnVectorGraphPersistedFixture{
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
	fixture := &columnVectorGraphDeep1BFixture{
		columnVectorGraphPersistedFixture: persisted,
		sourceReadNanos:                   sourceReadNanos,
		sourceFileBytes:                   data.sourceBytes,
		sourceCacheBytes:                  data.cacheBytes,
		queryFileBytes:                    data.queryBytes,
		sourceRows:                        data.sourceRows,
		subsetFrom10M:                     data.subsetFrom10M,
	}
	success = true
	return fixture
}

func columnVectorGraphDeep1BBatchRange(rows int, start int, vectors []float32) ColumnBatch {
	chunkRows := len(vectors) / columnVectorGraphDeep1BDims
	ids := make([]int64, chunkRows)
	invNorms := make([]float32, chunkRows)
	offsets := make([]uint32, chunkRows+1)
	neighbors := make([]uint32, 0, chunkRows*columnVectorGraphDeep1BDegree)
	for localRow := 0; localRow < chunkRows; localRow++ {
		ordinal := start + localRow
		ids[localRow] = int64(ordinal)
		rowValues := vectors[localRow*columnVectorGraphDeep1BDims : (localRow+1)*columnVectorGraphDeep1BDims]
		var normSquared float64
		for _, value := range rowValues {
			normSquared += float64(value) * float64(value)
		}
		if normSquared > 0 {
			invNorms[localRow] = float32(1 / math.Sqrt(normSquared))
		}
		offsets[localRow] = uint32(len(neighbors))
		for edge := 0; edge < columnVectorGraphDeep1BDegree; edge++ {
			step := edge/2 + 1
			neighbor := ordinal + step
			if edge%2 == 1 {
				neighbor = ordinal - step
			}
			neighbor %= rows
			if neighbor < 0 {
				neighbor += rows
			}
			neighbors = append(neighbors, uint32(neighbor))
		}
	}
	offsets[chunkRows] = uint32(len(neighbors))
	return ColumnBatch{
		Rows: chunkRows,
		Columns: map[string][]int64{
			"id": ids,
		},
		Float32Vectors: map[string]Float32VectorColumn{
			"embedding":          {Dims: columnVectorGraphDeep1BDims, Values: vectors},
			"embedding_inv_norm": {Dims: 1, Values: invNorms},
		},
		AdjacencyLists: map[string]AdjacencyListColumn{
			"neighbors": {Offsets: offsets, Values: neighbors},
		},
	}
}

func reportColumnVectorGraphDeep1BProductMetrics(b *testing.B, fixture *columnVectorGraphDeep1BFixture) {
	b.Helper()
	rows := float64(fixture.accounting.rows)
	if rows == 0 {
		rows = 1
	}
	b.ReportMetric(float64(fixture.sourceReadNanos)/1e6, "source_read_ms")
	b.ReportMetric(float64(fixture.sourceFileBytes)/rows, "source_fbin_B/entry")
	b.ReportMetric(float64(fixture.sourceCacheBytes)/rows, "source_cache_B/entry")
	b.ReportMetric(float64(fixture.queryFileBytes), "query_fbin_bytes")
	b.ReportMetric(float64(fixture.sourceRows), "source_fbin_rows")
	if fixture.subsetFrom10M {
		b.ReportMetric(1, "source_prefix_from_10m")
	}
}

func columnVectorGraphDeep1BEnsureData(tb testing.TB, rows int) columnVectorGraphDeep1BDataPaths {
	tb.Helper()
	dir := columnVectorGraphDeep1BDataDir(tb)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatalf("MkdirAll Deep1B data dir: %v", err)
	}
	queryPath := filepath.Join(dir, "query.public.10K.fbin")
	queryHeader := columnVectorGraphDeep1BEnsureFbin(tb, queryPath, columnVectorGraphDeep1BQueryURL, 1, columnVectorGraphDeep1BDims, 0)
	queryInfo, err := os.Stat(queryPath)
	if err != nil {
		tb.Fatalf("stat Deep1B query file: %v", err)
	}

	fullPath := filepath.Join(dir, "base.10M.fbin")
	if fullHeader, err := columnVectorGraphDeep1BValidateFbin(fullPath, rows, columnVectorGraphDeep1BDims); err == nil {
		info, statErr := os.Stat(fullPath)
		if statErr != nil {
			tb.Fatalf("stat Deep1B base file: %v", statErr)
		}
		sourceBytes := columnVectorGraphDeep1BRequiredBytes(tb, rows, fullHeader.Dims)
		return columnVectorGraphDeep1BDataPaths{
			basePath:    fullPath,
			queryPath:   queryPath,
			baseHeader:  fullHeader,
			queryHeader: queryHeader,
			sourceRows:  rows,
			sourceBytes: sourceBytes,
			cacheBytes:  info.Size(),
			queryBytes:  queryInfo.Size(),
		}
	}
	if rows <= 1_000_000 {
		subsetPath := filepath.Join(dir, "base.10M.first1M.fbin")
		subsetHeader := columnVectorGraphDeep1BEnsureFbin(tb, subsetPath, columnVectorGraphDeep1BBase10MURL, rows, columnVectorGraphDeep1BDims, rows)
		info, err := os.Stat(subsetPath)
		if err != nil {
			tb.Fatalf("stat Deep1B 1M subset: %v", err)
		}
		return columnVectorGraphDeep1BDataPaths{
			basePath:      subsetPath,
			queryPath:     queryPath,
			baseHeader:    subsetHeader,
			queryHeader:   queryHeader,
			sourceRows:    subsetHeader.Rows,
			sourceBytes:   info.Size(),
			cacheBytes:    info.Size(),
			queryBytes:    queryInfo.Size(),
			subsetFrom10M: true,
		}
	}
	fullHeader := columnVectorGraphDeep1BEnsureFbin(tb, fullPath, columnVectorGraphDeep1BBase10MURL, rows, columnVectorGraphDeep1BDims, 0)
	info, err := os.Stat(fullPath)
	if err != nil {
		tb.Fatalf("stat Deep1B base file: %v", err)
	}
	return columnVectorGraphDeep1BDataPaths{
		basePath:    fullPath,
		queryPath:   queryPath,
		baseHeader:  fullHeader,
		queryHeader: queryHeader,
		sourceRows:  rows,
		sourceBytes: columnVectorGraphDeep1BRequiredBytes(tb, rows, fullHeader.Dims),
		cacheBytes:  info.Size(),
		queryBytes:  queryInfo.Size(),
	}
}

func columnVectorGraphDeep1BRequiredBytes(tb testing.TB, rows int, dims int) int64 {
	tb.Helper()
	bytes, err := columnVectorGraphDeep1BFbinBytes(rows, dims)
	if err != nil {
		tb.Fatalf("Deep1B required bytes: %v", err)
	}
	return bytes
}

func columnVectorGraphDeep1BEnsureFbin(tb testing.TB, path string, url string, minRows int, dims int, prefixRows int) columnVectorGraphDeep1BFbinHeader {
	tb.Helper()
	if header, err := columnVectorGraphDeep1BValidateFbin(path, minRows, dims); err == nil {
		return header
	}
	if os.Getenv("COLUMN_VECTOR_DEEP1B_DOWNLOAD") != "1" {
		tb.Skipf("missing Deep1B file %s; set COLUMN_VECTOR_DEEP1B_DOWNLOAD=1 or run scripts/bench_column_vector_deep1b.sh to download it into %s", filepath.Base(path), filepath.Dir(path))
	}
	if err := columnVectorGraphDeep1BDownloadFbin(tb, path, url, prefixRows, dims); err != nil {
		tb.Fatalf("download Deep1B file %s: %v", filepath.Base(path), err)
	}
	header, err := columnVectorGraphDeep1BValidateFbin(path, minRows, dims)
	if err != nil {
		tb.Fatalf("validate downloaded Deep1B file %s: %v", filepath.Base(path), err)
	}
	return header
}

func columnVectorGraphDeep1BDataDir(tb testing.TB) string {
	tb.Helper()
	if dir := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_DIR")); dir != "" {
		return dir
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		tb.Fatalf("UserCacheDir: %v", err)
	}
	return filepath.Join(cacheDir, "gomap", "deep1b")
}

func columnVectorGraphDeep1BDownloadFbin(tb testing.TB, path string, url string, prefixRows int, dims int) error {
	tb.Helper()
	var expectedBytes int64
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if prefixRows > 0 {
		values, err := checkedMulInt(prefixRows, dims, "Deep1B prefix values")
		if err != nil {
			return err
		}
		payloadBytes, err := checkedMulInt(values, 4, "Deep1B prefix payload bytes")
		if err != nil {
			return err
		}
		expectedBytes = int64(8 + payloadBytes)
		req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", expectedBytes-1))
	}
	tb.Logf("downloading Deep1B %s to %s", url, path)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if prefixRows > 0 && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("range download status=%s want 206 Partial Content", resp.Status)
	}
	if prefixRows == 0 && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download status=%s want 200 OK", resp.Status)
	}
	tmp := path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	written, copyErr := io.Copy(out, resp.Body)
	closeErr := out.Close()
	if copyErr != nil {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if expectedBytes > 0 && written != expectedBytes {
		_ = os.Remove(tmp)
		return fmt.Errorf("downloaded bytes=%d want %d", written, expectedBytes)
	}
	if prefixRows > 0 {
		if err := columnVectorGraphDeep1BRewriteFbinRows(tmp, prefixRows); err != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	return os.Rename(tmp, path)
}

func columnVectorGraphDeep1BRewriteFbinRows(path string, rows int) error {
	if rows < 0 || rows > math.MaxUint32 {
		return fmt.Errorf("Deep1B fbin rows=%d outside uint32 range", rows)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(rows))
	_, err = file.WriteAt(header[:], 0)
	return err
}

func columnVectorGraphDeep1BValidateFbin(path string, minRows int, wantDims int) (columnVectorGraphDeep1BFbinHeader, error) {
	file, err := os.Open(path)
	if err != nil {
		return columnVectorGraphDeep1BFbinHeader{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return columnVectorGraphDeep1BFbinHeader{}, err
	}
	header, err := columnVectorGraphDeep1BReadFbinHeader(file)
	if err != nil {
		return columnVectorGraphDeep1BFbinHeader{}, err
	}
	header.Size = info.Size()
	if header.Dims != wantDims {
		return columnVectorGraphDeep1BFbinHeader{}, fmt.Errorf("Deep1B fbin dims=%d want %d", header.Dims, wantDims)
	}
	if header.Rows < minRows {
		return columnVectorGraphDeep1BFbinHeader{}, fmt.Errorf("Deep1B fbin rows=%d below requested %d", header.Rows, minRows)
	}
	needed, err := columnVectorGraphDeep1BFbinBytes(minRows, header.Dims)
	if err != nil {
		return columnVectorGraphDeep1BFbinHeader{}, err
	}
	if info.Size() < needed {
		return columnVectorGraphDeep1BFbinHeader{}, fmt.Errorf("Deep1B fbin size=%d below requested prefix bytes=%d", info.Size(), needed)
	}
	return header, nil
}

func columnVectorGraphDeep1BReadFbinHeader(file *os.File) (columnVectorGraphDeep1BFbinHeader, error) {
	var header [8]byte
	if _, err := file.ReadAt(header[:], 0); err != nil {
		return columnVectorGraphDeep1BFbinHeader{}, err
	}
	rows := int(binary.LittleEndian.Uint32(header[0:4]))
	dims := int(binary.LittleEndian.Uint32(header[4:8]))
	if rows <= 0 {
		return columnVectorGraphDeep1BFbinHeader{}, fmt.Errorf("Deep1B fbin invalid rows=%d", rows)
	}
	if dims <= 0 {
		return columnVectorGraphDeep1BFbinHeader{}, fmt.Errorf("Deep1B fbin invalid dims=%d", dims)
	}
	return columnVectorGraphDeep1BFbinHeader{Rows: rows, Dims: dims}, nil
}

func columnVectorGraphDeep1BFbinBytes(rows int, dims int) (int64, error) {
	if rows < 0 {
		return 0, fmt.Errorf("Deep1B fbin negative rows=%d", rows)
	}
	values, err := checkedMulInt(rows, dims, "Deep1B fbin values")
	if err != nil {
		return 0, err
	}
	payloadBytes, err := checkedMulInt(values, 4, "Deep1B fbin payload bytes")
	if err != nil {
		return 0, err
	}
	return int64(8 + payloadBytes), nil
}

func columnVectorGraphDeep1BReadFbinVectorsAt(file *os.File, header columnVectorGraphDeep1BFbinHeader, start int, rows int, raw []byte, dst []float32) ([]byte, []float32, error) {
	if start < 0 || rows <= 0 || start+rows > header.Rows {
		return nil, nil, fmt.Errorf("Deep1B fbin read start=%d rows=%d outside file rows=%d", start, rows, header.Rows)
	}
	values, err := checkedMulInt(rows, header.Dims, "Deep1B fbin read values")
	if err != nil {
		return nil, nil, err
	}
	byteCount, err := checkedMulInt(values, 4, "Deep1B fbin read bytes")
	if err != nil {
		return nil, nil, err
	}
	if cap(raw) < byteCount {
		raw = make([]byte, byteCount)
	} else {
		raw = raw[:byteCount]
	}
	offsetValues, err := checkedMulInt(start, header.Dims, "Deep1B fbin offset values")
	if err != nil {
		return nil, nil, err
	}
	offsetBytes, err := checkedMulInt(offsetValues, 4, "Deep1B fbin offset bytes")
	if err != nil {
		return nil, nil, err
	}
	if _, err := file.ReadAt(raw, int64(8+offsetBytes)); err != nil {
		return nil, nil, err
	}
	if cap(dst) < values {
		dst = make([]float32, values)
	} else {
		dst = dst[:values]
	}
	for i := range dst {
		dst[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:]))
	}
	return raw, dst, nil
}

func columnVectorGraphDeep1BReadQuery(tb testing.TB, path string, header columnVectorGraphDeep1BFbinHeader, queryIndex int) []float32 {
	tb.Helper()
	file, err := os.Open(path)
	if err != nil {
		tb.Fatalf("Open Deep1B query file: %v", err)
	}
	defer file.Close()
	var raw []byte
	var query []float32
	_, query, err = columnVectorGraphDeep1BReadFbinVectorsAt(file, header, queryIndex, 1, raw, query)
	if err != nil {
		tb.Fatalf("read Deep1B query %d: %v", queryIndex, err)
	}
	return append([]float32(nil), query...)
}

func writeColumnVectorGraphDeep1BTestFbin(path string, rows int, dims int, values []float32) error {
	if len(values) != rows*dims {
		return errors.New("test fbin values length does not match shape")
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(rows))
	binary.LittleEndian.PutUint32(header[4:8], uint32(dims))
	if _, err := file.Write(header[:]); err != nil {
		return err
	}
	var raw [4]byte
	for _, value := range values {
		binary.LittleEndian.PutUint32(raw[:], math.Float32bits(value))
		if _, err := file.Write(raw[:]); err != nil {
			return err
		}
	}
	return nil
}
