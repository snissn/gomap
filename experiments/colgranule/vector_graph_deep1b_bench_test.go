package colgranule

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/snissn/compress/zstd"
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

type columnVectorGraphDeep1BCompressionCase struct {
	name                 string
	vectorCompression    Compression
	adjacencyCompression Compression
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

func TestColumnVectorGraphDeep1BJZIPCodecRoundTrip(t *testing.T) {
	const (
		rows = 16
		dims = 8
	)
	vectors := make([]float32, rows*dims)
	for row := 0; row < rows; row++ {
		base := row * dims
		var normSquared float64
		for j := 0; j < dims; j++ {
			value := math.Sin(float64((row+1)*(j+2))*0.37) + math.Cos(float64((row+3)*(j+1))*0.19)
			vectors[base+j] = float32(value)
			normSquared += value * value
		}
		invNorm := 1 / math.Sqrt(normSquared)
		for j := 0; j < dims; j++ {
			vectors[base+j] = float32(float64(vectors[base+j]) * invNorm)
		}
	}
	transforms := []columnVectorGraphDeep1BJZIPTransform{
		{name: "cartesian_raw", kind: columnVectorGraphDeep1BJZIPTransformCartesianRaw},
		{name: "cartesian_transpose", kind: columnVectorGraphDeep1BJZIPTransformCartesianTranspose},
		{name: "cartesian_byte_shuffle", kind: columnVectorGraphDeep1BJZIPTransformCartesianByteShuffle},
		{name: "spherical", kind: columnVectorGraphDeep1BJZIPTransformSpherical},
		{name: "spherical_center_delta", kind: columnVectorGraphDeep1BJZIPTransformSphericalCenterDelta},
		{name: "spherical_prev_delta", kind: columnVectorGraphDeep1BJZIPTransformSphericalPrevDelta},
		{name: "householder_cartesian", kind: columnVectorGraphDeep1BJZIPTransformHouseholderCartesian},
	}
	codecs := []columnVectorGraphDeep1BJZIPByteCodec{
		{name: "raw", kind: columnVectorGraphDeep1BJZIPByteCodecRaw},
		{name: "snappy", kind: columnVectorGraphDeep1BJZIPByteCodecSnappy},
		{name: "lz4", kind: columnVectorGraphDeep1BJZIPByteCodecLZ4},
		{name: "zstd_fast", kind: columnVectorGraphDeep1BJZIPByteCodecZSTDFast},
		{name: "zstd_default", kind: columnVectorGraphDeep1BJZIPByteCodecZSTDDefault},
		{name: "zstd_better", kind: columnVectorGraphDeep1BJZIPByteCodecZSTDBetter},
	}
	for _, transform := range transforms {
		transform := transform
		t.Run(transform.name, func(t *testing.T) {
			for _, byteCodec := range codecs {
				byteCodec := byteCodec
				t.Run(byteCodec.name, func(t *testing.T) {
					codec := &columnVectorGraphDeep1BJZIPCodec{}
					encoded, err := codec.Encode(vectors, dims, transform, byteCodec)
					if err != nil {
						t.Fatalf("Encode: %v", err)
					}
					decoded, _, err := codec.Decode(encoded)
					if err != nil {
						t.Fatalf("Decode: %v", err)
					}
					maxAbsError, maxCosineError, meanCosineError := columnVectorGraphDeep1BJZIPErrorMetrics(vectors, decoded, dims)
					if maxAbsError > 1e-4 || maxCosineError > 1e-6 || meanCosineError > 1e-7 {
						t.Fatalf("errors max_abs=%g max_cosine=%g mean_cosine=%g", maxAbsError, maxCosineError, meanCosineError)
					}
				})
			}
		})
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
			for _, compressionCase := range columnVectorGraphDeep1BCompressionCases(b) {
				compressionCase := compressionCase
				b.Run(compressionCase.name, func(b *testing.B) {
					fixture := buildColumnVectorGraphDeep1BFixture(b, data, shape.rows, compressionCase.vectorCompression, compressionCase.adjacencyCompression)
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
			for _, compressionCase := range columnVectorGraphDeep1BCompressionCases(b) {
				compressionCase := compressionCase
				b.Run(compressionCase.name, func(b *testing.B) {
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
						fixture := buildColumnVectorGraphDeep1BFixtureWithPhaseTimer(b, b, data, shape.rows, compressionCase.vectorCompression, compressionCase.adjacencyCompression)
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

func BenchmarkColumnVectorGraphDeep1BNeighborhoodCompressionSmoke(b *testing.B) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE") != "1" {
		b.Skip("set COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 to run the opt-in Deep1B nearest-neighborhood compression smoke benchmark")
	}
	const rows = 1_000_000
	data := columnVectorGraphDeep1BEnsureData(b, rows)
	granuleRows := columnVectorGraphDeep1BEnvInt(b, "COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS", columnVectorGraphDeep1BBlockRows)
	if granuleRows <= 0 {
		b.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d must be positive", granuleRows)
	}
	if granuleRows > rows {
		b.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d exceeds rows=%d", granuleRows, rows)
	}
	baseFile, err := os.Open(data.basePath)
	if err != nil {
		b.Fatalf("Open Deep1B base: %v", err)
	}
	defer baseFile.Close()
	query := columnVectorGraphDeep1BReadQuery(b, data.queryPath, data.queryHeader, 0)
	exactStart := time.Now()
	nearest := columnVectorGraphDeep1BTopRowsByCosine(b, baseFile, data.baseHeader, rows, query, granuleRows)
	exactNanos := time.Since(exactStart).Nanoseconds()
	if len(nearest) != granuleRows {
		b.Fatalf("nearest rows=%d want %d", len(nearest), granuleRows)
	}
	nearestRankedRows := make([]int, len(nearest))
	nearestOrdinalRows := make([]int, len(nearest))
	for i, neighbor := range nearest {
		nearestRankedRows[i] = neighbor.row
		nearestOrdinalRows[i] = neighbor.row
	}
	sort.Ints(nearestOrdinalRows)
	orders := []struct {
		name string
		rows []int
	}{
		{name: "source_prefix", rows: columnVectorGraphDeep1BSequentialRows(granuleRows)},
		{name: "nearest_ranked", rows: nearestRankedRows},
		{name: "nearest_ordinal", rows: nearestOrdinalRows},
	}
	for _, order := range orders {
		order := order
		vectors := columnVectorGraphDeep1BReadFbinRows(b, baseFile, data.baseHeader, order.rows)
		b.Run(order.name, func(b *testing.B) {
			for _, compression := range columnVectorGraphDeep1BCompressions(b) {
				compression := compression
				b.Run(compression.String(), func(b *testing.B) {
					builder := NewGranuleBuilder(Config{Encoding: EncodingRawFloat32Vector, Compression: compression})
					warm, err := builder.BuildFloat32Vectors(vectors, columnVectorGraphDeep1BDims)
					if err != nil {
						b.Fatalf("BuildFloat32Vectors warm: %v", err)
					}
					if warm.RawBytes == 0 {
						b.Fatal("warm raw bytes is zero")
					}
					b.ReportAllocs()
					b.SetBytes(int64(warm.RawBytes))
					b.ResetTimer()
					start := time.Now()
					for i := 0; i < b.N; i++ {
						g, err := builder.BuildFloat32Vectors(vectors, columnVectorGraphDeep1BDims)
						if err != nil {
							b.Fatalf("BuildFloat32Vectors: %v", err)
						}
						benchSink += int64(g.StoredBytes)
					}
					elapsed := time.Since(start)
					b.StopTimer()
					if elapsed > 0 {
						b.ReportMetric(float64(b.N)/elapsed.Seconds(), "granules/s")
					}
					b.ReportMetric(float64(exactNanos)/1e6, "exact_scan_ms")
					b.ReportMetric(float64(rows), "exact_rows")
					b.ReportMetric(float64(granuleRows), "granule_rows")
					b.ReportMetric(nearest[0].score, "best_top_cosine")
					b.ReportMetric(nearest[len(nearest)-1].score, "worst_top_cosine")
					b.ReportMetric(float64(warm.RawBytes)/float64(granuleRows), "vector_raw_B/entry")
					b.ReportMetric(float64(warm.StoredBytes)/float64(granuleRows), "vector_stored_B/entry")
					b.ReportMetric(float64(warm.StoredBytes)/float64(warm.RawBytes), "stored_raw_ratio")
					if warm.CodecReport.CompressionFallbackReason != "" {
						b.ReportMetric(1, "fallback_"+metricToken(warm.CodecReport.CompressionFallbackReason))
					}
				})
			}
		})
	}
}

func BenchmarkColumnVectorGraphDeep1BJZIPNeighborhoodCompressionSmoke(b *testing.B) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE") != "1" {
		b.Skip("set COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 to run the opt-in Deep1B JZIP-style nearest-neighborhood compression smoke benchmark")
	}
	const rows = 1_000_000
	data := columnVectorGraphDeep1BEnsureData(b, rows)
	granuleRows := columnVectorGraphDeep1BEnvInt(b, "COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS", columnVectorGraphDeep1BBlockRows)
	if granuleRows <= 0 {
		b.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d must be positive", granuleRows)
	}
	if granuleRows > rows {
		b.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d exceeds rows=%d", granuleRows, rows)
	}
	baseFile, err := os.Open(data.basePath)
	if err != nil {
		b.Fatalf("Open Deep1B base: %v", err)
	}
	defer baseFile.Close()
	query := columnVectorGraphDeep1BReadQuery(b, data.queryPath, data.queryHeader, 0)
	exactStart := time.Now()
	nearest := columnVectorGraphDeep1BTopRowsByCosine(b, baseFile, data.baseHeader, rows, query, granuleRows)
	exactNanos := time.Since(exactStart).Nanoseconds()
	if len(nearest) != granuleRows {
		b.Fatalf("nearest rows=%d want %d", len(nearest), granuleRows)
	}
	nearestRankedRows := make([]int, len(nearest))
	nearestOrdinalRows := make([]int, len(nearest))
	for i, neighbor := range nearest {
		nearestRankedRows[i] = neighbor.row
		nearestOrdinalRows[i] = neighbor.row
	}
	sort.Ints(nearestOrdinalRows)
	orders := []struct {
		name string
		rows []int
	}{
		{name: "source_prefix", rows: columnVectorGraphDeep1BSequentialRows(granuleRows)},
		{name: "nearest_ranked", rows: nearestRankedRows},
		{name: "nearest_ordinal", rows: nearestOrdinalRows},
	}
	for _, order := range orders {
		order := order
		vectors := columnVectorGraphDeep1BReadFbinRows(b, baseFile, data.baseHeader, order.rows)
		b.Run(order.name, func(b *testing.B) {
			for _, transform := range columnVectorGraphDeep1BJZIPTransforms(b) {
				transform := transform
				b.Run(transform.name, func(b *testing.B) {
					for _, byteCodec := range columnVectorGraphDeep1BJZIPByteCodecs(b) {
						byteCodec := byteCodec
						b.Run(byteCodec.name, func(b *testing.B) {
							codec := &columnVectorGraphDeep1BJZIPCodec{}
							warm, err := codec.Encode(vectors, columnVectorGraphDeep1BDims, transform, byteCodec)
							if err != nil {
								b.Fatalf("JZIP-style encode warm: %v", err)
							}
							decoded, decodeNanos, err := codec.Decode(warm)
							if err != nil {
								b.Fatalf("JZIP-style decode warm: %v", err)
							}
							maxAbsError, maxCosineError, meanCosineError := columnVectorGraphDeep1BJZIPErrorMetrics(vectors, decoded, columnVectorGraphDeep1BDims)
							decodeEncoded := warm
							decodeEncoded.Payload = append([]byte(nil), warm.Payload...)
							decodeEncoded.Metadata = append([]float32(nil), warm.Metadata...)
							decodeCodec := &columnVectorGraphDeep1BJZIPCodec{}
							b.ReportAllocs()
							b.SetBytes(int64(warm.RawBytes))
							b.ResetTimer()
							start := time.Now()
							for i := 0; i < b.N; i++ {
								encoded, err := codec.Encode(vectors, columnVectorGraphDeep1BDims, transform, byteCodec)
								if err != nil {
									b.Fatalf("JZIP-style encode: %v", err)
								}
								benchSink += int64(encoded.StoredBytes)
							}
							elapsed := time.Since(start)
							b.StopTimer()
							decodeStart := time.Now()
							for i := 0; i < b.N; i++ {
								decoded, _, err := decodeCodec.Decode(decodeEncoded)
								if err != nil {
									b.Fatalf("JZIP-style decode: %v", err)
								}
								benchSink += int64(len(decoded))
							}
							decodeElapsed := time.Since(decodeStart)
							if b.N > 0 {
								decodeNanos = decodeElapsed.Nanoseconds() / int64(b.N)
							}
							if elapsed > 0 && b.N > 0 {
								b.ReportMetric(float64(b.N)/elapsed.Seconds(), "granules/s")
								b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N)/1e6, "encode_ms")
							}
							if decodeElapsed > 0 && b.N > 0 {
								decodeSeconds := float64(decodeNanos) / 1e9
								b.ReportMetric(float64(b.N)/decodeElapsed.Seconds(), "decode_granules/s")
								b.ReportMetric(float64(granuleRows)/decodeSeconds, "decode_vectors/s")
								b.ReportMetric(float64(warm.RawBytes)/decodeSeconds/1e6, "decode_raw_MB/s")
								b.ReportMetric(float64(warm.TransformRawBytes)/decodeSeconds/1e6, "decode_transform_MB/s")
							}
							b.ReportMetric(float64(exactNanos)/1e6, "exact_scan_ms")
							b.ReportMetric(float64(rows), "exact_rows")
							b.ReportMetric(float64(granuleRows), "granule_rows")
							b.ReportMetric(nearest[0].score, "best_top_cosine")
							b.ReportMetric(nearest[len(nearest)-1].score, "worst_top_cosine")
							b.ReportMetric(float64(warm.RawBytes)/float64(granuleRows), "vector_raw_B/entry")
							b.ReportMetric(float64(warm.TransformRawBytes)/float64(granuleRows), "transform_raw_B/entry")
							b.ReportMetric(float64(warm.MetadataBytes)/float64(granuleRows), "metadata_B/entry")
							b.ReportMetric(float64(warm.StoredBytes)/float64(granuleRows), "stored_B/entry")
							b.ReportMetric(float64(warm.RawBytes)/float64(warm.StoredBytes), "ratio_vs_raw")
							b.ReportMetric(float64(warm.StoredBytes)/float64(warm.RawBytes), "stored_raw_ratio")
							b.ReportMetric(float64(warm.StoredBytes)/float64(warm.TransformRawBytes), "stored_transform_raw_ratio")
							b.ReportMetric(float64(warm.TransformNanos)/1e6, "warm_transform_ms")
							if warm.TransformNanos > 0 {
								transformSeconds := float64(warm.TransformNanos) / 1e9
								b.ReportMetric(float64(granuleRows)/transformSeconds, "warm_transform_vectors/s")
								b.ReportMetric(float64(warm.RawBytes)/transformSeconds/1e6, "warm_transform_raw_MB/s")
							}
							b.ReportMetric(float64(warm.TransposeShuffleNanos)/1e6, "warm_layout_ms")
							b.ReportMetric(float64(warm.TransposeShuffleNanos)/1e6, "warm_transpose_shuffle_ms")
							b.ReportMetric(float64(warm.CompressionNanos)/1e6, "warm_compress_ms")
							b.ReportMetric(float64(decodeNanos)/1e6, "decode_ms")
							b.ReportMetric(maxAbsError, "max_abs_error")
							b.ReportMetric(maxCosineError, "max_cosine_error")
							b.ReportMetric(meanCosineError, "mean_cosine_error")
							if warm.ActualCodec != warm.RequestedCodec {
								b.ReportMetric(1, "fallback_"+metricToken(warm.ActualCodec.name))
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkColumnVectorGraphDeep1BJZIPDecodeAndScoreSmoke(b *testing.B) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE") != "1" {
		b.Skip("set COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_SMOKE=1 to run the opt-in Deep1B JZIP-style decode-and-score smoke benchmark")
	}
	const rows = 1_000_000
	data := columnVectorGraphDeep1BEnsureData(b, rows)
	granuleRows := columnVectorGraphDeep1BEnvInt(b, "COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS", columnVectorGraphDeep1BBlockRows)
	if granuleRows <= 0 {
		b.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d must be positive", granuleRows)
	}
	if granuleRows > rows {
		b.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d exceeds rows=%d", granuleRows, rows)
	}
	baseFile, err := os.Open(data.basePath)
	if err != nil {
		b.Fatalf("Open Deep1B base: %v", err)
	}
	defer baseFile.Close()
	query := columnVectorGraphDeep1BReadQuery(b, data.queryPath, data.queryHeader, 0)
	queryInvNorm := float32(columnVectorGraphDeep1BInvNorm(query))
	exactStart := time.Now()
	nearest := columnVectorGraphDeep1BTopRowsByCosine(b, baseFile, data.baseHeader, rows, query, granuleRows)
	exactNanos := time.Since(exactStart).Nanoseconds()
	if len(nearest) != granuleRows {
		b.Fatalf("nearest rows=%d want %d", len(nearest), granuleRows)
	}
	nearestRankedRows := make([]int, len(nearest))
	for i, neighbor := range nearest {
		nearestRankedRows[i] = neighbor.row
	}
	vectors := columnVectorGraphDeep1BReadFbinRows(b, baseFile, data.baseHeader, nearestRankedRows)
	invNorms := columnVectorGraphDeep1BInvNorms(vectors, columnVectorGraphDeep1BDims)

	reportDecodeAndScoreMetrics := func(b *testing.B, elapsed time.Duration, rawBytes int, storedBytes int, transformRawBytes int, decodeScoreNanos int64) {
		b.Helper()
		if elapsed > 0 {
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "granules/s")
		}
		if decodeScoreNanos > 0 {
			decodeScoreSeconds := float64(decodeScoreNanos) / 1e9
			b.ReportMetric(float64(decodeScoreNanos)/1e6, "decode_score_ms")
			b.ReportMetric(float64(granuleRows)/decodeScoreSeconds, "candidates/s")
			b.ReportMetric(float64(rawBytes)/decodeScoreSeconds/1e6, "raw_MB/s")
			if transformRawBytes > 0 {
				b.ReportMetric(float64(transformRawBytes)/decodeScoreSeconds/1e6, "transform_MB/s")
			}
		}
		b.ReportMetric(float64(exactNanos)/1e6, "exact_scan_ms")
		b.ReportMetric(float64(rows), "exact_rows")
		b.ReportMetric(float64(granuleRows), "granule_rows")
		b.ReportMetric(nearest[0].score, "best_top_cosine")
		b.ReportMetric(nearest[len(nearest)-1].score, "worst_top_cosine")
		b.ReportMetric(float64(rawBytes)/float64(granuleRows), "vector_raw_B/entry")
		if storedBytes > 0 {
			b.ReportMetric(float64(storedBytes)/float64(granuleRows), "stored_B/entry")
			b.ReportMetric(float64(rawBytes)/float64(storedBytes), "ratio_vs_raw")
			b.ReportMetric(float64(storedBytes)/float64(rawBytes), "stored_raw_ratio")
		}
	}

	b.Run("resident_fp32", func(b *testing.B) {
		bestScore, bestRow := columnVectorGraphDeep1BScoreBlock(query, queryInvNorm, vectors, invNorms, columnVectorGraphDeep1BDims)
		b.ReportAllocs()
		b.SetBytes(int64(len(vectors) * 4))
		b.ResetTimer()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			bestScore, bestRow = columnVectorGraphDeep1BScoreBlock(query, queryInvNorm, vectors, invNorms, columnVectorGraphDeep1BDims)
		}
		elapsed := time.Since(start)
		b.StopTimer()
		benchSink += int64(bestRow) + int64(math.Float32bits(bestScore))
		var scoreNanos int64
		if b.N > 0 {
			scoreNanos = elapsed.Nanoseconds() / int64(b.N)
		}
		reportDecodeAndScoreMetrics(b, elapsed, len(vectors)*4, len(vectors)*4, len(vectors)*4, scoreNanos)
		b.ReportMetric(float64(scoreNanos)/1e6, "score_only_ms")
	})

	for _, transform := range columnVectorGraphDeep1BJZIPTransforms(b) {
		transform := transform
		b.Run(transform.name, func(b *testing.B) {
			for _, byteCodec := range columnVectorGraphDeep1BJZIPByteCodecs(b) {
				byteCodec := byteCodec
				b.Run(byteCodec.name, func(b *testing.B) {
					encodeCodec := &columnVectorGraphDeep1BJZIPCodec{}
					warm, err := encodeCodec.Encode(vectors, columnVectorGraphDeep1BDims, transform, byteCodec)
					if err != nil {
						b.Fatalf("JZIP-style encode warm: %v", err)
					}
					decodeEncoded := warm
					decodeEncoded.Payload = append([]byte(nil), warm.Payload...)
					decodeEncoded.Metadata = append([]float32(nil), warm.Metadata...)
					decodeCodec := &columnVectorGraphDeep1BJZIPCodec{}
					decoded, _, err := decodeCodec.Decode(decodeEncoded)
					if err != nil {
						b.Fatalf("JZIP-style decode warm: %v", err)
					}
					bestScore, bestRow := columnVectorGraphDeep1BScoreBlock(query, queryInvNorm, decoded, invNorms, columnVectorGraphDeep1BDims)
					maxAbsError, maxCosineError, meanCosineError := columnVectorGraphDeep1BJZIPErrorMetrics(vectors, decoded, columnVectorGraphDeep1BDims)
					b.ReportAllocs()
					b.SetBytes(int64(warm.RawBytes))
					b.ResetTimer()
					start := time.Now()
					for i := 0; i < b.N; i++ {
						decoded, _, err = decodeCodec.Decode(decodeEncoded)
						if err != nil {
							b.Fatalf("JZIP-style decode: %v", err)
						}
						bestScore, bestRow = columnVectorGraphDeep1BScoreBlock(query, queryInvNorm, decoded, invNorms, columnVectorGraphDeep1BDims)
					}
					elapsed := time.Since(start)
					b.StopTimer()
					benchSink += int64(bestRow) + int64(math.Float32bits(bestScore))
					var decodeScoreNanos int64
					if b.N > 0 {
						decodeScoreNanos = elapsed.Nanoseconds() / int64(b.N)
					}
					reportDecodeAndScoreMetrics(b, elapsed, warm.RawBytes, warm.StoredBytes, warm.TransformRawBytes, decodeScoreNanos)
					b.ReportMetric(float64(warm.TransformRawBytes)/float64(granuleRows), "transform_raw_B/entry")
					b.ReportMetric(float64(warm.MetadataBytes)/float64(granuleRows), "metadata_B/entry")
					b.ReportMetric(float64(warm.StoredBytes)/float64(warm.TransformRawBytes), "stored_transform_raw_ratio")
					b.ReportMetric(float64(warm.TransformNanos)/1e6, "warm_transform_ms")
					b.ReportMetric(float64(warm.TransposeShuffleNanos)/1e6, "warm_layout_ms")
					b.ReportMetric(float64(warm.CompressionNanos)/1e6, "warm_compress_ms")
					b.ReportMetric(maxAbsError, "max_abs_error")
					b.ReportMetric(maxCosineError, "max_cosine_error")
					b.ReportMetric(meanCosineError, "mean_cosine_error")
					if warm.ActualCodec != warm.RequestedCodec {
						b.ReportMetric(1, "fallback_"+metricToken(warm.ActualCodec.name))
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
	return columnVectorGraphDeep1BParseCompressions(tb, "COLUMN_VECTOR_DEEP1B_COMPRESSIONS", []Compression{CompressionNone, CompressionZSTD})
}

func columnVectorGraphDeep1BCompressionCases(tb testing.TB) []columnVectorGraphDeep1BCompressionCase {
	tb.Helper()
	vectorCompressions := columnVectorGraphDeep1BCompressions(tb)
	rawAdjacency := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_ADJACENCY_COMPRESSIONS"))
	if rawAdjacency == "" {
		out := make([]columnVectorGraphDeep1BCompressionCase, 0, len(vectorCompressions))
		for _, vectorCompression := range vectorCompressions {
			out = append(out, columnVectorGraphDeep1BCompressionCase{
				name:                 vectorCompression.String(),
				vectorCompression:    vectorCompression,
				adjacencyCompression: CompressionNone,
			})
		}
		return out
	}
	adjacencyCompressions := columnVectorGraphDeep1BParseCompressions(tb, "COLUMN_VECTOR_DEEP1B_ADJACENCY_COMPRESSIONS", nil)
	out := make([]columnVectorGraphDeep1BCompressionCase, 0, len(vectorCompressions)*len(adjacencyCompressions))
	for _, vectorCompression := range vectorCompressions {
		for _, adjacencyCompression := range adjacencyCompressions {
			out = append(out, columnVectorGraphDeep1BCompressionCase{
				name:                 fmt.Sprintf("vec_%s_adj_%s", vectorCompression, adjacencyCompression),
				vectorCompression:    vectorCompression,
				adjacencyCompression: adjacencyCompression,
			})
		}
	}
	return out
}

func columnVectorGraphDeep1BParseCompressions(tb testing.TB, envName string, defaultCompressions []Compression) []Compression {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv(envName))
	if raw == "" {
		if len(defaultCompressions) == 0 {
			tb.Fatalf("%s selected no compression variants", envName)
		}
		return append([]Compression(nil), defaultCompressions...)
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
			tb.Fatalf("unknown %s value %q", envName, part)
		}
	}
	if len(out) == 0 {
		tb.Fatalf("%s selected no compression variants", envName)
	}
	return out
}

func buildColumnVectorGraphDeep1BFixture(tb testing.TB, data columnVectorGraphDeep1BDataPaths, rows int, vectorCompression Compression, adjacencyCompression Compression) *columnVectorGraphDeep1BFixture {
	return buildColumnVectorGraphDeep1BFixtureWithPhaseTimer(tb, nil, data, rows, vectorCompression, adjacencyCompression)
}

func buildColumnVectorGraphDeep1BFixtureWithPhaseTimer(tb testing.TB, phaseTimer *testing.B, data columnVectorGraphDeep1BDataPaths, rows int, vectorCompression Compression, adjacencyCompression Compression) *columnVectorGraphDeep1BFixture {
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
	opts := columnVectorGraphPersistedOptionsWithAdjacencyCompression(rows, columnVectorGraphDeep1BDims, columnVectorGraphDeep1BBlockRows, vectorCompression, adjacencyCompression)
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

func columnVectorGraphDeep1BEnvInt(tb testing.TB, name string, fallback int) int {
	tb.Helper()
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		tb.Fatalf("%s=%q is not an integer: %v", name, raw, err)
	}
	return value
}

func columnVectorGraphDeep1BSequentialRows(rows int) []int {
	out := make([]int, rows)
	for i := range out {
		out[i] = i
	}
	return out
}

type columnVectorGraphDeep1BNeighbor struct {
	row   int
	score float64
}

type columnVectorGraphDeep1BJZIPTransformKind uint8

const (
	columnVectorGraphDeep1BJZIPTransformCartesianRaw columnVectorGraphDeep1BJZIPTransformKind = iota + 1
	columnVectorGraphDeep1BJZIPTransformCartesianTranspose
	columnVectorGraphDeep1BJZIPTransformCartesianByteShuffle
	columnVectorGraphDeep1BJZIPTransformSpherical
	columnVectorGraphDeep1BJZIPTransformSphericalCenterDelta
	columnVectorGraphDeep1BJZIPTransformSphericalPrevDelta
	columnVectorGraphDeep1BJZIPTransformHouseholderCartesian
)

type columnVectorGraphDeep1BJZIPTransform struct {
	name string
	kind columnVectorGraphDeep1BJZIPTransformKind
}

type columnVectorGraphDeep1BJZIPByteCodecKind uint8

const (
	columnVectorGraphDeep1BJZIPByteCodecRaw columnVectorGraphDeep1BJZIPByteCodecKind = iota
	columnVectorGraphDeep1BJZIPByteCodecSnappy
	columnVectorGraphDeep1BJZIPByteCodecLZ4
	columnVectorGraphDeep1BJZIPByteCodecZSTDFast
	columnVectorGraphDeep1BJZIPByteCodecZSTDDefault
	columnVectorGraphDeep1BJZIPByteCodecZSTDBetter
)

type columnVectorGraphDeep1BJZIPByteCodec struct {
	name string
	kind columnVectorGraphDeep1BJZIPByteCodecKind
}

type columnVectorGraphDeep1BJZIPCodec struct {
	r2                 []float64
	angles             []float32
	values             []float32
	metadata           []float32
	centroid           []float64
	householder        []float64
	shuffled           []byte
	compressed         []byte
	decodedShuffle     []byte
	decodedValues      []float32
	decodedVectors     []float32
	zstdDefaultEncoder *zstd.Encoder
	zstdBetterEncoder  *zstd.Encoder
}

type columnVectorGraphDeep1BJZIPEncoded struct {
	Rows                  int
	Dims                  int
	ValuesPerRow          int
	RawBytes              int
	TransformRawBytes     int
	MetadataBytes         int
	StoredBytes           int
	TransformNanos        int64
	TransposeShuffleNanos int64
	CompressionNanos      int64
	Transform             columnVectorGraphDeep1BJZIPTransform
	RequestedCodec        columnVectorGraphDeep1BJZIPByteCodec
	ActualCodec           columnVectorGraphDeep1BJZIPByteCodec
	Payload               []byte
	Metadata              []float32
}

type columnVectorGraphDeep1BNeighborHeap []columnVectorGraphDeep1BNeighbor

func (h columnVectorGraphDeep1BNeighborHeap) Len() int {
	return len(h)
}

func (h columnVectorGraphDeep1BNeighborHeap) Less(i int, j int) bool {
	if h[i].score == h[j].score {
		return h[i].row > h[j].row
	}
	return h[i].score < h[j].score
}

func (h columnVectorGraphDeep1BNeighborHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *columnVectorGraphDeep1BNeighborHeap) Push(x interface{}) {
	*h = append(*h, x.(columnVectorGraphDeep1BNeighbor))
}

func (h *columnVectorGraphDeep1BNeighborHeap) Pop() interface{} {
	old := *h
	n := len(old)
	out := old[n-1]
	*h = old[:n-1]
	return out
}

const columnVectorGraphDeep1BJZIPHeaderBytes = 16

func columnVectorGraphDeep1BJZIPTransforms(tb testing.TB) []columnVectorGraphDeep1BJZIPTransform {
	tb.Helper()
	all := []columnVectorGraphDeep1BJZIPTransform{
		{name: "cartesian_raw", kind: columnVectorGraphDeep1BJZIPTransformCartesianRaw},
		{name: "cartesian_transpose", kind: columnVectorGraphDeep1BJZIPTransformCartesianTranspose},
		{name: "cartesian_byte_shuffle", kind: columnVectorGraphDeep1BJZIPTransformCartesianByteShuffle},
		{name: "spherical", kind: columnVectorGraphDeep1BJZIPTransformSpherical},
		{name: "spherical_center_delta", kind: columnVectorGraphDeep1BJZIPTransformSphericalCenterDelta},
		{name: "spherical_prev_delta", kind: columnVectorGraphDeep1BJZIPTransformSphericalPrevDelta},
		{name: "householder_cartesian", kind: columnVectorGraphDeep1BJZIPTransformHouseholderCartesian},
	}
	raw := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_JZIP_TRANSFORMS"))
	if raw == "" || strings.EqualFold(raw, "all") {
		return all
	}
	byName := make(map[string]columnVectorGraphDeep1BJZIPTransform, len(all))
	for _, transform := range all {
		byName[transform.name] = transform
	}
	byName["cartesian_transpose_byte_shuffle"] = byName["cartesian_byte_shuffle"]
	parts := strings.Split(raw, ",")
	out := make([]columnVectorGraphDeep1BJZIPTransform, 0, len(parts))
	for _, part := range parts {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" || name == "skip" {
			continue
		}
		transform, ok := byName[name]
		if !ok {
			tb.Fatalf("unknown COLUMN_VECTOR_DEEP1B_JZIP_TRANSFORMS value %q", part)
		}
		out = append(out, transform)
	}
	if len(out) == 0 {
		tb.Fatal("COLUMN_VECTOR_DEEP1B_JZIP_TRANSFORMS selected no transform variants")
	}
	return out
}

func columnVectorGraphDeep1BJZIPByteCodecs(tb testing.TB) []columnVectorGraphDeep1BJZIPByteCodec {
	tb.Helper()
	all := []columnVectorGraphDeep1BJZIPByteCodec{
		{name: "raw", kind: columnVectorGraphDeep1BJZIPByteCodecRaw},
		{name: "snappy", kind: columnVectorGraphDeep1BJZIPByteCodecSnappy},
		{name: "lz4", kind: columnVectorGraphDeep1BJZIPByteCodecLZ4},
		{name: "zstd_fast", kind: columnVectorGraphDeep1BJZIPByteCodecZSTDFast},
		{name: "zstd_default", kind: columnVectorGraphDeep1BJZIPByteCodecZSTDDefault},
		{name: "zstd_better", kind: columnVectorGraphDeep1BJZIPByteCodecZSTDBetter},
	}
	raw := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_JZIP_CODECS"))
	if raw == "" || strings.EqualFold(raw, "all") {
		return all
	}
	byName := make(map[string]columnVectorGraphDeep1BJZIPByteCodec, len(all))
	for _, codec := range all {
		byName[codec.name] = codec
	}
	parts := strings.Split(raw, ",")
	out := make([]columnVectorGraphDeep1BJZIPByteCodec, 0, len(parts))
	for _, part := range parts {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" || name == "skip" {
			continue
		}
		codec, ok := byName[name]
		if !ok {
			tb.Fatalf("unknown COLUMN_VECTOR_DEEP1B_JZIP_CODECS value %q", part)
		}
		out = append(out, codec)
	}
	if len(out) == 0 {
		tb.Fatal("COLUMN_VECTOR_DEEP1B_JZIP_CODECS selected no codec variants")
	}
	return out
}

func (c *columnVectorGraphDeep1BJZIPCodec) Encode(vectors []float32, dims int, transform columnVectorGraphDeep1BJZIPTransform, byteCodec columnVectorGraphDeep1BJZIPByteCodec) (columnVectorGraphDeep1BJZIPEncoded, error) {
	if dims < 2 {
		return columnVectorGraphDeep1BJZIPEncoded{}, fmt.Errorf("Deep1B JZIP dims=%d, need at least 2", dims)
	}
	if len(vectors)%dims != 0 {
		return columnVectorGraphDeep1BJZIPEncoded{}, fmt.Errorf("Deep1B JZIP vector values=%d not divisible by dims=%d", len(vectors), dims)
	}
	rows := len(vectors) / dims
	if rows == 0 {
		return columnVectorGraphDeep1BJZIPEncoded{}, errors.New("Deep1B JZIP empty vector block")
	}
	transformStart := time.Now()
	values, valuesPerRow, metadata, err := c.transformValues(vectors, rows, dims, transform)
	if err != nil {
		return columnVectorGraphDeep1BJZIPEncoded{}, err
	}
	transformNanos := time.Since(transformStart).Nanoseconds()

	shuffleStart := time.Now()
	switch transform.kind {
	case columnVectorGraphDeep1BJZIPTransformCartesianRaw:
		c.shuffled = columnVectorGraphDeep1BFloat32RowMajorBytes(values, c.shuffled)
	case columnVectorGraphDeep1BJZIPTransformCartesianTranspose:
		c.shuffled = columnVectorGraphDeep1BTransposeFloat32Bytes(values, rows, valuesPerRow, c.shuffled)
	default:
		c.shuffled = columnVectorGraphDeep1BTransposeByteShuffle(values, rows, valuesPerRow, c.shuffled)
	}
	shuffleNanos := time.Since(shuffleStart).Nanoseconds()

	payload, actualCodec, compressionNanos, err := c.compressBytes(c.shuffled, byteCodec)
	if err != nil {
		return columnVectorGraphDeep1BJZIPEncoded{}, err
	}
	transformRawBytes := len(values) * 4
	metadataBytes := columnVectorGraphDeep1BJZIPHeaderBytes + len(metadata)*4
	return columnVectorGraphDeep1BJZIPEncoded{
		Rows:                  rows,
		Dims:                  dims,
		ValuesPerRow:          valuesPerRow,
		RawBytes:              len(vectors) * 4,
		TransformRawBytes:     transformRawBytes,
		MetadataBytes:         metadataBytes,
		StoredBytes:           metadataBytes + len(payload),
		TransformNanos:        transformNanos,
		TransposeShuffleNanos: shuffleNanos,
		CompressionNanos:      compressionNanos,
		Transform:             transform,
		RequestedCodec:        byteCodec,
		ActualCodec:           actualCodec,
		Payload:               payload,
		Metadata:              metadata,
	}, nil
}

func (c *columnVectorGraphDeep1BJZIPCodec) Decode(encoded columnVectorGraphDeep1BJZIPEncoded) ([]float32, int64, error) {
	start := time.Now()
	raw, err := c.decompressBytes(encoded.Payload, encoded.TransformRawBytes, encoded.ActualCodec)
	if err != nil {
		return nil, 0, err
	}
	var values []float32
	switch encoded.Transform.kind {
	case columnVectorGraphDeep1BJZIPTransformCartesianRaw:
		values, err = columnVectorGraphDeep1BFloat32FromRowMajorBytes(raw, c.decodedValues)
	case columnVectorGraphDeep1BJZIPTransformCartesianTranspose:
		values, err = columnVectorGraphDeep1BUntransposeFloat32Bytes(raw, encoded.Rows, encoded.ValuesPerRow, c.decodedValues)
	default:
		values, err = columnVectorGraphDeep1BUnshuffleTranspose(raw, encoded.Rows, encoded.ValuesPerRow, c.decodedValues)
	}
	if err != nil {
		return nil, 0, err
	}
	c.decodedValues = values
	var out []float32
	switch encoded.Transform.kind {
	case columnVectorGraphDeep1BJZIPTransformCartesianRaw, columnVectorGraphDeep1BJZIPTransformCartesianTranspose, columnVectorGraphDeep1BJZIPTransformCartesianByteShuffle:
		out = c.decodedValues
	case columnVectorGraphDeep1BJZIPTransformSpherical:
		out = c.sphericalToCartesian(c.decodedValues, encoded.Rows, encoded.Dims)
	case columnVectorGraphDeep1BJZIPTransformSphericalCenterDelta:
		if len(encoded.Metadata) != encoded.ValuesPerRow {
			return nil, 0, fmt.Errorf("Deep1B JZIP center metadata values=%d want=%d", len(encoded.Metadata), encoded.ValuesPerRow)
		}
		c.restoreSphericalCenterDelta(c.decodedValues, encoded.Metadata, encoded.Rows, encoded.ValuesPerRow)
		out = c.sphericalToCartesian(c.decodedValues, encoded.Rows, encoded.Dims)
	case columnVectorGraphDeep1BJZIPTransformSphericalPrevDelta:
		c.restoreSphericalPrevDelta(c.decodedValues, encoded.Rows, encoded.ValuesPerRow)
		out = c.sphericalToCartesian(c.decodedValues, encoded.Rows, encoded.Dims)
	case columnVectorGraphDeep1BJZIPTransformHouseholderCartesian:
		if len(encoded.Metadata) != encoded.Dims {
			return nil, 0, fmt.Errorf("Deep1B JZIP householder metadata values=%d want=%d", len(encoded.Metadata), encoded.Dims)
		}
		out = c.inverseHouseholderCartesian(c.decodedValues, encoded.Metadata, encoded.Rows, encoded.Dims)
	default:
		return nil, 0, fmt.Errorf("Deep1B JZIP unsupported transform %d", encoded.Transform.kind)
	}
	return out, time.Since(start).Nanoseconds(), nil
}

func (c *columnVectorGraphDeep1BJZIPCodec) transformValues(vectors []float32, rows int, dims int, transform columnVectorGraphDeep1BJZIPTransform) ([]float32, int, []float32, error) {
	switch transform.kind {
	case columnVectorGraphDeep1BJZIPTransformCartesianRaw, columnVectorGraphDeep1BJZIPTransformCartesianTranspose, columnVectorGraphDeep1BJZIPTransformCartesianByteShuffle:
		c.metadata = c.metadata[:0]
		return vectors, dims, c.metadata, nil
	case columnVectorGraphDeep1BJZIPTransformSpherical:
		angles := c.cartesianToSpherical(vectors, rows, dims)
		c.metadata = c.metadata[:0]
		return angles, dims - 1, c.metadata, nil
	case columnVectorGraphDeep1BJZIPTransformSphericalCenterDelta:
		angles := c.cartesianToSpherical(vectors, rows, dims)
		values, metadata := c.sphericalCenterDelta(angles, rows, dims-1)
		return values, dims - 1, metadata, nil
	case columnVectorGraphDeep1BJZIPTransformSphericalPrevDelta:
		angles := c.cartesianToSpherical(vectors, rows, dims)
		values := c.sphericalPrevDelta(angles, rows, dims-1)
		c.metadata = c.metadata[:0]
		return values, dims - 1, c.metadata, nil
	case columnVectorGraphDeep1BJZIPTransformHouseholderCartesian:
		values, metadata := c.householderCartesian(vectors, rows, dims)
		return values, dims, metadata, nil
	default:
		return nil, 0, nil, fmt.Errorf("Deep1B JZIP unsupported transform %d", transform.kind)
	}
}

func (c *columnVectorGraphDeep1BJZIPCodec) cartesianToSpherical(vectors []float32, rows int, dims int) []float32 {
	angleDims := dims - 1
	c.angles = columnVectorGraphDeep1BEnsureFloat32(c.angles, rows*angleDims)
	if cap(c.r2) < dims {
		c.r2 = make([]float64, dims)
	} else {
		c.r2 = c.r2[:dims]
	}
	for row := 0; row < rows; row++ {
		vectorBase := row * dims
		angleBase := row * angleDims
		var suffix float64
		for j := dims - 1; j >= 0; j-- {
			value := float64(vectors[vectorBase+j])
			suffix += value * value
			c.r2[j] = suffix
		}
		for j := 0; j < dims-2; j++ {
			denom := math.Sqrt(c.r2[j])
			if denom == 0 {
				c.angles[angleBase+j] = 0
				continue
			}
			c.angles[angleBase+j] = float32(math.Acos(columnVectorGraphDeep1BClamp(float64(vectors[vectorBase+j])/denom, -1, 1)))
		}
		c.angles[angleBase+angleDims-1] = float32(math.Atan2(float64(vectors[vectorBase+dims-1]), float64(vectors[vectorBase+dims-2])))
	}
	return c.angles
}

func (c *columnVectorGraphDeep1BJZIPCodec) sphericalCenterDelta(angles []float32, rows int, angleDims int) ([]float32, []float32) {
	c.values = columnVectorGraphDeep1BEnsureFloat32(c.values, len(angles))
	c.metadata = columnVectorGraphDeep1BEnsureFloat32(c.metadata, angleDims)
	for j := 0; j < angleDims-1; j++ {
		var sum float64
		for row := 0; row < rows; row++ {
			sum += float64(angles[row*angleDims+j])
		}
		c.metadata[j] = float32(sum / float64(rows))
	}
	var sinSum float64
	var cosSum float64
	finalAngle := angleDims - 1
	for row := 0; row < rows; row++ {
		angle := float64(angles[row*angleDims+finalAngle])
		sinSum += math.Sin(angle)
		cosSum += math.Cos(angle)
	}
	c.metadata[finalAngle] = float32(math.Atan2(sinSum, cosSum))
	for row := 0; row < rows; row++ {
		base := row * angleDims
		for j := 0; j < angleDims-1; j++ {
			c.values[base+j] = angles[base+j] - c.metadata[j]
		}
		c.values[base+finalAngle] = float32(columnVectorGraphDeep1BWrapPi(float64(angles[base+finalAngle] - c.metadata[finalAngle])))
	}
	return c.values, c.metadata
}

func (c *columnVectorGraphDeep1BJZIPCodec) sphericalPrevDelta(angles []float32, rows int, angleDims int) []float32 {
	c.values = columnVectorGraphDeep1BEnsureFloat32(c.values, len(angles))
	copy(c.values[:angleDims], angles[:angleDims])
	finalAngle := angleDims - 1
	for row := 1; row < rows; row++ {
		base := row * angleDims
		prev := base - angleDims
		for j := 0; j < angleDims-1; j++ {
			c.values[base+j] = angles[base+j] - angles[prev+j]
		}
		c.values[base+finalAngle] = float32(columnVectorGraphDeep1BWrapPi(float64(angles[base+finalAngle] - angles[prev+finalAngle])))
	}
	return c.values
}

func (c *columnVectorGraphDeep1BJZIPCodec) restoreSphericalCenterDelta(values []float32, center []float32, rows int, angleDims int) {
	finalAngle := angleDims - 1
	for row := 0; row < rows; row++ {
		base := row * angleDims
		for j := 0; j < angleDims-1; j++ {
			values[base+j] += center[j]
		}
		values[base+finalAngle] = float32(columnVectorGraphDeep1BWrapPi(float64(values[base+finalAngle] + center[finalAngle])))
	}
}

func (c *columnVectorGraphDeep1BJZIPCodec) restoreSphericalPrevDelta(values []float32, rows int, angleDims int) {
	finalAngle := angleDims - 1
	for row := 1; row < rows; row++ {
		base := row * angleDims
		prev := base - angleDims
		for j := 0; j < angleDims-1; j++ {
			values[base+j] += values[prev+j]
		}
		values[base+finalAngle] = float32(columnVectorGraphDeep1BWrapPi(float64(values[base+finalAngle] + values[prev+finalAngle])))
	}
}

func (c *columnVectorGraphDeep1BJZIPCodec) sphericalToCartesian(angles []float32, rows int, dims int) []float32 {
	angleDims := dims - 1
	c.decodedVectors = columnVectorGraphDeep1BEnsureFloat32(c.decodedVectors, rows*dims)
	for row := 0; row < rows; row++ {
		angleBase := row * angleDims
		vectorBase := row * dims
		prod := 1.0
		for j := 0; j < dims-2; j++ {
			theta := float64(angles[angleBase+j])
			c.decodedVectors[vectorBase+j] = float32(prod * math.Cos(theta))
			prod *= math.Sin(theta)
		}
		theta := float64(angles[angleBase+angleDims-1])
		c.decodedVectors[vectorBase+dims-2] = float32(prod * math.Cos(theta))
		c.decodedVectors[vectorBase+dims-1] = float32(prod * math.Sin(theta))
	}
	return c.decodedVectors
}

func (c *columnVectorGraphDeep1BJZIPCodec) householderCartesian(vectors []float32, rows int, dims int) ([]float32, []float32) {
	c.metadata = columnVectorGraphDeep1BEnsureFloat32(c.metadata, dims)
	if cap(c.centroid) < dims {
		c.centroid = make([]float64, dims)
	} else {
		c.centroid = c.centroid[:dims]
		clear(c.centroid)
	}
	for row := 0; row < rows; row++ {
		base := row * dims
		for j := 0; j < dims; j++ {
			c.centroid[j] += float64(vectors[base+j])
		}
	}
	var normSquared float64
	for j := 0; j < dims; j++ {
		normSquared += c.centroid[j] * c.centroid[j]
	}
	if normSquared == 0 {
		c.metadata[0] = 1
		for j := 1; j < dims; j++ {
			c.metadata[j] = 0
		}
	} else {
		invNorm := 1 / math.Sqrt(normSquared)
		for j := 0; j < dims; j++ {
			c.metadata[j] = float32(c.centroid[j] * invNorm)
		}
	}
	c.values = columnVectorGraphDeep1BEnsureFloat32(c.values, len(vectors))
	c.applyHouseholder(vectors, c.values, c.metadata, rows, dims)
	return c.values, c.metadata
}

func (c *columnVectorGraphDeep1BJZIPCodec) inverseHouseholderCartesian(values []float32, metadata []float32, rows int, dims int) []float32 {
	c.decodedVectors = columnVectorGraphDeep1BEnsureFloat32(c.decodedVectors, rows*dims)
	c.applyHouseholder(values, c.decodedVectors, metadata, rows, dims)
	return c.decodedVectors
}

func (c *columnVectorGraphDeep1BJZIPCodec) applyHouseholder(input []float32, output []float32, metadata []float32, rows int, dims int) {
	if cap(c.householder) < dims {
		c.householder = make([]float64, dims)
	} else {
		c.householder = c.householder[:dims]
	}
	var normDiffSquared float64
	for j := 0; j < dims; j++ {
		value := float64(metadata[j])
		if j == 0 {
			value--
		}
		c.householder[j] = value
		normDiffSquared += value * value
	}
	if normDiffSquared < 1e-24 {
		copy(output, input)
		return
	}
	invNormDiff := 1 / math.Sqrt(normDiffSquared)
	for j := 0; j < dims; j++ {
		c.householder[j] *= invNormDiff
	}
	for row := 0; row < rows; row++ {
		base := row * dims
		var dot float64
		for j := 0; j < dims; j++ {
			dot += c.householder[j] * float64(input[base+j])
		}
		for j := 0; j < dims; j++ {
			output[base+j] = float32(float64(input[base+j]) - 2*c.householder[j]*dot)
		}
	}
}

func (c *columnVectorGraphDeep1BJZIPCodec) compressBytes(raw []byte, byteCodec columnVectorGraphDeep1BJZIPByteCodec) ([]byte, columnVectorGraphDeep1BJZIPByteCodec, int64, error) {
	switch byteCodec.kind {
	case columnVectorGraphDeep1BJZIPByteCodecRaw:
		return raw, byteCodec, 0, nil
	case columnVectorGraphDeep1BJZIPByteCodecSnappy:
		need := snappy.MaxEncodedLen(len(raw))
		if cap(c.compressed) < need {
			c.compressed = make([]byte, need)
		} else {
			c.compressed = c.compressed[:need]
		}
		start := time.Now()
		out := snappy.Encode(c.compressed, raw)
		return out, byteCodec, time.Since(start).Nanoseconds(), nil
	case columnVectorGraphDeep1BJZIPByteCodecLZ4:
		need := lz4.CompressBlockBound(len(raw))
		if cap(c.compressed) < need {
			c.compressed = make([]byte, need)
		} else {
			c.compressed = c.compressed[:need]
		}
		start := time.Now()
		n, err := lz4.CompressBlock(raw, c.compressed, nil)
		compressionNanos := time.Since(start).Nanoseconds()
		if err != nil {
			return nil, columnVectorGraphDeep1BJZIPByteCodec{}, 0, err
		}
		if n == 0 {
			return raw, columnVectorGraphDeep1BJZIPByteCodec{name: "raw", kind: columnVectorGraphDeep1BJZIPByteCodecRaw}, compressionNanos, nil
		}
		return c.compressed[:n], byteCodec, compressionNanos, nil
	case columnVectorGraphDeep1BJZIPByteCodecZSTDFast:
		enc, err := columnGranuleSharedZSTDEncoder()
		if err != nil {
			return nil, columnVectorGraphDeep1BJZIPByteCodec{}, 0, err
		}
		start := time.Now()
		out := enc.EncodeAll(raw, c.compressed[:0])
		c.compressed = out
		return out, byteCodec, time.Since(start).Nanoseconds(), nil
	case columnVectorGraphDeep1BJZIPByteCodecZSTDDefault:
		enc, err := c.zstdDefault()
		if err != nil {
			return nil, columnVectorGraphDeep1BJZIPByteCodec{}, 0, err
		}
		start := time.Now()
		out := enc.EncodeAll(raw, c.compressed[:0])
		c.compressed = out
		return out, byteCodec, time.Since(start).Nanoseconds(), nil
	case columnVectorGraphDeep1BJZIPByteCodecZSTDBetter:
		enc, err := c.zstdBetter()
		if err != nil {
			return nil, columnVectorGraphDeep1BJZIPByteCodec{}, 0, err
		}
		start := time.Now()
		out := enc.EncodeAll(raw, c.compressed[:0])
		c.compressed = out
		return out, byteCodec, time.Since(start).Nanoseconds(), nil
	default:
		return nil, columnVectorGraphDeep1BJZIPByteCodec{}, 0, fmt.Errorf("Deep1B JZIP unsupported byte codec %d", byteCodec.kind)
	}
}

func (c *columnVectorGraphDeep1BJZIPCodec) decompressBytes(payload []byte, rawBytes int, byteCodec columnVectorGraphDeep1BJZIPByteCodec) ([]byte, error) {
	switch byteCodec.kind {
	case columnVectorGraphDeep1BJZIPByteCodecRaw:
		if len(payload) != rawBytes {
			return nil, fmt.Errorf("Deep1B JZIP raw payload bytes=%d want=%d", len(payload), rawBytes)
		}
		return payload, nil
	case columnVectorGraphDeep1BJZIPByteCodecSnappy:
		decodedLen, err := snappy.DecodedLen(payload)
		if err != nil {
			return nil, err
		}
		if decodedLen != rawBytes {
			return nil, fmt.Errorf("Deep1B JZIP snappy decoded bytes=%d want=%d", decodedLen, rawBytes)
		}
		if cap(c.decodedShuffle) < rawBytes {
			c.decodedShuffle = make([]byte, rawBytes)
		} else {
			c.decodedShuffle = c.decodedShuffle[:rawBytes]
		}
		out, err := snappy.Decode(c.decodedShuffle, payload)
		if err != nil {
			return nil, err
		}
		return out, nil
	case columnVectorGraphDeep1BJZIPByteCodecLZ4:
		if cap(c.decodedShuffle) < rawBytes {
			c.decodedShuffle = make([]byte, rawBytes)
		} else {
			c.decodedShuffle = c.decodedShuffle[:rawBytes]
		}
		n, err := lz4.UncompressBlock(payload, c.decodedShuffle)
		if err != nil {
			return nil, err
		}
		if n != rawBytes {
			return nil, fmt.Errorf("Deep1B JZIP lz4 decoded bytes=%d want=%d", n, rawBytes)
		}
		return c.decodedShuffle, nil
	case columnVectorGraphDeep1BJZIPByteCodecZSTDFast, columnVectorGraphDeep1BJZIPByteCodecZSTDDefault, columnVectorGraphDeep1BJZIPByteCodecZSTDBetter:
		dec, err := columnGranuleSharedZSTDDecoder()
		if err != nil {
			return nil, err
		}
		if cap(c.decodedShuffle) < rawBytes {
			c.decodedShuffle = make([]byte, 0, rawBytes)
		} else {
			c.decodedShuffle = c.decodedShuffle[:0:rawBytes]
		}
		out, err := dec.DecodeAll(payload, c.decodedShuffle)
		if err != nil {
			return nil, err
		}
		if len(out) != rawBytes {
			return nil, fmt.Errorf("Deep1B JZIP zstd decoded bytes=%d want=%d", len(out), rawBytes)
		}
		c.decodedShuffle = out
		return out, nil
	default:
		return nil, fmt.Errorf("Deep1B JZIP unsupported byte codec %d", byteCodec.kind)
	}
}

func (c *columnVectorGraphDeep1BJZIPCodec) zstdDefault() (*zstd.Encoder, error) {
	if c.zstdDefaultEncoder != nil {
		return c.zstdDefaultEncoder, nil
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, err
	}
	c.zstdDefaultEncoder = enc
	return enc, nil
}

func (c *columnVectorGraphDeep1BJZIPCodec) zstdBetter() (*zstd.Encoder, error) {
	if c.zstdBetterEncoder != nil {
		return c.zstdBetterEncoder, nil
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, err
	}
	c.zstdBetterEncoder = enc
	return enc, nil
}

func columnVectorGraphDeep1BFloat32RowMajorBytes(values []float32, dst []byte) []byte {
	rawBytes := len(values) * 4
	if cap(dst) < rawBytes {
		dst = make([]byte, rawBytes)
	} else {
		dst = dst[:rawBytes]
	}
	for i, value := range values {
		bits := math.Float32bits(value)
		base := i * 4
		dst[base] = byte(bits)
		dst[base+1] = byte(bits >> 8)
		dst[base+2] = byte(bits >> 16)
		dst[base+3] = byte(bits >> 24)
	}
	return dst
}

func columnVectorGraphDeep1BFloat32FromRowMajorBytes(raw []byte, dst []float32) ([]float32, error) {
	if len(raw)%4 != 0 {
		return nil, fmt.Errorf("Deep1B JZIP row-major bytes=%d not divisible by 4", len(raw))
	}
	valueCount := len(raw) / 4
	dst = columnVectorGraphDeep1BEnsureFloat32(dst, valueCount)
	for i := 0; i < valueCount; i++ {
		base := i * 4
		bits := uint32(raw[base]) |
			uint32(raw[base+1])<<8 |
			uint32(raw[base+2])<<16 |
			uint32(raw[base+3])<<24
		dst[i] = math.Float32frombits(bits)
	}
	return dst, nil
}

func columnVectorGraphDeep1BTransposeFloat32Bytes(values []float32, rows int, valuesPerRow int, dst []byte) []byte {
	valueCount := rows * valuesPerRow
	rawBytes := valueCount * 4
	if cap(dst) < rawBytes {
		dst = make([]byte, rawBytes)
	} else {
		dst = dst[:rawBytes]
	}
	for j := 0; j < valuesPerRow; j++ {
		for row := 0; row < rows; row++ {
			transposed := j*rows + row
			bits := math.Float32bits(values[row*valuesPerRow+j])
			base := transposed * 4
			dst[base] = byte(bits)
			dst[base+1] = byte(bits >> 8)
			dst[base+2] = byte(bits >> 16)
			dst[base+3] = byte(bits >> 24)
		}
	}
	return dst
}

func columnVectorGraphDeep1BUntransposeFloat32Bytes(raw []byte, rows int, valuesPerRow int, dst []float32) ([]float32, error) {
	valueCount := rows * valuesPerRow
	rawBytes := valueCount * 4
	if len(raw) != rawBytes {
		return nil, fmt.Errorf("Deep1B JZIP transposed bytes=%d want=%d", len(raw), rawBytes)
	}
	dst = columnVectorGraphDeep1BEnsureFloat32(dst, valueCount)
	for j := 0; j < valuesPerRow; j++ {
		for row := 0; row < rows; row++ {
			transposed := j*rows + row
			base := transposed * 4
			bits := uint32(raw[base]) |
				uint32(raw[base+1])<<8 |
				uint32(raw[base+2])<<16 |
				uint32(raw[base+3])<<24
			dst[row*valuesPerRow+j] = math.Float32frombits(bits)
		}
	}
	return dst, nil
}

func columnVectorGraphDeep1BTransposeByteShuffle(values []float32, rows int, valuesPerRow int, dst []byte) []byte {
	valueCount := rows * valuesPerRow
	rawBytes := valueCount * 4
	if cap(dst) < rawBytes {
		dst = make([]byte, rawBytes)
	} else {
		dst = dst[:rawBytes]
	}
	for j := 0; j < valuesPerRow; j++ {
		for row := 0; row < rows; row++ {
			transposed := j*rows + row
			bits := math.Float32bits(values[row*valuesPerRow+j])
			dst[transposed] = byte(bits)
			dst[valueCount+transposed] = byte(bits >> 8)
			dst[2*valueCount+transposed] = byte(bits >> 16)
			dst[3*valueCount+transposed] = byte(bits >> 24)
		}
	}
	return dst
}

func columnVectorGraphDeep1BUnshuffleTranspose(raw []byte, rows int, valuesPerRow int, dst []float32) ([]float32, error) {
	valueCount := rows * valuesPerRow
	rawBytes := valueCount * 4
	if len(raw) != rawBytes {
		return nil, fmt.Errorf("Deep1B JZIP shuffled bytes=%d want=%d", len(raw), rawBytes)
	}
	dst = columnVectorGraphDeep1BEnsureFloat32(dst, valueCount)
	for j := 0; j < valuesPerRow; j++ {
		for row := 0; row < rows; row++ {
			transposed := j*rows + row
			bits := uint32(raw[transposed]) |
				uint32(raw[valueCount+transposed])<<8 |
				uint32(raw[2*valueCount+transposed])<<16 |
				uint32(raw[3*valueCount+transposed])<<24
			dst[row*valuesPerRow+j] = math.Float32frombits(bits)
		}
	}
	return dst, nil
}

func columnVectorGraphDeep1BJZIPErrorMetrics(original []float32, decoded []float32, dims int) (float64, float64, float64) {
	rows := len(original) / dims
	var maxAbsError float64
	for i, got := range decoded {
		errorValue := math.Abs(float64(original[i]) - float64(got))
		if errorValue > maxAbsError {
			maxAbsError = errorValue
		}
	}
	var maxCosineError float64
	var sumCosineError float64
	for row := 0; row < rows; row++ {
		base := row * dims
		var dot float64
		var originalNormSquared float64
		var decodedNormSquared float64
		for j := 0; j < dims; j++ {
			ov := float64(original[base+j])
			dv := float64(decoded[base+j])
			dot += ov * dv
			originalNormSquared += ov * ov
			decodedNormSquared += dv * dv
		}
		cosineError := 1.0
		if originalNormSquared > 0 && decodedNormSquared > 0 {
			cosine := dot / math.Sqrt(originalNormSquared*decodedNormSquared)
			cosineError = math.Abs(1 - columnVectorGraphDeep1BClamp(cosine, -1, 1))
		}
		if cosineError > maxCosineError {
			maxCosineError = cosineError
		}
		sumCosineError += cosineError
	}
	return maxAbsError, maxCosineError, sumCosineError / float64(rows)
}

func columnVectorGraphDeep1BEnsureFloat32(dst []float32, n int) []float32 {
	if cap(dst) < n {
		return make([]float32, n)
	}
	return dst[:n]
}

func columnVectorGraphDeep1BClamp(value float64, minValue float64, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func columnVectorGraphDeep1BWrapPi(value float64) float64 {
	for value <= -math.Pi {
		value += 2 * math.Pi
	}
	for value > math.Pi {
		value -= 2 * math.Pi
	}
	return value
}

func columnVectorGraphDeep1BTopRowsByCosine(tb testing.TB, file *os.File, header columnVectorGraphDeep1BFbinHeader, rows int, query []float32, topK int) []columnVectorGraphDeep1BNeighbor {
	tb.Helper()
	if rows <= 0 || rows > header.Rows {
		tb.Fatalf("top rows scan rows=%d outside fbin rows=%d", rows, header.Rows)
	}
	if topK <= 0 || topK > rows {
		tb.Fatalf("topK=%d outside rows=%d", topK, rows)
	}
	queryInvNorm := columnVectorGraphDeep1BInvNorm(query)
	if queryInvNorm == 0 {
		tb.Fatal("Deep1B query has zero norm")
	}
	h := make(columnVectorGraphDeep1BNeighborHeap, 0, topK)
	heap.Init(&h)
	var rawScratch []byte
	var vectorScratch []float32
	for start := 0; start < rows; start += columnVectorGraphDeep1BBlockRows {
		readRows := min(columnVectorGraphDeep1BBlockRows, rows-start)
		var err error
		rawScratch, vectorScratch, err = columnVectorGraphDeep1BReadFbinVectorsAt(file, header, start, readRows, rawScratch, vectorScratch)
		if err != nil {
			tb.Fatalf("read Deep1B base rows [%d,%d): %v", start, start+readRows, err)
		}
		for localRow := 0; localRow < readRows; localRow++ {
			vector := vectorScratch[localRow*header.Dims : (localRow+1)*header.Dims]
			score := columnVectorGraphDeep1BCosine(query, queryInvNorm, vector)
			neighbor := columnVectorGraphDeep1BNeighbor{row: start + localRow, score: score}
			if h.Len() < topK {
				heap.Push(&h, neighbor)
				continue
			}
			if h[0].score < score || (h[0].score == score && h[0].row > neighbor.row) {
				h[0] = neighbor
				heap.Fix(&h, 0)
			}
		}
	}
	out := append([]columnVectorGraphDeep1BNeighbor(nil), h...)
	sort.Slice(out, func(i int, j int) bool {
		if out[i].score == out[j].score {
			return out[i].row < out[j].row
		}
		return out[i].score > out[j].score
	})
	return out
}

func columnVectorGraphDeep1BCosine(query []float32, queryInvNorm float64, vector []float32) float64 {
	var dot float64
	var normSquared float64
	for i, value := range vector {
		fv := float64(value)
		dot += float64(query[i]) * fv
		normSquared += fv * fv
	}
	if normSquared == 0 {
		return 0
	}
	return dot * queryInvNorm / math.Sqrt(normSquared)
}

func columnVectorGraphDeep1BInvNorm(values []float32) float64 {
	var normSquared float64
	for _, value := range values {
		normSquared += float64(value) * float64(value)
	}
	if normSquared == 0 {
		return 0
	}
	return 1 / math.Sqrt(normSquared)
}

func columnVectorGraphDeep1BInvNorms(vectors []float32, dims int) []float32 {
	rows := len(vectors) / dims
	invNorms := make([]float32, rows)
	for row := 0; row < rows; row++ {
		base := row * dims
		var normSquared float64
		for j := 0; j < dims; j++ {
			value := float64(vectors[base+j])
			normSquared += value * value
		}
		if normSquared > 0 {
			invNorms[row] = float32(1 / math.Sqrt(normSquared))
		}
	}
	return invNorms
}

func columnVectorGraphDeep1BScoreBlock(query []float32, queryInvNorm float32, vectors []float32, invNorms []float32, dims int) (float32, int) {
	rows := len(vectors) / dims
	bestScore := float32(-math.MaxFloat32)
	bestRow := -1
	for row := 0; row < rows; row++ {
		base := row * dims
		dot := columnVectorGraphDotProductFloat32(query, vectors[base:base+dims])
		score := dot * queryInvNorm * invNorms[row]
		if score > bestScore {
			bestScore = score
			bestRow = row
		}
	}
	return bestScore, bestRow
}

func columnVectorGraphDeep1BReadFbinRows(tb testing.TB, file *os.File, header columnVectorGraphDeep1BFbinHeader, rows []int) []float32 {
	tb.Helper()
	values := make([]float32, len(rows)*header.Dims)
	var rawScratch []byte
	var rowScratch []float32
	for i, row := range rows {
		if row < 0 || row >= header.Rows {
			tb.Fatalf("Deep1B row %d outside fbin rows=%d", row, header.Rows)
		}
		var err error
		rawScratch, rowScratch, err = columnVectorGraphDeep1BReadFbinVectorsAt(file, header, row, 1, rawScratch, rowScratch)
		if err != nil {
			tb.Fatalf("read Deep1B row %d: %v", row, err)
		}
		copy(values[i*header.Dims:(i+1)*header.Dims], rowScratch)
	}
	return values
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
