package colgranule

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestColumnVectorGraphDeep1BInt8StorageTournament(t *testing.T) {
	if os.Getenv("COLUMN_VECTOR_DEEP1B_INT8_TOURNAMENT") != "1" {
		t.Skip("set COLUMN_VECTOR_DEEP1B_INT8_TOURNAMENT=1 to run the opt-in Deep1B int8 storage tournament")
	}
	const (
		sourceRows = 1_000_000
		topK       = 10
	)
	granuleRows := columnVectorGraphDeep1BEnvInt(t, "COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS", columnVectorGraphDeep1BBlockRows)
	if granuleRows <= 0 {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d must be positive", granuleRows)
	}
	if granuleRows < topK {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d must be at least %d", granuleRows, topK)
	}
	if granuleRows > sourceRows {
		t.Fatalf("COLUMN_VECTOR_DEEP1B_NEIGHBORHOOD_ROWS=%d exceeds source rows=%d", granuleRows, sourceRows)
	}
	outDir := strings.TrimSpace(os.Getenv("COLUMN_VECTOR_DEEP1B_INT8_TOURNAMENT_OUT"))
	if outDir == "" {
		outDir = filepath.Join(os.TempDir(), "gomap_deep1b_int8_tournament_"+time.Now().Format("20060102_150405"))
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatalf("create tournament output dir: %v", err)
	}

	data := columnVectorGraphDeep1BEnsureData(t, sourceRows)
	baseFile, err := os.Open(data.basePath)
	if err != nil {
		t.Fatalf("open Deep1B base: %v", err)
	}
	defer baseFile.Close()
	query := columnVectorGraphDeep1BReadQuery(t, data.queryPath, data.queryHeader, 0)
	queryInvNorm := float32(columnVectorGraphDeep1BInvNorm(query))
	nearest := columnVectorGraphDeep1BTopRowsByCosine(t, baseFile, data.baseHeader, sourceRows, query, granuleRows)
	nearestRows := make([]int, len(nearest))
	for i, neighbor := range nearest {
		nearestRows[i] = neighbor.row
	}
	vectors := columnVectorGraphDeep1BReadFbinRows(t, baseFile, data.baseHeader, nearestRows)
	invNorms := columnVectorGraphDeep1BInvNorms(vectors, columnVectorGraphDeep1BDims)

	frameCodec := &columnVectorGraphDeep1BJZIPCodec{}
	localVectors, centroid := frameCodec.householderCartesian(vectors, granuleRows, columnVectorGraphDeep1BDims)
	localVectors = append([]float32(nil), localVectors...)
	centroid = append([]float32(nil), centroid...)
	localQuery := make([]float32, columnVectorGraphDeep1BDims)
	frameCodec.applyHouseholder(query, localQuery, centroid, 1, columnVectorGraphDeep1BDims)

	exactScores := make([]float32, granuleRows)
	columnVectorGraphDeep1BScorePrefixInto(query, queryInvNorm, vectors, invNorms, columnVectorGraphDeep1BDims, columnVectorGraphDeep1BDims, exactScores)
	exactTopRows := make([]int, topK)
	exactTopScores := make([]float32, topK)
	columnVectorGraphDeep1BTopKFromScores(exactScores, topK, exactTopRows, exactTopScores)

	report := columnVectorGraphDeep1BInt8TournamentReport{
		GeneratedAt:         time.Now().UTC().Format(time.RFC3339),
		OutputDir:           outDir,
		SourceRows:          sourceRows,
		GranuleRows:         granuleRows,
		Dims:                columnVectorGraphDeep1BDims,
		QueryIndex:          0,
		BasePath:            data.basePath,
		QueryPath:           data.queryPath,
		RawInt8Bytes:        granuleRows * columnVectorGraphDeep1BDims,
		ExactTopRows:        append([]int(nil), exactTopRows...),
		ExactTopScores:      append([]float32(nil), exactTopScores...),
		ClickHouseAvailable: columnVectorGraphDeep1BClickHouseAvailable(),
	}

	orders := columnVectorGraphDeep1BInt8TournamentOrders(nearestRows, localVectors, exactScores, invNorms, columnVectorGraphDeep1BDims)
	for _, order := range orders {
		layout := columnVectorGraphDeep1BInt8TournamentLayout(t, order.localVectors, granuleRows, columnVectorGraphDeep1BDims)
		stats := columnVectorGraphDeep1BComputeInt8TournamentStats(layout.raw, granuleRows, columnVectorGraphDeep1BDims)
		if err := columnVectorGraphDeep1BWriteInt8TournamentArtifacts(outDir, order, layout, centroid, localQuery, stats); err != nil {
			t.Fatalf("write tournament artifacts for order %s: %v", order.name, err)
		}
		results := columnVectorGraphDeep1BRunInt8NativeTournament(t, order.name, layout.raw, granuleRows, columnVectorGraphDeep1BDims)
		report.Orders = append(report.Orders, columnVectorGraphDeep1BInt8TournamentOrderReport{
			Name:        order.name,
			Stats:       stats,
			Scales:      append([]float32(nil), layout.scales...),
			BestRows:    append([]int(nil), order.rowIDs[:min(10, len(order.rowIDs))]...),
			ResultCount: len(results),
		})
		report.Results = append(report.Results, results...)
	}
	sort.Slice(report.Results, func(i, j int) bool {
		if report.Results[i].Order == report.Results[j].Order {
			if report.Results[i].StoredBytes == report.Results[j].StoredBytes {
				return report.Results[i].Name < report.Results[j].Name
			}
			return report.Results[i].StoredBytes < report.Results[j].StoredBytes
		}
		return report.Results[i].Order < report.Results[j].Order
	})

	if err := columnVectorGraphDeep1BWriteClickHouseFixtures(outDir, orders, granuleRows, columnVectorGraphDeep1BDims); err != nil {
		t.Fatalf("write ClickHouse fixtures: %v", err)
	}
	if err := columnVectorGraphDeep1BWriteJSON(filepath.Join(outDir, "results.json"), report); err != nil {
		t.Fatalf("write results.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "report.md"), []byte(columnVectorGraphDeep1BRenderInt8TournamentMarkdown(report)), 0o644); err != nil {
		t.Fatalf("write report.md: %v", err)
	}
	t.Logf("Deep1B int8 storage tournament report: %s", filepath.Join(outDir, "report.md"))
}

type columnVectorGraphDeep1BInt8TournamentReport struct {
	GeneratedAt         string                                             `json:"generated_at"`
	OutputDir           string                                             `json:"output_dir"`
	SourceRows          int                                                `json:"source_rows"`
	GranuleRows         int                                                `json:"granule_rows"`
	Dims                int                                                `json:"dims"`
	QueryIndex          int                                                `json:"query_index"`
	BasePath            string                                             `json:"base_path"`
	QueryPath           string                                             `json:"query_path"`
	RawInt8Bytes        int                                                `json:"raw_int8_bytes"`
	ExactTopRows        []int                                              `json:"exact_top_rows"`
	ExactTopScores      []float32                                          `json:"exact_top_scores"`
	ClickHouseAvailable bool                                               `json:"clickhouse_available"`
	Orders              []columnVectorGraphDeep1BInt8TournamentOrderReport `json:"orders"`
	Results             []columnVectorGraphDeep1BInt8TournamentResult      `json:"results"`
}

type columnVectorGraphDeep1BInt8TournamentOrderReport struct {
	Name        string                                     `json:"name"`
	Stats       columnVectorGraphDeep1BInt8TournamentStats `json:"stats"`
	Scales      []float32                                  `json:"scales"`
	BestRows    []int                                      `json:"first_row_ids"`
	ResultCount int                                        `json:"result_count"`
}

type columnVectorGraphDeep1BInt8TournamentOrder struct {
	name         string
	rowIDs       []int
	localVectors []float32
	exactScores  []float32
	invNorms     []float32
}

type columnVectorGraphDeep1BInt8TournamentStats struct {
	ZeroFraction       float64 `json:"zero_fraction"`
	NegativeFraction   float64 `json:"negative_fraction"`
	MeanAbs            float64 `json:"mean_abs"`
	EntropyBitsPerByte float64 `json:"entropy_bits_per_byte"`
	MinDistinctPerDim  int     `json:"min_distinct_per_dim"`
	MaxDistinctPerDim  int     `json:"max_distinct_per_dim"`
	MeanDistinctPerDim float64 `json:"mean_distinct_per_dim"`
}

type columnVectorGraphDeep1BInt8TournamentResult struct {
	Order          string  `json:"order"`
	Name           string  `json:"name"`
	Transform      string  `json:"transform"`
	Compression    string  `json:"compression"`
	TransformBytes int     `json:"transform_bytes"`
	StoredBytes    int     `json:"stored_bytes"`
	RawBytes       int     `json:"raw_bytes"`
	BytesPerVector float64 `json:"bytes_per_vector"`
	RatioVsRawInt8 float64 `json:"ratio_vs_raw_int8"`
	RawInt8Ratio   float64 `json:"raw_int8_ratio"`
	EncodeNanos    int64   `json:"encode_nanos"`
	Notes          string  `json:"notes,omitempty"`
}

func columnVectorGraphDeep1BInt8TournamentOrders(rowIDs []int, localVectors []float32, exactScores []float32, invNorms []float32, dims int) []columnVectorGraphDeep1BInt8TournamentOrder {
	rows := len(rowIDs)
	nearestPerm := make([]int, rows)
	for i := range nearestPerm {
		nearestPerm[i] = i
	}
	rowIDPerm := append([]int(nil), nearestPerm...)
	sort.Slice(rowIDPerm, func(i, j int) bool {
		return rowIDs[rowIDPerm[i]] < rowIDs[rowIDPerm[j]]
	})
	localPerm := append([]int(nil), nearestPerm...)
	sort.Slice(localPerm, func(i, j int) bool {
		a := localPerm[i]
		b := localPerm[j]
		for _, dim := range []int{1, 2, 3, 4, 0} {
			av := localVectors[a*dims+dim]
			bv := localVectors[b*dims+dim]
			if av != bv {
				return av < bv
			}
		}
		return rowIDs[a] < rowIDs[b]
	})
	perms := []struct {
		name string
		perm []int
	}{
		{name: "nearest_ranked", perm: nearestPerm},
		{name: "row_id", perm: rowIDPerm},
		{name: "local_coord_sort", perm: localPerm},
	}
	orders := make([]columnVectorGraphDeep1BInt8TournamentOrder, 0, len(perms))
	for _, item := range perms {
		orders = append(orders, columnVectorGraphDeep1BInt8TournamentOrder{
			name:         item.name,
			rowIDs:       columnVectorGraphDeep1BPermuteInts(rowIDs, item.perm),
			localVectors: columnVectorGraphDeep1BPermuteFloat32Rows(localVectors, item.perm, dims),
			exactScores:  columnVectorGraphDeep1BPermuteFloat32s(exactScores, item.perm),
			invNorms:     columnVectorGraphDeep1BPermuteFloat32s(invNorms, item.perm),
		})
	}
	return orders
}

func columnVectorGraphDeep1BInt8TournamentLayout(tb testing.TB, localVectors []float32, rows int, dims int) columnVectorGraphDeep1BGranuleNativeLayout {
	tb.Helper()
	for _, layout := range columnVectorGraphDeep1BGranuleNativeLayouts(localVectors, rows, dims, dims) {
		if layout.kind == columnVectorGraphDeep1BGranuleNativeInt8Columns {
			return layout
		}
	}
	tb.Fatalf("missing full int8 granule-native layout")
	return columnVectorGraphDeep1BGranuleNativeLayout{}
}

func columnVectorGraphDeep1BInt8TournamentLayoutPanic(localVectors []float32, rows int, dims int) columnVectorGraphDeep1BGranuleNativeLayout {
	for _, layout := range columnVectorGraphDeep1BGranuleNativeLayouts(localVectors, rows, dims, dims) {
		if layout.kind == columnVectorGraphDeep1BGranuleNativeInt8Columns {
			return layout
		}
	}
	panic("missing full int8 granule-native layout")
}

func columnVectorGraphDeep1BRunInt8NativeTournament(tb testing.TB, order string, raw []byte, rows int, dims int) []columnVectorGraphDeep1BInt8TournamentResult {
	tb.Helper()
	rawBytes := len(raw)
	var results []columnVectorGraphDeep1BInt8TournamentResult
	add := func(transform string, payload []byte, codecs []string, notes string) {
		for _, codecName := range codecs {
			stored, actualCodec, nanos := columnVectorGraphDeep1BInt8TournamentCompress(tb, payload, codecName)
			name := transform + "/" + actualCodec
			results = append(results, columnVectorGraphDeep1BInt8TournamentResult{
				Order:          order,
				Name:           name,
				Transform:      transform,
				Compression:    actualCodec,
				TransformBytes: len(payload),
				StoredBytes:    stored,
				RawBytes:       rawBytes,
				BytesPerVector: float64(stored) / float64(rows),
				RatioVsRawInt8: float64(rawBytes) / float64(stored),
				RawInt8Ratio:   float64(stored) / float64(rawBytes),
				EncodeNanos:    nanos,
				Notes:          notes,
			})
		}
	}
	byteCodecs := []string{"raw", "snappy", "lz4", "zstd_fast", "zstd_better"}
	zstdCodecs := []string{"raw", "zstd_fast", "zstd_better"}
	varintCodecs := []string{"raw", "zstd_fast", "zstd_better"}
	biased := columnVectorGraphDeep1BInt8Biased(raw)
	rowMajor := columnVectorGraphDeep1BDimMajorToRowMajor(biased, rows, dims)
	zigzag := columnVectorGraphDeep1BInt8ZigZagBytes(raw)
	signMag := columnVectorGraphDeep1BInt8SignMagnitudeBytes(raw)
	add("i8_dim_major", raw, byteCodecs, "current native int8 payload")
	add("u8_biased_dim_major", biased, byteCodecs, "int8+128 for unsigned numeric codecs")
	add("u8_biased_row_major", rowMajor, zstdCodecs, "row-major UInt8 table shape")
	add("zigzag_u8_dim_major", zigzag, zstdCodecs, "signed int8 mapped by zigzag")
	add("signmag_u8_dim_major", signMag, zstdCodecs, "sign bit in high bit, abs(value) in low bits")
	add("sign_plane_magnitude", columnVectorGraphDeep1BInt8SignPlaneMagnitude(raw), zstdCodecs, "bitpacked signs followed by magnitudes")
	add("bitplane_i8_dim_major", columnVectorGraphDeep1BInt8BitPlanes(raw, rows, dims), zstdCodecs, "8 bit planes per dimension")
	add("bitplane_zigzag_dim_major", columnVectorGraphDeep1BInt8BitPlanes(zigzag, rows, dims), zstdCodecs, "zigzag bytes then 8 bit planes per dimension")
	add("bitplane_i8_byte_rle", columnVectorGraphDeep1BByteRunLength(columnVectorGraphDeep1BInt8BitPlanes(raw, rows, dims)), zstdCodecs, "byte-run RLE over bit-plane stream")
	add("palette_block64", columnVectorGraphDeep1BInt8PaletteBlocks(raw, rows, dims, 64), zstdCodecs, "per-dimension 64-row palette blocks with raw fallback")
	add("palette_block256", columnVectorGraphDeep1BInt8PaletteBlocks(raw, rows, dims, 256), zstdCodecs, "per-dimension 256-row palette blocks with raw fallback")

	varintPayloads := []struct {
		name    string
		payload []byte
		notes   string
	}{
		{name: "varint_i8_values", payload: columnVectorGraphDeep1BVarintI8Values(raw), notes: "binary.AppendVarint per signed int8 value"},
		{name: "uvarint_zigzag_i8_values", payload: columnVectorGraphDeep1BUvarintZigZagI8Values(raw), notes: "uvarint of zigzag(int8) per value"},
		{name: "varint_delta_i16_by_dim", payload: columnVectorGraphDeep1BVarintDeltaByDim(raw, rows, dims), notes: "signed varint first value plus exact int16 deltas per dimension"},
		{name: "uvarint_delta_zigzag_by_dim", payload: columnVectorGraphDeep1BUvarintDeltaZigZagByDim(raw, rows, dims), notes: "uvarint zigzag of exact int16 deltas per dimension"},
		{name: "uvarint_delta_wrap_by_dim", payload: columnVectorGraphDeep1BUvarintDeltaWrapByDim(raw, rows, dims), notes: "first byte plus modulo-256 uvarint deltas per dimension"},
		{name: "varint_double_delta_by_dim", payload: columnVectorGraphDeep1BVarintDoubleDeltaByDim(raw, rows, dims), notes: "signed varint first value, first delta, then delta-of-delta per dimension"},
		{name: "uvarint_xor_prev_by_dim", payload: columnVectorGraphDeep1BUvarintXORPrevByDim(raw, rows, dims), notes: "first byte plus uvarint(curr XOR prev) per dimension"},
		{name: "varint_rle_i8_values", payload: columnVectorGraphDeep1BVarintRLEI8Values(raw), notes: "signed value varint plus run-length uvarint"},
		{name: "uvarint_zero_rle_literals", payload: columnVectorGraphDeep1BUvarintZeroRLELiterals(raw), notes: "0 marker plus zero-run length; nonzero literals use zigzag+1"},
		{name: "uvarint_for_block64", payload: columnVectorGraphDeep1BUvarintFrameOfReference(raw, rows, dims, 64), notes: "per-dimension 64-row min plus uvarint(value-min)"},
		{name: "uvarint_for_block256", payload: columnVectorGraphDeep1BUvarintFrameOfReference(raw, rows, dims, 256), notes: "per-dimension 256-row min plus uvarint(value-min)"},
	}
	for _, item := range varintPayloads {
		add(item.name, item.payload, varintCodecs, item.notes)
	}
	return results
}

func columnVectorGraphDeep1BInt8TournamentCompress(tb testing.TB, payload []byte, codecName string) (int, string, int64) {
	tb.Helper()
	codecs := map[string]columnVectorGraphDeep1BJZIPByteCodec{
		"raw":         {name: "raw", kind: columnVectorGraphDeep1BJZIPByteCodecRaw},
		"snappy":      {name: "snappy", kind: columnVectorGraphDeep1BJZIPByteCodecSnappy},
		"lz4":         {name: "lz4", kind: columnVectorGraphDeep1BJZIPByteCodecLZ4},
		"zstd_fast":   {name: "zstd_fast", kind: columnVectorGraphDeep1BJZIPByteCodecZSTDFast},
		"zstd_better": {name: "zstd_better", kind: columnVectorGraphDeep1BJZIPByteCodecZSTDBetter},
	}
	byteCodec, ok := codecs[codecName]
	if !ok {
		tb.Fatalf("unknown tournament codec %q", codecName)
	}
	codec := &columnVectorGraphDeep1BJZIPCodec{}
	out, actual, nanos, err := codec.compressBytes(payload, byteCodec)
	if err != nil {
		tb.Fatalf("compress tournament payload %s: %v", codecName, err)
	}
	return len(out), actual.name, nanos
}

func columnVectorGraphDeep1BWriteInt8TournamentArtifacts(outDir string, order columnVectorGraphDeep1BInt8TournamentOrder, layout columnVectorGraphDeep1BGranuleNativeLayout, centroid []float32, localQuery []float32, stats columnVectorGraphDeep1BInt8TournamentStats) error {
	orderDir := filepath.Join(outDir, order.name)
	if err := os.MkdirAll(orderDir, 0o755); err != nil {
		return err
	}
	u8DimMajor := columnVectorGraphDeep1BInt8Biased(layout.raw)
	u8RowMajor := columnVectorGraphDeep1BDimMajorToRowMajor(u8DimMajor, layout.rows, layout.prefixDims)
	files := map[string][]byte{
		"matrix_i8_dim_major.bin": layout.raw,
		"matrix_u8_dim_major.bin": u8DimMajor,
		"matrix_u8_row_major.bin": u8RowMajor,
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(orderDir, name), data, 0o644); err != nil {
			return err
		}
	}
	if err := columnVectorGraphDeep1BWriteFloat32LE(filepath.Join(orderDir, "centroid.f32"), centroid); err != nil {
		return err
	}
	if err := columnVectorGraphDeep1BWriteFloat32LE(filepath.Join(orderDir, "local_query.f32"), localQuery); err != nil {
		return err
	}
	if err := columnVectorGraphDeep1BWriteFloat32LE(filepath.Join(orderDir, "scales.f32"), layout.scales); err != nil {
		return err
	}
	if err := columnVectorGraphDeep1BWriteFloat32LE(filepath.Join(orderDir, "exact_scores.f32"), order.exactScores); err != nil {
		return err
	}
	if err := columnVectorGraphDeep1BWriteFloat32LE(filepath.Join(orderDir, "inv_norms.f32"), order.invNorms); err != nil {
		return err
	}
	if err := columnVectorGraphDeep1BWriteUint32LE(filepath.Join(orderDir, "row_ids.u32"), order.rowIDs); err != nil {
		return err
	}
	metadata := map[string]any{
		"order":               order.name,
		"rows":                layout.rows,
		"dims":                layout.prefixDims,
		"raw_int8_bytes":      len(layout.raw),
		"metadata_bytes":      layout.metadataBytes,
		"matrix_i8_dim_major": "matrix_i8_dim_major.bin",
		"matrix_u8_dim_major": "matrix_u8_dim_major.bin",
		"matrix_u8_row_major": "matrix_u8_row_major.bin",
		"centroid":            "centroid.f32",
		"local_query":         "local_query.f32",
		"scales":              "scales.f32",
		"row_ids":             "row_ids.u32",
		"exact_scores":        "exact_scores.f32",
		"inv_norms":           "inv_norms.f32",
		"stats":               stats,
	}
	return columnVectorGraphDeep1BWriteJSON(filepath.Join(orderDir, "metadata.json"), metadata)
}

func columnVectorGraphDeep1BWriteClickHouseFixtures(outDir string, orders []columnVectorGraphDeep1BInt8TournamentOrder, rows int, dims int) error {
	chDir := filepath.Join(outDir, "clickhouse")
	if err := os.MkdirAll(chDir, 0o755); err != nil {
		return err
	}
	var schema strings.Builder
	schema.WriteString("-- Generated Deep1B int8 tournament ClickHouse fixtures.\n")
	schema.WriteString("-- The local machine used by this test did not need ClickHouse installed; run these against clickhouse-local/client when available.\n\n")
	for _, codec := range []struct {
		name  string
		codec string
	}{
		{name: "default", codec: ""},
		{name: "none", codec: " CODEC(NONE)"},
		{name: "lz4", codec: " CODEC(LZ4)"},
		{name: "zstd", codec: " CODEC(ZSTD(1))"},
		{name: "t64_zstd", codec: " CODEC(T64, ZSTD(1))"},
		{name: "delta_zstd", codec: " CODEC(Delta, ZSTD(1))"},
		{name: "double_delta_zstd", codec: " CODEC(DoubleDelta, ZSTD(1))"},
	} {
		table := "deep1b_int8_wide_" + codec.name
		schema.WriteString("CREATE TABLE IF NOT EXISTS ")
		schema.WriteString(table)
		schema.WriteString(" (order_name LowCardinality(String), row_idx UInt32, source_row UInt32")
		for j := 0; j < dims; j++ {
			schema.WriteString(fmt.Sprintf(", d%d UInt8%s", j, codec.codec))
		}
		schema.WriteString(") ENGINE = MergeTree ORDER BY (order_name, row_idx);\n")
	}
	schema.WriteString("\nCREATE TABLE IF NOT EXISTS deep1b_int8_dim_blob_zstd (order_name LowCardinality(String), dim UInt16, values String CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY (order_name, dim);\n")
	schema.WriteString("CREATE TABLE IF NOT EXISTS deep1b_int8_granule_blob_zstd (order_name LowCardinality(String), layout LowCardinality(String), values String CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY (order_name, layout);\n")
	if err := os.WriteFile(filepath.Join(chDir, "schema.sql"), []byte(schema.String()), 0o644); err != nil {
		return err
	}
	var commands strings.Builder
	commands.WriteString("#!/bin/sh\nset -eu\n\n")
	commands.WriteString("clickhouse-client --multiquery < schema.sql\n")
	for _, order := range orders {
		commands.WriteString(fmt.Sprintf("clickhouse-client --query \"INSERT INTO deep1b_int8_wide_zstd FORMAT TSV\" < %s_wide.tsv\n", order.name))
		commands.WriteString(fmt.Sprintf("clickhouse-client --query \"INSERT INTO deep1b_int8_dim_blob_zstd SELECT order_name, dim, unhex(values_hex) FROM input('order_name String, dim UInt16, values_hex String') FORMAT TSV\" < %s_dim_blob.tsv\n", order.name))
		commands.WriteString(fmt.Sprintf("clickhouse-client --query \"INSERT INTO deep1b_int8_granule_blob_zstd SELECT order_name, layout, unhex(values_hex) FROM input('order_name String, layout String, values_hex String') FORMAT TSV\" < %s_granule_blob.tsv\n", order.name))
	}
	if err := os.WriteFile(filepath.Join(chDir, "load_zstd_examples.sh"), []byte(commands.String()), 0o755); err != nil {
		return err
	}
	for _, order := range orders {
		layout := columnVectorGraphDeep1BInt8TournamentLayoutPanic(order.localVectors, rows, dims)
		u8 := columnVectorGraphDeep1BInt8Biased(layout.raw)
		if err := os.WriteFile(filepath.Join(chDir, order.name+"_wide.tsv"), []byte(columnVectorGraphDeep1BClickHouseWideTSV(order.name, order.rowIDs, u8, rows, dims)), 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(chDir, order.name+"_dim_blob.tsv"), []byte(columnVectorGraphDeep1BClickHouseDimBlobTSV(order.name, u8, rows, dims)), 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(chDir, order.name+"_granule_blob.tsv"), []byte(columnVectorGraphDeep1BClickHouseGranuleBlobTSV(order.name, u8, rows, dims)), 0o644); err != nil {
			return err
		}
	}
	readme := "ClickHouse fixture files for the Deep1B int8 storage tournament.\n\n" +
		"Layouts:\n" +
		"- *_wide.tsv: one row per vector with d0..d95 UInt8 columns.\n" +
		"- *_dim_blob.tsv: one row per dimension with a hex-encoded 8192-byte dim-major column.\n" +
		"- *_granule_blob.tsv: one row per order/layout with a hex-encoded full granule byte stream.\n\n" +
		"The blob TSVs are hex-encoded for transport. load_zstd_examples.sh uses unhex() so ClickHouse stores raw bytes before applying the table codec.\n\n" +
		"Start with schema.sql, then adapt load_zstd_examples.sh for the codec/table under test.\n"
	return os.WriteFile(filepath.Join(chDir, "README.md"), []byte(readme), 0o644)
}

func columnVectorGraphDeep1BClickHouseWideTSV(orderName string, rowIDs []int, u8 []byte, rows int, dims int) string {
	var b strings.Builder
	for row := 0; row < rows; row++ {
		b.WriteString(orderName)
		b.WriteByte('\t')
		b.WriteString(fmt.Sprint(row))
		b.WriteByte('\t')
		b.WriteString(fmt.Sprint(rowIDs[row]))
		for j := 0; j < dims; j++ {
			b.WriteByte('\t')
			b.WriteString(fmt.Sprint(u8[j*rows+row]))
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func columnVectorGraphDeep1BClickHouseDimBlobTSV(orderName string, u8 []byte, rows int, dims int) string {
	var b strings.Builder
	for j := 0; j < dims; j++ {
		b.WriteString(orderName)
		b.WriteByte('\t')
		b.WriteString(fmt.Sprint(j))
		b.WriteByte('\t')
		b.WriteString(hex.EncodeToString(u8[j*rows : (j+1)*rows]))
		b.WriteByte('\n')
	}
	return b.String()
}

func columnVectorGraphDeep1BClickHouseGranuleBlobTSV(orderName string, u8 []byte, rows int, dims int) string {
	return orderName + "\tu8_dim_major_hex\t" + hex.EncodeToString(u8) + "\n" +
		orderName + "\tu8_row_major_hex\t" + hex.EncodeToString(columnVectorGraphDeep1BDimMajorToRowMajor(u8, rows, dims)) + "\n"
}

func columnVectorGraphDeep1BRenderInt8TournamentMarkdown(report columnVectorGraphDeep1BInt8TournamentReport) string {
	var b strings.Builder
	b.WriteString("# Deep1B Granule-Native Int8 Storage Tournament\n\n")
	b.WriteString(fmt.Sprintf("- generated_at: `%s`\n", report.GeneratedAt))
	b.WriteString(fmt.Sprintf("- source_rows: `%d`\n", report.SourceRows))
	b.WriteString(fmt.Sprintf("- granule_rows: `%d`\n", report.GranuleRows))
	b.WriteString(fmt.Sprintf("- dims: `%d`\n", report.Dims))
	b.WriteString(fmt.Sprintf("- raw_int8_B/vector: `%.2f`\n", float64(report.RawInt8Bytes)/float64(report.GranuleRows)))
	b.WriteString(fmt.Sprintf("- clickhouse_available_on_runner: `%t`\n\n", report.ClickHouseAvailable))
	if !report.ClickHouseAvailable {
		b.WriteString("ClickHouse binaries were not available on this runner, so the harness generated ClickHouse fixture TSV/SQL files but did not execute ClickHouse part-size measurements.\n\n")
	}
	for _, order := range report.Orders {
		b.WriteString(fmt.Sprintf("## Order: %s\n\n", order.Name))
		b.WriteString(fmt.Sprintf("- zero_fraction: `%.4f`\n", order.Stats.ZeroFraction))
		b.WriteString(fmt.Sprintf("- negative_fraction: `%.4f`\n", order.Stats.NegativeFraction))
		b.WriteString(fmt.Sprintf("- mean_abs: `%.3f`\n", order.Stats.MeanAbs))
		b.WriteString(fmt.Sprintf("- entropy_bits_per_byte: `%.3f`\n", order.Stats.EntropyBitsPerByte))
		b.WriteString(fmt.Sprintf("- distinct_per_dim: min `%d`, mean `%.1f`, max `%d`\n\n", order.Stats.MinDistinctPerDim, order.Stats.MeanDistinctPerDim, order.Stats.MaxDistinctPerDim))
		b.WriteString("| Candidate | Transform B/vector | Stored B/vector | Ratio vs 96B | Compression | Notes |\n")
		b.WriteString("| --- | ---: | ---: | ---: | --- | --- |\n")
		var rows []columnVectorGraphDeep1BInt8TournamentResult
		for _, result := range report.Results {
			if result.Order == order.Name {
				rows = append(rows, result)
			}
		}
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].StoredBytes == rows[j].StoredBytes {
				return rows[i].Name < rows[j].Name
			}
			return rows[i].StoredBytes < rows[j].StoredBytes
		})
		for _, result := range rows {
			b.WriteString(fmt.Sprintf("| `%s` | %.2f | %.2f | %.3fx | `%s` | %s |\n",
				result.Name,
				float64(result.TransformBytes)/float64(report.GranuleRows),
				result.BytesPerVector,
				result.RatioVsRawInt8,
				result.Compression,
				result.Notes,
			))
		}
		b.WriteByte('\n')
	}
	b.WriteString("## ClickHouse Fixtures\n\n")
	b.WriteString("Generated under `clickhouse/`:\n\n")
	b.WriteString("- `schema.sql`\n")
	b.WriteString("- `load_zstd_examples.sh`\n")
	b.WriteString("- `*_wide.tsv`\n")
	b.WriteString("- `*_dim_blob.tsv`\n")
	b.WriteString("- `*_granule_blob.tsv`\n")
	b.WriteString("\nBlob TSVs carry hex text and `load_zstd_examples.sh` decodes them with `unhex()` before ClickHouse stores the `String` payload.\n")
	return b.String()
}

func columnVectorGraphDeep1BComputeInt8TournamentStats(raw []byte, rows int, dims int) columnVectorGraphDeep1BInt8TournamentStats {
	var counts [256]int
	var zeroCount int
	var negativeCount int
	var absSum float64
	for _, value := range raw {
		counts[value]++
		v := int8(value)
		if v == 0 {
			zeroCount++
		}
		if v < 0 {
			negativeCount++
		}
		absSum += math.Abs(float64(v))
	}
	var entropy float64
	for _, count := range counts {
		if count == 0 {
			continue
		}
		p := float64(count) / float64(len(raw))
		entropy -= p * math.Log2(p)
	}
	minDistinct := math.MaxInt
	maxDistinct := 0
	var distinctSum int
	for j := 0; j < dims; j++ {
		var seen [256]bool
		distinct := 0
		for row := 0; row < rows; row++ {
			value := raw[j*rows+row]
			if !seen[value] {
				seen[value] = true
				distinct++
			}
		}
		minDistinct = min(minDistinct, distinct)
		maxDistinct = max(maxDistinct, distinct)
		distinctSum += distinct
	}
	return columnVectorGraphDeep1BInt8TournamentStats{
		ZeroFraction:       float64(zeroCount) / float64(len(raw)),
		NegativeFraction:   float64(negativeCount) / float64(len(raw)),
		MeanAbs:            absSum / float64(len(raw)),
		EntropyBitsPerByte: entropy,
		MinDistinctPerDim:  minDistinct,
		MaxDistinctPerDim:  maxDistinct,
		MeanDistinctPerDim: float64(distinctSum) / float64(dims),
	}
}

func columnVectorGraphDeep1BInt8Biased(raw []byte) []byte {
	out := make([]byte, len(raw))
	for i, value := range raw {
		out[i] = byte(int16(int8(value)) + 128)
	}
	return out
}

func columnVectorGraphDeep1BInt8ZigZagBytes(raw []byte) []byte {
	out := make([]byte, len(raw))
	for i, value := range raw {
		out[i] = byte(columnVectorGraphDeep1BZigZagInt(int(int8(value))))
	}
	return out
}

func columnVectorGraphDeep1BInt8SignMagnitudeBytes(raw []byte) []byte {
	out := make([]byte, len(raw))
	for i, value := range raw {
		v := int8(value)
		if v < 0 {
			out[i] = 0x80 | byte(-int16(v))
		} else {
			out[i] = byte(v)
		}
	}
	return out
}

func columnVectorGraphDeep1BInt8SignPlaneMagnitude(raw []byte) []byte {
	signBytes := (len(raw) + 7) / 8
	out := make([]byte, signBytes+len(raw))
	magBase := signBytes
	for i, value := range raw {
		v := int8(value)
		if v < 0 {
			out[i/8] |= 1 << uint(i%8)
			out[magBase+i] = byte(-int16(v))
		} else {
			out[magBase+i] = byte(v)
		}
	}
	return out
}

func columnVectorGraphDeep1BInt8BitPlanes(raw []byte, rows int, dims int) []byte {
	planeBytesPerDim := (rows + 7) / 8
	out := make([]byte, dims*8*planeBytesPerDim)
	for j := 0; j < dims; j++ {
		colBase := j * rows
		for bit := 0; bit < 8; bit++ {
			dstBase := (j*8 + bit) * planeBytesPerDim
			for row := 0; row < rows; row++ {
				if (raw[colBase+row]>>uint(bit))&1 != 0 {
					out[dstBase+row/8] |= 1 << uint(row%8)
				}
			}
		}
	}
	return out
}

func columnVectorGraphDeep1BByteRunLength(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); {
		value := raw[i]
		run := 1
		for i+run < len(raw) && raw[i+run] == value {
			run++
		}
		out = append(out, value)
		out = binary.AppendUvarint(out, uint64(run))
		i += run
	}
	return out
}

func columnVectorGraphDeep1BInt8PaletteBlocks(raw []byte, rows int, dims int, blockRows int) []byte {
	out := make([]byte, 0, len(raw))
	for j := 0; j < dims; j++ {
		colBase := j * rows
		for start := 0; start < rows; start += blockRows {
			end := min(start+blockRows, rows)
			block := raw[colBase+start : colBase+end]
			encoded := columnVectorGraphDeep1BEncodePaletteBlock(block)
			if len(encoded) < len(block) {
				out = append(out, encoded...)
			} else {
				out = append(out, 0)
				out = append(out, block...)
			}
		}
	}
	return out
}

func columnVectorGraphDeep1BEncodePaletteBlock(block []byte) []byte {
	index := make(map[byte]int, len(block))
	dict := make([]byte, 0, 16)
	for _, value := range block {
		if _, ok := index[value]; ok {
			continue
		}
		index[value] = len(dict)
		dict = append(dict, value)
	}
	var width int
	switch {
	case len(dict) <= 1:
		width = 0
	case len(dict) <= 2:
		width = 1
	case len(dict) <= 4:
		width = 2
	case len(dict) <= 16:
		width = 4
	default:
		return append([]byte{0}, block...)
	}
	out := []byte{byte(width), byte(len(dict))}
	out = append(out, dict...)
	if width == 0 {
		return out
	}
	var current byte
	var used int
	for _, value := range block {
		code := byte(index[value])
		current |= code << uint(used)
		used += width
		if used >= 8 {
			out = append(out, current)
			if used > 8 {
				current = code >> uint(width-(used-8))
			} else {
				current = 0
			}
			used -= 8
		}
	}
	if used > 0 {
		out = append(out, current)
	}
	return out
}

func columnVectorGraphDeep1BVarintI8Values(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	for _, value := range raw {
		out = binary.AppendVarint(out, int64(int8(value)))
	}
	return out
}

func columnVectorGraphDeep1BUvarintZigZagI8Values(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	for _, value := range raw {
		out = binary.AppendUvarint(out, uint64(columnVectorGraphDeep1BZigZagInt(int(int8(value)))))
	}
	return out
}

func columnVectorGraphDeep1BVarintDeltaByDim(raw []byte, rows int, dims int) []byte {
	out := make([]byte, 0, len(raw))
	for j := 0; j < dims; j++ {
		colBase := j * rows
		prev := int(int8(raw[colBase]))
		out = binary.AppendVarint(out, int64(prev))
		for row := 1; row < rows; row++ {
			current := int(int8(raw[colBase+row]))
			out = binary.AppendVarint(out, int64(current-prev))
			prev = current
		}
	}
	return out
}

func columnVectorGraphDeep1BUvarintDeltaZigZagByDim(raw []byte, rows int, dims int) []byte {
	out := make([]byte, 0, len(raw))
	for j := 0; j < dims; j++ {
		colBase := j * rows
		prev := int(int8(raw[colBase]))
		out = binary.AppendUvarint(out, uint64(columnVectorGraphDeep1BZigZagInt(prev)))
		for row := 1; row < rows; row++ {
			current := int(int8(raw[colBase+row]))
			out = binary.AppendUvarint(out, uint64(columnVectorGraphDeep1BZigZagInt(current-prev)))
			prev = current
		}
	}
	return out
}

func columnVectorGraphDeep1BUvarintDeltaWrapByDim(raw []byte, rows int, dims int) []byte {
	out := make([]byte, 0, len(raw))
	for j := 0; j < dims; j++ {
		colBase := j * rows
		prev := raw[colBase]
		out = append(out, prev)
		for row := 1; row < rows; row++ {
			current := raw[colBase+row]
			out = binary.AppendUvarint(out, uint64(byte(current-prev)))
			prev = current
		}
	}
	return out
}

func columnVectorGraphDeep1BVarintDoubleDeltaByDim(raw []byte, rows int, dims int) []byte {
	out := make([]byte, 0, len(raw))
	for j := 0; j < dims; j++ {
		colBase := j * rows
		prev := int(int8(raw[colBase]))
		out = binary.AppendVarint(out, int64(prev))
		prevDelta := 0
		for row := 1; row < rows; row++ {
			current := int(int8(raw[colBase+row]))
			delta := current - prev
			out = binary.AppendVarint(out, int64(delta-prevDelta))
			prev = current
			prevDelta = delta
		}
	}
	return out
}

func columnVectorGraphDeep1BUvarintXORPrevByDim(raw []byte, rows int, dims int) []byte {
	out := make([]byte, 0, len(raw))
	for j := 0; j < dims; j++ {
		colBase := j * rows
		prev := raw[colBase]
		out = append(out, prev)
		for row := 1; row < rows; row++ {
			current := raw[colBase+row]
			out = binary.AppendUvarint(out, uint64(current^prev))
			prev = current
		}
	}
	return out
}

func columnVectorGraphDeep1BVarintRLEI8Values(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); {
		value := raw[i]
		run := 1
		for i+run < len(raw) && raw[i+run] == value {
			run++
		}
		out = binary.AppendVarint(out, int64(int8(value)))
		out = binary.AppendUvarint(out, uint64(run))
		i += run
	}
	return out
}

func columnVectorGraphDeep1BUvarintZeroRLELiterals(raw []byte) []byte {
	out := make([]byte, 0, len(raw))
	for i := 0; i < len(raw); {
		if raw[i] == 0 {
			run := 1
			for i+run < len(raw) && raw[i+run] == 0 {
				run++
			}
			out = binary.AppendUvarint(out, 0)
			out = binary.AppendUvarint(out, uint64(run))
			i += run
			continue
		}
		code := columnVectorGraphDeep1BZigZagInt(int(int8(raw[i]))) + 1
		out = binary.AppendUvarint(out, uint64(code))
		i++
	}
	return out
}

func columnVectorGraphDeep1BUvarintFrameOfReference(raw []byte, rows int, dims int, blockRows int) []byte {
	out := make([]byte, 0, len(raw))
	for j := 0; j < dims; j++ {
		colBase := j * rows
		for start := 0; start < rows; start += blockRows {
			end := min(start+blockRows, rows)
			minValue := int(int8(raw[colBase+start]))
			for row := start + 1; row < end; row++ {
				minValue = min(minValue, int(int8(raw[colBase+row])))
			}
			out = binary.AppendVarint(out, int64(minValue))
			for row := start; row < end; row++ {
				out = binary.AppendUvarint(out, uint64(int(int8(raw[colBase+row]))-minValue))
			}
		}
	}
	return out
}

func columnVectorGraphDeep1BZigZagInt(value int) int {
	if value < 0 {
		return -value*2 - 1
	}
	return value * 2
}

func columnVectorGraphDeep1BDimMajorToRowMajor(raw []byte, rows int, dims int) []byte {
	out := make([]byte, len(raw))
	for j := 0; j < dims; j++ {
		for row := 0; row < rows; row++ {
			out[row*dims+j] = raw[j*rows+row]
		}
	}
	return out
}

func columnVectorGraphDeep1BPermuteFloat32Rows(values []float32, perm []int, dims int) []float32 {
	out := make([]float32, len(perm)*dims)
	for dst, src := range perm {
		copy(out[dst*dims:(dst+1)*dims], values[src*dims:(src+1)*dims])
	}
	return out
}

func columnVectorGraphDeep1BPermuteFloat32s(values []float32, perm []int) []float32 {
	out := make([]float32, len(perm))
	for dst, src := range perm {
		out[dst] = values[src]
	}
	return out
}

func columnVectorGraphDeep1BPermuteInts(values []int, perm []int) []int {
	out := make([]int, len(perm))
	for dst, src := range perm {
		out[dst] = values[src]
	}
	return out
}

func columnVectorGraphDeep1BWriteFloat32LE(path string, values []float32) error {
	raw := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(raw[i*4:], math.Float32bits(value))
	}
	return os.WriteFile(path, raw, 0o644)
}

func columnVectorGraphDeep1BWriteUint32LE(path string, values []int) error {
	raw := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(raw[i*4:], uint32(value))
	}
	return os.WriteFile(path, raw, 0o644)
}

func columnVectorGraphDeep1BWriteJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	return os.WriteFile(path, raw, 0o644)
}

func columnVectorGraphDeep1BClickHouseAvailable() bool {
	for _, name := range []string{"clickhouse", "clickhouse-local", "clickhouse-client"} {
		if _, err := exec.LookPath(name); err == nil {
			return true
		}
	}
	return false
}
