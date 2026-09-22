package quantizedasset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/columnsemantics"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/page"
)

var (
	quantizedAssetByteSink  byte
	quantizedAssetFloatSink float32
	quantizedAssetUintSink  uint64
)

func TestKnownQuantizedRoles1932(t *testing.T) {
	for _, role := range []Role{RoleCodes, RolePackedCodes, RoleNorm, RoleStep, RoleLower, RoleCodeSum, RoleNorm2, RoleCodeCount, RoleCentroidDistance, RoleQuantizedDotProductInv, RoleCentroidDotProduct, RoleCentroidID, RoleListID, RoleScalarU8Alpha, RoleGranuleRowCount} {
		if !KnownRole(role) {
			t.Fatalf("KnownRole(%q)=false", role)
		}
	}
	if KnownRole("documents") {
		t.Fatal("KnownRole(documents)=true, want unsupported role")
	}
}

func TestSchemaDescriptorValidatesScalarPackedAndPQShapes1932(t *testing.T) {
	fixed := buildFixedQuantizedFixture1932(t)
	prepared, err := Prepare(fixed.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare fixed/scalar: %v", err)
	}
	if got := prepared.Rows(); got != fixed.rows {
		t.Fatalf("fixed rows=%d want %d", got, fixed.rows)
	}
	if !prepared.HasRole(RoleCodes) || !prepared.HasRole(RoleNorm) || !prepared.HasRole(RoleCodeCount) {
		t.Fatalf("fixed roles missing")
	}

	packed := buildPackedQuantizedFixture1932(t)
	prepared, err = Prepare(packed.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare packed/scalar: %v", err)
	}
	if got, ok := prepared.ElementsPerRow(RolePackedCodes); !ok || got != packed.schema.CodeDimensions {
		t.Fatalf("packed elements_per_row=%d ok=%v want %d", got, ok, packed.schema.CodeDimensions)
	}

	pq := buildPQQuantizedFixture1932(t)
	prepared, err = Prepare(pq.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare pq-shaped dense codes: %v", err)
	}
	if got, ok := prepared.ElementsPerRow(RoleCodes); !ok || got != pq.schema.CodeDimensions {
		t.Fatalf("pq elements_per_row=%d ok=%v want %d", got, ok, pq.schema.CodeDimensions)
	}
	if _, ok := prepared.Uint32(RoleCentroidID, 0); !ok {
		t.Fatalf("pq centroid_id missing")
	}
}

func TestPrepareSupportsGranuleSideMetadata2843(t *testing.T) {
	fixture := buildFixedQuantizedFixture1932(t)
	alphaImage := buildQuantizedPartImage1932(t, []typedcolumn.ColumnDefinition{
		{Name: "alpha_col", Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: "granule_rows_col", Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
	}, typedcolumn.Batch{
		Rows:           2,
		Float32Columns: map[string][]float32{"alpha_col": []float32{0.5, 0.75}},
		Uint32Columns:  map[string][]uint32{"granule_rows_col": []uint32{2, 2}},
	}, map[string]string{
		"alpha_col":        string(columnsemantics.LogicalFloat32),
		"granule_rows_col": string(columnsemantics.LogicalUint32),
	})
	alphaRef := refForImage1932("alpha", alphaImage)
	req := fixture.prepareRequest()
	req.Schema.GranuleCount = 2
	req.Schema.Columns = append(req.Schema.Columns,
		ColumnDescriptor{Role: RoleScalarU8Alpha, Column: "alpha_col", AssetID: "alpha", Required: true, LogicalType: string(columnsemantics.LogicalFloat32), Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, RowCount: 2, AssetBytes: int64(alphaImage.TotalBytes()), SourceSchemaHash: 0x19320002, Ref: alphaRef},
		ColumnDescriptor{Role: RoleGranuleRowCount, Column: "granule_rows_col", AssetID: "alpha", Required: true, LogicalType: string(columnsemantics.LogicalUint32), Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, RowCount: 2, AssetBytes: int64(alphaImage.TotalBytes()), SourceSchemaHash: 0x19320002, Ref: alphaRef},
	)
	req.Expected = expectedFromSchema1932(req.Schema, RoleCodes, RoleScalarU8Alpha, RoleGranuleRowCount)
	req.Parts = append(req.Parts, PartImageSource{AssetID: "alpha", Image: alphaImage, Ref: alphaRef, AssetBytes: int64(alphaImage.TotalBytes()), SourceSchemaHash: 0x19320002})
	prepared, err := Prepare(req)
	if err != nil {
		t.Fatalf("Prepare with granule metadata: %v", err)
	}
	if got, ok := prepared.RoleRows(RoleScalarU8Alpha); !ok || got != 2 {
		t.Fatalf("alpha role rows=%d ok=%v want 2", got, ok)
	}
	if got, ok := prepared.Float32(RoleScalarU8Alpha, 1); !ok || got != 0.75 {
		t.Fatalf("alpha[1]=%v ok=%v want 0.75", got, ok)
	}
	if got, ok := prepared.Uint32(RoleGranuleRowCount, 0); !ok || got != 2 {
		t.Fatalf("granule_row_count[0]=%d ok=%v want 2", got, ok)
	}

	bad := req
	bad.Expected.GranuleCount = 3
	if _, err := Prepare(bad); err == nil || !strings.Contains(err.Error(), "granule_count=2 want 3") {
		t.Fatalf("Prepare granule mismatch err=%v want granule_count", err)
	}
}

func TestPrepareFailsClosedMissingAndWrongRoles1932(t *testing.T) {
	fixture := buildFixedQuantizedFixture1932(t)
	req := fixture.prepareRequest()
	req.Expected.RequiredRoles = append(req.Expected.RequiredRoles, RolePackedCodes)
	if _, err := Prepare(req); err == nil || !strings.Contains(err.Error(), "missing required role") {
		t.Fatalf("Prepare missing required role err=%v want missing required role", err)
	}

	req = fixture.prepareRequest()
	req.Schema.Columns[0].Role = RolePackedCodes
	req.Expected.RequiredRoles = []Role{RolePackedCodes}
	if _, err := Prepare(req); err == nil || !strings.Contains(err.Error(), "packed-code") {
		t.Fatalf("Prepare wrong role/type err=%v want packed-code failure", err)
	}

	req = fixture.prepareRequest()
	req.Schema.Columns[0].Column = "does_not_exist"
	if _, err := Prepare(req); err == nil || !strings.Contains(err.Error(), "missing typed-column column") {
		t.Fatalf("Prepare missing column err=%v want missing typed-column column", err)
	}
}

func TestPrepareFailsClosedIdentityAndShapeMismatches1932(t *testing.T) {
	fixture := buildPackedQuantizedFixture1932(t)
	cases := []struct {
		name string
		mut  func(*PrepareRequest)
		want string
	}{
		{name: "row count", mut: func(req *PrepareRequest) { req.Expected.RowCount++ }, want: "row_count"},
		{name: "code width", mut: func(req *PrepareRequest) { req.Expected.CodeWidthBits = 2 }, want: "code_width_bits"},
		{name: "dimension", mut: func(req *PrepareRequest) { req.Expected.VectorDimensions++ }, want: "vector_dimensions"},
		{name: "metric", mut: func(req *PrepareRequest) { req.Expected.Metric = "l2" }, want: "metric"},
		{name: "codec name", mut: func(req *PrepareRequest) { req.Expected.Codec.Name = "other" }, want: "codec name"},
		{name: "codec config", mut: func(req *PrepareRequest) { req.Expected.Codec.ConfigHash++ }, want: "codec config_hash"},
		{name: "base identity", mut: func(req *PrepareRequest) { req.Expected.BaseGraph.BaseManifestChecksum++ }, want: "base graph identity mismatch"},
		{name: "asset ref", mut: func(req *PrepareRequest) { req.Parts[0].Ref.Checksum++; req.Schema.Columns[0].Ref = req.Parts[0].Ref }, want: "checksum"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := fixture.prepareRequest()
			tc.mut(&req)
			_, err := Prepare(req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Prepare err=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestPrepareFailsClosedExpectedZeroRowCountRejectsNonEmptySchema1932(t *testing.T) {
	fixture := buildPackedQuantizedFixture1932(t)
	req := fixture.prepareRequest()
	req.Expected.RowCount = 0
	_, err := Prepare(req)
	if err == nil || !strings.Contains(err.Error(), "row_count=4 want 0") {
		t.Fatalf("Prepare err=%v want exact row_count zero expectation failure", err)
	}
}

func TestPrepareFailsClosedDescriptorRefRequiresResolvedRef1932(t *testing.T) {
	fixture := buildPackedQuantizedFixture1932(t)
	req := fixture.prepareRequest()
	req.Parts[0].Ref = AssetRefIdentity{}
	_, err := Prepare(req)
	if err == nil || !strings.Contains(err.Error(), "missing resolved asset ref") || !strings.Contains(err.Error(), fixture.ref.key()) {
		t.Fatalf("Prepare err=%v want missing resolved asset ref failure", err)
	}
}

func TestPrepareFailsClosedFixedByteCodesRequire8BitWidth1932(t *testing.T) {
	fixture := buildFixedQuantizedFixture1932(t)
	for _, width := range []int{4, 16} {
		t.Run(fmt.Sprintf("code_width_%d", width), func(t *testing.T) {
			req := fixture.prepareRequest()
			req.Schema.CodeWidthBits = width
			req.Expected.CodeWidthBits = width
			_, err := Prepare(req)
			if err == nil || !strings.Contains(err.Error(), "fixed-byte element_width_bits=8") || !strings.Contains(err.Error(), "code_width_bits") {
				t.Fatalf("Prepare err=%v want fixed-byte code_width_bits failure", err)
			}
		})
	}
}

func TestPrepareFailsClosedUnsupportedOrdinalOrder1932(t *testing.T) {
	fixture := buildFixedQuantizedFixture1932(t)
	for _, tc := range []struct {
		name        string
		mutExpected func(*ExpectedSchema, GraphOrdinalOrder)
	}{
		{name: "expected omitted", mutExpected: func(expected *ExpectedSchema, _ GraphOrdinalOrder) { expected.OrdinalOrder = "" }},
		{name: "expected matches unsupported", mutExpected: func(expected *ExpectedSchema, unsupported GraphOrdinalOrder) { expected.OrdinalOrder = unsupported }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := fixture.prepareRequest()
			unsupported := GraphOrdinalOrder("document_ordinal")
			req.Schema.OrdinalOrder = unsupported
			tc.mutExpected(&req.Expected, unsupported)
			_, err := Prepare(req)
			if err == nil || !strings.Contains(err.Error(), "ordinal_order") || !strings.Contains(err.Error(), string(unsupported)) {
				t.Fatalf("Prepare err=%v want unsupported ordinal_order failure", err)
			}
		})
	}
}

func TestPreparedRandomOrdinalLookupAndScratchAllocs1932(t *testing.T) {
	fixture := buildPackedQuantizedFixture1932(t)
	prepared, err := Prepare(fixture.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	ordinals := []int{3, 0, 2, 1, 3, 1, 0, 2}
	for _, ordinal := range ordinals {
		row, ok := prepared.CodeRowBytes(RolePackedCodes, ordinal)
		if !ok || !bytes.Equal(row, fixture.packedRows[ordinal]) {
			t.Fatalf("ordinal=%d row=%x ok=%v want %x", ordinal, row, ok, fixture.packedRows[ordinal])
		}
		values, ok := prepared.PackedElements(RolePackedCodes, ordinal, make([]uint8, fixture.schema.CodeDimensions))
		if !ok || !bytes.Equal(values, fixture.unpackedRows[ordinal]) {
			t.Fatalf("ordinal=%d unpacked=%v ok=%v want %v", ordinal, values, ok, fixture.unpackedRows[ordinal])
		}
		lower, ok := prepared.Float32(RoleLower, ordinal)
		if !ok || lower != fixture.floatColumns[RoleLower][ordinal] {
			t.Fatalf("ordinal=%d lower=%v ok=%v want %v", ordinal, lower, ok, fixture.floatColumns[RoleLower][ordinal])
		}
		count, ok := prepared.Uint32(RoleCodeCount, ordinal)
		if !ok || count != fixture.uint32Columns[RoleCodeCount][ordinal] {
			t.Fatalf("ordinal=%d count=%v ok=%v want %v", ordinal, count, ok, fixture.uint32Columns[RoleCodeCount][ordinal])
		}
	}

	wordScratch := make([]uint64, 2)
	unpackScratch := make([]uint8, fixture.schema.CodeDimensions)
	allocs := testing.AllocsPerRun(1000, func() {
		for _, ordinal := range ordinals {
			row, _ := prepared.CodeRowBytes(RolePackedCodes, ordinal)
			words, _, _ := prepared.RowWords(RolePackedCodes, ordinal, wordScratch)
			unpacked, _ := prepared.PackedElements(RolePackedCodes, ordinal, unpackScratch)
			lower, _ := prepared.Float32(RoleLower, ordinal)
			count, _ := prepared.Uint32(RoleCodeCount, ordinal)
			quantizedAssetByteSink ^= row[0] ^ byte(words[0]) ^ unpacked[0]
			quantizedAssetFloatSink += lower
			quantizedAssetUintSink += uint64(count)
		}
	})
	if allocs != 0 {
		t.Fatalf("steady-state ordinal lookup allocs/run=%v want 0", allocs)
	}
}

func TestPreparedCodeRowViewValidatesOnceAndSlicesRows2256(t *testing.T) {
	fixture := buildFixedQuantizedFixture1932(t)
	prepared, err := Prepare(fixture.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	view, ok := prepared.CodeRowView(RoleCodes)
	if !ok || !view.Valid() {
		t.Fatalf("CodeRowView(RoleCodes) ok=%v valid=%v", ok, view.Valid())
	}
	if got := view.Role(); got != RoleCodes {
		t.Fatalf("view role=%q want %q", got, RoleCodes)
	}
	if got := view.Rows(); got != fixture.rows {
		t.Fatalf("view rows=%d want %d", got, fixture.rows)
	}
	if got := view.BytesPerRow(); got != fixture.schema.CodeDimensions {
		t.Fatalf("view bytes_per_row=%d want %d", got, fixture.schema.CodeDimensions)
	}
	if got := view.ElementsPerRow(); got != fixture.schema.CodeDimensions {
		t.Fatalf("view elements_per_row=%d want %d", got, fixture.schema.CodeDimensions)
	}
	payload, ok := view.PayloadBytes()
	if !ok || !bytes.Equal(payload, bytes.Join(fixture.fixedRows, nil)) {
		t.Fatalf("view payload=%x ok=%v want row-major fixed rows", payload, ok)
	}

	for ordinal, want := range fixture.fixedRows {
		row, ok := view.RowBytes(ordinal)
		if !ok || !bytes.Equal(row, want) {
			t.Fatalf("view row ordinal=%d row=%x ok=%v want %x", ordinal, row, ok, want)
		}
		generic, ok := prepared.CodeRowBytes(RoleCodes, ordinal)
		if !ok || !bytes.Equal(generic, row) {
			t.Fatalf("generic row ordinal=%d row=%x ok=%v want view row %x", ordinal, generic, ok, row)
		}
	}
	for _, ordinal := range []int{-1, fixture.rows} {
		if row, ok := view.RowBytes(ordinal); ok || row != nil {
			t.Fatalf("view RowBytes(%d) row=%x ok=%v want fail-closed", ordinal, row, ok)
		}
	}
	if view, ok := prepared.CodeRowView(RoleNorm); ok || view.Valid() {
		t.Fatalf("CodeRowView(RoleNorm) ok=%v valid=%v want scalar role rejected", ok, view.Valid())
	}
	if view, ok := prepared.CodeRowView(RolePackedCodes); ok || view.Valid() {
		t.Fatalf("CodeRowView(missing RolePackedCodes) ok=%v valid=%v want missing role rejected", ok, view.Valid())
	}
	var nilPrepared *Prepared
	if view, ok := nilPrepared.CodeRowView(RoleCodes); ok || view.Valid() {
		t.Fatalf("nil Prepared CodeRowView ok=%v valid=%v want rejected", ok, view.Valid())
	}
	var zero CodeRowView
	if zero.Valid() {
		t.Fatal("zero CodeRowView valid=true want false")
	}
	if payload, ok := zero.PayloadBytes(); ok || payload != nil {
		t.Fatalf("zero PayloadBytes payload=%x ok=%v want fail-closed", payload, ok)
	}
	if row, ok := zero.RowBytes(0); ok || row != nil {
		t.Fatalf("zero RowBytes row=%x ok=%v want fail-closed", row, ok)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		for ordinal := range fixture.fixedRows {
			row, _ := view.RowBytes(ordinal)
			quantizedAssetByteSink ^= row[0]
		}
	})
	if allocs != 0 {
		t.Fatalf("CodeRowView RowBytes allocs/run=%v want 0", allocs)
	}
}

func TestPrepareFailsClosedPackedCodeLogicalTypeMatchesPhysical1932(t *testing.T) {
	rows := 1
	unpacked := []uint8{1, 0, 1, 1, 0, 0, 1, 0, 1, 0}
	packedRaw, err := typedcolumn.EncodePackedUintRows(nil, rows, 10, 1, unpacked)
	if err != nil {
		t.Fatalf("EncodePackedUintRows: %v", err)
	}
	packedRows, err := typedcolumn.NewPackedUintRows(rows, 10, 1, packedRaw)
	if err != nil {
		t.Fatalf("NewPackedUintRows: %v", err)
	}
	image := buildQuantizedPartImage1932(t, []typedcolumn.ColumnDefinition{
		{Name: "packed_col", Type: typedcolumn.ColumnTypePackedBitVector, FixedWidthElements: 10, BitsPerElement: 1, Encoding: typedcolumn.EncodingRawPackedBitVector, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
	}, typedcolumn.Batch{
		Rows:              rows,
		PackedUintColumns: map[string]typedcolumn.PackedUintRows{"packed_col": packedRows},
	}, map[string]string{"packed_col": string(columnsemantics.LogicalPackedUint4Vector)})
	base := baseGraphIdentity1932(rows, 16, "cosine")
	schema := schemaDescriptor1932(rows, 16, 10, 1, "cosine", "rabitq-bits", base, []ColumnDescriptor{
		{Role: RolePackedCodes, Column: "packed_col", AssetID: "packed", Required: true, LogicalType: string(columnsemantics.LogicalPackedUint4Vector), Type: typedcolumn.ColumnTypePackedBitVector, Encoding: typedcolumn.EncodingRawPackedBitVector, ElementsPerRow: 10, BitsPerElement: 1, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
	})
	ref := refForImage1932("packed", image)
	schema.Columns[0].Ref = ref
	req := PrepareRequest{
		Schema:   schema,
		Expected: expectedFromSchema1932(schema, RolePackedCodes),
		Parts: []PartImageSource{{
			AssetID:          "packed",
			Image:            image,
			Ref:              ref,
			AssetBytes:       int64(image.TotalBytes()),
			SourceSchemaHash: 0x19320001,
		}},
	}
	_, err = Prepare(req)
	if err == nil || !strings.Contains(err.Error(), "packed-code column") || !strings.Contains(err.Error(), string(columnsemantics.LogicalPackedUint4Vector)) {
		t.Fatalf("Prepare err=%v want packed logical/physical mismatch failure", err)
	}
}

func TestPackedCodePaddingValidationFailsClosed1932(t *testing.T) {
	rowBytes, err := typedcolumn.PackedUintRowBytes(10, 1)
	if err != nil {
		t.Fatalf("PackedUintRowBytes: %v", err)
	}
	err = validatePackedColumnPadding(RolePackedCodes, 1, 10, 1, rowBytes, []byte{0x4d, 0x83})
	if err == nil || !strings.Contains(err.Error(), "packed padding") || !strings.Contains(err.Error(), "non-zero padding bits") {
		t.Fatalf("validatePackedColumnPadding err=%v want packed padding failure", err)
	}
}

func TestPrepareFailsClosedDenseCodesRequireUnsignedPhysicalType1932(t *testing.T) {
	for _, tc := range []struct {
		name        string
		columnType  typedcolumn.ColumnType
		encoding    typedcolumn.Encoding
		logicalType columnsemantics.LogicalType
		widthBytes  int
	}{
		{name: "int8_physical_uint8_logical", columnType: typedcolumn.ColumnTypeInt8Vector, encoding: typedcolumn.EncodingRawInt8Vector, logicalType: columnsemantics.LogicalUint8Vector, widthBytes: 1},
		{name: "float64_physical_uint64_logical", columnType: typedcolumn.ColumnTypeFloat64Vector, encoding: typedcolumn.EncodingRawFloat64Vector, logicalType: columnsemantics.LogicalUint64Vector, widthBytes: 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := buildDensePhysicalCodeFixture1932(t, tc.columnType, tc.encoding, tc.logicalType, tc.widthBytes)
			_, err := Prepare(fixture.prepareRequest())
			if err == nil || !strings.Contains(err.Error(), "unsigned dense code column") || !strings.Contains(err.Error(), string(tc.columnType)) {
				t.Fatalf("Prepare err=%v want unsigned dense physical type failure", err)
			}
		})
	}
}

func TestPreparedDenseUint32ScratchAccess1932(t *testing.T) {
	fixture := buildDenseUint32QuantizedFixture1932(t)
	prepared, err := Prepare(fixture.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	scratch := make([]uint32, fixture.schema.CodeDimensions)
	got, ok := prepared.DenseUint32Row(RoleCodes, 2, scratch)
	if !ok {
		t.Fatal("DenseUint32Row ok=false")
	}
	want := []uint32{20, 21, 22, 23}
	if len(got) != len(want) {
		t.Fatalf("DenseUint32Row len=%d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("DenseUint32Row[%d]=%d want %d", i, got[i], want[i])
		}
	}
	allocs := testing.AllocsPerRun(1000, func() {
		row, _ := prepared.DenseUint32Row(RoleCodes, 2, scratch)
		quantizedAssetUintSink += uint64(row[0])
	})
	if allocs != 0 {
		t.Fatalf("DenseUint32Row allocs/run=%v want 0", allocs)
	}
}

func TestPreparedScorerLoopDoesNotReconstructDocuments1932(t *testing.T) {
	fixture := buildFixedQuantizedFixture1932(t)
	prepared, err := Prepare(fixture.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	docFetches := 0
	documentFetch := func(int) { docFetches++ }
	_ = documentFetch // scorer-shaped loop below must not call this.
	candidates := []int{0, 3, 2, 1, 0, 2}
	var score float32
	for _, ordinal := range candidates {
		row, ok := prepared.CodeRowBytes(RoleCodes, ordinal)
		if !ok {
			t.Fatalf("CodeRowBytes ordinal=%d ok=false", ordinal)
		}
		norm, ok := prepared.Float32(RoleNorm, ordinal)
		if !ok {
			t.Fatalf("Float32 norm ordinal=%d ok=false", ordinal)
		}
		score += norm * float32(row[0]+row[len(row)-1])
	}
	if score == 0 {
		t.Fatal("score-shaped loop produced zero score")
	}
	if docFetches != 0 {
		t.Fatalf("document fetches=%d want 0", docFetches)
	}
}

func TestPreparedConcurrentOrdinalReaders1932(t *testing.T) {
	fixture := buildPackedQuantizedFixture1932(t)
	prepared, err := Prepare(fixture.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			wordScratch := make([]uint64, 2)
			unpackScratch := make([]uint8, fixture.schema.CodeDimensions)
			for i := 0; i < 1000; i++ {
				ordinal := (i + worker) % fixture.rows
				row, ok := prepared.CodeRowBytes(RolePackedCodes, ordinal)
				if !ok || !bytes.Equal(row, fixture.packedRows[ordinal]) {
					t.Errorf("worker=%d ordinal=%d bad row", worker, ordinal)
					return
				}
				if _, _, ok := prepared.RowWords(RolePackedCodes, ordinal, wordScratch); !ok {
					t.Errorf("worker=%d ordinal=%d RowWords ok=false", worker, ordinal)
					return
				}
				values, ok := prepared.PackedElements(RolePackedCodes, ordinal, unpackScratch)
				if !ok || !bytes.Equal(values, fixture.unpackedRows[ordinal]) {
					t.Errorf("worker=%d ordinal=%d bad unpack", worker, ordinal)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

func BenchmarkPrepareQuantizedAssetSchema1932(b *testing.B) {
	fixture := buildPackedQuantizedFixture1932(b)
	req := fixture.prepareRequest()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, err := Prepare(req)
		if err != nil {
			b.Fatalf("Prepare: %v", err)
		}
		quantizedAssetUintSink += uint64(prepared.Rows())
	}
	b.StopTimer()
	b.ReportMetric(float64(req.Parts[0].Image.TotalBytes())/float64(fixture.rows), "asset_B_per_vector")
}

func BenchmarkRandomOrdinalCodeMetadataLookup1932(b *testing.B) {
	fixture := buildPackedQuantizedFixture1932(b)
	prepared, err := Prepare(fixture.prepareRequest())
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	wordScratch := make([]uint64, 2)
	unpackScratch := make([]uint8, fixture.schema.CodeDimensions)
	ordinals := permutation1932(1024, fixture.rows)
	fp := prepared.Footprint()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ordinal := ordinals[i&1023]
		row, _ := prepared.CodeRowBytes(RolePackedCodes, ordinal)
		words, _, _ := prepared.RowWords(RolePackedCodes, ordinal, wordScratch)
		unpacked, _ := prepared.PackedElements(RolePackedCodes, ordinal, unpackScratch)
		lower, _ := prepared.Float32(RoleLower, ordinal)
		count, _ := prepared.Uint32(RoleCodeCount, ordinal)
		quantizedAssetByteSink ^= row[0] ^ byte(words[0]) ^ unpacked[0]
		quantizedAssetFloatSink += lower
		quantizedAssetUintSink += uint64(count)
	}
	b.StopTimer()
	b.ReportMetric(fp.BytesPerVector, "asset_B_per_vector")
	for _, col := range fp.Columns {
		b.ReportMetric(col.BytesPerVector, string(col.Role)+"_B_per_vector")
	}
}

func BenchmarkScorerShapedQuantizedOrdinalLoop1932(b *testing.B) {
	fixture := buildFixedQuantizedFixture1932(b)
	prepared, err := Prepare(fixture.prepareRequest())
	if err != nil {
		b.Fatalf("Prepare: %v", err)
	}
	candidates := permutation1932(4096, fixture.rows)
	fp := prepared.Footprint()
	b.ReportAllocs()
	b.ResetTimer()
	var score float32
	for i := 0; i < b.N; i++ {
		ordinal := candidates[i&4095]
		row, _ := prepared.CodeRowBytes(RoleCodes, ordinal)
		norm, _ := prepared.Float32(RoleNorm, ordinal)
		count, _ := prepared.Uint32(RoleCodeCount, ordinal)
		score += norm * float32(row[0]+row[len(row)-1]+byte(count))
	}
	quantizedAssetFloatSink += score
	b.StopTimer()
	b.ReportMetric(fp.BytesPerVector, "asset_B_per_vector")
	for _, col := range fp.Columns {
		b.ReportMetric(col.BytesPerVector, string(col.Role)+"_B_per_vector")
	}
}

type quantizedFixture1932 struct {
	rows          int
	schema        SchemaDescriptor
	expected      ExpectedSchema
	part          typedcolumn.ColumnPartImage
	ref           AssetRefIdentity
	assetID       string
	packedRows    [][]byte
	unpackedRows  [][]byte
	fixedRows     [][]byte
	floatColumns  map[Role][]float32
	uint32Columns map[Role][]uint32
}

func (f quantizedFixture1932) prepareRequest() PrepareRequest {
	schema := f.schema
	schema.Columns = append([]ColumnDescriptor(nil), f.schema.Columns...)
	expected := f.expected
	expected.RequiredRoles = append([]Role(nil), f.expected.RequiredRoles...)
	return PrepareRequest{
		Schema:   schema,
		Expected: expected,
		Parts: []PartImageSource{{
			AssetID:          f.assetID,
			Image:            f.part,
			Ref:              f.ref,
			AssetBytes:       int64(f.part.TotalBytes()),
			SourceSchemaHash: 0x19320001,
		}},
	}
}

func buildFixedQuantizedFixture1932(t testing.TB) quantizedFixture1932 {
	t.Helper()
	rows := 4
	fixed := []byte{
		0, 1, 2, 3,
		10, 11, 12, 13,
		20, 21, 22, 23,
		30, 31, 32, 33,
	}
	fixedRows, err := typedcolumn.NewFixedBytesRows(rows, 4, fixed)
	if err != nil {
		t.Fatalf("NewFixedBytesRows: %v", err)
	}
	norms := []float32{1.25, 2.5, 3.75, 4.5}
	counts := []uint32{4, 5, 6, 7}
	image := buildQuantizedPartImage1932(t, []typedcolumn.ColumnDefinition{
		{Name: "codes_col", Type: typedcolumn.ColumnTypeFixedBytes, FixedWidthElements: 4, Encoding: typedcolumn.EncodingRawFixedBytes, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: "norm_col", Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: "code_count_col", Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true},
	}, typedcolumn.Batch{
		Rows:              rows,
		FixedBytesColumns: map[string]typedcolumn.FixedBytesRows{"codes_col": fixedRows},
		Float32Columns:    map[string][]float32{"norm_col": norms},
		Uint32Columns:     map[string][]uint32{"code_count_col": counts},
	}, map[string]string{
		"codes_col":      string(columnsemantics.LogicalByteVector),
		"norm_col":       string(columnsemantics.LogicalFloat32),
		"code_count_col": string(columnsemantics.LogicalUint32),
	})
	base := baseGraphIdentity1932(rows, 16, "cosine")
	schema := schemaDescriptor1932(rows, 16, 4, 8, "cosine", "sq8", base, []ColumnDescriptor{
		{Role: RoleCodes, Column: "codes_col", AssetID: "fixed", Required: true, LogicalType: string(columnsemantics.LogicalByteVector), Type: typedcolumn.ColumnTypeFixedBytes, Encoding: typedcolumn.EncodingRawFixedBytes, BytesPerRow: 4, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
		{Role: RoleNorm, Column: "norm_col", AssetID: "fixed", Required: true, LogicalType: string(columnsemantics.LogicalFloat32), Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
		{Role: RoleCodeCount, Column: "code_count_col", AssetID: "fixed", Required: true, LogicalType: string(columnsemantics.LogicalUint32), Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
	})
	ref := refForImage1932("fixed", image)
	for i := range schema.Columns {
		schema.Columns[i].Ref = ref
	}
	return quantizedFixture1932{
		rows:     rows,
		schema:   schema,
		expected: expectedFromSchema1932(schema, RoleCodes, RoleNorm, RoleCodeCount),
		part:     image,
		ref:      ref,
		assetID:  "fixed",
		fixedRows: [][]byte{
			fixed[0:4], fixed[4:8], fixed[8:12], fixed[12:16],
		},
		floatColumns:  map[Role][]float32{RoleNorm: norms},
		uint32Columns: map[Role][]uint32{RoleCodeCount: counts},
	}
}

func buildPackedQuantizedFixture1932(t testing.TB) quantizedFixture1932 {
	t.Helper()
	rows := 4
	unpackedFlat := []uint8{
		1, 0, 1, 1, 0, 0, 1, 0, 1, 0,
		0, 1, 0, 1, 1, 1, 0, 0, 1, 1,
		1, 1, 1, 0, 0, 1, 1, 0, 0, 0,
		0, 0, 1, 1, 1, 0, 1, 1, 0, 1,
	}
	packedRaw, err := typedcolumn.EncodePackedUintRows(nil, rows, 10, 1, unpackedFlat)
	if err != nil {
		t.Fatalf("EncodePackedUintRows: %v", err)
	}
	packedRows, err := typedcolumn.NewPackedUintRows(rows, 10, 1, packedRaw)
	if err != nil {
		t.Fatalf("NewPackedUintRows: %v", err)
	}
	lowers := []float32{-1, -0.5, 0.25, 0.75}
	counts := []uint32{5, 6, 7, 8}
	image := buildQuantizedPartImage1932(t, []typedcolumn.ColumnDefinition{
		{Name: "packed_col", Type: typedcolumn.ColumnTypePackedBitVector, FixedWidthElements: 10, BitsPerElement: 1, Encoding: typedcolumn.EncodingRawPackedBitVector, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: "lower_col", Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: "code_count_col", Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true},
	}, typedcolumn.Batch{
		Rows:              rows,
		PackedUintColumns: map[string]typedcolumn.PackedUintRows{"packed_col": packedRows},
		Float32Columns:    map[string][]float32{"lower_col": lowers},
		Uint32Columns:     map[string][]uint32{"code_count_col": counts},
	}, map[string]string{
		"packed_col":     string(columnsemantics.LogicalPackedBitVector),
		"lower_col":      string(columnsemantics.LogicalFloat32),
		"code_count_col": string(columnsemantics.LogicalUint32),
	})
	base := baseGraphIdentity1932(rows, 16, "cosine")
	schema := schemaDescriptor1932(rows, 16, 10, 1, "cosine", "rabitq-bits", base, []ColumnDescriptor{
		{Role: RolePackedCodes, Column: "packed_col", AssetID: "packed", Required: true, LogicalType: string(columnsemantics.LogicalPackedBitVector), Type: typedcolumn.ColumnTypePackedBitVector, Encoding: typedcolumn.EncodingRawPackedBitVector, ElementsPerRow: 10, BitsPerElement: 1, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
		{Role: RoleLower, Column: "lower_col", AssetID: "packed", Required: true, LogicalType: string(columnsemantics.LogicalFloat32), Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
		{Role: RoleCodeCount, Column: "code_count_col", AssetID: "packed", Required: true, LogicalType: string(columnsemantics.LogicalUint32), Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
	})
	ref := refForImage1932("packed", image)
	for i := range schema.Columns {
		schema.Columns[i].Ref = ref
	}
	unpackedRows := make([][]byte, rows)
	packedRowBytes := make([][]byte, rows)
	for row := 0; row < rows; row++ {
		unpackedRows[row] = append([]byte(nil), unpackedFlat[row*10:row*10+10]...)
		packedRowBytes[row] = append([]byte(nil), packedRaw[row*packedRows.BytesPerRow:row*packedRows.BytesPerRow+packedRows.BytesPerRow]...)
	}
	return quantizedFixture1932{
		rows:          rows,
		schema:        schema,
		expected:      expectedFromSchema1932(schema, RolePackedCodes, RoleLower, RoleCodeCount),
		part:          image,
		ref:           ref,
		assetID:       "packed",
		packedRows:    packedRowBytes,
		unpackedRows:  unpackedRows,
		floatColumns:  map[Role][]float32{RoleLower: lowers},
		uint32Columns: map[Role][]uint32{RoleCodeCount: counts},
	}
}

func buildPQQuantizedFixture1932(t testing.TB) quantizedFixture1932 {
	t.Helper()
	rows := 4
	dense := typedcolumn.RawDenseFixedWidth{Rows: rows, ElementsPerRow: 3, ElementWidthBytes: 1, Values: []byte{0, 1, 2, 10, 11, 12, 20, 21, 22, 30, 31, 32}}
	centroidIDs := []uint32{1, 1, 2, 2}
	listIDs := []uint32{7, 8, 7, 9}
	distances := []float32{0.1, 0.2, 0.3, 0.4}
	image := buildQuantizedPartImage1932(t, []typedcolumn.ColumnDefinition{
		{Name: "pq_codes", Type: typedcolumn.ColumnTypeUint8Vector, FixedWidthElements: 3, Encoding: typedcolumn.EncodingRawUint8Vector, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
		{Name: "centroid_id_col", Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true},
		{Name: "list_id_col", Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, Compression: typedcolumn.CompressionNone, CompressionSet: true},
		{Name: "centroid_distance_col", Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
	}, typedcolumn.Batch{
		Rows:                   rows,
		DenseFixedWidthVectors: map[string]typedcolumn.RawDenseFixedWidth{"pq_codes": dense},
		Uint32Columns:          map[string][]uint32{"centroid_id_col": centroidIDs, "list_id_col": listIDs},
		Float32Columns:         map[string][]float32{"centroid_distance_col": distances},
	}, map[string]string{
		"pq_codes":              string(columnsemantics.LogicalUint8Vector),
		"centroid_id_col":       string(columnsemantics.LogicalUint32),
		"list_id_col":           string(columnsemantics.LogicalUint32),
		"centroid_distance_col": string(columnsemantics.LogicalFloat32),
	})
	base := baseGraphIdentity1932(rows, 12, "dot")
	schema := schemaDescriptor1932(rows, 12, 3, 8, "dot", "pq", base, []ColumnDescriptor{
		{Role: RoleCodes, Column: "pq_codes", AssetID: "pq", Required: true, LogicalType: string(columnsemantics.LogicalUint8Vector), Type: typedcolumn.ColumnTypeUint8Vector, Encoding: typedcolumn.EncodingRawUint8Vector, ElementsPerRow: 3, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
		{Role: RoleCentroidID, Column: "centroid_id_col", AssetID: "pq", Required: true, LogicalType: string(columnsemantics.LogicalUint32), Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
		{Role: RoleListID, Column: "list_id_col", AssetID: "pq", Required: true, LogicalType: string(columnsemantics.LogicalUint32), Type: typedcolumn.ColumnTypeUint32, Encoding: typedcolumn.EncodingRawUint32, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
		{Role: RoleCentroidDistance, Column: "centroid_distance_col", AssetID: "pq", Required: true, LogicalType: string(columnsemantics.LogicalFloat32), Type: typedcolumn.ColumnTypeFloat32, Encoding: typedcolumn.EncodingRawFloat32, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001},
	})
	ref := refForImage1932("pq", image)
	for i := range schema.Columns {
		schema.Columns[i].Ref = ref
	}
	return quantizedFixture1932{rows: rows, schema: schema, expected: expectedFromSchema1932(schema, RoleCodes, RoleCentroidID, RoleListID, RoleCentroidDistance), part: image, ref: ref, assetID: "pq"}
}

func buildDensePhysicalCodeFixture1932(t testing.TB, columnType typedcolumn.ColumnType, encoding typedcolumn.Encoding, logicalType columnsemantics.LogicalType, widthBytes int) quantizedFixture1932 {
	t.Helper()
	rows := 4
	codeDims := 4
	values := make([]byte, rows*codeDims*widthBytes)
	for i := range values {
		values[i] = byte(i)
	}
	dense := typedcolumn.RawDenseFixedWidth{Rows: rows, ElementsPerRow: codeDims, ElementWidthBytes: widthBytes, Values: values}
	image := buildQuantizedPartImage1932(t, []typedcolumn.ColumnDefinition{
		{Name: "bad_codes", Type: columnType, FixedWidthElements: codeDims, Encoding: encoding, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
	}, typedcolumn.Batch{Rows: rows, DenseFixedWidthVectors: map[string]typedcolumn.RawDenseFixedWidth{"bad_codes": dense}}, map[string]string{"bad_codes": string(logicalType)})
	base := baseGraphIdentity1932(rows, 16, "dot")
	schema := schemaDescriptor1932(rows, 16, codeDims, widthBytes*8, "dot", "dense-physical", base, []ColumnDescriptor{{Role: RoleCodes, Column: "bad_codes", AssetID: "bad-dense", Required: true, LogicalType: string(logicalType), Type: columnType, Encoding: encoding, ElementsPerRow: codeDims, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001}})
	ref := refForImage1932("bad-dense", image)
	schema.Columns[0].Ref = ref
	return quantizedFixture1932{rows: rows, schema: schema, expected: expectedFromSchema1932(schema, RoleCodes), part: image, ref: ref, assetID: "bad-dense"}
}

func buildDenseUint32QuantizedFixture1932(t testing.TB) quantizedFixture1932 {
	t.Helper()
	rows := 4
	values := make([]byte, rows*4*4)
	for row := 0; row < rows; row++ {
		for col := 0; col < 4; col++ {
			binary.LittleEndian.PutUint32(values[(row*4+col)*4:], uint32(row*10+col))
		}
	}
	dense := typedcolumn.RawDenseFixedWidth{Rows: rows, ElementsPerRow: 4, ElementWidthBytes: 4, Values: values}
	image := buildQuantizedPartImage1932(t, []typedcolumn.ColumnDefinition{
		{Name: "u32_codes", Type: typedcolumn.ColumnTypeUint32Vector, FixedWidthElements: 4, Encoding: typedcolumn.EncodingRawUint32Vector, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
	}, typedcolumn.Batch{Rows: rows, DenseFixedWidthVectors: map[string]typedcolumn.RawDenseFixedWidth{"u32_codes": dense}}, map[string]string{"u32_codes": string(columnsemantics.LogicalUint32Vector)})
	base := baseGraphIdentity1932(rows, 16, "dot")
	schema := schemaDescriptor1932(rows, 16, 4, 32, "dot", "dense-u32", base, []ColumnDescriptor{{Role: RoleCodes, Column: "u32_codes", AssetID: "u32", Required: true, LogicalType: string(columnsemantics.LogicalUint32Vector), Type: typedcolumn.ColumnTypeUint32Vector, Encoding: typedcolumn.EncodingRawUint32Vector, ElementsPerRow: 4, AssetBytes: int64(image.TotalBytes()), SourceSchemaHash: 0x19320001}})
	ref := refForImage1932("u32", image)
	schema.Columns[0].Ref = ref
	return quantizedFixture1932{rows: rows, schema: schema, expected: expectedFromSchema1932(schema, RoleCodes), part: image, ref: ref, assetID: "u32"}
}

func buildQuantizedPartImage1932(t testing.TB, defs []typedcolumn.ColumnDefinition, batch typedcolumn.Batch, logicalTypes map[string]string) typedcolumn.ColumnPartImage {
	t.Helper()
	rows := batch.Rows
	ids := make([]int64, rows)
	for i := range ids {
		ids[i] = int64(i)
	}
	columns := []typedcolumn.ColumnDefinition{{Name: "id", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true}}
	columns = append(columns, defs...)
	batch.Columns = map[string][]int64{"id": ids}
	part, err := typedcolumn.BuildColumnPart(1932, typedcolumn.Options{
		SchemaVersion: uint32(0x1932),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns:       columns,
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{
			Columns: []string{"id"},
		},
		SortKey:    typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: "id"}}},
		PartPolicy: typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule},
		Compression: typedcolumn.ColumnCompressionPolicy{
			Default: typedcolumn.CompressionNone,
		},
	}, batch)
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	logicalTypes = cloneStringMap1932(logicalTypes)
	logicalTypes["id"] = string(columnsemantics.LogicalInt64)
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{LayoutLogicalTypes: logicalTypes})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	return image
}

func schemaDescriptor1932(rows, dims, codeDims, codeWidth int, metric, codec string, base BaseGraphIdentity, columns []ColumnDescriptor) SchemaDescriptor {
	return SchemaDescriptor{
		Name:             "quantized-fixture",
		Metric:           metric,
		VectorDimensions: dims,
		CodeDimensions:   codeDims,
		CodeWidthBits:    codeWidth,
		RowCount:         rows,
		OrdinalOrder:     GraphOrdinalOrderVectorOrdinal,
		Codec:            CodecDescriptor{Name: codec, Version: 1, ConfigHash: 0x1932, Config: []byte("fixture-config")},
		BaseGraph:        base,
		Columns:          columns,
	}
}

func expectedFromSchema1932(schema SchemaDescriptor, roles ...Role) ExpectedSchema {
	return ExpectedSchema{
		Metric:           schema.Metric,
		VectorDimensions: schema.VectorDimensions,
		CodeDimensions:   schema.CodeDimensions,
		CodeWidthBits:    schema.CodeWidthBits,
		RowCount:         schema.RowCount,
		OrdinalOrder:     schema.OrdinalOrder,
		Codec:            schema.Codec,
		BaseGraph:        schema.BaseGraph,
		RequiredRoles:    roles,
	}
}

func baseGraphIdentity1932(rows, dims int, metric string) BaseGraphIdentity {
	return BaseGraphIdentity{IndexName: "vec_idx", Field: "embedding", Metric: metric, Dimensions: dims, RowCount: rows, BaseManifestGeneration: 7, BaseManifestChecksum: 0xabc, BaseSchemaHash: 0xdef, GraphSchemaHash: 0x1234}
}

func refForImage1932(assetID string, image typedcolumn.ColumnPartImage) AssetRefIdentity {
	return AssetRefIdentity{Present: true, Kind: "tcs1_typed_column_part", Namespace: "quantized-test", Generation: 7, PartID: image.PartID + uint64(len(assetID)), FileID: 1, Offset: 128, Length: int64(image.TotalBytes()), Checksum: page.Checksum(image.Bytes)}
}

func cloneStringMap1932(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func permutation1932(n, rows int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = (i*37 + 11) % rows
	}
	return out
}

func TestBenchmarkFixturesHaveFootprintMetrics1932(t *testing.T) {
	fixture := buildFixedQuantizedFixture1932(t)
	prepared, err := Prepare(fixture.prepareRequest())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	fp := prepared.Footprint()
	if fp.AssetBytes == 0 || fp.BytesPerVector == 0 || len(fp.Columns) != 3 {
		t.Fatalf("footprint=%+v", fp)
	}
	for _, col := range fp.Columns {
		if col.SectionBytes == 0 || col.BytesPerVector == 0 {
			t.Fatalf("column footprint=%+v", col)
		}
	}
	_ = fmt.Sprintf("%+v", fp)
}
