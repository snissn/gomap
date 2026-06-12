package typedcolumn

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"
)

func TestTypedColumnTransplantTCS1ChecksumAndReservedBytesFailClosed(t *testing.T) {
	image := mustTransplantImage(t, mustTransplantPart(t, 301, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	tcs1, record, err := EncodeTCS1ColumnPartImage(image)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}

	roundTrip, decoded, err := DecodeTCS1ColumnPartImage(tcs1)
	if err != nil {
		t.Fatalf("DecodeTCS1ColumnPartImage(valid): %v", err)
	}
	if roundTrip.PartID != image.PartID || decoded.PayloadCRC32 != record.PayloadCRC32 {
		t.Fatalf("valid TCS1 round trip part/checksum=(%d,%08x) want (%d,%08x)", roundTrip.PartID, decoded.PayloadCRC32, image.PartID, record.PayloadCRC32)
	}

	cases := []struct {
		name string
		edit func([]byte)
		want string
	}{
		{
			name: "payload_byte",
			edit: func(data []byte) { data[len(data)-1] ^= 0x80 },
			want: "payload checksum",
		},
		{
			name: "checksum_field",
			edit: func(data []byte) {
				binary.LittleEndian.PutUint32(data[tcs1PayloadCRC32Offset:tcs1PayloadOffset], record.PayloadCRC32^0xfeedface)
			},
			want: "payload checksum",
		},
		{
			name: "reserved_header",
			edit: func(data []byte) { binary.LittleEndian.PutUint16(data[tcs1ReservedOffset:tcs1PayloadCRC32Offset], 1) },
			want: "reserved",
		},
		{
			name: "payload_bytes_header",
			edit: func(data []byte) {
				binary.LittleEndian.PutUint64(data[tcs1PayloadBytesOffset:tcs1PartIDOffset], uint64(record.PayloadBytes-1))
			},
			want: "payload bytes",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := append([]byte(nil), tcs1...)
			tc.edit(corrupt)
			_, _, err := DecodeTCS1ColumnPartImage(corrupt)
			requireTypedColumnErrContains(t, err, tc.want)
		})
	}
}

func TestTypedColumnTransplantRejectsSectionOverlapDuplicateMissing(t *testing.T) {
	image := mustTransplantImage(t, mustTransplantPart(t, 302, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	if len(image.Sections) < 3 {
		t.Fatalf("test image sections=%d want at least 3", len(image.Sections))
	}

	t.Run("overlap", func(t *testing.T) {
		corrupt := append([]byte(nil), image.Bytes...)
		entry := manifestEntryForSection(t, image, 1)
		binary.LittleEndian.PutUint64(corrupt[entry.offset:entry.length], uint64(image.Sections[0].Offset))
		_, err := ParseColumnPartImage(corrupt)
		requireTypedColumnErrContains(t, err, "overlaps previous")
	})

	t.Run("duplicate", func(t *testing.T) {
		corrupt := append([]byte(nil), image.Bytes...)
		entry := manifestEntryForSection(t, image, 1)
		binary.LittleEndian.PutUint16(corrupt[entry.kind:entry.category], 1)   // descriptor
		binary.LittleEndian.PutUint16(corrupt[entry.category:entry.offset], 1) // descriptor category
		_, err := ParseColumnPartImage(corrupt)
		requireTypedColumnErrContains(t, err, "sections, want at most 1")
	})

	t.Run("missing_required_descriptor", func(t *testing.T) {
		corrupt := append([]byte(nil), image.Bytes...)
		entry := manifestEntryForSection(t, image, 0)
		binary.LittleEndian.PutUint16(corrupt[entry.kind:entry.category], 5)   // aggregate_metadata
		binary.LittleEndian.PutUint16(corrupt[entry.category:entry.offset], 5) // aggregate_metadata category
		parsed, err := ParseColumnPartImage(corrupt)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		_, err = ColumnPartFromImage(parsed)
		requireTypedColumnErrContains(t, err, "image has 0 descriptor sections")
	})

	t.Run("invalid_category", func(t *testing.T) {
		corrupt := append([]byte(nil), image.Bytes...)
		entry := manifestEntryForSection(t, image, 0)
		binary.LittleEndian.PutUint16(corrupt[entry.category:entry.offset], 3) // marks category on descriptor section
		_, err := ParseColumnPartImage(corrupt)
		requireTypedColumnErrContains(t, err, "category=marks want descriptor")
	})

	t.Run("invalid_kind", func(t *testing.T) {
		corrupt := append([]byte(nil), image.Bytes...)
		entry := manifestEntryForSection(t, image, 0)
		binary.LittleEndian.PutUint16(corrupt[entry.kind:entry.category], 0xffff)
		_, err := ParseColumnPartImage(corrupt)
		requireTypedColumnErrContains(t, err, "unknown image section kind code")
	})

	t.Run("missing_column_data_section", func(t *testing.T) {
		last := len(image.Sections) - 1
		if image.Sections[last].Kind != ColumnPartImageSectionColumnData {
			t.Fatalf("last section kind=%s want column_data", image.Sections[last].Kind)
		}
		prevEnd := image.ManifestBytes
		if last > 0 {
			prev := image.Sections[last-1]
			prevEnd = prev.Offset + prev.Length
		}
		corrupt := image
		corrupt.Bytes = append([]byte(nil), image.Bytes[:prevEnd]...)
		corrupt.Sections = append([]ColumnPartImageSection(nil), image.Sections[:last]...)
		_, err := ColumnPartFromImage(corrupt)
		requireTypedColumnErrContains(t, err, "image missing column data section")
	})
}

func TestTypedColumnTransplantCodecBoundaryMatrix(t *testing.T) {
	t.Run("zero_rows_fail_closed", func(t *testing.T) {
		batch, _ := transplantBoundaryBatch(0)
		_, err := BuildColumnPart(399, transplantCodecOptions(EncodingDeltaVarint, CompressionNone), batch)
		requireTypedColumnErrContains(t, err, "invalid part rows 0")
	})

	rowCounts := []int{1, 2, 3, 4, 5, 7, 10}
	encodings := []Encoding{EncodingRawInt64, EncodingDeltaVarint, EncodingDoubleDeltaVarint}
	for _, encoding := range encodings {
		for _, rows := range rowCounts {
			t.Run(fmt.Sprintf("%s_rows_%d", encoding, rows), func(t *testing.T) {
				opts := transplantCodecOptions(encoding, CompressionNone)
				opts.PartPolicy.RowsPerGranule = 3
				opts.PartPolicy.DefaultCodecBlockRows = 2
				batch, want := transplantBoundaryBatch(rows)
				part := mustTransplantPart(t, uint64(400+rows), opts, batch)
				if got := len(part.Descriptor.Granules); got != (rows+2)/3 {
					t.Fatalf("granules=%d want %d", got, (rows+2)/3)
				}
				image := mustTransplantImage(t, part)
				parsed, err := ParseColumnPartImage(image.Bytes)
				if err != nil {
					t.Fatalf("ParseColumnPartImage: %v", err)
				}
				reconstructed, err := ColumnPartFromImage(parsed)
				if err != nil {
					t.Fatalf("ColumnPartFromImage: %v", err)
				}
				scan, err := reconstructed.NewScanner().ScanProjected([]string{"value"})
				if err != nil {
					t.Fatalf("ScanProjected: %v", err)
				}
				assertTransplantInt64s(t, "boundary values", scan.Columns["value"], want)
				for primaryID := int64(1); primaryID <= int64(rows); primaryID++ {
					locator, ok := reconstructed.LocatePrimaryID(primaryID)
					if !ok {
						t.Fatalf("missing locator for id=%d", primaryID)
					}
					if locator.GranuleOrdinal != locator.PartRow/3 || locator.RowInGranule != locator.PartRow%3 {
						t.Fatalf("locator id=%d %+v inconsistent with 3-row granules", primaryID, locator)
					}
				}
			})
		}
	}
}

func TestTypedColumnTransplantCompressionRoundTrip(t *testing.T) {
	for _, compression := range []Compression{CompressionSnappy, CompressionLZ4} {
		t.Run(fmt.Sprintf("%s_kept", compression), func(t *testing.T) {
			rows := 256
			opts := transplantCodecOptions(EncodingRawInt64, compression)
			opts.PartPolicy.RowsPerGranule = rows
			batch := transplantConstantBatch(rows, 42)
			part := mustTransplantPart(t, 500+uint64(compression), opts, batch)
			block := part.Columns["value"].Blocks[0]
			if block.Granule.Compression != compression || !block.Granule.CodecReport.CompressionKept {
				t.Fatalf("compression=%s kept=%v report=%+v", block.Granule.Compression, block.Granule.CodecReport.CompressionKept, block.Granule.CodecReport)
			}
			image := mustTransplantImage(t, part)
			reconstructed, err := ColumnPartFromImage(image)
			if err != nil {
				t.Fatalf("ColumnPartFromImage: %v", err)
			}
			scan, err := reconstructed.NewScanner().ScanProjected([]string{"value"})
			if err != nil {
				t.Fatalf("ScanProjected: %v", err)
			}
			assertTransplantInt64s(t, "compressed values", scan.Columns["value"], batch.Columns["value"])
		})

		t.Run(fmt.Sprintf("%s_incompressible_fallback", compression), func(t *testing.T) {
			opts := transplantCodecOptions(EncodingRawInt64, compression)
			batch := transplantConstantBatch(1, splitmix64Int64(99))
			part := mustTransplantPart(t, 510+uint64(compression), opts, batch)
			block := part.Columns["value"].Blocks[0]
			if block.Granule.Compression != CompressionNone || !block.Granule.CodecReport.CompressionAttempted || block.Granule.CodecReport.CompressionFallbackReason != "not_smaller" {
				t.Fatalf("fallback granule compression=%s report=%+v", block.Granule.Compression, block.Granule.CodecReport)
			}
			scan, err := part.NewScanner().ScanProjected([]string{"value"})
			if err != nil {
				t.Fatalf("ScanProjected fallback: %v", err)
			}
			assertTransplantInt64s(t, "fallback values", scan.Columns["value"], batch.Columns["value"])
		})
	}
}

func TestTypedColumnTransplantScannerProjectionErrors(t *testing.T) {
	part := mustTransplantPart(t, 601, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	scanner := part.NewScanner()

	_, err := scanner.ScanProjected(nil)
	requireTypedColumnErrContains(t, err, "empty projection")
	_, err = scanner.ScanProjected([]string{"id", "id"})
	requireTypedColumnErrContains(t, err, "duplicate projection column id")
	_, err = scanner.ScanProjected([]string{"does_not_exist"})
	requireTypedColumnErrContains(t, err, "missing column does_not_exist")
	_, err = (&ColumnPartScanner{}).ScanProjected([]string{"id"})
	requireTypedColumnErrContains(t, err, "nil part scanner")

	locator, ok := part.LocatePrimaryID(1)
	if !ok {
		t.Fatalf("missing locator id=1")
	}
	wrongPart := locator
	wrongPart.PartID++
	_, err = scanner.ValueAt(wrongPart, "value")
	requireTypedColumnErrContains(t, err, "locator part")
	outside := locator
	outside.PartRow = part.Descriptor.RowCount
	_, err = scanner.ValueAt(outside, "value")
	requireTypedColumnErrContains(t, err, "outside column value")
	_, err = scanner.ValueAt(locator, "does_not_exist")
	requireTypedColumnErrContains(t, err, "missing column does_not_exist")

	_, _, err = scanner.scanColumnRowsInto("value", nil, []int{-1})
	requireTypedColumnErrContains(t, err, "outside part rows")
}

func TestTypedColumnTransplantPartSetTieBreakers(t *testing.T) {
	opts := transplantTestOptions([]SortKeyColumn{{Column: "id"}})
	base := mustTransplantPart(t, 701, opts, Batch{Columns: map[string][]int64{
		"id":        {1, 2, 3},
		"time_us":   {10, 20, 30},
		"value":     {100, 200, 300},
		"kind_code": {0, 1, 2},
		"has_reply": {1, 0, 1},
	}})
	deltaA := mustTransplantPart(t, 702, opts, Batch{Columns: map[string][]int64{
		"id":        {2, 4},
		"time_us":   {22, 40},
		"value":     {220, 400},
		"kind_code": {1, 0},
		"has_reply": {1, 0},
	}})
	deltaB := mustTransplantPart(t, 703, opts, Batch{Columns: map[string][]int64{
		"id":        {2, 5},
		"time_us":   {23, 50},
		"value":     {225, 500},
		"kind_code": {2, 1},
		"has_reply": {0, 1},
	}})
	reader, err := NewPartSetReader([]PartRef{
		{Role: PartRoleBase, GenerationID: 1, Part: base},
		{Role: PartRoleDelta, GenerationID: 2, Part: deltaA},
		{Role: PartRoleDelta, GenerationID: 2, Part: deltaB},
	}, []Tombstone{
		{PrimaryID: 1, GenerationID: 0}, // older than base row: must not delete.
		{PrimaryID: 3, GenerationID: 1}, // equal generation: deletes base row.
		{PrimaryID: 4, GenerationID: 1}, // older than delta row: must not delete.
		{PrimaryID: 5, GenerationID: 3}, // newer than delta row: deletes delta row.
	})
	if err != nil {
		t.Fatalf("NewPartSetReader: %v", err)
	}
	stats := reader.VisibilityStats()
	if stats.InputRows != 7 || stats.VisibleRows != 3 || stats.SupersededRows != 2 || stats.DeletedRows != 2 {
		t.Fatalf("visibility stats=%+v", stats)
	}
	wantValues := map[int64]int64{1: 100, 2: 225, 4: 400}
	for id := int64(1); id <= 5; id++ {
		latest, latestOK := reader.LatestLocator(id)
		scanned, scanOK := reader.ScanLatestLocator(id)
		if latestOK != scanOK || latest != scanned {
			t.Fatalf("id=%d latest=%+v ok=%v scan=%+v ok=%v", id, latest, latestOK, scanned, scanOK)
		}
		want, visible := wantValues[id]
		value, ok, err := reader.ValueAtLatest(id, "value")
		if err != nil {
			t.Fatalf("ValueAtLatest(%d): %v", id, err)
		}
		if ok != visible || (visible && value != want) {
			t.Fatalf("ValueAtLatest(%d)=(%d,%v) want (%d,%v)", id, value, ok, want, visible)
		}
	}
	if locator, ok := reader.LatestLocator(2); !ok || locator.PartID != 703 {
		t.Fatalf("same-generation tie breaker locator=%+v ok=%v want part 703", locator, ok)
	}
}

func TestTypedColumnTransplantDictionaryCorruptionFailsClosed(t *testing.T) {
	opts := transplantTestOptions([]SortKeyColumn{{Column: "id"}})
	part := mustTransplantPart(t, 801, opts, transplantTestBatch())

	t.Run("duplicate_code", func(t *testing.T) {
		image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
			"kind_code": {"user": 0, "reply": 1, "system": 3},
		}})
		if err != nil {
			t.Fatalf("BuildColumnPartImage: %v", err)
		}
		parsed, err := ParseColumnPartImage(image.Bytes)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		corrupt := append([]byte(nil), parsed.Bytes...)
		setDictionaryCodeForValue(t, parsed, corrupt, "reply", 0)
		parsed.Bytes = corrupt
		_, err = parsed.Dictionaries()
		requireTypedColumnErrContains(t, err, "duplicate dictionary code 0")
	})

	t.Run("missing_dictionary_section_for_low_cardinality", func(t *testing.T) {
		image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
		if err != nil {
			t.Fatalf("BuildColumnPartImage: %v", err)
		}
		parsed, err := ParseColumnPartImage(image.Bytes)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		_, err = parsed.Dictionaries()
		requireTypedColumnErrContains(t, err, "missing dictionary for low-cardinality column kind_code")
	})

	t.Run("missing_code", func(t *testing.T) {
		image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
			"kind_code": {"user": 0, "reply": 1},
		}})
		if err != nil {
			t.Fatalf("BuildColumnPartImage: %v", err)
		}
		parsed, err := ParseColumnPartImage(image.Bytes)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		_, err = parsed.Dictionaries()
		requireTypedColumnErrContains(t, err, "missing dictionary code 2")
	})

	t.Run("duplicate_value", func(t *testing.T) {
		image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
			"kind_code": {"aa": 0, "bb": 1, "cc": 3},
		}})
		if err != nil {
			t.Fatalf("BuildColumnPartImage: %v", err)
		}
		parsed, err := ParseColumnPartImage(image.Bytes)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		corrupt := append([]byte(nil), parsed.Bytes...)
		setDictionaryValueForCode(t, parsed, corrupt, 1, "aa")
		parsed.Bytes = corrupt
		_, err = parsed.Dictionaries()
		requireTypedColumnErrContains(t, err, "duplicate dictionary value aa")
	})

	t.Run("code_outside_cardinality", func(t *testing.T) {
		image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
			"kind_code": {"user": 0, "reply": 1, "system": 3},
		}})
		if err != nil {
			t.Fatalf("BuildColumnPartImage: %v", err)
		}
		parsed, err := ParseColumnPartImage(image.Bytes)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		_, err = parsed.Dictionaries()
		requireTypedColumnErrContains(t, err, "outside cardinality 3")
	})
}

func FuzzTypedColumnPartImageRoundTrip(f *testing.F) {
	for _, seed := range []uint64{0, 1, 2, 7, 11, 23, 101, 255, 1024, ^uint64(0)} {
		f.Add(seed, int(seed%24)+1)
	}
	f.Fuzz(func(t *testing.T, seed uint64, rows int) {
		if rows <= 0 || rows > 64 {
			return
		}
		encodings := []Encoding{EncodingRawInt64, EncodingDeltaVarint, EncodingDoubleDeltaVarint}
		compressions := []Compression{CompressionNone, CompressionSnappy, CompressionLZ4}
		encoding := encodings[int(seed%uint64(len(encodings)))]
		compression := compressions[int((seed/3)%uint64(len(compressions)))]
		opts := transplantCodecOptions(encoding, compression)
		opts.PartPolicy.RowsPerGranule = int(seed%7) + 1
		opts.PartPolicy.DefaultCodecBlockRows = int((seed/7)%5) + 1
		batch := transplantFuzzBatch(seed, rows)
		part, err := BuildColumnPart(900+seed%10_000, opts, batch)
		if err != nil {
			t.Fatalf("BuildColumnPart: %v", err)
		}
		image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
			"kind_code": {"zero": 0, "one": 1, "two": 2},
		}})
		if err != nil {
			t.Fatalf("BuildColumnPartImage: %v", err)
		}
		parsed, err := ParseColumnPartImage(image.Bytes)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		dictionaries, err := parsed.Dictionaries()
		if err != nil {
			t.Fatalf("Dictionaries: %v", err)
		}
		if dictionaries["kind_code"]["two"] != 2 {
			t.Fatalf("dictionary round trip: %+v", dictionaries)
		}
		reconstructed, err := ColumnPartFromImage(parsed)
		if err != nil {
			t.Fatalf("ColumnPartFromImage: %v", err)
		}
		scan, err := reconstructed.NewScanner().ScanProjected([]string{"id", "time_us", "value", "kind_code", "has_reply"})
		if err != nil {
			t.Fatalf("ScanProjected: %v", err)
		}
		if scan.Rows != rows {
			t.Fatalf("scan rows=%d want %d", scan.Rows, rows)
		}
		assertTransplantInt64s(t, "id", scan.Columns["id"], batch.Columns["id"])
		assertTransplantInt64s(t, "time_us", scan.Columns["time_us"], batch.Columns["time_us"])
		assertTransplantInt64s(t, "value", scan.Columns["value"], batch.Columns["value"])
		assertTransplantInt64s(t, "kind_code", scan.Columns["kind_code"], batch.Columns["kind_code"])
		assertTransplantInt64s(t, "has_reply", scan.Columns["has_reply"], batch.Columns["has_reply"])
	})
}

type manifestEntryOffsets struct {
	kind     int
	category int
	offset   int
	length   int
}

func manifestEntryForSection(t *testing.T, image ColumnPartImage, sectionIndex int) manifestEntryOffsets {
	t.Helper()
	if sectionIndex < 0 || sectionIndex >= len(image.Sections) {
		t.Fatalf("section index %d outside %d", sectionIndex, len(image.Sections))
	}
	off := 32
	for i := 0; i <= sectionIndex; i++ {
		if off+64 > image.ManifestBytes {
			t.Fatalf("manifest entry %d offset=%d exceeds manifest=%d", i, off, image.ManifestBytes)
		}
		entry := manifestEntryOffsets{kind: off, category: off + 2, offset: off + 4, length: off + 12}
		off += 2 + 2 + 8 + 8 + 8 + 8 + 8 + 2 + 2 + 8
		nameLen := int(binary.LittleEndian.Uint32(image.Bytes[off:]))
		off += 4 + nameLen
		columnLen := int(binary.LittleEndian.Uint32(image.Bytes[off:]))
		off += 4 + columnLen
		if i == sectionIndex {
			return entry
		}
	}
	t.Fatalf("unreachable section index %d", sectionIndex)
	return manifestEntryOffsets{}
}

func requireTypedColumnErrContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("err=%v want substring %q", err, want)
	}
}

func transplantCodecOptions(valueEncoding Encoding, valueCompression Compression) Options {
	opts := transplantTestOptions([]SortKeyColumn{{Column: "id"}})
	for i := range opts.Columns {
		if opts.Columns[i].Name == "value" {
			opts.Columns[i].Encoding = valueEncoding
			opts.Columns[i].Compression = valueCompression
			opts.Columns[i].CompressionSet = true
		}
	}
	return opts
}

func transplantBoundaryBatch(rows int) (Batch, []int64) {
	batch := transplantConstantBatch(rows, 0)
	pattern := []int64{
		math.MinInt64,
		math.MinInt64 + 1,
		-(1 << 60),
		-17,
		0,
		17,
		1 << 60,
		math.MaxInt64 - 1,
		math.MaxInt64,
		-(1 << 40),
	}
	want := make([]int64, rows)
	for i := 0; i < rows; i++ {
		want[i] = pattern[i%len(pattern)] + int64(i/len(pattern))
	}
	batch.Columns["value"] = want
	return batch, append([]int64(nil), want...)
}

func transplantConstantBatch(rows int, value int64) Batch {
	batch := Batch{Columns: map[string][]int64{
		"id":        make([]int64, rows),
		"time_us":   make([]int64, rows),
		"value":     make([]int64, rows),
		"kind_code": make([]int64, rows),
		"has_reply": make([]int64, rows),
	}}
	for i := 0; i < rows; i++ {
		batch.Columns["id"][i] = int64(i + 1)
		batch.Columns["time_us"][i] = int64((i + 1) * 10)
		batch.Columns["value"][i] = value
		batch.Columns["kind_code"][i] = int64(i % 3)
		batch.Columns["has_reply"][i] = int64(i % 2)
	}
	return batch
}

func transplantFuzzBatch(seed uint64, rows int) Batch {
	batch := transplantConstantBatch(rows, 0)
	for i := 0; i < rows; i++ {
		batch.Columns["time_us"][i] = int64(i)*10 + int64(seed%5)
		batch.Columns["value"][i] = splitmix64Int64(seed + uint64(i)*0x9e3779b97f4a7c15)
		batch.Columns["kind_code"][i] = int64((seed + uint64(i)) % 3)
		batch.Columns["has_reply"][i] = int64((seed >> uint(i%16)) & 1)
	}
	return batch
}

func splitmix64Int64(seed uint64) int64 {
	z := seed + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return int64(z ^ (z >> 31))
}

func setDictionaryCodeForValue(t *testing.T, image ColumnPartImage, data []byte, value string, code int64) {
	t.Helper()
	walkDictionaryEntries(t, image, data, func(name string, codeOffset int, gotCode int64, valueOffset int, gotValue string) bool {
		if name == "kind_code" && gotValue == value {
			binary.LittleEndian.PutUint64(data[codeOffset:], uint64(code))
			return true
		}
		return false
	})
}

func setDictionaryValueForCode(t *testing.T, image ColumnPartImage, data []byte, code int64, value string) {
	t.Helper()
	walkDictionaryEntries(t, image, data, func(name string, codeOffset int, gotCode int64, valueOffset int, gotValue string) bool {
		if name == "kind_code" && gotCode == code {
			if len(value) != len(gotValue) {
				t.Fatalf("replacement value %q length=%d want %d", value, len(value), len(gotValue))
			}
			copy(data[valueOffset:valueOffset+len(value)], value)
			return true
		}
		return false
	})
}

func walkDictionaryEntries(t *testing.T, image ColumnPartImage, data []byte, visit func(name string, codeOffset int, code int64, valueOffset int, value string) bool) {
	t.Helper()
	section, err := image.singleSection(ColumnPartImageSectionDictionaries)
	if err != nil {
		t.Fatalf("dictionary section: %v", err)
	}
	if section.Encoding != 0 {
		t.Fatalf("dictionary section encoding=%s does not expose legacy raw code offsets", section.Encoding)
	}
	off := section.Offset
	count := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4
	for dictIndex := 0; dictIndex < count; dictIndex++ {
		nameLen := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		name := string(data[off : off+nameLen])
		off += nameLen
		entries := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		for entryIndex := 0; entryIndex < entries; entryIndex++ {
			codeOffset := off
			gotCode := int64(binary.LittleEndian.Uint64(data[off:]))
			off += 8
			valueLen := int(binary.LittleEndian.Uint32(data[off:]))
			off += 4
			valueOffset := off
			gotValue := string(data[off : off+valueLen])
			off += valueLen
			if visit(name, codeOffset, gotCode, valueOffset, gotValue) {
				return
			}
		}
	}
	t.Fatalf("dictionary entry not found")
}
