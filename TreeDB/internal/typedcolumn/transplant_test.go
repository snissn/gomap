package typedcolumn

import (
	"encoding/binary"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestTypedColumnTransplantPartImageRoundTrip(t *testing.T) {
	part := mustTransplantPart(t, 101, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)

	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if parsed.PartID != image.PartID || parsed.Rows != image.Rows || parsed.Version != image.Version {
		t.Fatalf("parsed identity=%d/%d/%d want %d/%d/%d", parsed.PartID, parsed.Rows, parsed.Version, image.PartID, image.Rows, image.Version)
	}
	if parsed.TotalBytes() != image.TotalBytes() || parsed.ManifestBytes != image.ManifestBytes {
		t.Fatalf("parsed bytes total=%d/%d manifest=%d/%d", parsed.TotalBytes(), image.TotalBytes(), parsed.ManifestBytes, image.ManifestBytes)
	}
	if accounting := part.ByteAccountingFromImage(parsed); accounting.TotalStoredBytes != parsed.TotalBytes() || accounting.CategoryBytes() != parsed.TotalBytes() {
		t.Fatalf("accounting total/category=(%d,%d) image=%d accounting=%+v", accounting.TotalStoredBytes, accounting.CategoryBytes(), parsed.TotalBytes(), accounting)
	}

	reconstructed, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	scan, err := reconstructed.NewScanner().ScanProjected([]string{"id", "time_us", "value", "kind_code", "has_reply"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertTransplantInt64s(t, "id", scan.Columns["id"], []int64{1, 2, 3, 4, 5, 6})
	assertTransplantInt64s(t, "value", scan.Columns["value"], []int64{100, 200, 300, 400, 500, 600})

	tcs1, record, err := EncodeTCS1ColumnPartImage(parsed)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	decodedImage, decodedRecord, err := DecodeTCS1ColumnPartImage(tcs1)
	if err != nil {
		t.Fatalf("DecodeTCS1ColumnPartImage: %v", err)
	}
	if decodedImage.TotalBytes() != parsed.TotalBytes() || decodedRecord.PayloadCRC32 != record.PayloadCRC32 {
		t.Fatalf("decoded TCS1 image bytes/checksum=(%d,%08x) want (%d,%08x)", decodedImage.TotalBytes(), decodedRecord.PayloadCRC32, parsed.TotalBytes(), record.PayloadCRC32)
	}
}

func TestTypedColumnTransplantSectionDirectoryRoundTrip(t *testing.T) {
	part := mustTransplantPart(t, 102, transplantTestOptions([]SortKeyColumn{{Column: "kind_code"}, {Column: "time_us"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	if len(parsed.Sections) != len(image.Sections) {
		t.Fatalf("sections=%d want %d", len(parsed.Sections), len(image.Sections))
	}
	parsedReader := NewImageSectionReader(parsed)
	imageReader := NewImageSectionReader(image)
	for idx := range image.Sections {
		want := image.Sections[idx]
		got := parsed.Sections[idx]
		if got.Kind != want.Kind || got.Category != want.Category || got.Name != want.Name || got.Column != want.Column || got.Offset != want.Offset || got.Length != want.Length {
			t.Fatalf("section[%d]=%+v want %+v", idx, got, want)
		}
		gotBytes, err := parsedReader.ReadSection(got)
		if err != nil {
			t.Fatalf("ReadSection(parsed %s): %v", got.Kind, err)
		}
		wantBytes, err := imageReader.ReadSection(want)
		if err != nil {
			t.Fatalf("ReadSection(image %s): %v", want.Kind, err)
		}
		assertTransplantBytes(t, got.Kind, gotBytes, wantBytes)
	}
	if parsed.PaddingBytes() == 0 {
		t.Fatalf("expected aligned section directory to carry padding bytes")
	}
}

func TestTypedColumnTransplantRejectsInvalidMagicOrVersion(t *testing.T) {
	image := mustTransplantImage(t, mustTransplantPart(t, 103, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))

	badMagic := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint32(badMagic[:4], columnPartImageMagic+1)
	if _, err := ParseColumnPartImage(badMagic); err == nil || !strings.Contains(err.Error(), "invalid part image magic") {
		t.Fatalf("bad image magic err=%v want invalid magic", err)
	}
	badVersion := append([]byte(nil), image.Bytes...)
	binary.LittleEndian.PutUint16(badVersion[4:6], columnPartImageVersion+1)
	if _, err := ParseColumnPartImage(badVersion); err == nil || !strings.Contains(err.Error(), "unsupported part image version") {
		t.Fatalf("bad image version err=%v want unsupported version", err)
	}

	tcs1, _, err := EncodeTCS1ColumnPartImage(image)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	badTCS1Magic := append([]byte(nil), tcs1...)
	binary.LittleEndian.PutUint32(badTCS1Magic[tcs1MagicOffset:tcs1VersionOffset], tcs1Magic+1)
	if _, _, err := DecodeTCS1ColumnPartImage(badTCS1Magic); err == nil || !strings.Contains(err.Error(), "invalid TCS1 magic") {
		t.Fatalf("bad TCS1 magic err=%v want invalid magic", err)
	}
	badTCS1Version := append([]byte(nil), tcs1...)
	binary.LittleEndian.PutUint16(badTCS1Version[tcs1VersionOffset:tcs1KindOffset], tcs1Version+1)
	if _, _, err := DecodeTCS1ColumnPartImage(badTCS1Version); err == nil || !strings.Contains(err.Error(), "unsupported TCS1 version") {
		t.Fatalf("bad TCS1 version err=%v want unsupported version", err)
	}
}

func TestTypedColumnTransplantRejectsTruncatedOrOutOfBoundsSection(t *testing.T) {
	part := mustTransplantPart(t, 104, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	if _, err := ParseColumnPartImage(image.Bytes[:len(image.Bytes)-1]); err == nil {
		t.Fatalf("truncated image parsed successfully")
	}

	corrupt := image
	corrupt.Bytes = append([]byte(nil), image.Bytes...)
	corrupt.Sections = append([]ColumnPartImageSection(nil), image.Sections...)
	corrupt.Sections[0].Offset = alignColumnPartImageOffset(len(corrupt.Bytes) + 8)
	if _, err := ColumnPartFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "exceeds image bytes") {
		t.Fatalf("out-of-bounds section err=%v want exceeds image bytes", err)
	}

	unaligned := image
	unaligned.Bytes = append([]byte(nil), image.Bytes...)
	unaligned.Sections = append([]ColumnPartImageSection(nil), image.Sections...)
	unaligned.Sections[0].Offset++
	if _, err := ColumnPartFromImage(unaligned); err == nil || !strings.Contains(err.Error(), "aligned") {
		t.Fatalf("unaligned section err=%v want aligned", err)
	}
}

func TestTypedColumnTransplantRowLocatorRoundTrip(t *testing.T) {
	part := mustTransplantPart(t, 105, transplantTestOptions([]SortKeyColumn{{Column: "time_us"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	reconstructed, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	locator, ok := reconstructed.LocatePrimaryID(6)
	if !ok {
		t.Fatalf("missing row locator for primary id 6")
	}
	if locator.PartID != 105 || locator.PrimaryID != 6 || locator.PartRow < 0 || locator.GranuleOrdinal < 0 {
		t.Fatalf("bad locator: %+v", locator)
	}
	value, err := reconstructed.NewScanner().ValueAt(locator, "value")
	if err != nil {
		t.Fatalf("ValueAt(locator): %v", err)
	}
	if value != 600 {
		t.Fatalf("locator value=%d want 600", value)
	}
}

func TestTypedColumnTransplantContiguousRowLocatorEncoding2396(t *testing.T) {
	part := mustTransplantPart(t, 239601, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	section := mustValidationSection(t, image, ColumnPartImageSectionRowLocators)
	if section.Encoding != EncodingRowLocatorContiguous {
		t.Fatalf("row locator section encoding=%s want %s", section.Encoding, EncodingRowLocatorContiguous)
	}
	rawBytes, err := rowLocatorSectionRawBytes(image.Rows)
	if err != nil {
		t.Fatalf("rowLocatorSectionRawBytes: %v", err)
	}
	if section.Length >= rawBytes {
		t.Fatalf("compact row locator bytes=%d want below legacy raw=%d", section.Length, rawBytes)
	}
	if section.RawBytes != rowLocatorContiguousPayloadBytes || section.Length != rowLocatorContiguousPayloadBytes {
		t.Fatalf("compact section raw/length=(%d,%d) want %d", section.RawBytes, section.Length, rowLocatorContiguousPayloadBytes)
	}
	accounting := image.SectionByteAccounting()
	found := false
	for _, sectionAccounting := range accounting {
		if sectionAccounting.Kind != ColumnPartImageSectionRowLocators {
			continue
		}
		found = true
		if sectionAccounting.RawBytes != rawBytes || sectionAccounting.StoredBytes != rowLocatorContiguousPayloadBytes {
			t.Fatalf("locator accounting=%+v want logical raw=%d stored=%d", sectionAccounting, rawBytes, rowLocatorContiguousPayloadBytes)
		}
	}
	if !found {
		t.Fatalf("missing row locator accounting in %+v", accounting)
	}
	reconstructed, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	for primaryID, wantRow := range map[int64]int{1: 0, 6: 5} {
		locator, ok := reconstructed.LocatePrimaryID(primaryID)
		if !ok {
			t.Fatalf("missing locator for primary id %d", primaryID)
		}
		if locator.PartID != part.Descriptor.PartID || locator.PartRow != wantRow {
			t.Fatalf("locator for primary id %d=%+v want part=%d row=%d", primaryID, locator, part.Descriptor.PartID, wantRow)
		}
	}
}

func TestTypedColumnTransplantRowLocatorRawFallbackForSparsePrimaryIDs2396(t *testing.T) {
	batch := transplantTestBatch()
	batch.Columns["id"] = []int64{30, 10, 20, 60, 40, 50}
	part := mustTransplantPart(t, 239602, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), batch)
	image := mustTransplantImage(t, part)
	section := mustValidationSection(t, image, ColumnPartImageSectionRowLocators)
	if section.Encoding != 0 {
		t.Fatalf("row locator section encoding=%s want legacy raw fallback", section.Encoding)
	}
	rawBytes, err := rowLocatorSectionRawBytes(image.Rows)
	if err != nil {
		t.Fatalf("rowLocatorSectionRawBytes: %v", err)
	}
	if section.Length != rawBytes {
		t.Fatalf("raw fallback section bytes=%d want %d", section.Length, rawBytes)
	}
	reconstructed, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	if locator, ok := reconstructed.LocatePrimaryID(60); !ok || locator.PartRow != 5 {
		t.Fatalf("locator for sparse primary id 60=%+v ok=%v want row 5", locator, ok)
	}
}

func TestTypedColumnTransplantContiguousRowLocatorCorruptionFailsClosed2396(t *testing.T) {
	part := mustTransplantPart(t, 239603, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	section := mustValidationSection(t, image, ColumnPartImageSectionRowLocators)
	if section.Encoding != EncodingRowLocatorContiguous {
		t.Fatalf("row locator section encoding=%s want %s", section.Encoding, EncodingRowLocatorContiguous)
	}
	corrupt := cloneColumnPartImageBytes(image)
	binary.LittleEndian.PutUint16(corrupt.Bytes[section.Offset+6:section.Offset+8], 1)
	if _, err := ColumnPartFromImage(corrupt); err == nil || !strings.Contains(err.Error(), "contiguous row locator reserved") {
		t.Fatalf("ColumnPartFromImage corrupt compact locator err=%v want reserved fail-closed", err)
	}
}

func TestTypedColumnTransplantPartSetLatestVisibleRows(t *testing.T) {
	opts := transplantTestOptions([]SortKeyColumn{{Column: "id"}})
	base := mustTransplantPart(t, 201, opts, Batch{Columns: map[string][]int64{
		"id":        {1, 2, 3},
		"time_us":   {10, 20, 30},
		"value":     {100, 200, 300},
		"kind_code": {0, 1, 2},
		"has_reply": {1, 0, 1},
	}})
	delta := mustTransplantPart(t, 202, opts, Batch{Columns: map[string][]int64{
		"id":        {2, 4},
		"time_us":   {25, 40},
		"value":     {250, 400},
		"kind_code": {1, 0},
		"has_reply": {1, 0},
	}})
	reader, err := NewPartSetReader([]PartRef{
		{Role: PartRoleBase, GenerationID: 1, Part: base},
		{Role: PartRoleDelta, GenerationID: 2, Part: delta},
	}, []Tombstone{{PrimaryID: 3, GenerationID: 2}})
	if err != nil {
		t.Fatalf("NewPartSetReader: %v", err)
	}
	stats := reader.VisibilityStats()
	if stats.Parts != 2 || stats.BaseParts != 1 || stats.DeltaParts != 1 || stats.InputRows != 5 || stats.VisibleRows != 3 || stats.SupersededRows != 1 || stats.DeletedRows != 1 {
		t.Fatalf("visibility stats=%+v", stats)
	}
	value, ok, err := reader.ValueAtLatest(2, "value")
	if err != nil || !ok || value != 250 {
		t.Fatalf("latest id=2 value/ok/err=(%d,%v,%v) want 250,true,nil", value, ok, err)
	}
	if _, ok := reader.LatestLocator(3); ok {
		t.Fatalf("deleted primary id 3 remained visible")
	}
	if locator, ok := reader.ScanLatestLocator(4); !ok || locator.PartID != 202 {
		t.Fatalf("scan latest id=4 locator=%+v ok=%v want part 202", locator, ok)
	}
	baseRows, baseAll := reader.VisibleRowsForPart(0)
	deltaRows, deltaAll := reader.VisibleRowsForPart(1)
	if !slices.Equal(baseRows, []int{0}) || baseAll || !slices.Equal(deltaRows, []int{0, 1}) || !deltaAll {
		t.Fatalf("visible rows base=%v all=%v delta=%v all=%v", baseRows, baseAll, deltaRows, deltaAll)
	}
}

func TestTypedColumnTransplantPartSetRejectsMissingLocators(t *testing.T) {
	part := mustTransplantPart(t, 203, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	scanOnlyPart, err := ColumnPartFromImageWithOptions(image, ColumnPartImageReadOptions{})
	if err != nil {
		t.Fatalf("ColumnPartFromImageWithOptions(scan-only): %v", err)
	}
	if _, err := NewPartSetReader([]PartRef{{Role: PartRoleBase, GenerationID: 1, Part: scanOnlyPart}}, nil); err == nil || !strings.Contains(err.Error(), "row locator count=0") {
		t.Fatalf("scan-only part set err=%v want missing row locator count", err)
	}

	partial := clonePartWithLocators(part)
	delete(partial.Locators, int64(1))
	if _, err := NewPartSetReader([]PartRef{{Role: PartRoleBase, GenerationID: 1, Part: partial}}, nil); err == nil || !strings.Contains(err.Error(), "row locator count=5") {
		t.Fatalf("partial locator part set err=%v want partial row locator count", err)
	}

	duplicate := clonePartWithLocators(part)
	first, ok := duplicate.LocatePrimaryID(1)
	if !ok {
		t.Fatalf("missing locator for id 1")
	}
	duplicate.Locators[2] = RowLocator{
		PrimaryID:      2,
		PartID:         first.PartID,
		PartRow:        first.PartRow,
		GranuleOrdinal: first.GranuleOrdinal,
		RowInGranule:   first.RowInGranule,
	}
	if _, err := NewPartSetReader([]PartRef{{Role: PartRoleBase, GenerationID: 1, Part: duplicate}}, nil); err == nil || !strings.Contains(err.Error(), "duplicate row locator part row") {
		t.Fatalf("duplicate locator part set err=%v want duplicate row locator", err)
	}
}

func TestTypedColumnTransplantPredicateMetadataRoundTrip(t *testing.T) {
	part := mustTransplantPart(t, 106, transplantTestOptions([]SortKeyColumn{{Column: "kind_code"}, {Column: "time_us"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	reconstructed, err := ColumnPartFromImage(image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	if len(reconstructed.Marks) != len(part.Marks) || len(reconstructed.Marks) == 0 {
		t.Fatalf("marks=%d want %d", len(reconstructed.Marks), len(part.Marks))
	}
	mayContain, constrained, err := reconstructed.Marks[0].MayContainRanges([]Int64RangePredicate{{Column: "kind_code", Low: 2, High: 2}})
	if err != nil {
		t.Fatalf("MayContainRanges: %v", err)
	}
	if mayContain || !constrained {
		t.Fatalf("mark predicate mayContain/constrained=(%v,%v) want false,true", mayContain, constrained)
	}
	mayContain, constrained, err = reconstructed.Marks[len(reconstructed.Marks)-1].MayContainRanges([]Int64RangePredicate{{Column: "kind_code", Low: 0, High: 0}})
	if err != nil {
		t.Fatalf("MayContainRanges(last mark): %v", err)
	}
	if mayContain || !constrained {
		t.Fatalf("last mark predicate mayContain/constrained=(%v,%v) want false,true", mayContain, constrained)
	}
}

func TestTypedColumnTransplantBuildRejectsUndeclaredBatchColumn(t *testing.T) {
	batch := transplantTestBatch()
	batch.Columns["extra"] = []int64{1, 2, 3, 4, 5, 6}
	_, err := BuildColumnPart(204, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), batch)
	if err == nil || !strings.Contains(err.Error(), "undeclared column extra") {
		t.Fatalf("BuildColumnPart undeclared err=%v want undeclared column", err)
	}
}

func TestTypedColumnTransplantEncodeInt64PayloadResetsDestination(t *testing.T) {
	for _, encoding := range []Encoding{EncodingDeltaVarint, EncodingDoubleDeltaVarint} {
		got, err := encodeInt64Payload([]byte{0xff, 0xff, 0xff}, []int64{1, 2, 3}, encoding)
		if err != nil {
			t.Fatalf("encodeInt64Payload(%s): %v", encoding, err)
		}
		if len(got) == 0 || got[0] == 0xff {
			t.Fatalf("encodeInt64Payload(%s) kept stale destination prefix: %v", encoding, got)
		}
	}
}

func TestTypedColumnTransplantScanColumnRowsIntoResetsDestination(t *testing.T) {
	part := mustTransplantPart(t, 205, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	scanner := part.NewScanner()
	got, _, err := scanner.scanColumnRowsInto("value", []int64{999, 888}, []int{0, 2})
	if err != nil {
		t.Fatalf("scanColumnRowsInto: %v", err)
	}
	assertTransplantInt64s(t, "selected values", got, []int64{100, 300})
}

func TestTypedColumnTransplantBuildImageRejectsLocatorKeyMismatch(t *testing.T) {
	part := clonePartWithLocators(mustTransplantPart(t, 206, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	locator := part.Locators[1]
	locator.PrimaryID = 999
	part.Locators[1] = locator
	_, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err == nil || !strings.Contains(err.Error(), "row locator key 1 has primary id 999") {
		t.Fatalf("BuildColumnPartImage locator mismatch err=%v want primary id mismatch", err)
	}
}

func TestTypedColumnTransplantDictionaryAggregateDescriptorsRoundTrip(t *testing.T) {
	opts := transplantTestOptions([]SortKeyColumn{{Column: "id"}})
	opts.AggregateMetadata = []AggregateMetadataDefinition{transplantAggregateMetadataDefinition()}
	part := mustTransplantPart(t, 107, opts, transplantTestBatch())
	image := mustTransplantImage(t, part)
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	dictionaries, err := parsed.Dictionaries()
	if err != nil {
		t.Fatalf("Dictionaries: %v", err)
	}
	if dictionaries["kind_code"]["reply"] != 1 || dictionaries["kind_code"]["system"] != 2 {
		t.Fatalf("dictionary round trip: %+v", dictionaries)
	}
	reconstructed, err := ColumnPartFromImage(parsed)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	metadata, ok := reconstructed.AggregateMetadataByName("kind_time")
	if !ok {
		t.Fatalf("missing aggregate metadata")
	}
	if metadata.Definition.Kind != AggregateMetadataGroupMinMax || metadata.Definition.Scope != AggregateMetadataScopeGranule || len(metadata.Definition.Predicates) != 1 {
		t.Fatalf("aggregate definition=%+v", metadata.Definition)
	}
	if !metadata.Stats.Admitted || metadata.Stats.RowsMatched == 0 || len(metadata.Granules) == 0 {
		t.Fatalf("aggregate stats=%+v granules=%d", metadata.Stats, len(metadata.Granules))
	}
}

func TestTypedColumnTransplantDenseDictionaryEncoding2396(t *testing.T) {
	dictionary := map[string]map[string]int64{
		"kind_code": {"user": 0, "reply": 1, "system": 2},
	}
	part := mustTransplantPart(t, 2396, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: dictionary})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	section := mustValidationSection(t, image, ColumnPartImageSectionDictionaries)
	if section.Encoding != EncodingDictionaryDense {
		t.Fatalf("dictionary section encoding=%s want %s", section.Encoding, EncodingDictionaryDense)
	}
	rawData, compactData, compactOK := encodeDictionarySectionPayloads(dictionary)
	if !compactOK || len(compactData) >= len(rawData) {
		t.Fatalf("compact dictionary sizing raw=%d compact=%d ok=%v", len(rawData), len(compactData), compactOK)
	}
	if section.Length != len(compactData) || section.RawBytes != len(compactData) {
		t.Fatalf("dictionary section length/raw=%d/%d want compact bytes=%d", section.Length, section.RawBytes, len(compactData))
	}
	accounting := image.SectionByteAccounting()
	foundAccounting := false
	for _, row := range accounting {
		if row.Kind == ColumnPartImageSectionDictionaries {
			foundAccounting = true
			if row.Bytes != len(compactData) || row.RawBytes != len(compactData) || row.StoredBytes != len(compactData) {
				t.Fatalf("dictionary accounting=%+v want compact bytes=%d", row, len(compactData))
			}
		}
	}
	if !foundAccounting {
		t.Fatalf("dictionary section missing from accounting: %+v", accounting)
	}
	parsed, err := ParseColumnPartImage(image.Bytes)
	if err != nil {
		t.Fatalf("ParseColumnPartImage: %v", err)
	}
	dictionaries, err := parsed.Dictionaries()
	if err != nil {
		t.Fatalf("Dictionaries: %v", err)
	}
	if dictionaries["kind_code"]["reply"] != 1 || dictionaries["kind_code"]["system"] != 2 {
		t.Fatalf("dense dictionary round trip: %+v", dictionaries)
	}
	if _, err := ColumnPartFromImage(parsed); err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	tcs1, _, err := EncodeTCS1ColumnPartImage(parsed)
	if err != nil {
		t.Fatalf("EncodeTCS1ColumnPartImage: %v", err)
	}
	decoded, _, err := DecodeTCS1ColumnPartImage(tcs1)
	if err != nil {
		t.Fatalf("DecodeTCS1ColumnPartImage: %v", err)
	}
	decodedSection := mustValidationSection(t, decoded, ColumnPartImageSectionDictionaries)
	if decodedSection.Encoding != EncodingDictionaryDense {
		t.Fatalf("decoded TCS1 dictionary encoding=%s want %s", decodedSection.Encoding, EncodingDictionaryDense)
	}
}

func TestTypedColumnTransplantDictionaryRawFallbackForSparseCodes2396(t *testing.T) {
	part := mustTransplantPart(t, 2396, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
		"kind_code": {"user": 0, "reply": 1, "system": 3},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	section := mustValidationSection(t, image, ColumnPartImageSectionDictionaries)
	if section.Encoding != 0 {
		t.Fatalf("dictionary section encoding=%s want legacy raw fallback", section.Encoding)
	}
	if _, err := image.Dictionaries(); err == nil || !strings.Contains(err.Error(), "outside cardinality") {
		t.Fatalf("Dictionaries sparse err=%v want descriptor validation failure after raw fallback", err)
	}
}

func TestTypedColumnTransplantDenseDictionaryCorruptionFailsClosed2396(t *testing.T) {
	part := mustTransplantPart(t, 2396, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image := mustTransplantImage(t, part)
	section := mustValidationSection(t, image, ColumnPartImageSectionDictionaries)
	if section.Encoding != EncodingDictionaryDense {
		t.Fatalf("dictionary section encoding=%s want %s", section.Encoding, EncodingDictionaryDense)
	}

	t.Run("bad_magic", func(t *testing.T) {
		corrupt := image
		corrupt.Bytes = append([]byte(nil), image.Bytes...)
		corrupt.Bytes[section.Offset] ^= 0xff
		_, err := corrupt.Dictionaries()
		requireTypedColumnErrContains(t, err, "invalid dense dictionary magic")
	})

	t.Run("unsupported_encoding", func(t *testing.T) {
		corrupt := image
		corrupt.Sections = append([]ColumnPartImageSection(nil), image.Sections...)
		for i := range corrupt.Sections {
			if corrupt.Sections[i].Kind == ColumnPartImageSectionDictionaries {
				corrupt.Sections[i].Encoding = Encoding(250)
				break
			}
		}
		_, err := corrupt.Dictionaries()
		requireTypedColumnErrContains(t, err, "dictionaries encoding=encoding_250 is unsupported")
	})
}

func TestTypedColumnTransplantAggregateMetadataByNameReturnsDeepCopy(t *testing.T) {
	opts := transplantTestOptions([]SortKeyColumn{{Column: "id"}})
	opts.AggregateMetadata = []AggregateMetadataDefinition{transplantAggregateMetadataDefinition()}
	part := mustTransplantPart(t, 109, opts, transplantTestBatch())
	metadata, ok := part.AggregateMetadataByName("kind_time")
	if !ok || len(metadata.Definition.GroupKeys) == 0 || len(metadata.Granules) == 0 || len(metadata.Granules[0].Entries) == 0 {
		t.Fatalf("missing aggregate metadata shape: ok=%v metadata=%+v", ok, metadata)
	}
	metadata.Definition.GroupKeys[0] = "mutated"
	metadata.Granules[0].Entries[0].Count = 999

	again, ok := part.AggregateMetadataByName("kind_time")
	if !ok {
		t.Fatalf("missing aggregate metadata after mutation")
	}
	if again.Definition.GroupKeys[0] == "mutated" || again.Granules[0].Entries[0].Count == 999 {
		t.Fatalf("AggregateMetadataByName returned mutable internals: %+v", again)
	}
}

func TestTypedColumnTransplantFixedWidthSectionsAreAligned(t *testing.T) {
	image := mustTransplantImage(t, mustTransplantPart(t, 108, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	for _, section := range image.Sections {
		if section.Offset%columnPartImageSectionAlignment != 0 {
			t.Fatalf("section %s offset=%d is not %d-byte aligned", section.Kind, section.Offset, columnPartImageSectionAlignment)
		}
	}
	if _, err := ParseColumnPartImage(image.Bytes); err != nil {
		t.Fatalf("ParseColumnPartImage aligned image: %v", err)
	}
}

func TestTypedColumnTransplantRequestedSectionAlignment4234(t *testing.T) {
	part := mustTransplantPart(t, 108, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch())
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{SectionAlignment: 64})
	if err != nil {
		t.Fatalf("BuildColumnPartImage 64-byte alignment: %v", err)
	}
	for _, section := range image.Sections {
		if section.Offset%64 != 0 {
			t.Fatalf("section %s offset=%d is not 64-byte aligned", section.Kind, section.Offset)
		}
	}
	if padding := image.PaddingBytes(); padding > len(image.Sections)*63 {
		t.Fatalf("padding=%d exceeds bounded 64-byte section padding=%d", padding, len(image.Sections)*63)
	}
	if cap(image.Bytes) != image.TotalBytes() {
		t.Fatalf("image capacity=%d want final image bytes=%d", cap(image.Bytes), image.TotalBytes())
	}
	if _, err := ParseColumnPartImage(image.Bytes); err != nil {
		t.Fatalf("ParseColumnPartImage 64-byte aligned image: %v", err)
	}
	if _, err := BuildColumnPartImage(part, ColumnPartImageOptions{SectionAlignment: 16}); err == nil || !strings.Contains(err.Error(), "section alignment") {
		t.Fatalf("unsupported section alignment err=%v want rejection", err)
	}
}

func TestTypedColumnTransplantNoProductionPublication(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	collectionsDir := filepath.Join(repoRoot, "TreeDB", "collections")
	allowedImports := map[string]struct{}{
		filepath.Clean(filepath.Join(collectionsDir, "typed_column_adapter.go")): {},
		// #1929 keeps primitive scalar adapter build/read helpers in a narrow
		// typed-column adapter extension rather than publishing a generic data plane.
		filepath.Clean(filepath.Join(collectionsDir, "typed_column_scalar_primitive.go")): {},
		// #1952 is the scoped production codec/layout capability guard used by
		// the adapter and prepared-state seams without adding a new data plane.
		filepath.Clean(filepath.Join(collectionsDir, "typed_column_capability.go")): {},
		// #1952 is the benchmark-only opt-in policy parser for the typed-column
		// publication adapter; it is gated by the benchmark-relaxed profile.
		filepath.Clean(filepath.Join(collectionsDir, "typed_column_benchmark_policy.go")): {},
		// #1837 is the scoped prepared typed-column scan/session state seam
		// shared by concrete collection hot paths.
		filepath.Clean(filepath.Join(collectionsDir, "typed_column_prepared_state.go")): {},
		filepath.Clean(filepath.Join(collectionsDir, "typed_column_prepared_int64.go")): {},
		// #1845 is the scoped bool typed-column predicate/count slice using the
		// same prepared-state and row-selection substrate.
		filepath.Clean(filepath.Join(collectionsDir, "typed_column_bool_scan.go")): {},
		// #1782 is the scoped production vector graph reader that consumes
		// validated typed-column dense vector sections through mappedresource.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_typed_column.go")): {},
		// #1918 is the scoped production vector graph writer/validator that publishes
		// a raw_uint32_offsets_list layer-0 adjacency source through typed-column assets.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_adjacency_source.go")): {},
		// #1919 is the scoped production vector graph reader that consumes the
		// certified layer-0 raw_uint32_offsets_list adjacency source.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_adjacency_direct_source.go")): {},
		// #1987 is the scoped production vector-index state writer/validator that
		// publishes HNSW adjacency as generic uint32_list typed-column assets.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_index_state_adjacency.go")): {},
		// #1988 is the scoped production vector graph reader that consumes HNSW
		// adjacency from generic uint32_list vector-index state assets.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_adjacency_state_source.go")): {},
		// #1992 is the scoped production vector graph writer/reader that publishes
		// and consumes raw_float32 inverse-norm state through typed-column assets.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_inv_norm_state.go")): {},
		// #1993 is the scoped production vector graph writer/reader that publishes
		// and consumes raw_int64 row-reference state through typed-column assets.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_row_ref_state.go")): {},
		// #2013 is the scoped production vector graph writer/reader that publishes
		// and consumes raw_bytes_offsets document-ID state through typed-column assets.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_document_id_state.go")): {},
		// #1926 is the scoped production vector graph writer/reader that publishes
		// and validates scalar_u8 quantized code assets through typed-column storage.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_quantized_asset.go")): {},
		// #2041 is the scoped production vector graph prepared-view helper that
		// certifies row-ref and document-ID side-channel typed-column sections.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_prepared_state.go")): {},
		// #1848 keeps vector graph candidate filtering on the shared row-selection
		// substrate without exposing generic scalar typed-column scans.
		filepath.Clean(filepath.Join(collectionsDir, "column_vector_graph_search.go")): {},
		// #4617 reuses RowSelection for internal prepared scalar eligibility
		// and bounded filtered-pack candidate admission/seed enumeration. These
		// consumers do not own publication or introduce a new durable data plane.
		filepath.Clean(filepath.Join(collectionsDir, "typed_graph_filter.go")):        {},
		filepath.Clean(filepath.Join(collectionsDir, "typed_graph_filtered_pack.go")): {},
		// #1949 is the scoped production typed-column SortKey mark-pruning planner
		// that consumes validated section marks without publishing a new data plane.
		filepath.Clean(filepath.Join(collectionsDir, "column_physical_sortkey_pruning.go")): {},
		// #2118 is the scoped production physical-accounting reporter that
		// reads validated typed-column part images for byte accounting only.
		filepath.Clean(filepath.Join(collectionsDir, "column_store_physical_accounting.go")): {},
		// #3681 validates candidate typed-column part images only to determine
		// whether an older recoverable root can replay them. The GC path owns no
		// typed-column publication or query data plane.
		filepath.Clean(filepath.Join(collectionsDir, "column_asset_gc.go")): {},
		// #3698 is the scoped query-independent generation-open seam. It binds
		// validated QRBG/QRDG direct views to the existing collection physical
		// identity and reader leases without exposing query-shaped operators or
		// publication ownership.
		filepath.Clean(filepath.Join(collectionsDir, "query_ready_generation_open.go")): {},
		// #3700 is the scoped query-ready build/handoff bridge. It writes
		// rebuildable prepared generations through the existing asset manager;
		// it does not publish a root or introduce another recovery data plane.
		filepath.Clean(filepath.Join(collectionsDir, "query_ready_build.go")): {},
		// #3699 is the scoped shared encoded q1-q5/qexpr execution seam over
		// already-selected query-ready base-plus-delta generations. It does not
		// own publication, retention, document materialization, or legacy scans.
		filepath.Clean(filepath.Join(collectionsDir, "query_ready_execution.go")): {},
		// #3699 prepares the recovery-authoritative insert-only typed-column
		// inventory through the approved M5 asset-manager bridge. The QRBG stays
		// rebuildable and non-authoritative and owns no publication root.
		filepath.Clean(filepath.Join(collectionsDir, "query_ready_preparation.go")): {},
	}
	fset := token.NewFileSet()
	err := filepath.WalkDir(collectionsDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imp := range file.Imports {
			if strings.Trim(imp.Path.Value, "\"") != "github.com/snissn/gomap/TreeDB/internal/typedcolumn" {
				continue
			}
			if _, ok := allowedImports[filepath.Clean(path)]; ok {
				continue
			}
			t.Fatalf("production collections import typedcolumn in %s; imports must stay in the exact approved seams (#1754 adapter, #1782 vector graph, #1949 sort-key pruning, #1993 row-ref state, #2013 document-ID state, #2041 prepared views, #2118 physical accounting, #3698 query-ready generation open, #3699 query-ready encoded execution, #3700 query-ready build handoff, #4617 internal RowSelection consumers without publication ownership)", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk collections: %v", err)
	}
}

func transplantTestOptions(sortKey []SortKeyColumn) Options {
	return Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "time_us", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, Compression: CompressionNone},
			{Name: "value", Type: ColumnTypeInt64, Encoding: EncodingDeltaVarint, Compression: CompressionNone},
			{Name: "kind_code", Type: ColumnTypeLowCardinalityCode, Compression: CompressionNone, Cardinality: 3},
			{Name: "has_reply", Type: ColumnTypeBool, Compression: CompressionNone},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: sortKey},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 2},
	}
}

func transplantTestBatch() Batch {
	return Batch{Columns: map[string][]int64{
		"id":        {3, 1, 2, 6, 4, 5},
		"time_us":   {30, 10, 20, 60, 40, 50},
		"value":     {300, 100, 200, 600, 400, 500},
		"kind_code": {1, 0, 0, 2, 1, 1},
		"has_reply": {1, 1, 0, 1, 0, 1},
	}}
}

func transplantAggregateMetadataDefinition() AggregateMetadataDefinition {
	return AggregateMetadataDefinition{
		Name:      "kind_time",
		Version:   AggregateMetadataDefinitionVersion,
		Kind:      AggregateMetadataGroupMinMax,
		Scope:     AggregateMetadataScopeGranule,
		GroupKeys: []string{"kind_code"},
		Measures: []AggregateMetadataMeasure{
			{Op: AggregateMetadataMeasureCount},
			{Op: AggregateMetadataMeasureMin, Column: "time_us"},
			{Op: AggregateMetadataMeasureMax, Column: "time_us"},
		},
		Predicates:     []AggregateMetadataPredicate{{Column: "has_reply", Op: AggregateMetadataPredicateEq, Value: 1}},
		MaxBytesPerRow: 256,
	}
}

func mustTransplantPart(t *testing.T, partID uint64, opts Options, batch Batch) *ColumnPart {
	t.Helper()
	part, err := BuildColumnPart(partID, opts, batch)
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	return part
}

func mustTransplantImage(t *testing.T, part *ColumnPart) ColumnPartImage {
	t.Helper()
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
		"kind_code": {"user": 0, "reply": 1, "system": 2},
	}})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	return image
}

func clonePartWithLocators(part *ColumnPart) *ColumnPart {
	clone := *part
	clone.Locators = make(map[int64]RowLocator, len(part.Locators))
	for primaryID, locator := range part.Locators {
		clone.Locators[primaryID] = locator
	}
	return &clone
}

func assertTransplantInt64s(t *testing.T, name string, got []int64, want []int64) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s=%v want %v", name, got, want)
	}
}

func assertTransplantBytes(t *testing.T, kind ColumnPartImageSectionKind, got []byte, want []byte) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("section %s bytes differ: got %d bytes want %d bytes", kind, len(got), len(want))
	}
}
