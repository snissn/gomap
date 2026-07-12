package typedcolumn

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestQueryReadyBaseGenerationReopenParity(t *testing.T) {
	identity := queryReadyBaseTestIdentity(41)
	image := mustTransplantImage(t, mustTransplantPart(t, 701, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 17, Image: image}})
	if err != nil {
		t.Fatalf("BuildQueryReadyBaseGeneration: %v", err)
	}
	path := filepath.Join(t.TempDir(), "base.qrb")
	if err := os.WriteFile(path, result.Bytes, 0o600); err != nil {
		t.Fatalf("write base: %v", err)
	}
	var base *QueryReadyBaseGeneration
	if queryReadyBaseMmapSupported() {
		base, err = OpenQueryReadyBaseGenerationFile(path, identity)
		if err != nil {
			t.Fatalf("OpenQueryReadyBaseGenerationFile: %v", err)
		}
	} else {
		if _, err := OpenQueryReadyBaseGenerationFile(path, identity); err == nil || !strings.Contains(err.Error(), "mmap") {
			t.Fatalf("unsupported mmap err=%v", err)
		}
		reopenedBytes, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read base: %v", err)
		}
		base, err = OpenQueryReadyBaseGeneration(reopenedBytes, identity)
		if err != nil {
			t.Fatalf("OpenQueryReadyBaseGeneration: %v", err)
		}
	}
	t.Cleanup(func() { _ = base.Close() })
	if len(base.Parts) != 1 || base.Parts[0].Dependency.SourceGeneration != 17 {
		t.Fatalf("parts=%+v", base.Parts)
	}
	part, err := ColumnPartFromImage(base.Parts[0].Image)
	if err != nil {
		t.Fatalf("ColumnPartFromImage: %v", err)
	}
	scan, err := part.NewScanner().ScanProjected([]string{"value", "kind_code"})
	if err != nil {
		t.Fatalf("ScanProjected: %v", err)
	}
	assertTransplantInt64s(t, "value", scan.Columns["value"], []int64{100, 200, 300, 400, 500, 600})
	assertTransplantInt64s(t, "kind_code", scan.Columns["kind_code"], []int64{0, 0, 1, 1, 1, 2})
}

func TestQueryReadyBaseGenerationRejectsCorruptionAndTruncation(t *testing.T) {
	identity := queryReadyBaseTestIdentity(42)
	image := mustTransplantImage(t, mustTransplantPart(t, 702, transplantTestOptions(nil), transplantTestBatch()))
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 18, Image: image}})
	if err != nil {
		t.Fatalf("BuildQueryReadyBaseGeneration: %v", err)
	}
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{name: "truncated", data: result.Bytes[:len(result.Bytes)-1], want: "total bytes"},
		{name: "payload checksum", data: queryReadyBaseCorruptLastByte(result.Bytes), want: "checksum"},
		{name: "padding", data: queryReadyBaseCorruptByte(result.Bytes, queryReadyBaseHeaderBytes+queryReadyBasePartEntryBytes), want: "padding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := OpenQueryReadyBaseGeneration(tc.data, identity)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want containing %q", err, tc.want)
			}
		})
	}
}

func TestQueryReadyBaseGenerationRejectsUnaccountedTrailingBytes(t *testing.T) {
	identity := queryReadyBaseTestIdentity(48)
	image := mustTransplantImage(t, mustTransplantPart(t, 708, transplantTestOptions(nil), transplantTestBatch()))
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 25, Image: image}})
	if err != nil {
		t.Fatalf("BuildQueryReadyBaseGeneration: %v", err)
	}
	shorter := mustTransplantImage(t, mustTransplantPart(t, 708, transplantTestOptions(nil), transplantConstantBatch(1, 42)))
	forged := queryReadyBaseReplaceFinalPartWithShorterImage(t, result.Bytes, shorter)
	if _, err := OpenQueryReadyBaseGeneration(forged, identity); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("forged trailing bytes err=%v want trailing rejection", err)
	}
}

func TestQueryReadyBaseGenerationRejectsSchemaOrGenerationMismatch(t *testing.T) {
	identity := queryReadyBaseTestIdentity(43)
	image := mustTransplantImage(t, mustTransplantPart(t, 703, transplantTestOptions(nil), transplantTestBatch()))
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 19, Image: image}})
	if err != nil {
		t.Fatalf("BuildQueryReadyBaseGeneration: %v", err)
	}
	wrongGeneration := identity
	wrongGeneration.Generation++
	if _, err := OpenQueryReadyBaseGeneration(result.Bytes, wrongGeneration); err == nil || !strings.Contains(err.Error(), "generation") {
		t.Fatalf("generation mismatch err=%v", err)
	}
	wrongSchema := identity
	wrongSchema.SchemaHash[0] ^= 0xff
	if _, err := OpenQueryReadyBaseGeneration(result.Bytes, wrongSchema); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("schema mismatch err=%v", err)
	}
}

func TestQueryReadyBaseGenerationBuildIsDeterministic(t *testing.T) {
	identity := queryReadyBaseTestIdentity(44)
	left := mustTransplantImage(t, mustTransplantPart(t, 705, transplantTestOptions(nil), transplantTestBatch()))
	right := mustTransplantImage(t, mustTransplantPart(t, 704, transplantTestOptions(nil), transplantTestBatch()))
	a, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 22, Image: left}, {SourceGeneration: 21, Image: right}})
	if err != nil {
		t.Fatalf("build a: %v", err)
	}
	b, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 21, Image: right}, {SourceGeneration: 22, Image: left}})
	if err != nil {
		t.Fatalf("build b: %v", err)
	}
	if !slices.Equal(a.Bytes, b.Bytes) {
		t.Fatalf("deterministic builds differ: %d/%d bytes", len(a.Bytes), len(b.Bytes))
	}
	if a.Dependencies[0].PartID != 704 || a.Dependencies[1].PartID != 705 {
		t.Fatalf("dependency order=%+v", a.Dependencies)
	}
}

func TestQueryReadyBaseGenerationEmptyBaseReopens(t *testing.T) {
	identity := queryReadyBaseTestIdentity(47)
	result, err := BuildQueryReadyBaseGeneration(identity, nil)
	if err != nil {
		t.Fatalf("build empty base: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(result.Bytes, identity)
	if err != nil {
		t.Fatalf("open empty base: %v", err)
	}
	if len(base.Parts) != 0 || base.Stats.Rows != 0 || base.Stats.WholePartDecodes != 0 {
		t.Fatalf("empty base=%+v stats=%+v", base.Parts, base.Stats)
	}
}

func TestQueryReadyBaseGenerationOpenAvoidsWholePartDecode(t *testing.T) {
	identity := queryReadyBaseTestIdentity(45)
	image := mustTransplantImage(t, mustTransplantPart(t, 706, transplantTestOptions(nil), transplantTestBatch()))
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 23, Image: image}})
	if err != nil {
		t.Fatalf("BuildQueryReadyBaseGeneration: %v", err)
	}
	path := filepath.Join(t.TempDir(), "base.qrb")
	if err := os.WriteFile(path, result.Bytes, 0o600); err != nil {
		t.Fatalf("write base: %v", err)
	}
	var base *QueryReadyBaseGeneration
	if queryReadyBaseMmapSupported() {
		base, err = OpenQueryReadyBaseGenerationFile(path, identity)
		if err != nil {
			t.Fatalf("open base: %v", err)
		}
	} else {
		if _, err := OpenQueryReadyBaseGenerationFile(path, identity); err == nil || !strings.Contains(err.Error(), "mmap") {
			t.Fatalf("unsupported mmap err=%v", err)
		}
		base, err = OpenQueryReadyBaseGeneration(result.Bytes, identity)
		if err != nil {
			t.Fatalf("open in-memory base: %v", err)
		}
	}
	t.Cleanup(func() { _ = base.Close() })
	if base.Stats.Mapped != queryReadyBaseMmapSupported() || base.Stats.BytesCopied != 0 || base.Stats.WholePartDecodes != 0 || base.Stats.DictionaryConstructions != 0 {
		t.Fatalf("open stats=%+v", base.Stats)
	}
	if base.Stats.BytesRead != int64(len(result.Bytes)) || base.Stats.BytesValidated != int64(len(result.Bytes)) {
		t.Fatalf("read/validated bytes=%d/%d want %d", base.Stats.BytesRead, base.Stats.BytesValidated, len(result.Bytes))
	}
	if base.Stats.BytesDecoded >= int64(len(result.Bytes)) {
		t.Fatalf("decoded bytes=%d want below asset bytes=%d", base.Stats.BytesDecoded, len(result.Bytes))
	}
	if len(base.Parts[0].Image.Bytes) == 0 || &base.Parts[0].Image.Bytes[0] != &base.Bytes()[base.Parts[0].Offset] {
		t.Fatalf("part image is not a direct view of mapped base bytes")
	}
}

func TestQueryReadyBaseGenerationPreservesNullableAndDictionaryDomains(t *testing.T) {
	opts := transplantTestOptions(nil)
	opts.Columns[2].Encoding = EncodingNullableInt64
	batch := transplantTestBatch()
	batch.Nulls = map[string][]bool{"value": {false, true, false, false, true, false}}
	part := mustTransplantPart(t, 707, opts, batch)
	image := mustTransplantImage(t, part)
	identity := queryReadyBaseTestIdentity(46)
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 24, Image: image}})
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(result.Bytes, identity)
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	reopened, err := ColumnPartFromImage(base.Parts[0].Image)
	if err != nil {
		t.Fatalf("reopen part: %v", err)
	}
	if got := reopened.Options.Columns[3].Cardinality; got != 3 {
		t.Fatalf("dictionary cardinality=%d want 3", got)
	}
	if got := reopened.Options.Columns[2].Name; got != "value" {
		t.Fatalf("nullable column=%q", got)
	}
	block := reopened.Columns["value"].Blocks[0].Granule
	values, nulls, _, err := new(GranuleReader).DecodeNullableInt64(block)
	if err != nil {
		t.Fatalf("DecodeNullableInt64: %v", err)
	}
	if len(values) != len(nulls) || !slices.Equal(nulls, []bool{true, false}) {
		t.Fatalf("nullable values/nulls=%v/%v", values, nulls)
	}
}

func BenchmarkQueryReadyBaseGenerationBuild(b *testing.B) {
	identity := queryReadyBaseTestIdentity(100)
	image := queryReadyBaseBenchmarkImage(b, 1001, queryReadyBaseBenchmarkRows(b))
	input := []QueryReadyBasePartInput{{SourceGeneration: 99, Image: image}}
	sample, err := BuildQueryReadyBaseGeneration(identity, input)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(image.Bytes)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := BuildQueryReadyBaseGeneration(identity, input)
		if err != nil {
			b.Fatal(err)
		}
		if len(result.Bytes) == 0 {
			b.Fatal("empty base")
		}
	}
	b.ReportMetric(float64(len(sample.Bytes)), "asset-bytes")
	b.ReportMetric(float64(len(sample.Bytes)-len(image.Bytes)), "overhead-bytes")
}

func BenchmarkQueryReadyBaseGenerationOpen(b *testing.B) {
	identity := queryReadyBaseTestIdentity(101)
	image := queryReadyBaseBenchmarkImage(b, 1002, queryReadyBaseBenchmarkRows(b))
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 100, Image: image}})
	if err != nil {
		b.Fatal(err)
	}
	sample, err := OpenQueryReadyBaseGeneration(result.Bytes, identity)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(result.Bytes)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		base, err := OpenQueryReadyBaseGeneration(result.Bytes, identity)
		if err != nil {
			b.Fatal(err)
		}
		if base.Stats.WholePartDecodes != 0 {
			b.Fatalf("whole part decodes=%d", base.Stats.WholePartDecodes)
		}
	}
	b.ReportMetric(float64(sample.Stats.BytesDecoded), "decoded-bytes")
	b.ReportMetric(float64(sample.Stats.BytesValidated), "validated-bytes")
}

// BenchmarkQueryReadyBaseGenerationLegacyPartOpen is the pre-QRBG comparison:
// parse and reconstruct the complete typed-column part state that callers
// historically prepared before executing a query.
func BenchmarkQueryReadyBaseGenerationLegacyPartOpen(b *testing.B) {
	image := queryReadyBaseBenchmarkImage(b, 1004, queryReadyBaseBenchmarkRows(b))
	b.ReportAllocs()
	b.SetBytes(int64(len(image.Bytes)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parsed, err := ParseColumnPartImage(image.Bytes)
		if err != nil {
			b.Fatal(err)
		}
		part, err := ColumnPartFromImage(parsed)
		if err != nil {
			b.Fatal(err)
		}
		if part.Descriptor.RowCount == 0 {
			b.Fatal("empty part")
		}
	}
}

func BenchmarkQueryReadyBaseGenerationDirectViewAccess(b *testing.B) {
	identity := queryReadyBaseTestIdentity(102)
	image := queryReadyBaseBenchmarkImage(b, 1003, queryReadyBaseBenchmarkRows(b))
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 101, Image: image}})
	if err != nil {
		b.Fatal(err)
	}
	base, err := OpenQueryReadyBaseGeneration(result.Bytes, identity)
	if err != nil {
		b.Fatal(err)
	}
	var section ColumnPartImageSection
	for _, candidate := range base.Parts[0].Image.Sections {
		if candidate.Kind == ColumnPartImageSectionColumnData {
			section = candidate
			break
		}
	}
	if section.Length == 0 {
		b.Fatal("missing column data section")
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sink byte
	for i := 0; i < b.N; i++ {
		view := base.Parts[0].Image.sectionBytes(section)
		sink ^= view[len(view)-1]
	}
	if sink == 0xff {
		b.Log(sink)
	}
}

func queryReadyBaseBenchmarkImage(tb testing.TB, partID uint64, rows int) ColumnPartImage {
	tb.Helper()
	opts := transplantTestOptions(nil)
	opts.PartPolicy.RowsPerGranule = 8192
	part, err := BuildColumnPart(partID, opts, transplantConstantBatch(rows, 42))
	if err != nil {
		tb.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{
		"kind_code": {"user": 0, "reply": 1, "system": 2},
	}})
	if err != nil {
		tb.Fatalf("BuildColumnPartImage: %v", err)
	}
	return image
}

func queryReadyBaseBenchmarkRows(tb testing.TB) int {
	tb.Helper()
	const defaultRows = 1 << 15
	raw := os.Getenv("TREEDB_QUERY_READY_BASE_BENCH_ROWS")
	if raw == "" {
		return defaultRows
	}
	rows, err := strconv.Atoi(raw)
	if err != nil || rows <= 0 {
		tb.Fatalf("TREEDB_QUERY_READY_BASE_BENCH_ROWS=%q must be a positive integer", raw)
	}
	return rows
}

func queryReadyBaseTestIdentity(generation uint64) QueryReadyBaseIdentity {
	return QueryReadyBaseIdentity{Generation: generation, SchemaHash: sha256.Sum256([]byte("schema-v1"))}
}

func queryReadyBaseCorruptLastByte(data []byte) []byte {
	out := slices.Clone(data)
	out[len(out)-1] ^= 0xff
	return out
}

func queryReadyBaseCorruptByte(data []byte, offset int) []byte {
	out := slices.Clone(data)
	out[offset] ^= 0xff
	return out
}

func queryReadyBaseReplaceFinalPartWithShorterImage(tb testing.TB, data []byte, image ColumnPartImage) []byte {
	tb.Helper()
	out := slices.Clone(data)
	partCount := int(binary.LittleEndian.Uint32(out[48:52]))
	if partCount == 0 {
		tb.Fatal("cannot shorten empty query-ready base")
	}
	entryOffset := queryReadyBaseHeaderBytes + (partCount-1)*queryReadyBasePartEntryBytes
	entry := out[entryOffset : entryOffset+queryReadyBasePartEntryBytes]
	partOffset := int(binary.LittleEndian.Uint64(entry[16:24]))
	oldPartLength := int(binary.LittleEndian.Uint64(entry[24:32]))
	if len(image.Bytes) >= oldPartLength {
		tb.Fatalf("replacement image bytes=%d must be shorter than final part=%d", len(image.Bytes), oldPartLength)
	}
	copy(out[partOffset:], image.Bytes)
	binary.LittleEndian.PutUint64(entry[24:32], uint64(len(image.Bytes)))
	binary.LittleEndian.PutUint64(entry[32:40], uint64(image.Rows))
	binary.LittleEndian.PutUint64(entry[40:48], uint64(image.ManifestBytes))
	checksum := sha256.Sum256(out[partOffset : partOffset+len(image.Bytes)])
	copy(entry[48:80], checksum[:])
	tableEnd := queryReadyBaseHeaderBytes + partCount*queryReadyBasePartEntryBytes
	tableChecksum := crc32.Checksum(out[queryReadyBaseHeaderBytes:tableEnd], queryReadyBaseCRCTable)
	binary.LittleEndian.PutUint32(out[56:60], tableChecksum)
	binary.LittleEndian.PutUint32(out[52:56], queryReadyBaseHeaderChecksum(out[:queryReadyBaseHeaderBytes]))
	return out
}
