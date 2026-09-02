package typedcolumn

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/crc"
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

func TestQueryReadyBaseStreamingPlanRejectsChangedSecondPassSource(t *testing.T) {
	identity := queryReadyBaseTestIdentity(42)
	image := mustTransplantImage(t, mustTransplantPart(t, 702, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	planner, err := NewQueryReadyBaseStreamingPlanner(identity, 1)
	if err != nil {
		t.Fatal(err)
	}
	input := QueryReadyBasePartInput{SourceGeneration: 17, Image: image}
	if err := planner.Add(input); err != nil {
		t.Fatal(err)
	}
	plan, err := planner.Finish()
	if err != nil {
		t.Fatal(err)
	}
	changed := input
	changed.Image.Bytes = slices.Clone(input.Image.Bytes)
	changed.Image.Bytes[len(changed.Image.Bytes)-1] ^= 1
	if _, err := plan.Emit(func(int) (QueryReadyBasePartInput, error) { return changed, nil }); err == nil {
		t.Fatal("second-pass changed source unexpectedly emitted")
	}
}

func TestQueryReadyBaseStreamingPlanMatchesLegacyBuildWithShuffledInputs(t *testing.T) {
	identity := queryReadyBaseTestIdentity(43)
	left := mustTransplantImage(t, mustTransplantPart(t, 703, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	right := mustTransplantImage(t, mustTransplantPart(t, 704, transplantTestOptions([]SortKeyColumn{{Column: "id"}}), transplantTestBatch()))
	inputs := []QueryReadyBasePartInput{{SourceGeneration: 18, Image: right}, {SourceGeneration: 17, Image: left}}
	legacy, err := BuildQueryReadyBaseGeneration(identity, inputs)
	if err != nil {
		t.Fatal(err)
	}
	planner, err := NewQueryReadyBaseStreamingPlanner(identity, len(inputs))
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range inputs {
		if err := planner.Add(input); err != nil {
			t.Fatal(err)
		}
	}
	plan, err := planner.Finish()
	if err != nil {
		t.Fatal(err)
	}
	streamed, err := plan.Emit(func(ordinal int) (QueryReadyBasePartInput, error) { return inputs[ordinal], nil })
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(streamed.Bytes, legacy.Bytes) {
		t.Fatal("streaming QRBG bytes differ from legacy build")
	}
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
	if _, err := OpenQueryReadyBaseGeneration(forged, identity); err == nil || !strings.Contains(err.Error(), "padding") {
		t.Fatalf("forged unaccounted bytes err=%v want padding rejection", err)
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

func TestQueryReadyBaseGenerationPersistsDensePartLocalPrimaryIDDomains(t *testing.T) {
	identity := queryReadyBaseTestIdentity(45)
	left := queryReadyDeltaTestImage(t, 711, map[int64]int64{0: 10, 1: 11})
	right := queryReadyDeltaTestImage(t, 712, map[int64]int64{0: 20, 1: 21})
	built, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{
		{SourceGeneration: 44, Image: left, PrimaryIDMode: QueryReadyPrimaryIDDensePartLocal, PrimaryIDBase: 0},
		{SourceGeneration: 44, Image: right, PrimaryIDMode: QueryReadyPrimaryIDDensePartLocal, PrimaryIDBase: 2},
	})
	if err != nil {
		t.Fatalf("build dense part-local base: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, identity)
	if err != nil {
		t.Fatalf("open dense part-local base: %v", err)
	}
	defer func() { _ = base.Close() }()
	if got := base.Dependencies; len(got) != 2 || got[0].PrimaryIDMode != QueryReadyPrimaryIDDensePartLocal || got[0].PrimaryIDBase != 0 || got[1].PrimaryIDBase != 2 {
		t.Fatalf("persisted dependencies=%+v", got)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: identity.Generation, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 1, MaxRows: 4, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatalf("open dense part-local reader: %v", err)
	}
	if stats := reader.reader.VisibilityStats(); stats.InputRows != 4 || stats.VisibleRows != 4 || stats.SupersededRows != 0 {
		t.Fatalf("visibility stats=%+v", stats)
	}
	for primaryID, want := range []int64{10, 11, 20, 21} {
		got, ok, err := reader.ValueAtLatest(int64(primaryID), "value")
		if err != nil || !ok || got != want {
			t.Fatalf("primary ID %d value=(%d,%v,%v) want=%d", primaryID, got, ok, err, want)
		}
		cached, cachedOK := reader.reader.LatestLocator(int64(primaryID))
		scanned, scannedOK := reader.reader.ScanLatestLocator(int64(primaryID))
		if !cachedOK || !scannedOK || cached != scanned {
			t.Fatalf("primary ID %d cached/scanned locator=(%+v,%v)/(%+v,%v)", primaryID, cached, cachedOK, scanned, scannedOK)
		}
	}
}

func TestQueryReadyBaseGenerationRejectsNonDensePartLocalPrimaryIDs(t *testing.T) {
	identity := queryReadyBaseTestIdentity(46)
	image := queryReadyDeltaTestImage(t, 713, map[int64]int64{1: 10, 2: 20})
	_, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{
		SourceGeneration: 45,
		Image:            image,
		PrimaryIDMode:    QueryReadyPrimaryIDDensePartLocal,
	}})
	if err == nil || !strings.Contains(err.Error(), "dense part-local ID") {
		t.Fatalf("non-dense primary IDs err=%v", err)
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

func TestQueryReadyBaseGenerationVariableWidthOpenIsPayloadAllocationIndependent(t *testing.T) {
	const (
		rows        = 4096
		bytesPerRow = 1024
		listPerRow  = 16
		openRuns    = 3
	)
	identity := queryReadyBaseTestIdentity(49)
	image, payloadBytes := queryReadyBaseVariableWidthImage(t, 709, rows, bytesPerRow, listPerRow)
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 26, Image: image}})
	if err != nil {
		t.Fatalf("BuildQueryReadyBaseGeneration: %v", err)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for range openRuns {
		base, err := OpenQueryReadyBaseGeneration(result.Bytes, identity)
		if err != nil {
			t.Fatalf("OpenQueryReadyBaseGeneration: %v", err)
		}
		wantDecoded := int64(queryReadyBaseHeaderBytes + queryReadyBasePartEntryBytes + image.ManifestBytes + queryReadyBaseStructuralBytes(image))
		if base.Stats.BytesDecoded != wantDecoded || base.Stats.BytesCopied != 0 || base.Stats.WholePartDecodes != 0 || base.Stats.DictionaryConstructions != 0 {
			t.Fatalf("open stats=%+v want decoded=%d with no payload copies/decodes/dictionaries", base.Stats, wantDecoded)
		}
		if base.Stats.BytesRead != int64(len(result.Bytes)) || base.Stats.BytesValidated != int64(len(result.Bytes)) {
			t.Fatalf("read/validated bytes=%d/%d want %d", base.Stats.BytesRead, base.Stats.BytesValidated, len(result.Bytes))
		}
		queryReadyBaseGenerationSink = base
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	allocatedPerOpen := int64(after.TotalAlloc-before.TotalAlloc) / openRuns
	if max := int64(payloadBytes / 4); allocatedPerOpen > max {
		t.Fatalf("variable-width open allocated %d bytes/op for %d payload bytes; want <= %d (bounded metadata only)", allocatedPerOpen, payloadBytes, max)
	}
}

func TestQueryReadyBaseGenerationVariableWidthValidationFailsClosed(t *testing.T) {
	image, _ := queryReadyBaseVariableWidthImage(t, 710, 16, 8, 4)
	for _, tc := range []struct {
		name   string
		column string
		offset int
		value  uint64
		want   string
	}{
		{name: "bytes non-monotonic", column: "opaque", offset: 2, value: 0, want: "before previous"},
		{name: "list final mismatch", column: "neighbors", offset: 16, value: 63, want: "final offset=63 values=64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := cloneColumnPartImageBytes(image)
			offsetsSection, _, ok := corrupt.columnOffsetsListSections(tc.column)
			if !ok {
				t.Fatalf("missing variable-width sections for %s", tc.column)
			}
			binary.LittleEndian.PutUint64(corrupt.Bytes[offsetsSection.Offset+tc.offset*8:], tc.value)
			queryReadyBaseRewriteLayoutContractChecksum(t, &corrupt, tc.column, crc.Checksum(corrupt.sectionBytes(offsetsSection)))
			if err := validateQueryReadyBasePartStructures(corrupt); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateQueryReadyBasePartStructures err=%v want containing %q", err, tc.want)
			}
		})
	}

	t.Run("bytes block boundary mismatch", func(t *testing.T) {
		boundaryImage, _ := queryReadyBaseVariableWidthImage(t, 711, 1024, 8, 1)
		corrupt := cloneColumnPartImageBytes(boundaryImage)
		offsetsSection, _, ok := corrupt.columnOffsetsListSections("opaque")
		if !ok {
			t.Fatal("missing bytes sections")
		}
		binary.LittleEndian.PutUint64(corrupt.Bytes[offsetsSection.Offset+512*8:], 512*8-1)
		queryReadyBaseRewriteLayoutContractChecksum(t, &corrupt, "opaque", crc.Checksum(corrupt.sectionBytes(offsetsSection)))
		if err := validateQueryReadyBasePartStructures(corrupt); err == nil || !strings.Contains(err.Error(), "block 0 stored bytes") {
			t.Fatalf("validateQueryReadyBasePartStructures err=%v want block-boundary rejection", err)
		}
	})
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
	b.ReportMetric(float64(sample.Stats.BytesCopied), "copied-bytes/op")
	b.ReportMetric(float64(sample.Stats.BytesHashed), "hashed-bytes/op")
	b.ReportMetric(float64(sample.Stats.BytesChecksummed), "checksummed-bytes/op")
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

func BenchmarkQueryReadyBaseGenerationVariableWidthOpen(b *testing.B) {
	const (
		bytesPerRow = 64
		listPerRow  = 4
	)
	identity := queryReadyBaseTestIdentity(103)
	image, payloadBytes := queryReadyBaseVariableWidthImage(b, 1005, queryReadyBaseBenchmarkRows(b), bytesPerRow, listPerRow)
	result, err := BuildQueryReadyBaseGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 102, Image: image}})
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
		queryReadyBaseGenerationSink = base
	}
	b.ReportMetric(float64(payloadBytes), "payload-bytes")
	b.ReportMetric(float64(sample.Stats.BytesDecoded), "decoded-bytes")
	b.ReportMetric(float64(sample.Stats.BytesCopied), "copied-bytes")
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

func queryReadyBaseVariableWidthImage(tb testing.TB, partID uint64, rows, bytesPerRow, listPerRow int) (ColumnPartImage, int) {
	tb.Helper()
	ids := make([]int64, rows)
	byteOffsets := make([]uint64, rows+1)
	byteValues := make([]byte, rows*bytesPerRow)
	listOffsets := make([]uint64, rows+1)
	listValues := make([]uint32, rows*listPerRow)
	for row := 0; row < rows; row++ {
		ids[row] = int64(row)
		byteOffsets[row+1] = uint64((row + 1) * bytesPerRow)
		listOffsets[row+1] = uint64((row + 1) * listPerRow)
		for i := row * bytesPerRow; i < (row+1)*bytesPerRow; i++ {
			byteValues[i] = byte(row + i)
		}
		for i := row * listPerRow; i < (row+1)*listPerRow; i++ {
			listValues[i] = uint32(row + i)
		}
	}
	part, err := BuildColumnPart(partID, Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone, CompressionSet: true, StatsDisabled: true},
			{Name: "opaque", Type: ColumnTypeBytes, Encoding: EncodingRawBytesOffsets, Compression: CompressionNone, CompressionSet: true},
			{Name: "neighbors", Type: ColumnTypeAdjacencyList, Encoding: EncodingRawUint32OffsetsList, Compression: CompressionNone, CompressionSet: true},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 512, DefaultCodecBlockRows: 512},
		Compression:       ColumnCompressionPolicy{Default: CompressionNone},
	}, Batch{
		Rows:    rows,
		Columns: map[string][]int64{"id": ids},
		BytesColumns: map[string]RawBytesOffsets{
			"opaque": {Rows: rows, Offsets: byteOffsets, Values: byteValues},
		},
		Uint32OffsetsLists: map[string]RawUint32OffsetsList{
			"neighbors": {Rows: rows, Offsets: listOffsets, Values: listValues},
		},
	})
	if err != nil {
		tb.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{LayoutLogicalTypes: map[string]string{
		"id": "int64", "opaque": "bytes", "neighbors": "adjacency_list",
	}})
	if err != nil {
		tb.Fatalf("BuildColumnPartImage: %v", err)
	}
	payloadBytes := len(byteOffsets)*8 + len(byteValues) + len(listOffsets)*8 + len(listValues)*4
	return image, payloadBytes
}

func queryReadyBaseRewriteLayoutContractChecksum(tb testing.TB, image *ColumnPartImage, column string, checksum uint32) {
	tb.Helper()
	section, err := image.LayoutContractSection()
	if err != nil {
		tb.Fatalf("LayoutContractSection: %v", err)
	}
	contract, err := DecodeColumnPartLayoutContract(image.sectionBytes(section))
	if err != nil {
		tb.Fatalf("DecodeColumnPartLayoutContract: %v", err)
	}
	found := false
	for i := range contract.Columns {
		if contract.Columns[i].Name == column {
			contract.Columns[i].OffsetsSection.Checksum = checksum
			found = true
			break
		}
	}
	if !found {
		tb.Fatalf("layout contract missing column %s", column)
	}
	var enc columnPartImageEncoder
	enc.u16(contract.Version)
	enc.u16(0)
	enc.u64(contract.PartID)
	enc.i64(int64(contract.Rows))
	enc.u16(contract.ImageVersion)
	enc.u16(0)
	enc.i64(int64(contract.ManifestBytes))
	encodeColumnPartLayoutContractSection(&enc, contract.Descriptor)
	enc.u32(uint32(len(contract.Columns)))
	for _, contractColumn := range contract.Columns {
		if err := encodeColumnPartLayoutContractColumn(&enc, contractColumn); err != nil {
			tb.Fatalf("encode layout contract column %s: %v", contractColumn.Name, err)
		}
	}
	raw := enc.bytes()
	if len(raw) != section.Length {
		tb.Fatalf("rewritten layout contract bytes=%d want %d", len(raw), section.Length)
	}
	copy(image.Bytes[section.Offset:section.Offset+section.Length], raw)
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

var queryReadyBaseGenerationSink *QueryReadyBaseGeneration
