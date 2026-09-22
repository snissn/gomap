package typedcolumn

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"testing"
)

var (
	queryReadyDeltaReaderSink        *QueryReadyBaseDeltaReader
	queryReadyDeltaConsolidationSink QueryReadyConsolidationResult
	queryReadyDeltaStringSink        string
)

func TestQueryReadyBaseDeltaInsertUpdateDeleteVisibility(t *testing.T) {
	base := queryReadyDeltaTestBase(t, 1, map[int64]int64{1: 10, 2: 20})
	delta := queryReadyDeltaTestGeneration(t, 2, map[int64]int64{2: 200, 3: 300}, []Tombstone{{PrimaryID: 1, GenerationID: 2}})
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta}, QueryReadyBaseDeltaOptions{
		SnapshotGeneration: 2,
		Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4},
	})
	if err != nil {
		t.Fatalf("NewQueryReadyBaseDeltaReader: %v", err)
	}
	queryReadyDeltaAssertValues(t, reader, map[int64]int64{2: 200, 3: 300}, 1, 2, 3)
	stats := reader.Stats()
	if stats.VisibleDeltaGenerations != 1 || stats.TombstonesApplied != 1 || stats.RowsMerged != 4 || stats.Fallbacks != 0 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestQueryReadyDeltaBuildDoesNotRewriteImmutableBase(t *testing.T) {
	baseImage := queryReadyDeltaTestImage(t, 3051, map[int64]int64{1: 10, 2: 20})
	baseBuilt, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: baseImage}})
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	before := slices.Clone(baseBuilt.Bytes)
	deltaImage := queryReadyDeltaTestImage(t, 3052, map[int64]int64{2: 200})
	deltaBuilt, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(2), []QueryReadyBasePartInput{{SourceGeneration: 2, Image: deltaImage}}, nil)
	if err != nil {
		t.Fatalf("build delta: %v", err)
	}
	if !slices.Equal(before, baseBuilt.Bytes) {
		t.Fatalf("one-row delta mutated/rebuilt immutable base")
	}
	if deltaBuilt.Stats.Rows != 1 || deltaBuilt.Stats.Parts != 1 || deltaBuilt.Stats.InputBytes != int64(len(deltaImage.Bytes)) {
		t.Fatalf("delta build stats include unrelated base work: %+v base_bytes=%d delta_image=%d", deltaBuilt.Stats, len(baseBuilt.Bytes), len(deltaImage.Bytes))
	}
}

func TestQueryReadyDeltaBuildUsesSingleBoundedOutputBuffer(t *testing.T) {
	image := queryReadyDeltaTestImage(t, 2201, map[int64]int64{1: 10, 2: 20, 3: 30})
	identity := queryReadyBaseTestIdentity(2)
	parts := []QueryReadyBasePartInput{{SourceGeneration: 2, Image: image}}
	standalone, err := BuildQueryReadyBaseGeneration(identity, parts)
	if err != nil {
		t.Fatalf("BuildQueryReadyBaseGeneration: %v", err)
	}
	result, err := BuildQueryReadyDeltaGeneration(identity, parts, []Tombstone{{PrimaryID: 9, GenerationID: 2}})
	if err != nil {
		t.Fatalf("BuildQueryReadyDeltaGeneration: %v", err)
	}
	if got, want := result.Stats.PeakEncodedBufferBytes, result.Stats.OutputBytes; got != want {
		t.Fatalf("peak encoded buffer bytes=%d want one final output buffer=%d", got, want)
	}
	if got, want := result.Stats.BytesCopied, result.Stats.InputBytes+standalone.Stats.ExecutionBytes; got != want {
		t.Fatalf("bytes copied=%d want source plus generated execution bytes copied once=%d", got, want)
	}
	opened, err := OpenQueryReadyDeltaGeneration(result.Bytes, identity)
	if err != nil {
		t.Fatalf("OpenQueryReadyDeltaGeneration: %v", err)
	}
	if !bytes.Equal(opened.Base.Parts[0].Image.Bytes, image.Bytes) {
		t.Fatal("embedded typed-column image changed")
	}
	if !bytes.Equal(opened.Base.Bytes(), standalone.Bytes) {
		t.Fatal("directly encoded embedded QRBG differs from standalone deterministic QRBG")
	}
}

func TestQueryReadyDeltaBuildPhaseCountersAreComplete(t *testing.T) {
	image := queryReadyDeltaTestImage(t, 2202, map[int64]int64{1: 10, 2: 20})
	result, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(2), []QueryReadyBasePartInput{{SourceGeneration: 2, Image: image}}, nil)
	if err != nil {
		t.Fatalf("BuildQueryReadyDeltaGeneration: %v", err)
	}
	if result.Stats.BaseBuildTime < result.Stats.ValidationTime || result.Stats.BuildTime < result.Stats.BaseBuildTime || result.Stats.BuildTime < result.Stats.EnvelopeBuildTime || result.Stats.BuildTime < result.Stats.TombstonePrepareTime {
		t.Fatalf("inconsistent phase timing: %+v", result.Stats)
	}
	// Windows' monotonic clock may report zero for every sub-millisecond
	// phase. Preserve the ordering assertions above there rather than treating
	// clock granularity as missing instrumentation.
	if runtime.GOOS != "windows" && (result.Stats.ValidationTime <= 0 || result.Stats.BaseBuildTime <= 0 || result.Stats.EnvelopeBuildTime <= 0) {
		t.Fatalf("missing phase timing: %+v", result.Stats)
	}
	if got, want := result.Stats.BytesHashed, int64(len(image.Bytes))+result.Stats.ExecutionBytes; got != want {
		t.Fatalf("bytes hashed=%d want %d", got, want)
	}
	if result.Stats.BytesChecksummed <= int64(queryReadyBaseHeaderBytes+queryReadyDeltaHeaderBytes) {
		t.Fatalf("bytes checksummed=%d want headers plus metadata tables", result.Stats.BytesChecksummed)
	}
	if len(result.Dependencies) != 1 || result.Dependencies[0].PartID != image.PartID {
		t.Fatalf("dependencies=%+v want complete part dependency", result.Dependencies)
	}
}

func TestQueryReadyBaseDeltaSnapshotIsolationAcrossGenerations(t *testing.T) {
	base := queryReadyDeltaTestBase(t, 1, map[int64]int64{1: 10})
	delta2 := queryReadyDeltaTestGeneration(t, 2, map[int64]int64{1: 20}, nil)
	delta3 := queryReadyDeltaTestGeneration(t, 3, map[int64]int64{1: 30}, nil)
	for _, tc := range []struct {
		snapshot uint64
		want     int64
		visible  int
	}{{1, 10, 0}, {2, 20, 1}, {3, 30, 2}} {
		reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta3, delta2}, QueryReadyBaseDeltaOptions{
			SnapshotGeneration: tc.snapshot,
			Bound:              QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4},
		})
		if err != nil {
			t.Fatalf("snapshot %d: %v", tc.snapshot, err)
		}
		value, ok, err := reader.ValueAtLatest(1, "value")
		if err != nil || !ok || value != tc.want {
			t.Fatalf("snapshot %d value/ok/err=(%d,%v,%v) want %d,true,nil", tc.snapshot, value, ok, err, tc.want)
		}
		if got := reader.Stats().VisibleDeltaGenerations; got != tc.visible {
			t.Fatalf("snapshot %d visible deltas=%d want %d", tc.snapshot, got, tc.visible)
		}
	}
}

func TestQueryReadyDeltaTombstoneSuppressesBaseAndOlderDelta(t *testing.T) {
	base := queryReadyDeltaTestBase(t, 1, map[int64]int64{1: 10, 2: 20})
	delta2 := queryReadyDeltaTestGeneration(t, 2, map[int64]int64{1: 20, 3: 30}, nil)
	delta3 := queryReadyDeltaTestGeneration(t, 3, nil, []Tombstone{{PrimaryID: 1, GenerationID: 3}, {PrimaryID: 2, GenerationID: 3}})
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta2, delta3}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 3, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4}})
	if err != nil {
		t.Fatalf("NewQueryReadyBaseDeltaReader: %v", err)
	}
	queryReadyDeltaAssertValues(t, reader, map[int64]int64{3: 30}, 1, 2, 3)
}

func TestQueryReadyDeltaDictionaryDomainExtensionParity(t *testing.T) {
	baseImage := queryReadyDeltaDictionaryImage(t, 3101, map[int64]int64{1: 0, 2: 1}, map[string]int64{"user": 0, "reply": 1})
	baseBuilt, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: baseImage}})
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(baseBuilt.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	deltaImage := queryReadyDeltaDictionaryImage(t, 3102, map[int64]int64{2: 0, 3: 2}, map[string]int64{"moderator": 0, "reply": 1, "system": 2})
	deltaBuilt, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(2), []QueryReadyBasePartInput{{SourceGeneration: 2, Image: deltaImage}}, nil)
	if err != nil {
		t.Fatalf("build delta: %v", err)
	}
	delta, err := OpenQueryReadyDeltaGeneration(deltaBuilt.Bytes, queryReadyBaseTestIdentity(2))
	if err != nil {
		t.Fatalf("open delta: %v", err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 2, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4}})
	if err != nil {
		t.Fatalf("NewQueryReadyBaseDeltaReader: %v", err)
	}
	for id, want := range map[int64]int64{1: 0, 2: 0, 3: 2} {
		got, ok, err := reader.ValueAtLatest(id, "kind_code")
		if err != nil || !ok || got != want {
			t.Fatalf("kind_code id=%d got=(%d,%v,%v) want=%d", id, got, ok, err, want)
		}
	}
	for id, want := range map[int64]string{1: "user", 2: "moderator", 3: "system"} {
		got, ok, err := reader.DictionaryValueAtLatest(id, "kind_code")
		if err != nil || !ok || got != want {
			t.Fatalf("dictionary value id=%d got=(%q,%v,%v) want=%q", id, got, ok, err, want)
		}
	}
	if stats := reader.Stats(); stats.CodeTranslations != 0 || stats.GlobalDictionaryConstructions != 0 || stats.LocalDictionaryDecodes != 2 {
		t.Fatalf("local per-part domains should not be globally translated/rebuilt: %+v", stats)
	}
}

func TestQueryReadyDeltaDictionaryHighCardinalityBoundaryAndLastGranule(t *testing.T) {
	baseValues := make(map[int64]int64, 17)
	baseDictionary := make(map[string]int64, 256)
	for code := 0; code < 256; code++ {
		baseDictionary["base_"+strconv.Itoa(code)] = int64(code)
	}
	for id := int64(1); id <= 17; id++ {
		baseValues[id] = id - 1
	}
	baseImage := queryReadyDeltaDictionaryImage(t, 3151, baseValues, baseDictionary)
	baseBuilt, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: baseImage}})
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(baseBuilt.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	deltaDictionary := make(map[string]int64, 257)
	for code := 0; code <= 256; code++ {
		deltaDictionary["delta_"+strconv.Itoa(code)] = int64(code)
	}
	// id=17 is the first row of the second 16-row granule. Code 256 crosses
	// the dense dictionary uint8/uint16 width boundary.
	deltaValues := make(map[int64]int64, 17)
	for id := int64(1); id <= 16; id++ {
		deltaValues[id] = id - 1
	}
	deltaValues[17] = 256
	deltaImage := queryReadyDeltaDictionaryImage(t, 3152, deltaValues, deltaDictionary)
	deltaBuilt, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(2), []QueryReadyBasePartInput{{SourceGeneration: 2, Image: deltaImage}}, nil)
	if err != nil {
		t.Fatalf("build delta: %v", err)
	}
	delta, err := OpenQueryReadyDeltaGeneration(deltaBuilt.Bytes, queryReadyBaseTestIdentity(2))
	if err != nil {
		t.Fatalf("open delta: %v", err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 2, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8}})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	value, ok, err := reader.DictionaryValueAtLatest(17, "kind_code")
	if err != nil || !ok || value != "delta_256" {
		t.Fatalf("boundary dictionary value=(%q,%v,%v) want delta_256,true,nil", value, ok, err)
	}
}

func TestQueryReadyBaseDeltaNullableTransitionsSurviveConsolidation(t *testing.T) {
	base := queryReadyDeltaNullableBase(t, 1, 3201, []int64{1, 2}, []int64{10, 20}, []bool{false, true})
	delta := queryReadyDeltaNullableGeneration(t, 2, 3202, []int64{1, 2}, []int64{100, 200}, []bool{true, false})
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 2, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4}})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	if value, null, _, ok, err := reader.NullableInt64AtLatest(1, "maybe"); err != nil || !ok || !null || value != 0 {
		t.Fatalf("id=1 state=(%d,%v,%v,%v)", value, null, ok, err)
	}
	if value, null, _, ok, err := reader.NullableInt64AtLatest(2, "maybe"); err != nil || !ok || null || value != 200 {
		t.Fatalf("id=2 state=(%d,%v,%v,%v)", value, null, ok, err)
	}
	result, err := ConsolidateQueryReadyBaseDelta(base, []*QueryReadyDeltaGeneration{delta}, 2)
	if err != nil {
		t.Fatalf("consolidate: %v", err)
	}
	consolidated, err := OpenQueryReadyConsolidatedBaseGeneration(result.Bytes, queryReadyBaseTestIdentity(2))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	after, err := NewQueryReadyConsolidatedBaseDeltaReader(consolidated, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 2, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4}})
	if err != nil {
		t.Fatalf("reader after: %v", err)
	}
	for _, id := range []int64{1, 2} {
		beforeValue, beforeNull, beforeDefault, beforeOK, beforeErr := reader.NullableInt64AtLatest(id, "maybe")
		afterValue, afterNull, afterDefault, afterOK, afterErr := after.NullableInt64AtLatest(id, "maybe")
		if beforeErr != nil || afterErr != nil || beforeValue != afterValue || beforeNull != afterNull || beforeDefault != afterDefault || beforeOK != afterOK {
			t.Fatalf("id=%d before=(%d,%v,%v,%v,%v) after=(%d,%v,%v,%v,%v)", id, beforeValue, beforeNull, beforeDefault, beforeOK, beforeErr, afterValue, afterNull, afterDefault, afterOK, afterErr)
		}
	}
}

func TestQueryReadyDeltaEmptyRepeatedDeleteReinsertAndSameGenerationTieBreak(t *testing.T) {
	emptyBuilt, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(5), nil, nil)
	if err != nil {
		t.Fatalf("build empty: %v", err)
	}
	empty, err := OpenQueryReadyDeltaGeneration(emptyBuilt.Bytes, queryReadyBaseTestIdentity(5))
	if err != nil || len(empty.Base.Parts) != 0 {
		t.Fatalf("open empty parts=%d err=%v", len(empty.Base.Parts), err)
	}
	base := queryReadyDeltaTestBase(t, 1, map[int64]int64{1: 10})
	deleted := queryReadyDeltaTestGeneration(t, 2, nil, []Tombstone{{PrimaryID: 1, GenerationID: 2}})
	reinserted := queryReadyDeltaTestGeneration(t, 3, map[int64]int64{1: 30}, nil)
	left := queryReadyDeltaTestImage(t, 3301, map[int64]int64{1: 31})
	right := queryReadyDeltaTestImage(t, 3302, map[int64]int64{1: 32})
	tieBuilt, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(4), []QueryReadyBasePartInput{{SourceGeneration: 4, Image: right}, {SourceGeneration: 4, Image: left}}, nil)
	if err != nil {
		t.Fatalf("build tie: %v", err)
	}
	tie, err := OpenQueryReadyDeltaGeneration(tieBuilt.Bytes, queryReadyBaseTestIdentity(4))
	if err != nil {
		t.Fatalf("open tie: %v", err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{tie, reinserted, deleted, empty}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 5, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4}})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	value, ok, err := reader.ValueAtLatest(1, "value")
	if err != nil || !ok || value != 32 {
		t.Fatalf("same-generation tie value/ok/err=(%d,%v,%v) want 32,true,nil", value, ok, err)
	}
}

func TestQueryReadyDeltaBoundTriggersAtConfiguredWorkLimit(t *testing.T) {
	base := queryReadyDeltaTestBase(t, 1, map[int64]int64{1: 1})
	var deltas []*QueryReadyDeltaGeneration
	for generation := uint64(2); generation <= 6; generation++ {
		deltas = append(deltas, queryReadyDeltaTestGeneration(t, generation, map[int64]int64{1: int64(generation)}, nil))
	}
	decision := EvaluateQueryReadyDeltaBound(deltas, 6, QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4})
	if !decision.Triggered || decision.VisibleGenerations != 5 || decision.GenerationLimitTriggers != 1 {
		t.Fatalf("decision=%+v", decision)
	}
	if _, err := NewQueryReadyBaseDeltaReader(base, deltas, QueryReadyBaseDeltaOptions{SnapshotGeneration: 6, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8}}); err == nil {
		t.Fatalf("expected configured delta bound failure")
	} else {
		var boundErr *QueryReadyDeltaBoundError
		if !errors.As(err, &boundErr) || boundErr.Decision.GenerationLimitTriggers != 1 || boundErr.Decision.VisibleGenerations != 5 {
			t.Fatalf("generation bound error=%v", err)
		}
	}
	rows := EvaluateQueryReadyDeltaBound(deltas, 6, QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 8, MaxRows: 4})
	if !rows.Triggered || rows.RowLimitTriggers != 1 || rows.Reason != "rows" || rows.Rows != 5 {
		t.Fatalf("row decision=%+v", rows)
	}
	bytes := EvaluateQueryReadyDeltaBound(deltas, 6, QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 8, MaxBytes: 1})
	if !bytes.Triggered || bytes.ByteLimitTriggers != 1 || bytes.Reason != "bytes" || bytes.Bytes <= 1 {
		t.Fatalf("byte decision=%+v", bytes)
	}
	override := EvaluateQueryReadyDeltaBound(deltas, 6, QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 8, MaxRows: rows.Rows, MaxBytes: bytes.Bytes})
	if override.Triggered {
		t.Fatalf("inclusive configured limits should allow exact work: %+v", override)
	}
	for _, tc := range []struct {
		name   string
		policy QueryReadyDeltaBoundPolicy
		check  func(QueryReadyDeltaBoundDecision) bool
	}{
		{name: "rows", policy: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 8, MaxAccumulatedDeltaParts: 8, MaxRows: 5}, check: func(d QueryReadyDeltaBoundDecision) bool { return d.RowLimitTriggers == 1 && d.Reason == "rows" }},
		{name: "bytes", policy: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 8, MaxAccumulatedDeltaParts: 8, MaxBytes: 1}, check: func(d QueryReadyDeltaBoundDecision) bool { return d.ByteLimitTriggers == 1 && d.Reason == "bytes" }},
	} {
		t.Run("public error "+tc.name, func(t *testing.T) {
			_, err := NewQueryReadyBaseDeltaReader(base, deltas, QueryReadyBaseDeltaOptions{SnapshotGeneration: 6, Bound: tc.policy})
			var boundErr *QueryReadyDeltaBoundError
			if !errors.As(err, &boundErr) || !tc.check(boundErr.Decision) {
				t.Fatalf("bound error=%v decision=%+v", err, func() QueryReadyDeltaBoundDecision {
					if boundErr == nil {
						return QueryReadyDeltaBoundDecision{}
					}
					return boundErr.Decision
				}())
			}
		})
	}
}

func TestQueryReadyDeltaDefaultBoundAllowsLargeOriginBaseButCapsGrowth(t *testing.T) {
	identity := queryReadyBaseTestIdentity(1)
	parts := make([]QueryReadyBasePartInput, 10)
	for i := range parts {
		parts[i] = QueryReadyBasePartInput{SourceGeneration: 1, Image: queryReadyDeltaTestImage(t, uint64(3500+i), map[int64]int64{int64(i + 1): int64(i)})}
	}
	built, err := BuildQueryReadyBaseGeneration(identity, parts)
	if err != nil {
		t.Fatalf("build origin base: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, identity)
	if err != nil {
		t.Fatalf("open origin base: %v", err)
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 1})
	if err != nil {
		t.Fatalf("default relative bound rejected origin base: %v", err)
	}
	stats := reader.Stats()
	if stats.OriginBaseParts != 10 || stats.AccumulatedDeltaParts != 0 || stats.TotalParts != 10 {
		t.Fatalf("relative bound stats=%+v", stats)
	}
}

func TestQueryReadyDeltaGenerationFailsClosedOnIdentityOrderAndCorruption(t *testing.T) {
	identity := queryReadyBaseTestIdentity(5)
	image := queryReadyDeltaTestImage(t, 3401, map[int64]int64{1: 10})
	built, err := BuildQueryReadyDeltaGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 5, Image: image}}, []Tombstone{{PrimaryID: 1, GenerationID: 4}, {PrimaryID: 2, GenerationID: 5}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	wrongGeneration := identity
	wrongGeneration.Generation++
	if _, err := OpenQueryReadyDeltaGeneration(built.Bytes, wrongGeneration); err == nil {
		t.Fatalf("expected generation mismatch")
	}
	wrongSchema := identity
	wrongSchema.SchemaHash[0] ^= 0xff
	if _, err := OpenQueryReadyDeltaGeneration(built.Bytes, wrongSchema); err == nil {
		t.Fatalf("expected schema mismatch")
	}
	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "truncated", data: slices.Clone(built.Bytes[:len(built.Bytes)-1])},
		{name: "header", data: queryReadyDeltaCorruptByte(built.Bytes, 8)},
		{name: "inner", data: queryReadyDeltaCorruptByte(built.Bytes, len(built.Bytes)-1)},
		{name: "tombstone order", data: queryReadyDeltaReverseTombstones(built.Bytes)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := OpenQueryReadyDeltaGeneration(tc.data, identity); err == nil {
				t.Fatalf("expected fail-closed error")
			}
		})
	}
	if _, err := BuildQueryReadyDeltaGeneration(identity, nil, []Tombstone{{PrimaryID: 1, GenerationID: 6}}); err == nil {
		t.Fatalf("expected future tombstone generation rejection")
	}
	if _, err := BuildQueryReadyDeltaGeneration(identity, []QueryReadyBasePartInput{{SourceGeneration: 4, Image: image}}, nil); err == nil {
		t.Fatalf("expected mixed historical source generation rejection for ordinary delta")
	}
	tamperedLineage := slices.Clone(built.Bytes)
	binary.LittleEndian.PutUint32(tamperedLineage[88:92], 1)
	binary.LittleEndian.PutUint32(tamperedLineage[52:56], queryReadyDeltaHeaderChecksum(tamperedLineage[:queryReadyDeltaHeaderBytes]))
	if _, err := OpenQueryReadyDeltaGeneration(tamperedLineage, identity); err == nil {
		t.Fatalf("expected ordinary delta lineage rejection")
	}
}

func TestQueryReadyDeltaRepeatedConsolidationCarriesLineageTombstonesAndBound(t *testing.T) {
	base := queryReadyDeltaTestBase(t, 1, map[int64]int64{1: 10, 2: 20})
	delta2 := queryReadyDeltaTestGeneration(t, 2, map[int64]int64{2: 200}, []Tombstone{{PrimaryID: 1, GenerationID: 2}})
	policy := QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 2}
	first, err := ConsolidateQueryReadyBaseDeltaWithPolicy(base, []*QueryReadyDeltaGeneration{delta2}, 2, policy)
	if err != nil {
		t.Fatalf("first consolidation: %v", err)
	}
	consolidated2, err := OpenQueryReadyConsolidatedBaseGeneration(first.Bytes, queryReadyBaseTestIdentity(2))
	if err != nil {
		t.Fatalf("open first consolidation: %v", err)
	}
	if consolidated2.OriginBaseParts != 1 || consolidated2.AccumulatedDeltaParts != 1 || len(consolidated2.Tombstones) != 1 {
		t.Fatalf("first lineage/tombstones=%d/%d/%v", consolidated2.OriginBaseParts, consolidated2.AccumulatedDeltaParts, consolidated2.Tombstones)
	}
	oldSnapshot, err := NewQueryReadyConsolidatedBaseDeltaReader(consolidated2, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 2, Bound: policy})
	if err != nil {
		t.Fatalf("old snapshot reader: %v", err)
	}
	queryReadyDeltaAssertValues(t, oldSnapshot, map[int64]int64{2: 200}, 1, 2)

	delta3 := queryReadyDeltaTestGeneration(t, 3, map[int64]int64{1: 300}, nil)
	second, err := ConsolidateQueryReadyConsolidatedBaseDeltaWithPolicy(consolidated2, []*QueryReadyDeltaGeneration{delta3}, 3, policy)
	if err != nil {
		t.Fatalf("second consolidation: %v", err)
	}
	consolidated3, err := OpenQueryReadyConsolidatedBaseGeneration(second.Bytes, queryReadyBaseTestIdentity(3))
	if err != nil {
		t.Fatalf("open second consolidation: %v", err)
	}
	if consolidated3.OriginBaseParts != 1 || consolidated3.AccumulatedDeltaParts != 2 || len(consolidated3.Tombstones) != 1 {
		t.Fatalf("second lineage/tombstones=%d/%d/%v", consolidated3.OriginBaseParts, consolidated3.AccumulatedDeltaParts, consolidated3.Tombstones)
	}
	newSnapshot, err := NewQueryReadyConsolidatedBaseDeltaReader(consolidated3, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 3, Bound: policy})
	if err != nil {
		t.Fatalf("new snapshot reader: %v", err)
	}
	queryReadyDeltaAssertValues(t, newSnapshot, map[int64]int64{1: 300, 2: 200}, 1, 2)
	// The prior consolidated object remains independently readable after the
	// replacement exists; its tombstone still suppresses id=1.
	queryReadyDeltaAssertValues(t, oldSnapshot, map[int64]int64{2: 200}, 1, 2)

	delta4 := queryReadyDeltaTestGeneration(t, 4, map[int64]int64{2: 400}, nil)
	decision := evaluateQueryReadyBaseDeltaBound(consolidated3.Base, consolidated3.Tombstones, consolidated3.OriginBaseParts, consolidated3.AccumulatedDeltaParts, []*QueryReadyDeltaGeneration{delta4}, 4, policy)
	if !decision.Triggered || decision.Reason != "accumulated_delta_parts" || decision.OriginBaseParts != 1 || decision.BaseDeltaDerivedParts != 2 || decision.DeltaParts != 1 || decision.AccumulatedDeltaParts != 3 || decision.PartLimitTriggers != 1 {
		t.Fatalf("repeated-cycle decision=%+v", decision)
	}
	if _, err := ConsolidateQueryReadyConsolidatedBaseDeltaWithPolicy(consolidated3, []*QueryReadyDeltaGeneration{delta4}, 4, policy); err == nil {
		t.Fatalf("expected repeated consolidation bound rejection")
	} else {
		var boundErr *QueryReadyDeltaBoundError
		if !errors.As(err, &boundErr) || boundErr.Decision.PartLimitTriggers != 1 || boundErr.Decision.AccumulatedDeltaParts != 3 {
			t.Fatalf("consolidation bound error=%v", err)
		}
	}
	if _, err := NewQueryReadyConsolidatedBaseDeltaReader(consolidated3, []*QueryReadyDeltaGeneration{delta4}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 4, Bound: policy}); err == nil {
		t.Fatalf("expected reader bound rejection before decode")
	}

	tampered := slices.Clone(second.Bytes)
	binary.LittleEndian.PutUint32(tampered[88:92], uint32(consolidated3.OriginBaseParts+1))
	binary.LittleEndian.PutUint32(tampered[52:56], queryReadyDeltaHeaderChecksum(tampered[:queryReadyDeltaHeaderBytes]))
	if _, err := OpenQueryReadyConsolidatedBaseGeneration(tampered, queryReadyBaseTestIdentity(3)); err == nil {
		t.Fatalf("expected tampered consolidated lineage rejection")
	}
}

func TestQueryReadyBaseDeltaConsolidationRejectsGenerationOverclaim(t *testing.T) {
	base := queryReadyDeltaTestBase(t, 1, map[int64]int64{1: 10})
	delta2 := queryReadyDeltaTestGeneration(t, 2, map[int64]int64{1: 20}, nil)
	if _, err := ConsolidateQueryReadyBaseDelta(base, nil, 2); err == nil {
		t.Fatalf("expected no-delta generation overclaim rejection")
	}
	if _, err := ConsolidateQueryReadyBaseDelta(base, []*QueryReadyDeltaGeneration{delta2}, 3); err == nil {
		t.Fatalf("expected selected-prefix generation overclaim rejection")
	}
}

func TestQueryReadyBaseDeltaConsolidationParityAfterReopen(t *testing.T) {
	base := queryReadyDeltaTestBase(t, 1, map[int64]int64{1: 10, 2: 20})
	delta2 := queryReadyDeltaTestGeneration(t, 2, map[int64]int64{2: 200, 3: 300}, nil)
	delta3 := queryReadyDeltaTestGeneration(t, 3, map[int64]int64{1: 100}, []Tombstone{{PrimaryID: 3, GenerationID: 3}})
	want, err := NewQueryReadyBaseDeltaReader(base, []*QueryReadyDeltaGeneration{delta2, delta3}, QueryReadyBaseDeltaOptions{SnapshotGeneration: 3, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4}})
	if err != nil {
		t.Fatalf("before consolidation: %v", err)
	}
	result, err := ConsolidateQueryReadyBaseDelta(base, []*QueryReadyDeltaGeneration{delta2, delta3}, 3)
	if err != nil {
		t.Fatalf("ConsolidateQueryReadyBaseDelta: %v", err)
	}
	again, err := ConsolidateQueryReadyBaseDelta(base, []*QueryReadyDeltaGeneration{delta3, delta2}, 3)
	if err != nil {
		t.Fatalf("deterministic retry: %v", err)
	}
	if !slices.Equal(result.Bytes, again.Bytes) {
		t.Fatalf("retried consolidation bytes differ")
	}
	path := filepath.Join(t.TempDir(), "consolidated.qrd")
	if err := os.WriteFile(path, result.Bytes, 0o600); err != nil {
		t.Fatalf("write consolidated: %v", err)
	}
	reopenedBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read consolidated: %v", err)
	}
	consolidated, err := OpenQueryReadyConsolidatedBaseGeneration(reopenedBytes, queryReadyBaseTestIdentity(3))
	if err != nil {
		t.Fatalf("OpenQueryReadyConsolidatedBaseGeneration: %v", err)
	}
	after, err := NewQueryReadyConsolidatedBaseDeltaReader(consolidated, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 3, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4}})
	if err != nil {
		t.Fatalf("after consolidation: %v", err)
	}
	for id := int64(1); id <= 3; id++ {
		beforeValue, beforeOK, beforeErr := want.ValueAtLatest(id, "value")
		afterValue, afterOK, afterErr := after.ValueAtLatest(id, "value")
		if beforeErr != nil || afterErr != nil || beforeOK != afterOK || beforeValue != afterValue {
			t.Fatalf("id=%d before=(%d,%v,%v) after=(%d,%v,%v)", id, beforeValue, beforeOK, beforeErr, afterValue, afterOK, afterErr)
		}
	}
	if result.Stats.OutputGenerations != 1 || result.Stats.SelectedDeltaGenerations != 2 || result.Stats.TombstonesMerged != 1 || result.Stats.DocumentMaterializations != 0 {
		t.Fatalf("consolidation stats=%+v", result.Stats)
	}
	old, err := NewQueryReadyBaseDeltaReader(base, nil, QueryReadyBaseDeltaOptions{SnapshotGeneration: 1, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4}})
	if err != nil {
		t.Fatalf("old snapshot after consolidation: %v", err)
	}
	queryReadyDeltaAssertValues(t, old, map[int64]int64{1: 10, 2: 20}, 1, 2, 3)
}

func TestQueryReadyBaseDeltaRandomizedModelParity(t *testing.T) {
	rng := rand.New(rand.NewSource(3697))
	model := map[int64]int64{1: 10, 2: 20, 3: 30}
	base := queryReadyDeltaTestBase(t, 1, model)
	var deltas []*QueryReadyDeltaGeneration
	for generation := uint64(2); generation <= 25; generation++ {
		writes := make(map[int64]int64)
		var tombstones []Tombstone
		for range 1 + rng.Intn(4) {
			id := int64(1 + rng.Intn(12))
			if rng.Intn(4) == 0 {
				delete(model, id)
				tombstones = append(tombstones, Tombstone{PrimaryID: id, GenerationID: generation})
				delete(writes, id)
			} else {
				value := int64(rng.Intn(10000))
				model[id] = value
				writes[id] = value
			}
		}
		deltas = append(deltas, queryReadyDeltaTestGeneration(t, generation, writes, tombstones))
	}
	reader, err := NewQueryReadyBaseDeltaReader(base, deltas, QueryReadyBaseDeltaOptions{SnapshotGeneration: 25, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 32}})
	if err != nil {
		t.Fatalf("NewQueryReadyBaseDeltaReader: %v", err)
	}
	for id := int64(1); id <= 12; id++ {
		got, ok, err := reader.ValueAtLatest(id, "value")
		want, wantOK := model[id]
		if err != nil || ok != wantOK || (ok && got != want) {
			t.Fatalf("id=%d got=(%d,%v,%v) want=(%d,%v)", id, got, ok, err, want, wantOK)
		}
	}
}

func queryReadyDeltaTestBase(t *testing.T, generation uint64, values map[int64]int64) *QueryReadyBaseGeneration {
	t.Helper()
	image := queryReadyDeltaTestImage(t, 1000+generation, values)
	result, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(generation), []QueryReadyBasePartInput{{SourceGeneration: generation, Image: image}})
	if err != nil {
		t.Fatalf("BuildQueryReadyBaseGeneration: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(result.Bytes, queryReadyBaseTestIdentity(generation))
	if err != nil {
		t.Fatalf("OpenQueryReadyBaseGeneration: %v", err)
	}
	return base
}

func queryReadyDeltaTestGeneration(t *testing.T, generation uint64, values map[int64]int64, tombstones []Tombstone) *QueryReadyDeltaGeneration {
	t.Helper()
	var parts []QueryReadyBasePartInput
	if len(values) > 0 {
		parts = append(parts, QueryReadyBasePartInput{SourceGeneration: generation, Image: queryReadyDeltaTestImage(t, 2000+generation, values)})
	}
	result, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(generation), parts, tombstones)
	if err != nil {
		t.Fatalf("BuildQueryReadyDeltaGeneration: %v", err)
	}
	delta, err := OpenQueryReadyDeltaGeneration(result.Bytes, queryReadyBaseTestIdentity(generation))
	if err != nil {
		t.Fatalf("OpenQueryReadyDeltaGeneration: %v", err)
	}
	return delta
}

func queryReadyDeltaTestImage(t *testing.T, partID uint64, values map[int64]int64) ColumnPartImage {
	t.Helper()
	ids := make([]int64, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	vals := make([]int64, len(ids))
	for i, id := range ids {
		vals[i] = values[id]
	}
	opts := Options{
		SchemaVersion: 1,
		SchemaMode:    ColumnSchemaFixed,
		Columns: []ColumnDefinition{
			{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
			{Name: "value", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
		},
		LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           SortKey{Columns: []SortKeyColumn{{Column: "id"}}},
		PartPolicy:        ColumnPartPolicy{RowsPerGranule: 16},
	}
	part, err := BuildColumnPart(partID, opts, Batch{Rows: len(ids), Columns: map[string][]int64{"id": ids, "value": vals}})
	if err != nil {
		t.Fatalf("BuildColumnPart: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("BuildColumnPartImage: %v", err)
	}
	return image
}

func queryReadyDeltaDictionaryImage(t *testing.T, partID uint64, values map[int64]int64, dictionary map[string]int64) ColumnPartImage {
	t.Helper()
	ids := make([]int64, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	codes := make([]int64, len(ids))
	for i, id := range ids {
		codes[i] = values[id]
	}
	opts := Options{SchemaVersion: 1, SchemaMode: ColumnSchemaFixed, Columns: []ColumnDefinition{
		{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Name: "kind_code", Type: ColumnTypeLowCardinalityCode, Compression: CompressionNone, Cardinality: uint32(len(dictionary))},
	}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}, SortKey: SortKey{Columns: []SortKeyColumn{{Column: "id"}}}, PartPolicy: ColumnPartPolicy{RowsPerGranule: 16}}
	part, err := BuildColumnPart(partID, opts, Batch{Rows: len(ids), Columns: map[string][]int64{"id": ids, "kind_code": codes}})
	if err != nil {
		t.Fatalf("build dictionary part: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{"kind_code": dictionary}})
	if err != nil {
		t.Fatalf("build dictionary image: %v", err)
	}
	return image
}

func queryReadyDeltaNullableBase(t *testing.T, generation, partID uint64, ids, values []int64, nulls []bool) *QueryReadyBaseGeneration {
	t.Helper()
	image := queryReadyDeltaNullableImage(t, partID, ids, values, nulls)
	built, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(generation), []QueryReadyBasePartInput{{SourceGeneration: generation, Image: image}})
	if err != nil {
		t.Fatalf("build nullable base: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(built.Bytes, queryReadyBaseTestIdentity(generation))
	if err != nil {
		t.Fatalf("open nullable base: %v", err)
	}
	return base
}

func queryReadyDeltaNullableGeneration(t *testing.T, generation, partID uint64, ids, values []int64, nulls []bool) *QueryReadyDeltaGeneration {
	t.Helper()
	image := queryReadyDeltaNullableImage(t, partID, ids, values, nulls)
	built, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(generation), []QueryReadyBasePartInput{{SourceGeneration: generation, Image: image}}, nil)
	if err != nil {
		t.Fatalf("build nullable delta: %v", err)
	}
	delta, err := OpenQueryReadyDeltaGeneration(built.Bytes, queryReadyBaseTestIdentity(generation))
	if err != nil {
		t.Fatalf("open nullable delta: %v", err)
	}
	return delta
}

func queryReadyDeltaNullableImage(t *testing.T, partID uint64, ids, values []int64, nulls []bool) ColumnPartImage {
	t.Helper()
	opts := Options{SchemaVersion: 1, SchemaMode: ColumnSchemaFixed, Columns: []ColumnDefinition{
		{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Name: "maybe", Type: ColumnTypeInt64, Encoding: EncodingNullableInt64, Compression: CompressionNone},
	}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}, SortKey: SortKey{Columns: []SortKeyColumn{{Column: "id"}}}, PartPolicy: ColumnPartPolicy{RowsPerGranule: 16}}
	part, err := BuildColumnPart(partID, opts, Batch{Rows: len(ids), Columns: map[string][]int64{"id": ids, "maybe": values}, Nulls: map[string][]bool{"maybe": nulls}})
	if err != nil {
		t.Fatalf("build nullable part: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("build nullable image: %v", err)
	}
	return image
}

func queryReadyDeltaCorruptByte(data []byte, offset int) []byte {
	out := slices.Clone(data)
	out[offset] ^= 0xff
	return out
}

func queryReadyDeltaReverseTombstones(data []byte) []byte {
	out := slices.Clone(data)
	first := slices.Clone(out[queryReadyDeltaHeaderBytes : queryReadyDeltaHeaderBytes+queryReadyDeltaTombstoneBytes])
	second := slices.Clone(out[queryReadyDeltaHeaderBytes+queryReadyDeltaTombstoneBytes : queryReadyDeltaHeaderBytes+2*queryReadyDeltaTombstoneBytes])
	copy(out[queryReadyDeltaHeaderBytes:], second)
	copy(out[queryReadyDeltaHeaderBytes+queryReadyDeltaTombstoneBytes:], first)
	table := out[queryReadyDeltaHeaderBytes : queryReadyDeltaHeaderBytes+2*queryReadyDeltaTombstoneBytes]
	binary.LittleEndian.PutUint32(out[56:60], crc32.Checksum(table, queryReadyBaseCRCTable))
	binary.LittleEndian.PutUint32(out[52:56], queryReadyDeltaHeaderChecksum(out[:queryReadyDeltaHeaderBytes]))
	return out
}

func BenchmarkQueryReadyBaseDeltaPrepareCurve(b *testing.B) {
	for _, shape := range []string{"low_cardinality", "high_cardinality", "tombstone_heavy", "nullable_mixed_reinsert"} {
		for _, generations := range []int{0, 1, 2, 4, 8, 9} {
			b.Run(shape+"/N="+strconv.Itoa(generations), func(b *testing.B) {
				base, deltas := queryReadyDeltaBenchmarkFixture(b, generations, shape)
				policy := QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 16, MaxAccumulatedDeltaParts: 16}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					reader, err := NewQueryReadyBaseDeltaReader(base, deltas, QueryReadyBaseDeltaOptions{SnapshotGeneration: uint64(generations + 1), Bound: policy})
					if err != nil {
						b.Fatal(err)
					}
					queryReadyDeltaReaderSink = reader
				}
				b.StopTimer()
				decision := evaluateQueryReadyBaseDeltaBound(base, nil, len(base.Parts), 0, deltas, uint64(generations+1), policy)
				b.ReportMetric(float64(decision.TotalParts), "merge_parts/op")
				b.ReportMetric(float64(decision.Rows), "merge_rows/op")
				b.ReportMetric(float64(decision.Bytes), "merge_bytes/op")
			})
		}
	}
}

func BenchmarkQueryReadyBaseDeltaWarmLookup(b *testing.B) {
	base, deltas := queryReadyDeltaBenchmarkFixture(b, 4, "high_cardinality")
	reader, err := NewQueryReadyBaseDeltaReader(base, deltas, QueryReadyBaseDeltaOptions{SnapshotGeneration: 5, Bound: QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 8, MaxAccumulatedDeltaParts: 8}})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		value, ok, err := reader.DictionaryValueAtLatest(1, "kind_code")
		if err != nil || !ok {
			b.Fatalf("lookup=%q/%v/%v", value, ok, err)
		}
		queryReadyDeltaStringSink = value
	}
}

func BenchmarkQueryReadyBaseDeltaConsolidation(b *testing.B) {
	base, deltas := queryReadyDeltaBenchmarkFixture(b, 4, "high_cardinality")
	policy := QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 8, MaxAccumulatedDeltaParts: 8}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ConsolidateQueryReadyBaseDeltaWithPolicy(base, deltas, 5, policy)
		if err != nil {
			b.Fatal(err)
		}
		queryReadyDeltaConsolidationSink = result
	}
	b.StopTimer()
	result := queryReadyDeltaConsolidationSink
	b.ReportMetric(result.Stats.WriteAmplification, "write_amp/op")
	b.ReportMetric(float64(result.Stats.PeakEncodedBufferBytes), "peak_encoded_buffer_bytes/op")
	b.ReportMetric(float64(result.Stats.BytesCopied), "copied-bytes/op")
	b.ReportMetric(float64(result.Stats.BytesHashed), "hashed-bytes/op")
	b.ReportMetric(float64(result.Stats.BytesChecksummed), "checksummed-bytes/op")
}

func BenchmarkQueryReadyDeltaGenerationBuild(b *testing.B) {
	rows := queryReadyDeltaBenchmarkRows(b, "QUERY_READY_BENCH_DELTA_ROWS", 512)
	image := queryReadyDeltaBenchmarkImage(b, 6001, rows, min(rows, 64), 0)
	parts := []QueryReadyBasePartInput{{SourceGeneration: 2, Image: image}}
	tombstones := make([]Tombstone, 128)
	for i := range tombstones {
		tombstones[i] = Tombstone{PrimaryID: int64(i + 1), GenerationID: 2}
	}
	identity := queryReadyBaseTestIdentity(2)
	b.Run("validated_part_container_only", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			result, err := BuildQueryReadyBaseGeneration(identity, parts)
			if err != nil {
				b.Fatal(err)
			}
			queryReadyDeltaConsolidationSink.Bytes = result.Bytes
		}
	})
	b.Run("qrdg_derived_envelope_build", func(b *testing.B) {
		sample, err := BuildQueryReadyDeltaGeneration(identity, parts, tombstones)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(image.Bytes)))
		b.ResetTimer()
		for range b.N {
			result, err := BuildQueryReadyDeltaGeneration(identity, parts, tombstones)
			if err != nil {
				b.Fatal(err)
			}
			queryReadyDeltaConsolidationSink.Bytes = result.Bytes
		}
		b.ReportMetric(float64(sample.Stats.BytesCopied), "copied-bytes/op")
		b.ReportMetric(float64(sample.Stats.BytesHashed), "hashed-bytes/op")
		b.ReportMetric(float64(sample.Stats.BytesChecksummed), "checksummed-bytes/op")
		b.ReportMetric(float64(sample.Stats.PeakEncodedBufferBytes), "peak_encoded_buffer_bytes/op")
		b.ReportMetric(sample.Stats.WriteAmplification, "write_amp/op")
	})
}

func queryReadyDeltaBenchmarkFixture(tb testing.TB, generations int, shape string) (*QueryReadyBaseGeneration, []*QueryReadyDeltaGeneration) {
	tb.Helper()
	baseRows := queryReadyDeltaBenchmarkRows(tb, "QUERY_READY_BENCH_BASE_ROWS", 512)
	deltaRows := queryReadyDeltaBenchmarkRows(tb, "QUERY_READY_BENCH_DELTA_ROWS", 64)
	cardinality := 4
	if shape == "high_cardinality" {
		cardinality = baseRows
	}
	baseImage := queryReadyDeltaBenchmarkImage(tb, 5001, baseRows, cardinality, 0)
	if shape == "nullable_mixed_reinsert" {
		baseImage = queryReadyDeltaBenchmarkNullableImage(tb, 5001, baseRows, 0)
	}
	baseBuilt, err := BuildQueryReadyBaseGeneration(queryReadyBaseTestIdentity(1), []QueryReadyBasePartInput{{SourceGeneration: 1, Image: baseImage}})
	if err != nil {
		tb.Fatalf("build benchmark base: %v", err)
	}
	base, err := OpenQueryReadyBaseGeneration(baseBuilt.Bytes, queryReadyBaseTestIdentity(1))
	if err != nil {
		tb.Fatalf("open benchmark base: %v", err)
	}
	deltas := make([]*QueryReadyDeltaGeneration, 0, generations)
	for i := 0; i < generations; i++ {
		generation := uint64(i + 2)
		var parts []QueryReadyBasePartInput
		var tombstones []Tombstone
		if shape == "tombstone_heavy" {
			for row := 0; row < deltaRows; row++ {
				tombstones = append(tombstones, Tombstone{PrimaryID: int64(row + 1), GenerationID: generation})
			}
		} else if shape == "nullable_mixed_reinsert" {
			parts = append(parts, QueryReadyBasePartInput{SourceGeneration: generation, Image: queryReadyDeltaBenchmarkNullableImage(tb, 5100+generation, deltaRows, i+1)})
			for row := 0; row < deltaRows; row += 4 {
				tombstones = append(tombstones, Tombstone{PrimaryID: int64(row + 1), GenerationID: generation})
			}
		} else {
			parts = append(parts, QueryReadyBasePartInput{SourceGeneration: generation, Image: queryReadyDeltaBenchmarkImage(tb, 5100+generation, deltaRows, cardinality, i+1)})
		}
		built, err := BuildQueryReadyDeltaGeneration(queryReadyBaseTestIdentity(generation), parts, tombstones)
		if err != nil {
			tb.Fatalf("build benchmark delta %d: %v", generation, err)
		}
		delta, err := OpenQueryReadyDeltaGeneration(built.Bytes, queryReadyBaseTestIdentity(generation))
		if err != nil {
			tb.Fatalf("open benchmark delta %d: %v", generation, err)
		}
		deltas = append(deltas, delta)
	}
	return base, deltas
}

func queryReadyDeltaBenchmarkRows(tb testing.TB, name string, fallback int) int {
	tb.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	rows, err := strconv.Atoi(raw)
	if err != nil || rows <= 0 {
		tb.Fatalf("%s=%q must be a positive integer", name, raw)
	}
	return rows
}

func queryReadyDeltaBenchmarkNullableImage(tb testing.TB, partID uint64, rows, generationOffset int) ColumnPartImage {
	tb.Helper()
	ids := make([]int64, rows)
	values := make([]int64, rows)
	nulls := make([]bool, rows)
	for i := range rows {
		ids[i] = int64(i + 1)
		values[i] = int64((generationOffset+1)*1000 + i)
		nulls[i] = (i+generationOffset)%3 == 0
	}
	opts := Options{SchemaVersion: 1, SchemaMode: ColumnSchemaFixed, Columns: []ColumnDefinition{
		{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Name: "maybe", Type: ColumnTypeInt64, Encoding: EncodingNullableInt64, Compression: CompressionNone},
	}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}, SortKey: SortKey{Columns: []SortKeyColumn{{Column: "id"}}}, PartPolicy: ColumnPartPolicy{RowsPerGranule: 128}}
	part, err := BuildColumnPart(partID, opts, Batch{Rows: rows, Columns: map[string][]int64{"id": ids, "maybe": values}, Nulls: map[string][]bool{"maybe": nulls}})
	if err != nil {
		tb.Fatalf("build nullable benchmark part: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{})
	if err != nil {
		tb.Fatalf("build nullable benchmark image: %v", err)
	}
	return image
}

func queryReadyDeltaBenchmarkImage(tb testing.TB, partID uint64, rows, cardinality, generationOffset int) ColumnPartImage {
	tb.Helper()
	ids := make([]int64, rows)
	codes := make([]int64, rows)
	for i := range rows {
		ids[i] = int64(i + 1)
		codes[i] = int64((i + generationOffset) % cardinality)
	}
	dictionary := make(map[string]int64, cardinality)
	for code := 0; code < cardinality; code++ {
		dictionary["value_"+strconv.Itoa((code+generationOffset)%cardinality)] = int64(code)
	}
	opts := Options{SchemaVersion: 1, SchemaMode: ColumnSchemaFixed, Columns: []ColumnDefinition{
		{Name: "id", Type: ColumnTypeInt64, Encoding: EncodingRawInt64, Compression: CompressionNone},
		{Name: "kind_code", Type: ColumnTypeLowCardinalityCode, Compression: CompressionNone, Cardinality: uint32(cardinality)},
	}, LogicalPrimaryKey: LogicalPrimaryKey{Columns: []string{"id"}}, SortKey: SortKey{Columns: []SortKeyColumn{{Column: "id"}}}, PartPolicy: ColumnPartPolicy{RowsPerGranule: 128}}
	part, err := BuildColumnPart(partID, opts, Batch{Rows: rows, Columns: map[string][]int64{"id": ids, "kind_code": codes}})
	if err != nil {
		tb.Fatalf("build benchmark part: %v", err)
	}
	image, err := BuildColumnPartImage(part, ColumnPartImageOptions{Dictionaries: map[string]map[string]int64{"kind_code": dictionary}})
	if err != nil {
		tb.Fatalf("build benchmark image: %v", err)
	}
	return image
}

func queryReadyDeltaAssertValues(t *testing.T, reader *QueryReadyBaseDeltaReader, want map[int64]int64, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		value, ok, err := reader.ValueAtLatest(id, "value")
		wantValue, wantOK := want[id]
		if err != nil || ok != wantOK || (ok && value != wantValue) {
			t.Fatalf("id=%d got=(%d,%v,%v) want=(%d,%v)", id, value, ok, err, wantValue, wantOK)
		}
	}
}
