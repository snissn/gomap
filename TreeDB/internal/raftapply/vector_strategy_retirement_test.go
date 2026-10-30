package raftapply

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
)

func TestRaftApplyRemovedVectorStrategyRejectsBeforeEffects(t *testing.T) {
	for _, version := range []uint64{3, 5} {
		t.Run(fmt.Sprintf("metadata_v%d", version), func(t *testing.T) {
			dir := t.TempDir()
			db := openApplyHarnessDBWithOptions(t, dir, backenddb.Options{ResolvedProfile: backenddb.ProfileCommandWALDurable})
			defer func() { _ = db.Close() }()
			progress := NewMemoryApplyProgressStore(8, 8)
			results := NewMemoryApplyResultStore(8)
			options := Options{ProgressStore: progress, ResultStore: results}
			create := modernColumnGraphCreateEntryForRetirementTest(t, "existing", "retirement:modern-control")
			result, err := ApplyCommittedEntryV1(db, create, applyMeta(1, 1), options)
			if err != nil {
				t.Fatalf("modern create control: %+v %v", result, err)
			}
			assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
			seed := deterministicInsertBatchEntry(t, "existing", "retirement:seed", nativewire.DocumentFormatJSON, [][]byte{[]byte("keep")}, [][]byte{[]byte(`{"embedding":[1,0]}`)})
			result, err = ApplyCommittedEntryV1(db, seed, applyMeta(1, 2), options)
			if err != nil {
				t.Fatalf("modern insert control: %+v %v", result, err)
			}
			assertApplied(t, result, raftentry.ApplyStatusApplied, 1)
			beforeDigest, err := LogicalDigestV1ForDB(db, LogicalDigestOptionsV1{})
			if err != nil {
				t.Fatal(err)
			}
			beforeFrames := readCommandWALFrames(t, dir)
			beforeLSN, beforeNextLSN := db.State().AppliedCommandLSN, db.CommandWALNextLSN()

			// Encode the retired peer's canonical envelope directly: the current
			// producer must refuse to generate this entry in the first place.
			sections := []nativewire.Section{
				{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("retirement:removed")},
				{ID: nativewire.SectionCollectionMeta, Bytes: testCreateCollectionMetaPayload("retired", testCreateCollectionMetaOptions{version: version, includeIndex: true, includeVectorIndex: true, vectorStrategy: 1})},
				{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
			}
			sort.Slice(sections, func(i, j int) bool { return sections[i].ID < sections[j].ID })
			raw := append([]byte(nil), nativewire.DeterministicEntryMagic...)
			for _, value := range []uint64{nativewire.DeterministicEntryVersion, uint64(nativewire.CommandCreateCollection), 1, 0, uint64(len(sections))} {
				raw = binary.AppendUvarint(raw, value)
			}
			for _, section := range sections {
				raw = binary.AppendUvarint(raw, uint64(section.ID))
				raw = binary.AppendUvarint(raw, uint64(len(section.Bytes)))
				raw = append(raw, section.Bytes...)
			}
			_, decodeErr := nativewire.DecodeDeterministicEntry(raw, nativewire.Limits{})
			if code, _ := nativewire.ErrorCodeOf(decodeErr); code != nativewire.ErrUnsupportedFeature || !strings.Contains(decodeErr.Error(), "native_runtime") {
				t.Fatalf("retired-tag decode=%v, want explicit unsupported strategy", decodeErr)
			}
			seam := &countingCommandWALApplySeam{}
			options.CommandWALApplySeam = seam
			if _, err := PreflightCommandEntryV1(db, raw, applyMeta(1, 3), options); codeOf(err) != raftentry.ErrorUnsupportedFeatureV1 {
				t.Fatalf("retired-tag preflight=%v, want unsupported feature", err)
			}
			result, err = ApplyCommittedEntryV1(db, raw, applyMeta(1, 3), options)
			assertRejected(t, result, err, raftentry.ApplyStatusRejectedUnsupported, raftentry.ErrorUnsupportedFeatureV1)
			if seam.appendCalls != 0 || seam.finalizeCalls != 0 {
				t.Fatalf("retired tag reached WAL seam append/finalize=%d/%d", seam.appendCalls, seam.finalizeCalls)
			}
			if got := readCommandWALFrames(t, dir); !reflect.DeepEqual(beforeFrames, got) {
				t.Fatal("retired tag changed command WAL frames")
			}
			if db.State().AppliedCommandLSN != beforeLSN || db.CommandWALNextLSN() != beforeNextLSN {
				t.Fatalf("retired tag changed WAL frontiers applied/next=%d/%d, want %d/%d", db.State().AppliedCommandLSN, db.CommandWALNextLSN(), beforeLSN, beforeNextLSN)
			}
			if progress.Len() != 2 || results.Len() != 2 {
				t.Fatalf("retired tag changed progress/results=%d/%d, want 2/2", progress.Len(), results.Len())
			}
			afterDigest, err := LogicalDigestV1ForDB(db, LogicalDigestOptionsV1{})
			if err != nil || afterDigest != beforeDigest {
				t.Fatalf("retired tag changed catalog/documents/index identity: before=%s after=%s err=%v", beforeDigest.Hex(), afterDigest.Hex(), err)
			}
		})
	}
}

func modernColumnGraphCreateEntryForRetirementTest(t *testing.T, name, idempotency string) []byte {
	t.Helper()
	meta := collections.CollectionMeta{
		Name: name,
		Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{
			Enabled: true,
			Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}},
		}},
		VectorIndexes: []collections.VectorIndexDefinition{{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8, Encoding: collections.VectorIndexEncodingFloat32, Strategy: collections.VectorIndexStrategyColumnGraph}},
	}
	payload, err := collections.EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		t.Fatal(err)
	}
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandCreateCollection, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte(idempotency)},
		{ID: nativewire.SectionCollectionMeta, Bytes: append(binary.AppendUvarint(nil, 7), payload...)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
	}
	command, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := nativewire.AppendDeterministicEntry(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type postVisibleCommitAmbiguousFaultForRetirementTest struct{ reached bool }

func (f *postVisibleCommitAmbiguousFaultForRetirementTest) InjectApplyFault(point FaultPointV1, context ApplyFaultContextV1) error {
	if point != FaultAfterVisibleBeforeResultRecordV1 {
		return nil
	}
	f.reached = true
	if context.AppliedCommandLSN == 0 {
		return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "missing committed command-WAL frontier")
	}
	return codedError(raftentry.ErrorUnsafeDurabilityModeV1, "%w", &collections.CommitAmbiguousError{Operation: "post-publication response", Err: fmt.Errorf("injected response failure")})
}
