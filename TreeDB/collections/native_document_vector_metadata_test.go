package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
)

func nativeDocumentMetadataFixture(t *testing.T, format DocumentFormat) CollectionMeta {
	t.Helper()
	meta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "documents", Options: CollectionOptions{DocumentFormat: format},
		VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, Strategy: VectorIndexStrategyNativeRuntime}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return meta
}

func TestNativeDocumentMetadataEncodingAuthority(t *testing.T) {
	for _, format := range []DocumentFormat{DocumentFormatJSON, DocumentFormatBSON} {
		t.Run(string(format), func(t *testing.T) {
			meta := nativeDocumentMetadataFixture(t, format)
			// Compare against the complete original v7 field sequence, rather
			// than allowing the new disk helper to define both sides.
			legacy, err := json.Marshal(struct {
				Version       int                     `json:"version"`
				Name          string                  `json:"name"`
				Options       CollectionOptions       `json:"options,omitempty"`
				Indexes       []IndexDefinition       `json:"indexes,omitempty"`
				VectorIndexes []VectorIndexDefinition `json:"vector_indexes,omitempty"`
				TextIndexes   []TextIndexDefinition   `json:"text_indexes,omitempty"`
			}{collectionMetaVersion, meta.Name, meta.Options, meta.Indexes, meta.VectorIndexes, meta.TextIndexes})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := encodeCollectionMeta(meta)
			if err != nil || !bytes.Equal(raw, legacy) {
				t.Fatalf("unmarked v7 bytes changed: err=%v got=%s want=%s", err, raw, legacy)
			}
			marked := *meta.copy()
			marked.nativeDocumentCoverageVersion = nativeDocumentVectorCoverageVersion
			if sameCollectionMeta(meta, marked) {
				t.Fatal("coverage authority ignored by schema equality")
			}
			raw, err = encodeCollectionMeta(marked)
			if err != nil {
				t.Fatal(err)
			}
			var disk collectionMetaDisk
			if err := json.Unmarshal(raw, &disk); err != nil {
				t.Fatal(err)
			}
			if disk.Version != collectionMetaVersionNativeDocument || disk.NativeDocumentCoverageVersion != nativeDocumentVectorCoverageVersion {
				t.Fatalf("missing explicit metadata8/coverage5: %+v", disk)
			}
			decoded, err := decodeCollectionMeta(raw)
			if err != nil || !sameCollectionMeta(decoded, marked) || !sameCollectionMeta(*decoded.copy(), marked) {
				t.Fatalf("marked roundtrip/copy lost authority: %v", err)
			}
			indexed, _, err := addIndexToCollectionMeta(marked, IndexDefinition{Name: "name", Field: "name", ValueType: IndexValueString})
			if err != nil || indexed.nativeDocumentCoverageVersion != nativeDocumentVectorCoverageVersion {
				t.Fatalf("scalar schema planning lost authority: %v", err)
			}
			// The physical import census must admit the declared graph without
			// aliasing the same collection's scalar domain.
			graphDomain := nativeJSONDomainNumber(indexed, "vector-index/embedding")
			if graphDomain == 0 || graphDomain == nativeJSONDomainNumber(indexed, "index/name") {
				t.Fatal("declared native graph descriptor has no independent domain identity")
			}
			if nativeJSONDomainNumber(indexed, "vector-index/undeclared") != 0 || nativeJSONDomainNumber(meta, "vector-index/embedding") != 0 {
				t.Fatal("undeclared or unmarked graph descriptor gained import authority")
			}
		})
	}
}

func TestNativeDocumentMetadataRejectsUnadvertisedAuthority(t *testing.T) {
	meta := nativeDocumentMetadataFixture(t, DocumentFormatJSON)
	for _, tc := range []struct {
		name            string
		version, marker int
	}{
		{"v5-marker", collectionMetaVersionV5, 5},
		{"v6-marker", collectionMetaVersionV6, 5},
		{"v7-marker", collectionMetaVersion, 5},
		{"v8-absent", collectionMetaVersionNativeDocument, 0},
		{"v8-unknown", collectionMetaVersionNativeDocument, 6},
		{"unknown-version", collectionMetaVersionNativeDocument + 1, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disk := collectionMetaDiskValue(meta)
			disk.Version, disk.NativeDocumentCoverageVersion = tc.version, tc.marker
			raw, err := json.Marshal(disk)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeCollectionMeta(raw); err == nil {
				t.Fatal("unadvertised coverage authority decoded")
			}
		})
	}
	for _, mutate := range []func(*CollectionMeta){
		func(m *CollectionMeta) { m.nativeDocumentCoverageVersion++ },
		func(m *CollectionMeta) { m.VectorIndexes = nil },
		func(m *CollectionMeta) { m.VectorIndexes[0].Strategy = VectorIndexStrategyColumnGraph },
		func(m *CollectionMeta) { m.Options.ColumnStore = &ColumnStoreConfig{Enabled: true} },
	} {
		marked := *meta.copy()
		marked.nativeDocumentCoverageVersion = nativeDocumentVectorCoverageVersion
		mutate(&marked)
		if nativeJSONDomainNumber(marked, "vector-index/embedding") != 0 {
			t.Fatal("invalid coverage shape gained graph descriptor authority")
		}
		if _, err := normalizeCollectionMeta(marked); err == nil {
			t.Fatal("invalid marked shape normalized")
		}
		if _, err := encodeNormalizedCollectionMeta(marked); err == nil {
			t.Fatal("invalid marked shape encoded")
		}
	}
}

func TestNativeDocumentMetadataPublicCreateCannotMintReplayAuthority(t *testing.T) {
	dir := t.TempDir()
	if err := saveCollectionCommandWALFormat(dir); err != nil {
		t.Fatal(err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	manager := NewCommandWALReplayCollectionManager(d)
	defer func() { _ = manager.Close() }()
	meta := nativeDocumentMetadataFixture(t, DocumentFormatJSON)
	meta.nativeDocumentCoverageVersion = nativeDocumentVectorCoverageVersion
	if _, err := manager.CreateCollection(&meta); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy CreateCollection error=%v", err)
	}
	prepared := false
	if _, err := manager.CreateCollectionWithPreparedCommandWALIntent(meta, func() (*backenddb.CommandWALIntent, error) {
		prepared = true
		return nil, errors.New("must not append")
	}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || prepared {
		t.Fatalf("public callback gained authority: err=%v called=%v", err, prepared)
	}
	payload, err := EncodeCatalogCreateCollectionCommandWALPayload(meta)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := d.NewCommandWALIntent(commitlog.CommandKindCatalogCreateCollection, commitlog.CommandScopeCatalog, commitlog.PayloadFormatCatalogCreateCollectionV1, payload)
	if err != nil {
		t.Fatal(err)
	}
	unlock := d.LockCommandWALStaging()
	defer unlock()
	if _, err := d.AppendStagedCommandWALIntent(intent, false); err != nil {
		t.Fatal(err)
	}
	if intent.AssignedLSN() == 0 {
		t.Fatal("control never obtained a real assigned LSN")
	}
	if _, err := manager.CreateCollectionWithCommandWALIntent(meta, intent); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("public assigned intent gained authority: %v", err)
	}
	if _, err := manager.createCollectionWithReplayCommandWALIntent(meta, intent); !errors.Is(err, backenddb.ErrCommandWALRejected) {
		t.Fatalf("staged live intent passed original replay authentication: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 0 {
		t.Fatalf("refused create advanced applied frontier to %d", got)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, meta.Name)
	if err != nil || catalog != nil {
		t.Fatalf("refused create published metadata: catalog=%v err=%v", catalog, err)
	}
}

func TestNativeDocumentMetadataOriginalCatalogReplayAndLegacyRefusal(t *testing.T) {
	dir := t.TempDir()
	if err := saveCollectionCommandWALFormat(dir); err != nil {
		t.Fatal(err)
	}
	meta := nativeDocumentMetadataFixture(t, DocumentFormatJSON)
	meta.nativeDocumentCoverageVersion = nativeDocumentVectorCoverageVersion
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCatalogCreateCollection, commitlog.PayloadFormatCatalogCreateCollectionV1, catalogCreateCollectionPayload(t, meta))
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("actual startup CatalogCreate replay applied=%d want1", got)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("missing snapshot")
	}
	defer func() { _ = snap.Close() }()
	identity, present, err := getSystemValue(snap, nativeCatalogIncarnationPrefix+meta.Name)
	if err != nil || !present {
		t.Fatalf("actual collection incarnation absent: %v", err)
	}
	incarnation, err := nativeCatalogIdentityValue(identity, false)
	if err != nil {
		t.Fatal(err)
	}
	key, err := nativeDocumentGenerationKey(incarnation)
	if err != nil {
		t.Fatal(err)
	}
	generation, present, err := getSystemValue(snap, key)
	if err != nil || !present || !bytes.Equal(generation, encodeRootID(0)) {
		t.Fatalf("original create did not bootstrap actual-incarnation counter0: present=%v value=%x err=%v", present, generation, err)
	}
	manager := NewCollectionManager(d)
	defer func() { _ = manager.Close() }()
	col, err := manager.OpenCollection(meta.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !sameCollectionMeta(col.Meta(), meta) {
		t.Fatal("public metadata copy lost replayed authority")
	}
	if _, err := col.Insert([]byte("one"), []byte(`{"embedding":[1,0]}`)); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy Insert accepted marked schema: %v", err)
	}
	callback := false
	if _, _, err := col.Update([]byte("one"), func(raw []byte) ([]byte, bool, error) { callback = true; return raw, false, nil }); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) || callback {
		t.Fatalf("legacy Update reached callback: err=%v called=%v", err, callback)
	}
	if _, err := col.DeleteDocument([]byte("one")); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy Delete accepted marked schema: %v", err)
	}
	if _, err := col.CreateIndex(IndexDefinition{Name: "name", Field: "name", ValueType: IndexValueString}); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy scalar DDL accepted marked schema: %v", err)
	}
	if _, err := col.DropVectorIndex("embedding"); !errors.Is(err, backenddb.ErrNativeSemanticUnsupported) {
		t.Fatalf("legacy vector DDL accepted marked schema: %v", err)
	}
	if got := d.State().AppliedCommandLSN; got != 1 {
		t.Fatalf("refused mutations advanced applied frontier: %d", got)
	}
}
