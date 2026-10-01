package raftapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// Local native apply/WAL controls; authenticated integration tests separately
// enforce actual Raft and serving authority. These reopen controls are graceful.
func TestSplitVectorInsertApplyRecoveryV1(t *testing.T) {
	for _, phase := range []string{"source", "project", "clear"} {
		for _, point := range []FaultPointV1{FaultAfterLocalWALAppendBeforeVisibleV1, FaultAfterVisibleBeforeResultRecordV1} {
			t.Run(fmt.Sprintf("%s/%s", phase, point), func(t *testing.T) {
				dir, database, collection, manifest := newSplitApplyRecoveryFixtureV1(t)
				defer func() { _ = database.Close() }()
				v := splitApplyRecoveryValueV1(manifest)
				if phase == "clear" {
					result, err := ApplyCommittedEntryV1(database, splitApplyRecoveryEntryV1(t, v), splitApplyRecoveryMetaV1(v), Options{})
					if err != nil || result.Status != raftentry.ApplyStatusApplied {
						t.Fatalf("source seed: %+v %v", result, err)
					}
				}
				v.Operation = phase
				if phase != "source" {
					v.Document = nil
					v.SourceTerm, v.SourceIndex = 1, 1
				}
				if phase == "clear" {
					v.TargetTerm, v.TargetIndex, v.LiveRevision = 2, 9, 1
				}
				raw := splitApplyRecoveryEntryV1(t, v)
				beforeLSN := database.State().AppliedCommandLSN
				frames := len(readCommandWALFrames(t, dir))
				progress, results := NewMemoryApplyProgressStore(8, 8), NewMemoryApplyResultStore(8)
				result, err := ApplyCommittedEntryV1(database, raw, splitApplyRecoveryMetaV1(v), Options{
					ProgressStore: progress, ResultStore: results, FaultInjector: singlePointFaultInjector{point: point},
				})
				if err == nil || result.Status != raftentry.ApplyStatusRecoveryRequired {
					t.Fatalf("fault result=%+v err=%v", result, err)
				}
				if progress.Len() != 0 || results.Len() != 0 {
					t.Fatalf("fault advanced stores: %d/%d", progress.Len(), results.Len())
				}
				if got := len(readCommandWALFrames(t, dir)); got != frames+1 {
					t.Fatalf("frames=%d want %d", got, frames+1)
				}
				if point == FaultAfterLocalWALAppendBeforeVisibleV1 {
					if database.State().AppliedCommandLSN != beforeLSN {
						t.Fatal("post-append fault advanced publication")
					}
					row, err := collection.Get(v.ID)
					if err != nil {
						t.Fatal(err)
					}
					pending, _, known, err := collection.VectorPartitionSplitInsertStateV1(v)
					if err != nil || known || (phase == "clear") != (pending != nil) {
						t.Fatalf("prepublication pending=%+v known=%v err=%v", pending, known, err)
					}
					if phase == "clear" {
						if !bytes.Equal(row, splitApplyRecoveryValueV1(manifest).Document) {
							t.Fatalf("seed row=%q", row)
						}
					} else if row != nil {
						t.Fatalf("prepublication row=%q", row)
					}
				}
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database = openApplyHarnessDBWithOptions(t, dir, backenddb.Options{ResolvedProfile: backenddb.ProfileCommandWALDurable})
				collection, err = collections.NewCollectionManager(database).OpenCollection(v.Collection)
				if err != nil {
					t.Fatal(err)
				}
				splitApplyRecoveryAssertV1(t, collection, manifest, v)
				result, err = ApplyCommittedEntryV1(database, raw, splitApplyRecoveryMetaV1(v), Options{})
				if err != nil || (result.Status != raftentry.ApplyStatusApplied && result.Status != raftentry.ApplyStatusAlreadyApplied) {
					t.Fatalf("exact replay: %+v %v", result, err)
				}
				splitApplyRecoveryAssertV1(t, collection, manifest, v)
			})
		}
	}
}

func TestSplitVectorInsertTargetStoredResultRecoveryV1(t *testing.T) {
	dir, database, collection, manifest := newSplitApplyRecoveryFixtureV1(t)
	defer func() { _ = database.Close() }()
	v := splitApplyRecoveryValueV1(manifest)
	v.Operation, v.Document, v.SourceTerm, v.SourceIndex = "project", nil, 1, 1
	raw := splitApplyRecoveryEntryV1(t, v)
	progress, results := NewMemoryApplyProgressStore(8, 8), NewMemoryApplyResultStore(8)
	result, err := ApplyCommittedEntryV1(database, raw, splitApplyRecoveryMetaV1(v), Options{
		ProgressStore: progress, ResultStore: results, FaultInjector: singlePointFaultInjector{point: FaultAfterResultRecordBeforeProgressV1},
	})
	if err == nil || result.Status != raftentry.ApplyStatusRecoveryRequired || progress.Len() != 0 || results.Len() != 1 {
		t.Fatalf("result-before-progress: %+v %v %d/%d", result, err, progress.Len(), results.Len())
	}
	frames := len(readCommandWALFrames(t, dir))
	splitApplyRecoveryAssertV1(t, collection, manifest, v)
	replayed, err := ApplyCommittedEntryV1(database, raw, splitApplyRecoveryMetaV1(v), Options{ProgressStore: progress, ResultStore: results})
	if err != nil || replayed.Status != raftentry.ApplyStatusApplied || progress.Len() != 1 || results.Len() != 1 {
		t.Fatalf("stored replay: %+v %v %d/%d", replayed, err, progress.Len(), results.Len())
	}
	if len(readCommandWALFrames(t, dir)) != frames {
		t.Fatal("stored-result replay appended another frame")
	}
	splitApplyRecoveryAssertV1(t, collection, manifest, v)
}

func splitApplyRecoveryValueV1(m collections.VectorPartitionManifestV1) commitlog.SplitVectorInsertV1 {
	doc := []byte(`{"embedding":[0,1],"kind":"split-new"}`)
	digest := sha256.Sum256(doc)
	fixed := fmt.Sprintf("%064x", 1)
	return commitlog.SplitVectorInsertV1{Version: 1, Operation: "source", Collection: m.Collection, Index: m.IndexName, Generation: m.Generation,
		SourceGroup: "group-a", TargetGroup: "group-b", CatalogEpoch: 7, CatalogDigest: fixed, ReadySetDigest: fixed, ModelDigest: fixed,
		Attempt: []byte("split-recovery-attempt"), ID: []byte("split-new"), Vector: []float32{0, 1}, Document: doc, DocumentDigest: hex.EncodeToString(digest[:])}
}

func splitApplyRecoveryMetaV1(v commitlog.SplitVectorInsertV1) ApplyMetadataV1 {
	meta := applyMeta(1, 1)
	meta.GroupID = v.SourceGroup
	if v.Operation == "project" {
		meta.GroupID = v.TargetGroup
	}
	meta.SyncLocalCommandWAL = true
	return meta
}

func splitApplyRecoveryEntryV1(t *testing.T, v commitlog.SplitVectorInsertV1) []byte {
	t.Helper()
	payload, err := commitlog.EncodeSplitVectorInsertPayloadV1(v)
	if err != nil {
		t.Fatal(err)
	}
	sections := []nativewire.Section{
		{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: nativewire.CommandSplitVectorInsertV1, Version: 1})},
		{ID: nativewire.SectionIdempotencyKey, Bytes: []byte("split-recovery/" + v.Operation)},
		{ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef(v.Collection)},
		{ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)},
		{ID: nativewire.SectionSplitVectorInsertV1, Bytes: payload},
	}
	cmd, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := nativewire.AppendDeterministicEntry(nil, cmd)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func splitApplyRecoveryAssertV1(t *testing.T, c *collections.Collection, m collections.VectorPartitionManifestV1, v commitlog.SplitVectorInsertV1) {
	t.Helper()
	pending, receipt, known, err := c.VectorPartitionSplitInsertStateV1(v)
	if err != nil {
		t.Fatal(err)
	}
	row, err := c.Get(v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v.Operation == "source" {
		original := splitApplyRecoveryValueV1(m)
		original.SourceTerm, original.SourceIndex = 1, 1
		want, err := commitlog.EncodeSplitVectorInsertPayloadV1(original)
		if err != nil {
			t.Fatal(err)
		}
		if pending == nil {
			t.Fatal("source row lacks pending intent")
		}
		got, err := commitlog.EncodeSplitVectorInsertPayloadV1(*pending)
		if err != nil || !bytes.Equal(got, want) || known || !bytes.Equal(row, original.Document) {
			t.Fatalf("source row/intent row=%q pending=%+v known=%v err=%v", row, pending, known, err)
		}
		return
	}
	digest, err := v.DigestV1()
	if err != nil {
		t.Fatal(err)
	}
	term, index := uint64(1), uint64(1)
	if v.Operation == "clear" {
		term, index = v.TargetTerm, v.TargetIndex
	}
	if pending != nil || !known || receipt.Digest != digest || receipt.Attempt != sha256.Sum256(v.Attempt) || !bytes.Equal(receipt.ID, v.ID) || receipt.SourceTerm != 1 || receipt.SourceIndex != 1 || receipt.TargetTerm != term || receipt.TargetIndex != index || receipt.LiveRevision != 1 {
		t.Fatalf("receipt/retirement pending=%+v receipt=%+v known=%v", pending, receipt, known)
	}
	if v.Operation == "clear" {
		if !bytes.Equal(row, splitApplyRecoveryValueV1(m).Document) {
			t.Fatalf("clear lost canonical row=%q", row)
		}
		return
	}
	if row != nil {
		t.Fatalf("target received canonical row=%q", row)
	}
	if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	pin, err := c.AcquireVectorPartitionLiveSearchPinV1(m)
	if err != nil {
		t.Fatal(err)
	}
	defer pin.Release()
	hits, _, err := pin.SearchDomainV1(t.Context(), 0, v.Vector, collections.VectorPartitionSearchOptionsV1{TopK: 1, EfSearch: 8, MaxStableIDBytes: 32})
	if err != nil || len(hits) != 1 || hits[0].ID != string(v.ID) || pin.StatusV1().Revision != receipt.LiveRevision {
		t.Fatalf("projection hits=%+v status=%+v err=%v", hits, pin.StatusV1(), err)
	}
}

// Same bounded construction as fixedPeerVectorSeedV1; no transport or authority
// exemption and no new exported fixture API.
func newSplitApplyRecoveryFixtureV1(t *testing.T) (string, *backenddb.DB, *collections.Collection, collections.VectorPartitionManifestV1) {
	t.Helper()
	if !collections.VectorPartitionNamespacePersistenceSupportedForTestingV1() {
		t.Skip("vector partition namespace persistence unsupported on this platform")
	}
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}, DurabilityProfile: backenddb.ProfileCommandWALDurable}); err != nil {
		t.Fatal(err)
	}
	// Direct construction is trusted fixture genesis. The opaque command-WAL
	// coverage is bound to Raft apply progress before any child is started; all
	// behavior under test is a later real committed command.
	database, err := backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	if err != nil {
		t.Fatal(err)
	}
	definition := collections.VectorIndexDefinition{Name: "embedding_graph", Field: "embedding", Metric: collections.VectorMetricCosine, Dimensions: 2, M: 2, EfConstruction: 8, EfSearch: 8, Strategy: collections.VectorIndexStrategyColumnGraph}
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON, ColumnStore: &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2}}}}, VectorIndexes: []collections.VectorIndexDefinition{definition}}
	manager := collections.NewCollectionManager(database)
	if _, err := manager.CreateCollection(&meta); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	collection, err := manager.OpenCollection(meta.Name)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	for _, document := range []struct{ id, body string }{{"base-x", `{"embedding":[1,0],"kind":"base-x"}`}, {"base-diagonal", `{"embedding":[0.6,0.8],"kind":"base-diagonal"}`}} {
		if _, err := collection.Insert([]byte(document.id), []byte(document.body)); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
	}
	if _, err := collection.RebuildVectorIndex(definition.Name); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	source, rows, err := collection.ReadVectorPartitionRouterSourceRowsV1(definition.Name)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manifest := collections.VectorPartitionManifestV1{
		State: "building", Collection: meta.Name, IndexName: definition.Name,
		IndexDefinitionDigest: collections.VectorIndexDefinitionDigestV1(definition),
		SourceGeneration:      source.Generation, SourceChecksum: source.Checksum, SourceSchemaHash: source.SchemaHash, SourceRowCount: source.RowCount,
		Generation: source.Generation + 100, PartitionCount: 1, DomainCount: 1,
		DomainPacks: []collections.VectorPartitionDomainPackV1{{DomainID: 0, PackID: 0}}, BalancePolicy: "disjoint_v1",
		Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-b"}},
	}
	partition := internalrouter.RouterPartitionV1{PartitionID: 0}
	for _, row := range rows {
		manifest.Memberships = append(manifest.Memberships, collections.VectorPartitionMembershipV1{VectorOrdinal: row.VectorOrdinal, PartitionID: 0})
		partition.Vectors = append(partition.Vectors, internalrouter.RouterVectorV1{Ordinal: row.VectorOrdinal, Values: append([]float32(nil), row.Values...), MembershipKind: string(collections.VectorPartitionMembershipHomeV1)})
	}
	manifest.Canonicalize()
	assets, resources, err := collection.MaterializeVectorPartitionLocalSearchAssetsV1(definition.Name, manifest, 4701, []collections.VectorPartitionSearchAssetV1{{Source: source, Generation: manifest.Generation, PartitionID: 0, Dimensions: definition.Dimensions}})
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	manifest.Assets = assets
	manifest.Canonicalize()
	if err := collection.PublishVectorPartitionManifestV1(manifest, nil); err != nil {
		resources.Release()
		_ = database.Close()
		t.Fatal(err)
	}
	routerConfig := internalrouter.DefaultRouterConfigV1()
	routerConfig.BranchFactor, routerConfig.LeafSize, routerConfig.RepresentativeBudget = 2, 1, 1
	routerConfig.MaxDepth, routerConfig.MaxIterations, routerConfig.MaxVectors = 4, 8, 8
	routerConfig.MaxDimensions, routerConfig.MaxRepresentatives, routerConfig.MaxScalarWork = 8, 32, 1_000_000
	if _, err := collection.BuildAndPublishVectorPartitionRouterV1(context.Background(), manifest, []internalrouter.RouterPartitionV1{partition}, collections.VectorPartitionRouterBuildOptionsV1{Config: routerConfig, AssetFileID: 4702, AssetPartID: 1, M: 2, EfConstruction: 8, EfSearch: 8}); err != nil {
		resources.Release()
		_ = database.Close()
		t.Fatal(err)
	}
	prepared, err := collection.PreparedVectorPartitionManifestWithContextV1(context.Background(), definition.Name, manifest.Generation)
	resources.Release()
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := collection.EnsureVectorPartitionLiveBindingV1(t.Context(), prepared); err != nil {
		t.Fatal(err)
	}
	return dir, database, collection, prepared
}
