package raftfsm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	internalrouter "github.com/snissn/gomap/TreeDB/internal/vectorpartition"
)

// Real FSM Open and committed apply exercise the production gap/order fences;
// Harness-only reopen cannot establish this recovery contract.
func TestColocatedVectorMutationFSMCoveredRecoveryV1(t *testing.T) {
	for _, operation := range []string{"replace", "delete", "same-content", "missing-delete"} {
		for _, point := range []raftapply.FaultPointV1{raftapply.FaultAfterLocalWALAppendBeforeVisibleV1, raftapply.FaultAfterVisibleBeforeResultRecordV1, raftapply.FaultAfterResultRecordBeforeProgressV1} {
			t.Run(fmt.Sprintf("%s/%s", operation, point), func(t *testing.T) {
				dir, database, c, manifest := newColocatedFSMRecoveryFixtureV1(t)
				fsm := openFSMForTest(t, database, dir)
				defer func() { _ = fsm.Close(); _ = database.Close() }()
				scope := commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: manifest.IndexName, Generation: manifest.Generation, OwnerGroup: "default", Digest: sha256.Sum256([]byte("fsm-colocated-scope"))}
				id, document := []byte("base-x"), []byte(`{"embedding":[0,1],"kind":"replacement"}`)
				deletion := operation == "delete" || operation == "missing-delete"
				matched, affected := uint64(1), uint64(1)
				if deletion {
					matched = 0
				}
				if operation == "missing-delete" {
					id, affected = []byte("missing"), 0
				}
				if operation == "same-content" {
					// Same column/retained content, with reordered fields and numbers.
					document = []byte(` {"kind":"base-x","embedding":[1e0,0.0]} `)
					affected = 0
				}
				beforeSource, err := c.Get(id)
				if err != nil {
					t.Fatal(err)
				}
				beforePin, err := c.AcquireVectorPartitionLiveSearchPinV1(manifest)
				if err != nil {
					t.Fatal(err)
				}
				beforeLive := beforePin.StatusV1()
				beforePin.Release()
				raw := colocatedFSMRecoveryEntryV1(t, scope, id, document, deletion, []byte("original"))
				entry := committedCommand(2, 2, raw)
				entry.SyncLocalCommandWAL = true
				applyColocatedCommittedEntryWithRecordedFaultV1(t, fsm, entry, point)
				if err := fsm.Close(); err != nil {
					t.Fatal(err)
				}
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
				database, err = backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
				if err != nil {
					t.Fatal(err)
				}
				// This production Open previously refused the source-visible/result-missing cut.
				fsm = openFSMForTest(t, database, dir)
				c, err = collections.NewCollectionManager(database).OpenCollection("docs")
				if err != nil {
					t.Fatal(err)
				}
				digest := [sha256.Size]byte(raftentry.CommandDigestV1ForBytes(raw, raftentry.DecodeOptions{}))
				original, known, err := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte("original"), digest)
				if err != nil || !known || original.Term != entry.Term || original.Index != entry.Index || original.Matched != matched || original.Affected != affected {
					t.Fatalf("original=%+v known=%v err=%v", original, known, err)
				}
				if err := c.EnsureVectorPartitionLiveBindingV1(t.Context(), manifest); err != nil {
					t.Fatal(err)
				}
				pin, err := c.AcquireVectorPartitionLiveSearchPinV1(manifest)
				if err != nil {
					t.Fatal(err)
				}
				live := pin.StatusV1()
				pin.Release()
				if affected == 0 && (original.Coverage != beforeLive.Coverage || original.Revision != beforeLive.Revision || live.Coverage != beforeLive.Coverage || live.Revision != beforeLive.Revision) {
					t.Fatalf("no-op changed live/source floor before=%+v live=%+v witness=%+v", beforeLive, live, original)
				}
				if affected != 0 && (original.Coverage <= beforeLive.Coverage || original.Revision <= beforeLive.Revision) {
					t.Fatalf("changed content failed to advance before=%+v witness=%+v", beforeLive, original)
				}
				afterSource, err := c.Get(id)
				if err != nil || (affected == 0 && !bytes.Equal(beforeSource, afterSource)) {
					t.Fatalf("no-op changed source before=%q after=%q err=%v", beforeSource, afterSource, err)
				}
				beforeLSN := database.State().AppliedCommandLSN
				before, err := c.Get(id)
				if err != nil {
					t.Fatal(err)
				}
				// Neither changed bytes nor caller term/index can supply the
				// authority needed to cross the singleton recovery fence.
				wrongCommand := entry
				wrongCommand.Bytes = colocatedFSMRecoveryEntryV1(t, scope, id, []byte(`{"embedding":[-1,0]}`), !deletion, []byte("original"))
				wrongTerm, wrongIndex := entry, entry
				wrongTerm.Term++
				wrongIndex.Index++
				beforeState, stateOK := database.StateToken()
				if !stateOK {
					t.Fatal("missing state before recovery refusal controls")
				}
				beforeNextLSN := database.CommandWALNextLSN()
				beforeResults, beforeProgress := fsm.results.Len(), fsm.progress.Len()
				for _, invalid := range []struct {
					name  string
					entry CommittedEntryV1
				}{{"wrong-command", wrongCommand}, {"wrong-term", wrongTerm}, {"wrong-index", wrongIndex}} {
					if _, err := fsm.ApplyCommittedEntryV1(invalid.entry); err == nil {
						t.Fatalf("%s crossed production recovery fence", invalid.name)
					}
					afterState, stateOK := database.StateToken()
					after, err := c.Get(id)
					witness, known, witnessErr := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte("original"), digest)
					if !stateOK || afterState != beforeState || database.CommandWALNextLSN() != beforeNextLSN || err != nil || !bytes.Equal(before, after) || witnessErr != nil || !known || witness != original || fsm.results.Len() != beforeResults || fsm.progress.Len() != beforeProgress {
						t.Fatalf("%s refusal altered source/WAL/witness/results/progress: sourceErr=%v witness=%+v known=%v witnessErr=%v", invalid.name, err, witness, known, witnessErr)
					}
				}

				result, err := fsm.ApplyCommittedEntryV1(entry)
				if err != nil || (result.Status != raftentry.ApplyStatusAlreadyApplied && result.Status != raftentry.ApplyStatusApplied) || (result.Status == raftentry.ApplyStatusAlreadyApplied && (result.MatchedCount != 0 || result.AffectedCount != 0)) {
					t.Fatalf("retry=%+v err=%v", result, err)
				}
				after, err := c.Get(id)
				if err != nil || !bytes.Equal(before, after) || database.State().AppliedCommandLSN != beforeLSN {
					t.Fatalf("retry changed source/WAL before=%s after=%s err=%v", before, after, err)
				}
				if record, known, err := fsm.results.LookupApplyResult(raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}); err != nil || !known || record.CommandDigest != result.CommandDigest || record.AppliedCommandLSN != beforeLSN {
					t.Fatalf("result not durably recorded before recovery success: record=%+v known=%v err=%v", record, known, err)
				}
				if last, known := fsm.LastApplied(); !known || last.Term != entry.Term || last.Index != entry.Index {
					t.Fatalf("success did not record progress: last=%+v known=%v", last, known)
				}
				recovered, known, err := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte("original"), digest)
				if err != nil || !known || recovered != original {
					t.Fatalf("original outcome changed: recovered=%+v original=%+v err=%v", recovered, original, err)
				}
			})
		}
	}
}

// Valid retained witnesses cannot widen the production Open exception from
// one interrupted physical command LSN to two. No external result/progress is
// recorded by either deliberately interrupted publication.
func TestColocatedVectorMutationFSMRejectsTwoLSNCoverageGapV1(t *testing.T) {
	dir, database, c, manifest := newColocatedFSMRecoveryFixtureV1(t)
	fsm := openFSMForTest(t, database, dir)
	defer func() { _ = fsm.Close(); _ = database.Close() }()
	scope := commitlog.ColocatedVectorMutationScopeV1{Version: 1, Index: manifest.IndexName, Generation: manifest.Generation, OwnerGroup: "default", Digest: sha256.Sum256([]byte("fsm-colocated-two-gap-scope"))}
	baseline, known := fsm.progress.LastAppliedRecord()
	if !known || baseline.AppliedCommandLSN != database.State().AppliedCommandLSN {
		t.Fatal("fixture genesis lacks exact progress coverage")
	}
	for i, key := range []string{"first", "second"} {
		raw := colocatedFSMRecoveryEntryV1(t, scope, []byte("missing"), nil, true, []byte(key))
		entry := committedCommand(2, uint64(2+i), raw)
		entry.SyncLocalCommandWAL = true
		// The established fault helper cuts the actual apply executor after
		// publication. The second cut deliberately constructs the otherwise
		// unsupported two-LSN state for production Open to reject.
		applyColocatedCommittedEntryWithRecordedFaultV1(t, fsm, entry, raftapply.FaultAfterVisibleBeforeResultRecordV1)
		digest := [sha256.Size]byte(raftentry.CommandDigestV1ForBytes(raw, raftentry.DecodeOptions{}))
		outcome, known, err := c.ReadVectorPartitionColocatedOutcomeV1(scope, []byte(key), digest)
		if err != nil || !known || outcome.Term != entry.Term || outcome.Index != entry.Index || outcome.AppliedCommandLSN != baseline.AppliedCommandLSN+uint64(i+1) || outcome.Matched != 0 || outcome.Affected != 0 {
			t.Fatalf("invalid retained outcome: %+v known=%v err=%v", outcome, known, err)
		}
	}
	if fsm.results.Len() != 0 || fsm.progress.Len() != 1 || database.State().AppliedCommandLSN != baseline.AppliedCommandLSN+2 {
		t.Fatal("fault fixture did not retain exactly a two-physical-LSN progress gap")
	}
	if _, _, err := c.VerifyVectorPartitionColocatedMutationLogicalStateV1(t.Context()); err != nil {
		t.Fatalf("two-gap fixture must have a valid complete witness chain: %v", err)
	}
	if err := fsm.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	database, err = backenddb.Open(backenddb.Options{Dir: dir, CommandWAL: true, DisableBackgroundPrune: true, ResolvedProfile: backenddb.ProfileCommandWALDurable})
	if err != nil {
		t.Fatal(err)
	}
	c, err = collections.NewCollectionManager(database).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	_, latest, err := c.VerifyVectorPartitionColocatedMutationLogicalStateV1(t.Context())
	if err != nil || latest.AppliedCommandLSN != baseline.AppliedCommandLSN+2 || database.State().AppliedCommandLSN != latest.AppliedCommandLSN {
		t.Fatalf("reopened gap must retain valid covered witness authority: latest=%+v err=%v", latest, err)
	}
	opened, err := Open(Options{DB: database, Cluster: validFSMClusterConfig(dir), StoreOptions: raftapply.DurableApplyStoreOptions{DisableSync: true}})
	if opened != nil {
		_ = opened.Close()
		t.Fatal("production FSM Open admitted a two-physical-LSN gap")
	}
	if code, _ := ErrorCodeOf(err); err == nil || code != raftentry.ErrorUnsafeDurabilityModeV1 {
		t.Fatalf("production FSM Open error=%v code=%s, want unsafe durability refusal", err, code)
	}
}

func colocatedFSMRecoveryEntryV1(t testing.TB, scope commitlog.ColocatedVectorMutationScopeV1, id, document []byte, deletion bool, key []byte) []byte {
	t.Helper()
	rawScope, err := commitlog.EncodeColocatedVectorMutationScopeV1(scope)
	if err != nil {
		t.Fatal(err)
	}
	command := nativewire.CommandReplaceBatch
	if deletion {
		command = nativewire.CommandDeleteBatch
	}
	sections := []nativewire.Section{{ID: nativewire.SectionCommandHeader, Bytes: nativewire.AppendCommandHeader(nil, nativewire.CommandHeader{ID: command, Version: 1})}, {ID: nativewire.SectionCollectionRef, Bytes: deterministicTestCollectionNameRef("docs")}, {ID: nativewire.SectionIdempotencyKey, Bytes: key}, {ID: nativewire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, testCatalogVersionStart)}, {ID: nativewire.SectionDocumentIDs, Bytes: nativewire.AppendByteVector(nil, id)}, {ID: nativewire.SectionColocatedVectorMutationScopeV1, Bytes: rawScope}}
	if !deletion {
		sections = append(sections, nativewire.Section{ID: nativewire.SectionDocumentFormat, Bytes: binary.AppendUvarint(nil, uint64(nativewire.DocumentFormatJSON))}, nativewire.Section{ID: nativewire.SectionDocuments, Bytes: nativewire.AppendByteVector(nil, document)}, nativewire.Section{ID: nativewire.SectionReplacementMode, Bytes: binary.AppendUvarint(nil, 1)})
	}
	valid, err := nativewire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := nativewire.AppendDeterministicEntry(nil, valid)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Reuse the established materialization and genesis coverage pattern; all work
// after fixture genesis uses the real production FSM and durable result stores.
func newColocatedFSMRecoveryFixtureV1(t testing.TB) (string, *backenddb.DB, *collections.Collection, collections.VectorPartitionManifestV1) {
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
		Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "default"}},
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
	bootstrapRaftSnapshotFSMCoverageForDirectVectorPartitionFixture(t, database, dir)
	return dir, database, collection, prepared
}

func applyColocatedCommittedEntryWithRecordedFaultV1(t *testing.T, fsm *FSM, entry CommittedEntryV1, point raftapply.FaultPointV1) {
	t.Helper()
	id := raftentry.ApplyEntryID{Term: entry.Term, Index: entry.Index}
	meta, err := fsm.applyMetadata(entry, id)
	if err != nil {
		t.Fatalf("applyMetadata: %v", err)
	}
	fault := &colocatedFSMRecordedFaultV1{fsmSinglePointFaultInjector: fsmSinglePointFaultInjector{point: point}}
	result, err := raftapply.ApplyCommittedEntryV1(fsm.db, entry.Bytes, meta, raftapply.Options{DecodeLimits: fsm.decodeLimits, ProgressStore: fsm.progress, ResultStore: fsm.results, FaultInjector: fault})
	if err == nil || !fault.reached || result.Status != raftentry.ApplyStatusRecoveryRequired {
		t.Fatalf("intended fault %s reached=%v result=%+v err=%v", point, fault.reached, result, err)
	}
}

type colocatedFSMRecordedFaultV1 struct {
	fsmSinglePointFaultInjector
	reached bool
}

func (f *colocatedFSMRecordedFaultV1) InjectApplyFault(point raftapply.FaultPointV1, ctx raftapply.ApplyFaultContextV1) error {
	if point == f.point {
		f.reached = true
	}
	return f.fsmSinglePointFaultInjector.InjectApplyFault(point, ctx)
}
