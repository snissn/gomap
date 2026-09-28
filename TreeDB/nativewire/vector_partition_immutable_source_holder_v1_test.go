package nativewire

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/raftapply"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftentry"
	"github.com/snissn/gomap/TreeDB/internal/raftfsm"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

func TestVectorPartitionImmutableSourceHolderPreparationUsesPreparedBytesAndCatalogLeaderV1(t *testing.T) {
	fixture := newVectorPartitionLiveNativewireFixtureV1(t)
	t.Cleanup(func() { _ = fixture.database.Close() })
	features := raftplacement.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{
		Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle],
	})
	collection := raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: fixture.manifest.Collection}
	otherCollection := raftplacement.CollectionRefV1{Database: "other", Catalog: "other", Collection: fixture.manifest.Collection}
	record, err := raftplacement.NewCatalogMetaRecordV1(1, raftplacement.CatalogV1{
		Features: features,
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"source-meta"}, LeaderHint: "source-meta"},
			{ID: "group-b", Members: []raftcluster.NodeID{"owner-b"}, LeaderHint: "owner-b"},
		},
		Placements: []raftplacement.CollectionPlacementV1{
			{Collection: collection, Mode: raftplacement.PlacementModeCollectionV1, GroupID: "group-a"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	authority, provider := openVectorSourceHolderTestCatalogV1(t, ctx, record)
	dataReads := &fakeVectorPartitionReadCoordinatorV1{
		proof:    raftcluster.ReadIndexProof{NodeID: "source-meta", GroupID: "group-a", Term: 1, Index: 1, HasQuorum: true, EvidenceKind: raftcluster.ReadIndexEvidenceProduction},
		progress: raftcluster.AppliedProgress{NodeID: "source-meta", GroupID: "group-a", Term: 1, Index: 1, HasApplied: true},
	}
	prepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, authority, provider, sourceHolderTestCaptureV1(fixture.collection, dataReads))
	if err != nil {
		t.Fatal(err)
	}
	manifest := fixture.manifest
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection: collection, CollectionIncarnation: 1, IndexName: manifest.IndexName,
			IndexDefinitionDigest: manifest.IndexDefinitionDigest, IndexEpoch: 1,
			CatalogEpoch: record.Epoch, CatalogDigest: record.Digest,
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: manifest.SourceGeneration, Checksum: manifest.SourceChecksum,
			SchemaHash: manifest.SourceSchemaHash, RowCount: manifest.SourceRowCount,
		},
		Generation: manifest.Generation,
	}
	verified, groups, err := prepare(ctx, identity)
	manifestBytes, encodeErr := collections.EncodeVectorPartitionManifestV1(manifest)
	placementDigest, placementErr := collections.VectorPartitionPlacementDigestV1(manifest)
	if encodeErr != nil || placementErr != nil {
		t.Fatalf("fixture manifest encoding=%v placement=%v", encodeErr, placementErr)
	}
	if err != nil || verified.ManifestDigest != fmt.Sprintf("%x", sha256.Sum256(manifestBytes)) || verified.PlacementDigest != placementDigest ||
		len(groups) != 2 || groups[0] != "group-a" || groups[1] != "group-b" {
		t.Fatalf("source and catalog preparation=%+v groups=%v err=%v", verified, groups, err)
	}
	// A local collection exposes only its leaf name. Even when the catalog
	// places an identically named collection elsewhere, its bytes cannot
	// certify that other full collection reference.
	wrongCollection := identity
	wrongCollection.Index.Collection = otherCollection
	if _, _, err := prepare(ctx, wrongCollection); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("foreign full collection reference accepted: %v", err)
	}
	if _, ok := authority.VectorPartitionLifecycleRecordV1(wrongCollection); ok {
		t.Fatal("foreign collection reference published a BUILD record")
	}
	// A second full reference with the same leaf in this data group makes
	// local source bytes ambiguous. Neither reference may be attested.
	ambiguousCatalog := record.Catalog
	ambiguousCatalog.Placements = append(append([]raftplacement.CollectionPlacementV1(nil), record.Catalog.Placements...),
		raftplacement.CollectionPlacementV1{Collection: otherCollection, GroupID: "group-a"})
	ambiguousRecord, err := raftplacement.NewCatalogMetaRecordV1(1, ambiguousCatalog)
	if err != nil {
		t.Fatal(err)
	}
	ambiguousAuthority, ambiguousProvider := openVectorSourceHolderTestCatalogV1(t, ctx, ambiguousRecord)
	ambiguousPrepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, ambiguousAuthority, ambiguousProvider, sourceHolderTestCaptureV1(fixture.collection, dataReads))
	if err != nil {
		t.Fatal(err)
	}
	ambiguousIdentity := identity
	ambiguousIdentity.Index.CatalogDigest = ambiguousRecord.Digest
	if _, _, err := ambiguousPrepare(ctx, ambiguousIdentity); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("ambiguous same-name source accepted: %v", err)
	}
	// The same leaf in a different data group has a separate local DB and
	// must not prevent this group's source holder from attesting its bytes.
	otherGroupCatalog := record.Catalog
	otherGroupCatalog.Placements = append(append([]raftplacement.CollectionPlacementV1(nil), record.Catalog.Placements...),
		raftplacement.CollectionPlacementV1{Collection: otherCollection, GroupID: "group-b"})
	otherGroupRecord, err := raftplacement.NewCatalogMetaRecordV1(1, otherGroupCatalog)
	if err != nil {
		t.Fatal(err)
	}
	otherGroupCtx, cancelOtherGroup := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancelOtherGroup()
	otherGroupAuthority, otherGroupProvider := openVectorSourceHolderTestCatalogV1(t, otherGroupCtx, otherGroupRecord)
	otherGroupPrepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, otherGroupAuthority, otherGroupProvider, sourceHolderTestCaptureV1(fixture.collection, dataReads))
	if err != nil {
		t.Fatal(err)
	}
	otherGroupIdentity := identity
	otherGroupIdentity.Index.CatalogDigest = otherGroupRecord.Digest
	if got, gotGroups, err := otherGroupPrepare(otherGroupCtx, otherGroupIdentity); err != nil || got.ManifestDigest != verified.ManifestDigest ||
		got.PlacementDigest != verified.PlacementDigest || len(gotGroups) != len(groups) || gotGroups[0] != groups[0] || gotGroups[1] != groups[1] {
		t.Fatalf("different-group same-name source proof=%+v groups=%v err=%v", got, gotGroups, err)
	}
	// A token-routed reference can still use this group's leaf-name DB.
	// Reject it even though its CollectionPlacementV1.GroupID is empty.
	sameLeafTokenCatalog := record.Catalog
	split := ^uint64(0) / 2
	sameLeafTokenCatalog.Placements = append(append([]raftplacement.CollectionPlacementV1(nil), record.Catalog.Placements...),
		raftplacement.CollectionPlacementV1{Collection: otherCollection, Mode: raftplacement.PlacementModeTokenV1,
			TokenPartitions: []raftplacement.TokenPartitionV1{
				{ID: "token-0", GroupID: "group-a", Start: 0, End: split},
				{ID: "token-1", GroupID: "group-b", Start: split + 1, End: ^uint64(0)},
			}})
	sameLeafTokenRecord, err := raftplacement.NewCatalogMetaRecordV1(1, sameLeafTokenCatalog)
	if err != nil {
		t.Fatal(err)
	}
	sameLeafTokenCtx, cancelSameLeafToken := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancelSameLeafToken()
	sameLeafTokenAuthority, sameLeafTokenProvider := openVectorSourceHolderTestCatalogV1(t, sameLeafTokenCtx, sameLeafTokenRecord)
	sameLeafTokenPrepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, sameLeafTokenAuthority, sameLeafTokenProvider, sourceHolderTestCaptureV1(fixture.collection, dataReads))
	if err != nil {
		t.Fatal(err)
	}
	sameLeafTokenIdentity := identity
	sameLeafTokenIdentity.Index.CatalogDigest = sameLeafTokenRecord.Digest
	if _, _, err := sameLeafTokenPrepare(sameLeafTokenCtx, sameLeafTokenIdentity); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("token-routed same-group leaf accepted: %v", err)
	}
	if _, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, authority, provider, nil); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
		t.Fatalf("missing data read authority accepted: %v", err)
	}
	// A copied source on a stale follower cannot certify a prepared generation,
	// even when its local manifest bytes still match the requested identity.
	staleReads := &fakeVectorPartitionReadCoordinatorV1{
		proof:    raftcluster.ReadIndexProof{NodeID: "stale-follower", GroupID: "group-a", Term: 1, Index: 1, HasQuorum: true, EvidenceKind: raftcluster.ReadIndexEvidenceProduction},
		progress: raftcluster.AppliedProgress{NodeID: "stale-follower", GroupID: "group-a", Term: 1, Index: 1, HasApplied: true},
	}
	stalePrepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, authority, provider, sourceHolderTestCaptureV1(fixture.collection, staleReads))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := stalePrepare(ctx, identity); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("stale follower source accepted: %v", err)
	}
	unappliedReads := &fakeVectorPartitionReadCoordinatorV1{
		proof:    raftcluster.ReadIndexProof{NodeID: "source-meta", GroupID: "group-a", Term: 1, Index: 2, HasQuorum: true, EvidenceKind: raftcluster.ReadIndexEvidenceProduction},
		progress: raftcluster.AppliedProgress{NodeID: "source-meta", GroupID: "group-a", Term: 1, Index: 1, HasApplied: true},
	}
	unappliedPrepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, authority, provider, sourceHolderTestCaptureV1(fixture.collection, unappliedReads))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := unappliedPrepare(ctx, identity); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("unapplied data read index accepted: %v", err)
	}
	// These V1 identity fields are absent from both the prepared manifest and
	// catalog record. The callback deliberately returns only the proven pair
	// and owners; it cannot certify these caller fields for serving authority.
	unbound := identity
	unbound.Index.CollectionIncarnation++
	unbound.Index.IndexEpoch++
	unboundProof, unboundGroups, err := prepare(ctx, unbound)
	if err != nil || unboundProof != verified || len(unboundGroups) != len(groups) ||
		unboundGroups[0] != groups[0] || unboundGroups[1] != groups[1] {
		t.Fatalf("V1 callback unexpectedly certified incarnation/index epoch: proof=%+v groups=%v err=%v", unboundProof, unboundGroups, err)
	}
	coordinator := raftplacement.VectorPartitionLifecycleCoordinatorV1{Authority: authority, Committer: provider, PrepareImmutableV1: prepare}
	mutationEpoch, err := coordinator.BuildSourceMutationEpochV1(collection)
	if err != nil {
		t.Fatal(err)
	}
	bad := identity
	bad.Immutable = verified
	bad.Immutable.ManifestDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if _, err := coordinator.BeginBuildV1(ctx, bad, groups, 0, mutationEpoch); !errors.Is(err, raftplacement.ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("caller-supplied digest accepted: %v", err)
	}
	if _, ok := authority.VectorPartitionLifecycleRecordV1(bad); ok {
		t.Fatal("bad digest published a BUILD record")
	}
	identity.Immutable = verified
	if _, err := coordinator.BeginBuildV1(ctx, identity, groups[:1], 0, mutationEpoch); !errors.Is(err, raftplacement.ErrVectorPartitionLifecycleConflict) {
		t.Fatalf("incomplete owner set accepted: %v", err)
	}
	if _, ok := authority.VectorPartitionLifecycleRecordV1(identity); ok {
		t.Fatal("incomplete owner set published a BUILD record")
	}
	wrongCatalog := identity
	wrongCatalog.Index.CatalogDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, _, err := prepare(ctx, wrongCatalog); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("stale catalog accepted: %v", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	capturedAfterCancel := false
	canceledPrepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, authority, provider,
		func(context.Context, raftcluster.GroupID, string, uint64) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error) {
			capturedAfterCancel = true
			return raftcluster.ReadIndexProof{}, raftcluster.AppliedProgress{}, collections.VectorPartitionManifestV1{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := canceledPrepare(canceled, identity); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) || !errors.Is(err, context.Canceled) {
		t.Fatalf("unavailable catalog proof accepted: %v", err)
	}
	if capturedAfterCancel {
		t.Fatal("canceled preparation captured source")
	}
	if _, err := coordinator.BeginBuildV1(ctx, identity, groups, 0, mutationEpoch); err != nil {
		t.Fatalf("verified immutable BUILD: %v", err)
	}
	if _, ok := authority.VectorPartitionLifecycleRecordV1(identity); !ok {
		t.Fatal("verified BUILD has no committed record")
	}
	// A token-routed catalog can place the same collection while this local
	// source holder still has only one collection's bytes. It cannot attest
	// the distributed source through the full-local V1 preparation path.
	tokenCatalog := record.Catalog
	tokenCatalog.Placements = append([]raftplacement.CollectionPlacementV1(nil), record.Catalog.Placements...)
	tokenCatalog.Placements[0] = raftplacement.CollectionPlacementV1{
		Collection: collection, Mode: raftplacement.PlacementModeTokenV1,
		TokenPartitions: []raftplacement.TokenPartitionV1{{ID: "token-0", GroupID: "group-a", Start: 0, End: ^uint64(0)}},
	}
	tokenRecord, err := raftplacement.NewCatalogMetaRecordV1(1, tokenCatalog)
	if err != nil {
		t.Fatal(err)
	}
	tokenAuthority, tokenProvider := openVectorSourceHolderTestCatalogV1(t, ctx, tokenRecord)
	tokenPrepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, tokenAuthority, tokenProvider, sourceHolderTestCaptureV1(fixture.collection, dataReads))
	if err != nil {
		t.Fatal(err)
	}
	tokenIdentity := identity
	tokenIdentity.Index.CatalogEpoch, tokenIdentity.Index.CatalogDigest = tokenRecord.Epoch, tokenRecord.Digest
	if _, _, err := tokenPrepare(ctx, tokenIdentity); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("token-routed source accepted as full-local: %v", err)
	}
	if _, ok := tokenAuthority.VectorPartitionLifecycleRecordV1(tokenIdentity); ok {
		t.Fatal("token-routed source published a BUILD record")
	}
	// The meta leader may have a stale local copy of the source while the
	// catalog assigns the collection to another group's members. That copy
	// cannot certify the collection's authoritative source.
	nonmemberCatalog := record.Catalog
	nonmemberCatalog.Placements = append([]raftplacement.CollectionPlacementV1(nil), record.Catalog.Placements...)
	nonmemberCatalog.Placements[0].GroupID = "group-b"
	nonmemberRecord, err := raftplacement.NewCatalogMetaRecordV1(1, nonmemberCatalog)
	if err != nil {
		t.Fatal(err)
	}
	nonmemberCtx, stopNonmember := context.WithTimeout(t.Context(), 8*time.Second)
	defer stopNonmember()
	nonmemberAuthority, nonmemberProvider := openVectorSourceHolderTestCatalogV1(t, nonmemberCtx, nonmemberRecord)
	nonmemberPrepare, err := newVectorPartitionImmutableSourceHolderPreparationWithCaptureV1(collection, nonmemberAuthority, nonmemberProvider, sourceHolderTestCaptureV1(fixture.collection, dataReads))
	if err != nil {
		t.Fatal(err)
	}
	nonmemberIdentity := identity
	nonmemberIdentity.Index.CatalogEpoch, nonmemberIdentity.Index.CatalogDigest = nonmemberRecord.Epoch, nonmemberRecord.Digest
	if _, _, err := nonmemberPrepare(nonmemberCtx, nonmemberIdentity); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("nonmember source holder accepted: %v", err)
	}
	if _, ok := nonmemberAuthority.VectorPartitionLifecycleRecordV1(nonmemberIdentity); ok {
		t.Fatal("nonmember source holder published a BUILD record")
	}
	if _, err := fixture.collection.Insert([]byte("new-row"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := prepare(ctx, identity); !errors.Is(err, ErrFixedPeerVectorProofStaleV1) {
		t.Fatalf("changed source accepted: %v", err)
	}
}

func TestVectorPartitionImmutableSourceHolderCapturesCurrentFSMStoreV1(t *testing.T) {
	seed := fixedPeerVectorSeedV1(t)
	root := t.TempDir()
	dbDir := filepath.Join(root, "data")
	if err := os.CopyFS(dbDir, os.DirFS(seed.dir)); err != nil {
		t.Fatal(err)
	}
	if err := backenddb.RebindDurableRootSnapshotV1(dbDir); err != nil {
		t.Fatal(err)
	}
	database, err := backenddb.Open(backenddb.Options{Dir: dbDir, CommandWAL: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const node raftcluster.NodeID = "source-meta"
	const group raftcluster.GroupID = "group-b"
	_, transport := hraft.NewInmemTransport(hraft.ServerAddress(node))
	t.Cleanup(func() { _ = transport.Close() })
	dataConfig := raftcluster.Config{
		Dir: dbDir, ClusterDir: filepath.Join(root, "raft"), NodeID: node, GroupID: group,
		Peers: []raftcluster.Peer{{ID: node, Address: string(transport.LocalAddr())}},
	}
	resolved, err := raftcluster.Validate(dataConfig)
	if err != nil {
		t.Fatal(err)
	}
	storeOptions := raftapply.DurableApplyStoreOptions{DisableSync: true, AllowInitialIndexGap: true}
	progressStore, err := raftapply.OpenDurableApplyProgressStore(resolved.Layout.ApplyDir, storeOptions)
	if err != nil {
		t.Fatal(err)
	}
	recordErr := progressStore.RecordApplied(raftapply.ApplyProgressRecordV1{
		EntryID: raftentry.ApplyEntryID{Term: 1, Index: 1}, AppliedCommandLSN: seed.appliedCommandLSN,
	})
	if err := errors.Join(recordErr, progressStore.Close()); err != nil {
		t.Fatal(err)
	}
	fsm, err := raftfsm.Open(raftfsm.Options{DB: database, Cluster: dataConfig, StoreOptions: storeOptions})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fsm.Close() })
	raftConfig := hraft.DefaultConfig()
	raftConfig.HeartbeatTimeout, raftConfig.ElectionTimeout, raftConfig.LeaderLeaseTimeout = 50*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond
	raftConfig.LogOutput = io.Discard
	dataProvider, err := raftcluster.OpenHashicorpRaftProvider(raftcluster.HashicorpRaftProviderOptions{
		Cluster: dataConfig, Applier: fsm, Transport: transport, RaftConfig: raftConfig, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dataProvider.Close() })
	catalog := seed.catalog
	catalog.Groups = append([]raftplacement.GroupV1(nil), catalog.Groups...)
	for i := range catalog.Groups {
		if catalog.Groups[i].ID == group {
			catalog.Groups[i].Members = []raftcluster.NodeID{node}
			catalog.Groups[i].LeaderHint = node
		}
	}
	metaRecord, err := raftplacement.NewCatalogMetaRecordV1(1, catalog)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	authority, metaProvider := openVectorSourceHolderTestCatalogV1(t, ctx, metaRecord)
	collectionRef := metaRecord.Catalog.Placements[0].Collection
	runtime := &FixedPeerTCPRuntimeV1{
		config:    FixedPeerTCPConfigV1{NodeID: node, Vector: &FixedPeerTCPVectorConfigV1{Collection: collectionRef}},
		authority: authority, meta: metaProvider,
		data: map[raftcluster.GroupID]*fixedPeerDataV1{group: {db: database, fsm: fsm, provider: dataProvider}},
	}
	if _, err := NewVectorPartitionImmutableSourceHolderPreparationV1(runtime, raftplacement.CollectionRefV1{Database: "foreign", Catalog: "foreign", Collection: collectionRef.Collection}); !errors.Is(err, ErrFixedPeerVectorUnavailableV1) {
		t.Fatalf("same-name foreign collection reference accepted by runtime factory: %v", err)
	}
	prepare, err := NewVectorPartitionImmutableSourceHolderPreparationV1(runtime, collectionRef)
	if err != nil {
		t.Fatal(err)
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection: collectionRef, CollectionIncarnation: 1, IndexName: seed.manifest.IndexName,
			IndexDefinitionDigest: seed.manifest.IndexDefinitionDigest, IndexEpoch: 1,
			CatalogEpoch: metaRecord.Epoch, CatalogDigest: metaRecord.Digest,
		},
		Source: raftplacement.VectorPartitionLifecycleSourceIdentityV1{
			Generation: seed.manifest.SourceGeneration, Checksum: seed.manifest.SourceChecksum,
			SchemaHash: seed.manifest.SourceSchemaHash, RowCount: seed.manifest.SourceRowCount,
		},
		Generation: seed.manifest.Generation,
	}
	var prepared raftplacement.VectorPartitionLifecycleImmutableAuthorityV1
	for ctx.Err() == nil {
		prepared, _, err = prepare(ctx, identity)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("single-node source-holder proof: %v", err)
	}
	manifestBytes, err := collections.EncodeVectorPartitionManifestV1(seed.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.ManifestDigest != fmt.Sprintf("%x", sha256.Sum256(manifestBytes)) {
		t.Fatalf("current FSM manifest digest=%s", prepared.ManifestDigest)
	}
	// This stale runtime DB handle has the same collection name but changed
	// source rows. Production preparation must still read the FSM's current DB.
	foreign := newVectorPartitionLiveNativewireFixtureV1(t)
	t.Cleanup(func() { _ = foreign.database.Close() })
	if _, err := foreign.collection.Insert([]byte("foreign-row"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatal(err)
	}
	runtime.data[group].db = foreign.database
	verified, _, err := prepare(ctx, identity)
	if err != nil || verified != prepared {
		t.Fatalf("runtime stale DB handle substituted for FSM DB: proof=%+v err=%v", verified, err)
	}
}

func sourceHolderTestCaptureV1(collection *collections.Collection, reads raftcluster.RoutedReadIndexCoordinator) func(context.Context, raftcluster.GroupID, string, uint64) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error) {
	return func(ctx context.Context, group raftcluster.GroupID, index string, generation uint64) (raftcluster.ReadIndexProof, raftcluster.AppliedProgress, collections.VectorPartitionManifestV1, error) {
		proof, progress, err := reads.CoordinateRoutedReadIndex(ctx, raftcluster.ReadIndexBarrier{GroupID: group})
		if err != nil {
			return proof, progress, collections.VectorPartitionManifestV1{}, err
		}
		manifest, err := collection.PreparedVectorPartitionManifestWithContextV1(ctx, index, generation)
		return proof, progress, manifest, err
	}
}

func openVectorSourceHolderTestCatalogV1(t *testing.T, ctx context.Context, record raftplacement.CatalogMetaRecordV1) (*raftplacement.CatalogMetaAuthorityV1, *raftcluster.CatalogMetaRaftProviderV1) {
	t.Helper()
	authority := raftplacement.NewCatalogMetaAuthorityV1()
	_, transport := hraft.NewInmemTransport("source-meta")
	t.Cleanup(func() { _ = transport.Close() })
	providerFeatures := raftcluster.FeatureSet{
		ConfigVersion: raftcluster.SupportedConfigVersion,
		Required: []raftcluster.RequiredFeature{
			{Name: raftcluster.FeatureSingleGroupProvider, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureSingleGroupProvider]},
			{Name: raftcluster.FeatureCatalogMetaAuthority, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureCatalogMetaAuthority]},
		},
	}
	provider, err := raftcluster.OpenCatalogMetaRaftProviderV1(raftcluster.CatalogMetaRaftProviderOptionsV1{
		Cluster: raftcluster.Config{
			Dir: t.TempDir(), NodeID: "source-meta", GroupID: "meta", Features: providerFeatures,
			Peers: []raftcluster.Peer{{ID: "source-meta", Address: "source-meta", Capabilities: providerFeatures}},
		},
		State: authority, Transport: transport, Bootstrap: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	for {
		status, err := provider.ClusterAdmissionStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.Leader {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	raw, err := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{ExpectedEpoch: 0, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := provider.SubmitCatalogMetaCommandV1(ctx, raw); err != nil {
		t.Fatal(err)
	}
	return authority, provider
}
