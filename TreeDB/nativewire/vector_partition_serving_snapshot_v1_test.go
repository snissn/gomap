package nativewire

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
)

type vectorPartitionServingSnapshotFixtureV1 struct {
	harness     *raftplacement.CatalogMetaLifecycleHarnessV1
	lifecycle   raftplacement.VectorPartitionLifecycleCoordinatorV1
	identity    raftplacement.VectorPartitionLifecycleIdentityV1
	coordinator *VectorPartitionCoordinatorV1
	authority   *LinearizableCatalogVectorPartitionLifecycleAuthorityV1
	publisher   *VectorPartitionServingSnapshotPublisherV1
	router      *testVectorPartitionCoordinatorRouterV1
	sources     map[raftcluster.GroupID]*fakeVectorPartitionGenerationSourceV1
}

func TestVectorPartitionProductionAssetBindingsAllowSingleOwnerV1(t *testing.T) {
	manifest := collections.VectorPartitionManifestV1{
		Format: collections.VectorPartitionManifestFormatV1, State: "ready", Collection: "docs",
		IndexName: "embedding", IndexDefinitionDigest: strings.Repeat("a", 64),
		SourceGeneration: 1, SourceChecksum: 2, SourceSchemaHash: 3, SourceRowCount: 1,
		Generation: 4, RouterGeneration: 4, PartitionCount: 1,
		Placements:  []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}},
		Memberships: []collections.VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}},
		Assets: []collections.VectorPartitionAssetV1{{
			ID: "partition-0", Checksum: strings.Repeat("0", 64), PartitionID: 0, Bytes: 1,
			Ref: collections.ColumnAssetRef{Kind: collections.ColumnAssetKindTCS1PartImage, Namespace: "test", Generation: 4, PartID: 1, FileID: 1, Length: 1},
		}},
		RouterAsset: collections.VectorPartitionAssetV1{
			ID: "router", Checksum: strings.Repeat("2", 64), Bytes: 1,
			Ref: collections.ColumnAssetRef{Kind: collections.ColumnAssetKindTCS1PartImage, Namespace: "test", Generation: 4, PartID: 2, FileID: 2, Length: 1},
		},
	}
	manifest.Canonicalize()
	digests := map[string]string{"group-a": vectorPartitionM8GroupAssetSetDigestV1("group-a", manifest)}
	groups, err := vectorPartitionValidateAssetBindingsV1(manifest, digests)
	if err != nil || len(groups) != 1 || groups[0] != "group-a" {
		t.Fatalf("production asset bindings groups=%v err=%v", groups, err)
	}
	if _, err := vectorPartitionM8ValidateAssetsV1(manifest, digests); err == nil {
		t.Fatal("M8 accepted a single owner")
	}
}

func newVectorPartitionServingSnapshotFixtureV1(tb testing.TB) *vectorPartitionServingSnapshotFixtureV1 {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref := raftplacement.CollectionRefV1{Database: "db", Catalog: "default", Collection: "docs"}
	features := raftplacement.DefaultFeatureSet()
	features.Required = append(features.Required, raftcluster.RequiredFeature{Name: raftcluster.FeatureVectorPartitionLifecycle, Version: raftcluster.SupportedFeatureFloors[raftcluster.FeatureVectorPartitionLifecycle]})
	catalogSpec := raftplacement.CatalogV1{
		Features: features,
		Groups: []raftplacement.GroupV1{
			{ID: "group-a", Members: []raftcluster.NodeID{"node-a"}, LeaderHint: "node-a"},
			{ID: "group-b", Members: []raftcluster.NodeID{"node-b"}, LeaderHint: "node-b"},
		},
		Placements: []raftplacement.CollectionPlacementV1{{Collection: ref, GroupID: "group-a", Mode: raftplacement.PlacementModeCollectionV1}},
	}
	catalog, err := raftplacement.Validate(catalogSpec)
	if err != nil {
		tb.Fatal(err)
	}
	harness, err := raftplacement.OpenCatalogMetaLifecycleHarnessV1(ctx, raftplacement.CatalogMetaLifecycleHarnessOptionsV1{Catalog: catalogSpec, Prefix: "serving-snapshot"})
	if err != nil {
		tb.Fatal(err)
	}
	meta, ok := harness.LeaderAuthority().Status()
	if !ok {
		_ = harness.Close()
		tb.Fatal("catalog authority unavailable")
	}
	identity := raftplacement.VectorPartitionLifecycleIdentityV1{
		Index: raftplacement.VectorPartitionLifecycleIndexIdentityV1{
			Collection: ref, CollectionIncarnation: 1, IndexName: "embedding",
			IndexDefinitionDigest: strings.Repeat("a", 64), IndexEpoch: 1,
			CatalogEpoch: meta.Epoch, CatalogDigest: meta.Digest,
		},
		Source:     raftplacement.VectorPartitionLifecycleSourceIdentityV1{Generation: 11, Checksum: 22, SchemaHash: 33, RowCount: 2},
		Generation: 7,
	}
	manifest := collections.VectorPartitionManifestV1{
		Format: collections.VectorPartitionManifestFormatV1, State: "ready", Collection: ref.Collection,
		IndexName: identity.Index.IndexName, IndexDefinitionDigest: identity.Index.IndexDefinitionDigest,
		IntegrityDigest: strings.Repeat("d", 64), SourceGeneration: identity.Source.Generation,
		SourceChecksum: identity.Source.Checksum, SourceSchemaHash: identity.Source.SchemaHash,
		SourceRowCount: identity.Source.RowCount, Generation: identity.Generation, RouterGeneration: identity.Generation,
		PartitionCount: 2, ReadySetDigest: strings.Repeat("b", 64),
		Placements: []collections.VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}},
		Assets: []collections.VectorPartitionAssetV1{
			{ID: "partition-0", Checksum: strings.Repeat("0", 64), PartitionID: 0, Bytes: 1},
			{ID: "partition-1", Checksum: strings.Repeat("1", 64), PartitionID: 1, Bytes: 1},
		},
		RouterAsset: collections.VectorPartitionAssetV1{ID: "router", Checksum: strings.Repeat("2", 64), Bytes: 1},
	}
	lifecycle := harness.LifecycleCoordinator()
	required := []raftcluster.GroupID{"group-a", "group-b"}
	if _, err := lifecycle.BeginBuildV1(ctx, identity, required, 0, 1); err != nil {
		_ = harness.Close()
		tb.Fatal(err)
	}
	for i, group := range required {
		if _, err := lifecycle.RecordGroupReadyV1(ctx, identity, raftplacement.VectorPartitionLifecycleGroupReadyV1{
			GroupID: group, AppliedIndex: uint64(9 + i), AssetSetDigest: vectorPartitionM8GroupAssetSetDigestV1(string(group), manifest),
		}); err != nil {
			_ = harness.Close()
			tb.Fatal(err)
		}
	}
	if _, err := lifecycle.PrepareV1(ctx, identity); err != nil {
		_ = harness.Close()
		tb.Fatal(err)
	}
	active, err := lifecycle.ActivateV1(ctx, identity)
	if err != nil {
		_ = harness.Close()
		tb.Fatal(err)
	}
	placement := raftplacement.VectorPartitionPlacementRecordV1{
		Collection: ref, IndexName: identity.Index.IndexName, IndexDefinitionDigest: identity.Index.IndexDefinitionDigest,
		SourceGeneration: identity.Source.Generation, SourceChecksum: identity.Source.Checksum,
		SourceSchemaHash: identity.Source.SchemaHash, SourceRowCount: identity.Source.RowCount,
		PartitionGeneration: identity.Generation, PartitionCount: 2,
		Partitions: []raftplacement.VectorPartitionGroupV1{{PartitionID: 0, GroupID: "group-a"}, {PartitionID: 1, GroupID: "group-b"}},
	}
	pinnedManifest := manifest
	pinnedManifest.ReadySetDigest = active.ReadySetDigest
	router := &testVectorPartitionCoordinatorRouterV1{
		status: collections.VectorPartitionRouterRuntimeStatusV1{
			Manifest: manifest, ModelDigest: strings.Repeat("c", 64), Representatives: 2, Partitions: 2,
		},
		partitions: []collections.VectorPartitionRouterPartitionScoreV1{{PartitionID: 0}, {PartitionID: 1}},
	}
	authority, err := NewLinearizableCatalogVectorPartitionLifecycleAuthorityV1(harness.LeaderAuthority(), harness.LeaderFence())
	if err != nil {
		_ = harness.Close()
		tb.Fatal(err)
	}
	coordinator, err := NewVectorPartitionCoordinatorV1(VectorPartitionCoordinatorOptionsV1{
		Catalog: catalog, Placement: placement, RouterSource: &testVectorPartitionCoordinatorRouterSourceV1{router: router},
		Dispatcher: &testVectorPartitionCoordinatorDispatcherV1{}, ReplicatedLifecycle: authority, RequireReplicatedLifecycle: true,
	})
	if err != nil {
		_ = harness.Close()
		tb.Fatal(err)
	}
	sources := map[raftcluster.GroupID]*fakeVectorPartitionGenerationSourceV1{
		"group-a": {manifest: pinnedManifest, assets: map[uint32]collections.VectorPartitionSearchAssetV1{0: vectorPartitionShardSearchAssetTestV1(0, []string{"a"}, [][]float32{{1, 0}})}, openErr: map[uint32]error{}},
		"group-b": {manifest: pinnedManifest, assets: map[uint32]collections.VectorPartitionSearchAssetV1{1: vectorPartitionShardSearchAssetTestV1(1, []string{"b"}, [][]float32{{1, 0}})}, openErr: map[uint32]error{}},
	}
	generationSources := make(map[raftcluster.GroupID]VectorPartitionGenerationSourceV1, len(sources))
	for group, source := range sources {
		generationSources[group] = source
	}
	publisher, err := NewVectorPartitionServingSnapshotPublisherV1(VectorPartitionServingSnapshotPublisherOptionsV1{
		Coordinator: coordinator, Authority: authority, GenerationSources: generationSources,
		TopologyDigest: strings.Repeat("e", 64), AuthorizationOverlayDigest: strings.Repeat("f", 64), IndexedThrough: 11,
	})
	if err != nil {
		_ = coordinator.Close()
		_ = harness.Close()
		tb.Fatal(err)
	}
	fixture := &vectorPartitionServingSnapshotFixtureV1{
		harness: harness, lifecycle: lifecycle, identity: identity, coordinator: coordinator,
		authority: authority, publisher: publisher, router: router, sources: sources,
	}
	tb.Cleanup(func() {
		_ = fixture.publisher.Close()
		_ = fixture.coordinator.Close()
		_ = fixture.harness.Close()
	})
	return fixture
}

func TestVectorPartitionServingSnapshotPublicationRejectsAssetSetMismatchV1(t *testing.T) {
	fixture := newVectorPartitionServingSnapshotFixtureV1(t)
	fixture.router.status.Manifest.Assets[0].Checksum = strings.Repeat("8", 64)
	if err := fixture.publisher.PublishV1(t.Context()); !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		t.Fatalf("asset-set mismatch publication error=%v", err)
	}
}

func TestVectorPartitionServingSnapshotPublicationClosesMalformedLeaseV1(t *testing.T) {
	fixture := newVectorPartitionServingSnapshotFixtureV1(t)
	malformedCloses := 0
	fixture.sources["group-a"].openLease = map[uint32]*VectorPartitionPartitionSearchLeaseV1{0: {
		closeFn: func() error { malformedCloses++; return nil },
	}}
	if err := fixture.publisher.PublishV1(t.Context()); !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) || malformedCloses != 1 {
		t.Fatalf("malformed lease publication error=%v closes=%d", err, malformedCloses)
	}
}

func TestVectorPartitionServingSnapshotPublicationRejectsNilGenerationV1(t *testing.T) {
	fixture := newVectorPartitionServingSnapshotFixtureV1(t)
	fixture.sources["group-a"].nilPin = true
	if err := fixture.publisher.PublishV1(t.Context()); !errors.Is(err, ErrVectorPartitionShardSearchAssetsUnavailable) {
		t.Fatalf("nil generation publication error=%v", err)
	}
}

func TestVectorPartitionServingSnapshotInvalidationWinsPublicationRaceV1(t *testing.T) {
	fixture := newVectorPartitionServingSnapshotFixtureV1(t)
	started, proceed := make(chan struct{}), make(chan struct{})
	fixture.sources["group-a"].pinStarted, fixture.sources["group-a"].pinContinue = started, proceed
	done := make(chan error, 1)
	go func() { done <- fixture.publisher.PublishV1(t.Context()) }()
	<-started
	if err := fixture.publisher.InvalidateV1(); err != nil {
		t.Fatal(err)
	}
	close(proceed)
	if err := <-done; !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		t.Fatalf("publication after invalidation error=%v", err)
	}
	if lease, err := fixture.publisher.AcquireV1(); err == nil {
		_ = lease.Close()
		t.Fatal("publication that raced with invalidation became acquirable")
	}
}

func TestVectorPartitionServingSnapshotProofRefreshIsMonotonicV1(t *testing.T) {
	fixture := newVectorPartitionServingSnapshotFixtureV1(t)
	if err := fixture.publisher.PublishV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.publisher.cancel()
	fixture.publisher.wg.Wait()

	fixture.publisher.mu.Lock()
	snapshot := fixture.publisher.current
	older := snapshot.proof
	fixture.publisher.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	placement := fixture.coordinator.placement
	newer, err := fixture.authority.captureVectorPartitionServingAuthorityV1(
		t.Context(), placement.Collection, placement.IndexName, placement.PartitionGeneration, placement.IndexDefinitionDigest,
		placement.SourceGeneration, placement.SourceChecksum, placement.SourceSchemaHash, placement.SourceRowCount,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !older.read.ValidThroughV1().Before(newer.read.ValidThroughV1()) {
		t.Fatalf("new proof deadline=%s old=%s", newer.read.ValidThroughV1(), older.read.ValidThroughV1())
	}
	if err := fixture.publisher.installProofV1(snapshot, newer, nil); err != nil {
		t.Fatal(err)
	}
	if err := fixture.publisher.installProofV1(snapshot, older, nil); err != nil {
		t.Fatal(err)
	}
	fixture.publisher.mu.Lock()
	got := fixture.publisher.current.proof.read.ValidThroughUnixNano
	fixture.publisher.mu.Unlock()
	if got != newer.read.ValidThroughUnixNano {
		t.Fatalf("installed proof deadline=%d want=%d", got, newer.read.ValidThroughUnixNano)
	}
}

func TestVectorPartitionServingSnapshotProofRefreshRetriesWithinRemainingLeaseV1(t *testing.T) {
	now := time.Unix(100, 0)
	if got, want := vectorPartitionServingSnapshotRefreshDelayV1(now.Add(100*time.Millisecond), now), 50*time.Millisecond; got != want {
		t.Fatalf("refresh retry delay=%s want %s", got, want)
	}
}

func TestVectorPartitionServingSnapshotReplacementIdentityAdvancesV1(t *testing.T) {
	previous := &vectorPartitionServingSnapshotV1{identity: VectorPartitionServingSnapshotIdentityV1{PublishedAtUnixNano: 7}}
	next := &vectorPartitionServingSnapshotV1{identity: previous.identity}
	var err error
	previous.identity.SnapshotDigest, err = vectorPartitionServingSnapshotDigestV1(previous.identity)
	if err != nil {
		t.Fatal(err)
	}
	next.identity.SnapshotDigest = previous.identity.SnapshotDigest
	if err := vectorPartitionServingSnapshotAdvanceIdentityV1(next, previous); err != nil {
		t.Fatal(err)
	}
	if next.identity.PublishedAtUnixNano != 8 || next.identity.SnapshotDigest == previous.identity.SnapshotDigest {
		t.Fatalf("replacement identity=%+v previous=%+v", next.identity, previous.identity)
	}
}

func TestVectorPartitionServingSnapshotPublicationPinsAndDrainsV1(t *testing.T) {
	fixture := newVectorPartitionServingSnapshotFixtureV1(t)
	injected := errors.New("injected partition open failure")
	fixture.sources["group-b"].openErr[1] = injected
	if err := fixture.publisher.PublishV1(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("partial publication error = %v", err)
	}
	for group, source := range fixture.sources {
		pins, releases, _ := source.counts()
		if pins != 1 || releases != 1 {
			t.Fatalf("partial publication group=%s pins/releases=%d/%d", group, pins, releases)
		}
	}
	delete(fixture.sources["group-b"].openErr, uint32(1))
	fixture.sources["group-b"].manifest.IntegrityDigest = strings.Repeat("9", 64)
	if err := fixture.publisher.PublishV1(t.Context()); !errors.Is(err, ErrVectorPartitionShardSearchGenerationMismatch) {
		t.Fatalf("mixed manifest publication error = %v", err)
	}
	fixture.sources["group-b"].manifest.IntegrityDigest = strings.Repeat("d", 64)
	if err := fixture.publisher.PublishV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	first, err := fixture.publisher.AcquireV1()
	if err != nil {
		t.Fatal(err)
	}
	firstIdentity := first.IdentityV1()
	if firstIdentity.SnapshotDigest == "" || firstIdentity.ReadySetDigest == "" || firstIdentity.IndexedThrough != 11 ||
		firstIdentity.ManifestIntegrityDigest != strings.Repeat("d", 64) || len(firstIdentity.ReadyGroups) != 2 || len(firstIdentity.LocalGroups) != 2 {
		t.Fatalf("published identity = %+v", firstIdentity)
	}
	before := fixture.harness.LeaderFence().CatalogMetaLinearizableReadStatsV1()
	if err := fixture.publisher.RefreshProofV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := fixture.harness.LeaderFence().CatalogMetaLinearizableReadStatsV1()
	if after.LastRaftLog != before.LastRaftLog || after.Total.LogBarriers != 0 || after.Total.NoLogProofs <= before.Total.NoLogProofs {
		t.Fatalf("proof refresh stats before=%+v after=%+v", before, after)
	}
	if err := fixture.publisher.PublishV1(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := fixture.publisher.AcquireV1()
	if err != nil {
		t.Fatal(err)
	}
	if second.IdentityV1().SnapshotDigest == firstIdentity.SnapshotDigest {
		t.Fatal("replacement reused snapshot publication identity")
	}
	for group, source := range fixture.sources {
		_, releases, _ := source.counts()
		if releases != 2 {
			// The first failed open and manifest-mismatch publication closed; the
			// first successful publication remains pinned by first.
			t.Fatalf("pre-drain group=%s releases=%d want=2", group, releases)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	for group, source := range fixture.sources {
		_, releases, _ := source.counts()
		if releases != 3 {
			t.Fatalf("old snapshot group=%s releases=%d want=3", group, releases)
		}
	}
	if _, err := fixture.lifecycle.InvalidateGenerationBeforeRelevantMutationV1(t.Context(), fixture.identity, "snapshot test"); err != nil {
		t.Fatal(err)
	}
	if lease, err := fixture.publisher.AcquireV1(); err == nil {
		_ = lease.Close()
		t.Fatal("acquisition admitted invalidated authority")
	}
	if err := fixture.publisher.InvalidateV1(); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	for group, source := range fixture.sources {
		_, releases, _ := source.counts()
		if releases != 4 {
			t.Fatalf("replacement drain group=%s releases=%d want=4", group, releases)
		}
	}
	stats := fixture.publisher.StatsV1()
	if stats.Publications != 2 || stats.Replacements != 1 || stats.Invalidations != 1 || stats.Acquisitions != 2 ||
		stats.AcquisitionRejections == 0 || stats.CurrentPins != 0 || stats.CurrentSnapshots != 0 || stats.ProofRefreshes == 0 {
		t.Fatalf("publisher stats = %+v", stats)
	}
}

func BenchmarkVectorPartitionServingSnapshotAcquireV1(b *testing.B) {
	fixture := newVectorPartitionServingSnapshotFixtureV1(b)
	if err := fixture.publisher.PublishV1(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.Run("snapshot_pin", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			lease, err := fixture.publisher.AcquireV1()
			if err != nil {
				b.Fatal(err)
			}
			if err := lease.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fresh_no_log_authority", func(b *testing.B) {
		placement := fixture.coordinator.placement
		b.ReportAllocs()
		for b.Loop() {
			if _, err := fixture.authority.ValidateVectorPartitionGenerationSearchV1(
				context.Background(), placement.Collection, placement.IndexName,
				placement.PartitionGeneration, placement.IndexDefinitionDigest,
				placement.SourceGeneration, placement.SourceChecksum,
				placement.SourceSchemaHash, placement.SourceRowCount,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}
