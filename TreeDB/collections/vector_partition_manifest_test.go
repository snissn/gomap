package collections

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func requireVectorPartitionPersistenceV1(t testing.TB) {
	t.Helper()
	if !vpmNamespacePersistenceSupported() {
		t.Skip("vector partition namespace persistence unsupported")
	}
}

func TestVerifyVectorPartitionAssetsWithContextV1RejectsCanceledOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := verifyVectorPartitionAssetsWithContextV1(ctx, t.TempDir(), "namespace", []VectorPartitionAssetV1{{ID: "asset"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("verify canceled err=%v", err)
	}
}

// These helpers exercise the store's private already-authorized primitives in
// storage-format tests. Production lifecycle mutation must go through the
// collection-owned APIs so it has DB maintenance authority.
func deactivateVectorPartitionStoreForTest(s *VectorPartitionStoreV1, collection, index string) error {
	return WithVectorPartitionStorageBarrierV1(s.root, func() error { return s.deactivateLocked(collection, index) })
}

func deleteVectorPartitionStoreForTest(s *VectorPartitionStoreV1, collection, index string, generation uint64, eligibility VectorPartitionCleanupEligibilityV1) error {
	return WithVectorPartitionStorageBarrierV1(s.root, func() error { return s.deleteLocked(collection, index, generation, eligibility) })
}

func TestShouldRefreshVectorPartitionReclaimGCPlanV1(t *testing.T) {
	stale := backenddb.ErrRecoverableRootSetStale
	tests := []struct {
		name    string
		err     error
		stats   ColumnAssetGCStats
		attempt int
		want    bool
	}{
		{name: "fresh stale authority", err: stale, attempt: 0, want: true},
		{name: "stale authority after deletion", err: stale, stats: ColumnAssetGCStats{SegmentsDeleted: 1}, attempt: 0},
		{name: "unrelated error", err: errors.New("injected"), attempt: 0},
		{name: "attempt bound exhausted", err: stale, attempt: vectorPartitionReclaimRecoverableRootAttemptsV1 - 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRefreshVectorPartitionReclaimGCPlanV1(tt.err, tt.stats, tt.attempt); got != tt.want {
				t.Fatalf("shouldRefreshVectorPartitionReclaimGCPlanV1()=%v want %v", got, tt.want)
			}
		})
	}
}

func appendVectorPartitionStableAssetsV1(t testing.TB, d *backenddb.DB, col *Collection, fileID uint32, generation uint64) ([]ColumnAssetRef, *rootpublication.StableResourceSet) {
	t.Helper()
	lease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	cfg := *col.meta.Options.ColumnStore
	refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(d.ColumnAssetRootDir(), cfg, fileID, []StableColumnPhysicalAssetAppend{
		{Payload: []byte("partition-0"), Kind: ColumnAssetKindTCS1PartImage, Generation: generation, PartID: 1},
		{Payload: []byte("partition-1"), Kind: ColumnAssetKindTCS1PartImage, Generation: generation, PartID: 2},
		{Payload: []byte("router"), Kind: ColumnAssetKindTCS1PartImage, Generation: generation, PartID: 3},
	}, d.StableResourceIdentityPinRegistry(), lease)
	if err != nil {
		t.Fatal(err)
	}
	return refs, resources
}

// vectorPartitionManifestWithFreshStableAssetsV1 keeps negative authority
// tests from short-circuiting on absent resource authority: the caller gets a
// structurally valid ready manifest and the exact nonnil authority for its
// referenced assets.
func vectorPartitionManifestWithFreshStableAssetsV1(t testing.TB, d *backenddb.DB, col *Collection, base VectorPartitionManifestV1, fileID uint32) (VectorPartitionManifestV1, *rootpublication.StableResourceSet) {
	t.Helper()
	refs, resources := appendVectorPartitionStableAssetsV1(t, d, col, fileID, base.Generation)
	for i := range base.Assets {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[i])
		if err != nil {
			resources.Release()
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		base.Assets[i].Ref, base.Assets[i].Bytes, base.Assets[i].Checksum = refs[i], uint64(refs[i].Length), hex.EncodeToString(sum[:])
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[len(refs)-1])
	if err != nil {
		resources.Release()
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	base.RouterAsset.Ref, base.RouterAsset.Bytes, base.RouterAsset.Checksum = refs[len(refs)-1], uint64(refs[len(refs)-1].Length), hex.EncodeToString(sum[:])
	base.Canonicalize()
	return base, resources
}

func TestCollectionVectorPartitionManifestV1BindsIndexAndReopens(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	dir, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexName = def.Name
	m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	_, graph, view, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	if len(view.VectorIndexState.Assets) == 0 {
		t.Fatal("missing typed state asset")
	}
	_ = view
	refs, resources := appendVectorPartitionStableAssetsV1(t, d, col, 701, m.Generation)
	oldSegment, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		t.Fatal(err)
	}
	for i := range m.Assets {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[i])
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		m.Assets[i].Ref, m.Assets[i].Bytes, m.Assets[i].Checksum = refs[i], uint64(refs[i].Length), hex.EncodeToString(sum[:])
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[2])
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	m.RouterAsset.Ref, m.RouterAsset.Bytes, m.RouterAsset.Checksum = refs[2], uint64(refs[2].Length), hex.EncodeToString(sum[:])
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m, resources); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close after ready VPM publication: %v", err)
	}
	d = openCollectionCommandWALDB(t, dir)
	var reopenErr error
	col, reopenErr = NewCollectionManager(d).OpenCollection("docs")
	if reopenErr != nil {
		t.Fatalf("reopen collection after ready VPM publication: %v", reopenErr)
	}
	if active, err := OpenVectorPartitionStoreV1(d.Dir()); err != nil {
		t.Fatal(err)
	} else if got, err := active.OpenActive(m.Collection, m.IndexName); err != nil || got.Generation != m.Generation || got.State != "ready" {
		t.Fatalf("reopened active ready VPM=%+v err=%v", got, err)
	}
	status, err := col.VectorPartitionStatusV1(def.Name, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Ready || status.GroupCount != 1 || status.AssetBytes != uint64(refs[0].Length+refs[1].Length+refs[2].Length) {
		t.Fatalf("unexpected status: %+v", status)
	}
	pin, err := col.AcquireVectorPartitionReaderPinV1(def.Name, m.Generation)
	if err != nil {
		t.Fatal(err)
	}
	status, err = col.VectorPartitionStatusV1(def.Name, m.Generation)
	if err != nil || status.ReaderPins != 1 {
		t.Fatalf("pinned status=%+v err=%v", status, err)
	}
	pin.Release()
	pin.Release()
	status, err = col.VectorPartitionStatusV1(def.Name, m.Generation)
	if err != nil || status.ReaderPins != 0 {
		t.Fatalf("released status=%+v err=%v", status, err)
	}
	_, authorityToken, err := col.ActiveVectorPartitionManifestAndAuthorityTokenWithContextV1(t.Context(), def.Name, m.Generation)
	if err != nil {
		t.Fatal(err)
	}
	defer authorityToken.Release()
	checkpointReads := 0
	restoreLifecycleHook := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "after_slot_read" {
			checkpointReads++
		}
		return nil
	})
	if err := col.ValidateActiveVectorPartitionAuthorityTokenWithContextV1(t.Context(), def.Name, m.Generation, authorityToken); err != nil {
		restoreLifecycleHook()
		t.Fatalf("bounded active authority validation: %v", err)
	}
	restoreLifecycleHook()
	if checkpointReads != 0 {
		t.Fatalf("warm active authority validation reread %d lifecycle slots", checkpointReads)
	}
	plan, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Sources.PreparedRefs != 3 || plan.Sources.PinnedRefs != 3 {
		t.Fatalf("partition assets were not retained as prepared+pinned: %+v", plan.Sources)
	}
	store, err := OpenVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	activeGC, err := col.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{DryRun: true, CandidateRefs: refs})
	if err != nil {
		t.Fatal(err)
	}
	if activeGC.BytesEligible != 0 {
		t.Fatalf("active VPM assets became reclaimable: %+v", activeGC)
	}
	if err := col.DeactivateVectorPartitionV1(m.IndexName); err != nil {
		t.Fatal(err)
	}
	if err := col.ValidateActiveVectorPartitionAuthorityTokenWithContextV1(t.Context(), def.Name, m.Generation, authorityToken); err == nil {
		t.Fatal("pre-deactivation active authority token survived lifecycle mutation")
	}
	pin, err = col.AcquireVectorPartitionReaderPinV1(def.Name, m.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.DeleteVectorPartitionGenerationV1(m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("delete succeeded while partition reader was pinned")
	}
	pin.Release()
	if err := col.DeleteVectorPartitionGenerationV1(m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	retiredGC, err := col.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{DryRun: true, CandidateRefs: refs})
	if err != nil {
		t.Fatal(err)
	}
	if retiredGC.BytesEligible != 0 {
		t.Fatalf("reclaim record failed to retain deleted VPM assets: %+v", retiredGC)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var mutationOperations []string
	restoreMutationHook := setVectorPartitionBarrierBeforeMutationHookForTestV1(func(operation string) {
		mutationOperations = append(mutationOperations, operation)
	})
	defer restoreMutationHook()
	if _, err := col.ReclaimVectorPartitionGenerationV1(ctx, m.IndexName, m.Generation); err != nil {
		t.Fatalf("public VPM reclaim: %v", err)
	}
	restoreMutationHook()
	if got, want := mutationOperations, []string{vectorPartitionMutationOperationReclaimV1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reclaim storage mutation operations=%v want %v", got, want)
	}
	if _, err := os.Stat(store.deleteTombstonePath(m.Collection, m.IndexName, m.Generation)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed reclaim record remains: %v", err)
	}
	if generation, err := store.readInactiveGeneration(m.Collection, m.IndexName); err != nil || generation != m.Generation {
		t.Fatalf("reclaim lost durable inactive lifecycle marker generation=%d err=%v", generation, err)
	}
	if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err != nil {
		t.Fatalf("zero-history inactive lifecycle marker rejected after reclaim: %v", err)
	}
	if _, err := os.Stat(oldSegment); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reclaimed partition segment still present: %v", err)
	}
	if err := os.WriteFile(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".active"), []byte("not-a-generation\n"), 0600); err != nil {
		t.Fatal(err)
	}
	status, err = col.VectorPartitionStatusV1(def.Name, m.Generation)
	if !errors.Is(err, ErrVectorPartitionManifestInvalid) || status.Active {
		t.Fatalf("legacy active authority status=%+v err=%v", status, err)
	}
	if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err == nil {
		t.Fatal("legacy active vector partition authority did not fail closed")
	}
	if err := os.Remove(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".active")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+"-999.vpm"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err == nil {
		t.Fatal("corrupt retained vector partition manifest did not fail closed")
	}
	wrongIndex, wrongIndexResources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, m, 801)
	wrongIndex.IndexDefinitionDigest = strings.Repeat("f", 64)
	wrongIndex.Canonicalize()
	wrongIndexErr := col.PublishVectorPartitionManifestV1(wrongIndex, wrongIndexResources)
	wrongIndexResources.Release()
	if wrongIndexErr == nil || !strings.Contains(wrongIndexErr.Error(), "index definition digest mismatch") {
		t.Fatalf("wrong index digest err=%v want intended digest mismatch", wrongIndexErr)
	}
	for i, mutate := range []func(*VectorPartitionManifestV1){func(x *VectorPartitionManifestV1) { x.SourceGeneration++ }, func(x *VectorPartitionManifestV1) { x.SourceChecksum++ }, func(x *VectorPartitionManifestV1) { x.SourceSchemaHash++ }, func(x *VectorPartitionManifestV1) { x.SourceRowCount++ }} {
		bad, staleResources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, m, 802+uint32(i))
		mutate(&bad)
		if bad.SourceRowCount > m.SourceRowCount {
			bad.Memberships = append(bad.Memberships, VectorPartitionMembershipV1{VectorOrdinal: uint64(len(bad.Memberships)), PartitionID: 0})
		}
		bad.Canonicalize()
		staleErr := col.PublishVectorPartitionManifestV1(bad, staleResources)
		staleResources.Release()
		if staleErr == nil || !strings.Contains(staleErr.Error(), "source identity mismatch") {
			t.Fatalf("stale source identity err=%v want intended mismatch", staleErr)
		}
	}
}

func TestVectorPartitionActiveAuthoritySurvivesReplacementBuildAndPinCancellationV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{
		{id: "a", vector: []float32{1, 0, 0}},
		{id: "b", vector: []float32{0, 1, 0}},
	})
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	active := testVectorPartitionManifestV1()
	active.IndexName = def.Name
	active.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	active.SourceGeneration, active.SourceChecksum, active.SourceSchemaHash, active.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	active, activeResources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, active, 704)
	if err := col.PublishVectorPartitionManifestV1(active, activeResources); err != nil {
		t.Fatal(err)
	}
	_, token, err := col.ActiveVectorPartitionManifestAndAuthorityTokenWithContextV1(t.Context(), def.Name, active.Generation)
	if err != nil {
		t.Fatal(err)
	}
	defer token.Release()

	replacement := cloneVectorPartitionManifestForCheckpointV1(active)
	replacement.Generation++
	replacement.RouterGeneration = replacement.Generation
	replacement, replacementResources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, replacement, 705)
	replacementResources.Release()
	replacement.State = "building"
	replacement.RouterGeneration = 0
	replacement.RouterAsset = VectorPartitionAssetV1{}
	replacement.ReadySetDigest = ""
	replacement.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(replacement, nil); err != nil {
		t.Fatalf("publish replacement BUILD: %v", err)
	}
	if err := col.ValidateActiveVectorPartitionAuthorityTokenWithContextV1(t.Context(), def.Name, active.Generation, token); err != nil {
		t.Fatalf("replacement BUILD invalidated active generation: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	hookCalls := 0
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "after_slot_read" && hookCalls == 0 {
			hookCalls++
			cancel()
		}
		return nil
	})
	pin, err := col.AcquireVectorPartitionReaderPinWithContextV1(ctx, def.Name, active.Generation)
	restore()
	if !errors.Is(err, context.Canceled) {
		if pin != nil {
			pin.Release()
		}
		t.Fatalf("reader pin cancellation err=%v want context canceled", err)
	}
	if pin != nil {
		t.Fatal("canceled reader pin acquisition returned a pin")
	}
	if hookCalls != 1 {
		t.Fatalf("reader pin lifecycle hook calls=%d want 1", hookCalls)
	}
	if got := vectorPartitionReaderPinCountV1(d.Dir(), col.name, def.Name, active.Generation); got != 0 {
		t.Fatalf("canceled reader pin count=%d want 0", got)
	}
}

// legacyCollectionVectorPartitionReadyDeleteCrashWindowReopenReachability is
// retained as format archaeology. Immutable checkpoint fault/reopen coverage
// lives in vector_partition_lifecycle_checkpoint_store_v1_test.go.
func legacyCollectionVectorPartitionReadyDeleteCrashWindowReopenReachability(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	newFixture := func(t *testing.T) (*backenddb.DB, *Collection, *VectorPartitionStoreV1, VectorPartitionManifestV1, []ColumnAssetRef) {
		t.Helper()
		_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatal(err)
		}
		if err := col.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
		if err != nil {
			t.Fatal(err)
		}
		m := testVectorPartitionManifestV1()
		m.IndexName, m.IndexDefinitionDigest = def.Name, VectorIndexDefinitionDigestV1(def)
		m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
		m, resources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, m, 991)
		if err := col.PublishVectorPartitionManifestV1(m, resources); err != nil {
			resources.Release()
			t.Fatal(err)
		}
		refs := append([]ColumnAssetRef(nil), m.Assets[0].Ref, m.Assets[1].Ref, m.RouterAsset.Ref)
		store, err := OpenExistingVectorPartitionStoreV1(d.Dir())
		if err != nil {
			t.Fatal(err)
		}
		if err := col.DeactivateVectorPartitionV1(m.IndexName); err != nil {
			t.Fatal(err)
		}
		return d, col, store, m, refs
	}
	removeRetired := func(t *testing.T, store *VectorPartitionStoreV1, m VectorPartitionManifestV1) {
		t.Helper()
		retired := filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".retired")
		if err := os.Remove(retired); err != nil {
			t.Fatal(err)
		}
		if err := syncDirVPM(store.dir); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("durable_inactive_marker_is_prepared_only_after_reopen", func(t *testing.T) {
		d, col, store, m, refs := newFixture(t)
		if err := store.writeInactiveMarker(m.Collection, m.IndexName, m.Generation); err != nil {
			t.Fatal(err)
		}
		removeRetired(t, store, m)
		dir := d.Dir()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		reopenedDB := openCollectionCommandWALDB(t, dir)
		defer reopenedDB.Close()
		reopened, err := NewCollectionManager(reopenedDB).OpenCollection(col.name)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		if _, err := reopened.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err != nil {
			t.Fatalf("reopen reachability rejected tombstoned ready crash window: %v", err)
		}
		gc, err := reopened.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{DryRun: true, CandidateRefs: refs})
		if err != nil {
			t.Fatalf("reopen GC: %v", err)
		}
		if gc.BytesEligible != 0 {
			t.Fatalf("tombstoned ready assets became eligible: %+v", gc)
		}
		if err := reopened.DeleteVectorPartitionGenerationV1(m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
			t.Fatalf("retry exact delete: %v", err)
		}
	})
	t.Run("older_ready_generations_remain_prepared_after_latest_delete_crash", func(t *testing.T) {
		d, col, store, older, _ := newFixture(t)
		latest := older
		latest.Generation++
		latest.RouterGeneration = latest.Generation
		latest.Canonicalize()
		latest, latestResources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, latest, 992)
		if err := col.PublishVectorPartitionManifestV1(latest, latestResources); err != nil {
			latestResources.Release()
			t.Fatalf("publish latest ready generation: %v", err)
		}
		if err := col.DeactivateVectorPartitionV1(latest.IndexName); err != nil {
			t.Fatal(err)
		}
		if err := col.DeleteVectorPartitionGenerationV1(latest.IndexName, latest.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
			t.Fatalf("delete latest ready generation: %v", err)
		}
		if _, err := col.ReclaimVectorPartitionGenerationV1(t.Context(), latest.IndexName, latest.Generation); err != nil {
			t.Fatalf("reclaim latest ready generation: %v", err)
		}
		if _, err := os.Stat(store.deleteTombstonePath(latest.Collection, latest.IndexName, latest.Generation)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("latest reclaim tombstone remains: %v", err)
		}
		if generation, err := store.readInactiveGeneration(latest.Collection, latest.IndexName); err != nil || generation != latest.Generation {
			t.Fatalf("latest reclaim inactive marker generation=%d err=%v", generation, err)
		}
		dir := d.Dir()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		reopenedDB := openCollectionCommandWALDB(t, dir)
		reopened, err := NewCollectionManager(reopenedDB).OpenCollection(col.name)
		if err != nil {
			reopenedDB.Close()
			t.Fatal(err)
		}
		plan, err := reopened.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{})
		if err != nil {
			reopenedDB.Close()
			t.Fatalf("reachability with older ready generation: %v", err)
		}
		if plan.Sources.PreparedRefs != 3 || plan.Sources.PinnedRefs != 0 {
			reopenedDB.Close()
			t.Fatalf("pointerless crash roots prepared=%d pinned=%d, want 3 prepared and 0 pinned", plan.Sources.PreparedRefs, plan.Sources.PinnedRefs)
		}
		reopenedStore, err := OpenExistingVectorPartitionStoreV1(reopenedDB.Dir())
		if err != nil {
			reopenedDB.Close()
			t.Fatal(err)
		}
		third := latest
		third.Generation++
		third.RouterGeneration = third.Generation
		third.Canonicalize()
		third, thirdResources := vectorPartitionManifestWithFreshStableAssetsV1(t, reopenedDB, reopened, third, 993)
		if err := reopened.PublishVectorPartitionManifestV1(third, thirdResources); err != nil {
			thirdResources.Release()
			reopenedDB.Close()
			t.Fatalf("republish after inactive state: %v", err)
		}
		if _, err := reopenedStore.readInactiveGeneration(third.Collection, third.IndexName); !errors.Is(err, os.ErrNotExist) {
			reopenedDB.Close()
			t.Fatalf("new active generation retained stale inactive marker: %v", err)
		}
		if err := reopened.DeactivateVectorPartitionV1(third.IndexName); err != nil {
			reopenedDB.Close()
			t.Fatal(err)
		}
		if err := reopened.DeleteVectorPartitionGenerationV1(third.IndexName, third.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
			reopenedDB.Close()
			t.Fatalf("delete republished generation: %v", err)
		}
		if _, err := reopened.ReclaimVectorPartitionGenerationV1(t.Context(), third.IndexName, third.Generation); err != nil {
			reopenedDB.Close()
			t.Fatalf("reclaim republished generation: %v", err)
		}
		if _, err := reopened.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err != nil {
			reopenedDB.Close()
			t.Fatalf("reachability after exact delete: %v", err)
		}
		if err := reopenedDB.Close(); err != nil {
			t.Fatal(err)
		}
		againDB := openCollectionCommandWALDB(t, dir)
		defer againDB.Close()
		again, err := NewCollectionManager(againDB).OpenCollection(col.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := again.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err != nil {
			t.Fatalf("reopen after exact delete: %v", err)
		}
	})
	t.Run("tombstone_cannot_authorize_pointerless_ready_state", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			configure func(*vectorPartitionReclaimStateV1)
		}{
			{name: "matching_refs", configure: func(*vectorPartitionReclaimStateV1) {}},
			{name: "mismatched_refs", configure: func(state *vectorPartitionReclaimStateV1) {
				state.OriginalRefs[0].Generation++
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				d, col, store, m, _ := newFixture(t)
				defer d.Close()
				state, err := newVectorPartitionReclaimStateV1(m)
				if err != nil {
					t.Fatal(err)
				}
				tc.configure(&state)
				if err := store.writeDeleteTombstoneState(state); err != nil {
					t.Fatal(err)
				}
				removeRetired(t, store, m)
				if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err == nil {
					t.Fatal("non-explanatory pointerless ready state did not fail closed")
				}
			})
		}
	})
	t.Run("pointerless_ready_without_tombstone_fails_closed", func(t *testing.T) {
		d, col, store, m, _ := newFixture(t)
		defer d.Close()
		removeRetired(t, store, m)
		if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err == nil {
			t.Fatal("pointerless ready manifest without tombstone did not fail closed")
		}
	})
	t.Run("malformed_inactive_marker_fails_closed", func(t *testing.T) {
		d, col, store, m, _ := newFixture(t)
		defer d.Close()
		if err := os.WriteFile(store.inactivePath(m.Collection, m.IndexName), []byte("corrupt"), 0600); err != nil {
			t.Fatal(err)
		}
		removeRetired(t, store, m)
		if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err == nil {
			t.Fatal("malformed inactive marker did not fail closed")
		}
	})
	t.Run("status_requires_valid_inactive_authority", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			configure func(*VectorPartitionStoreV1, VectorPartitionManifestV1) error
			want      string
		}{
			{name: "valid", configure: func(store *VectorPartitionStoreV1, m VectorPartitionManifestV1) error {
				if err := store.writeInactiveMarker(m.Collection, m.IndexName, m.Generation); err != nil {
					return err
				}
				return os.Remove(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".retired"))
			}, want: "inactive"},
			{name: "absent", configure: func(store *VectorPartitionStoreV1, m VectorPartitionManifestV1) error {
				return os.Remove(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".retired"))
			}, want: "pointer_invalid"},
			{name: "corrupt", configure: func(store *VectorPartitionStoreV1, m VectorPartitionManifestV1) error {
				if err := os.WriteFile(store.inactivePath(m.Collection, m.IndexName), []byte("corrupt"), 0600); err != nil {
					return err
				}
				return os.Remove(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".retired"))
			}, want: "pointer_invalid"},
			{name: "foreign", configure: func(store *VectorPartitionStoreV1, m VectorPartitionManifestV1) error {
				raw, err := encodeVectorPartitionInactiveStateV1(vectorPartitionInactiveStateV1{Collection: m.Collection, IndexName: "other", Generation: m.Generation})
				if err != nil {
					return err
				}
				if err := os.WriteFile(store.inactivePath(m.Collection, m.IndexName), raw, 0600); err != nil {
					return err
				}
				return os.Remove(filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".retired"))
			}, want: "pointer_invalid"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				d, col, store, m, _ := newFixture(t)
				defer d.Close()
				if err := tc.configure(store, m); err != nil {
					t.Fatal(err)
				}
				status, err := col.VectorPartitionStatusV1(m.IndexName, m.Generation)
				if err != nil || status.StaleReason != tc.want || status.Active {
					t.Fatalf("status=%+v err=%v, want inactive status %q", status, err, tc.want)
				}
			})
		}
	})
	t.Run("stale_active_pointer_with_valid_inactive_and_retained_history_fails_closed", func(t *testing.T) {
		d, col, store, m, _ := newFixture(t)
		defer d.Close()
		if err := store.writeInactiveMarker(m.Collection, m.IndexName, m.Generation); err != nil {
			t.Fatal(err)
		}
		removeRetired(t, store, m)
		activePath := filepath.Join(store.dir, safeVPM(m.Collection)+"-"+safeVPM(m.IndexName)+".active")
		if err := os.WriteFile(activePath, []byte("999\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := syncDirVPM(store.dir); err != nil {
			t.Fatal(err)
		}
		status, err := col.VectorPartitionStatusV1(m.IndexName, m.Generation)
		if err != nil || status.Active || status.StaleReason != "pointer_invalid" {
			t.Fatalf("stale active status=%+v err=%v", status, err)
		}
		if _, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{}); err == nil {
			t.Fatal("stale active pointer with valid inactive marker passed reachability")
		}
		candidate := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, 917, []byte("must-not-delete-with-stale-active-pointer"))
		candidatePath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
		if err != nil {
			t.Fatal(err)
		}
		stats, err := col.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{CandidateRefs: []ColumnAssetRef{candidate}})
		if err == nil || stats.SegmentsDeleted != 0 {
			t.Fatalf("GC with stale active pointer stats=%+v err=%v", stats, err)
		}
		if _, err := os.Stat(candidatePath); err != nil {
			t.Fatalf("GC removed candidate before rejecting stale active pointer: %v", err)
		}
	})
}

func TestLegacyVectorPartitionInactiveStateV1BoundedCodec(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	limits := DefaultVectorPartitionManifestLimits()
	state := vectorPartitionInactiveStateV1{
		Collection: strings.Repeat("c", limits.MaxStringBytes),
		IndexName:  strings.Repeat("i", limits.MaxStringBytes),
		Generation: 1,
	}
	raw, err := encodeVectorPartitionInactiveStateV1(state)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != vectorPartitionInactiveStateMaxEncodedBytesV1() {
		t.Fatalf("maximum inactive record bytes=%d want %d", len(raw), vectorPartitionInactiveStateMaxEncodedBytesV1())
	}
	decoded, err := decodeVectorPartitionInactiveStateV1(raw)
	if err != nil || decoded != state {
		t.Fatalf("maximum inactive record decoded=%+v err=%v", decoded, err)
	}
	if _, err := encodeVectorPartitionInactiveStateV1(vectorPartitionInactiveStateV1{Collection: state.Collection + "x", IndexName: state.IndexName, Generation: 1}); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
		t.Fatalf("over-cap inactive identity err=%v", err)
	}
	for name, malformed := range map[string][]byte{
		"trailing": append(append([]byte(nil), raw...), 0),
		"corrupt":  func() []byte { x := append([]byte(nil), raw...); x[len(x)-1] ^= 0xff; return x }(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeVectorPartitionInactiveStateV1(malformed); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("decode err=%v, want fail-closed invalid marker", err)
			}
		})
	}
}

func TestVectorPartitionOwnedTempInstallNoReplaceV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.beginMutationDirV1()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	temp, err := d.write(".temp", []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	defer temp.cleanup()
	if err := temp.installNoReplace("target"); err != nil {
		t.Fatal(err)
	}
	got, err := d.read("target", 32)
	if err != nil || string(got) != "payload" {
		t.Fatalf("target=%q err=%v", got, err)
	}
	if err := temp.cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.read(".temp", 32); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary name err=%v, want absent", err)
	}
	if _, err := d.read("target", 32); err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionOwnedTempInstallNoReplaceExistingTargetV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.beginMutationDirV1()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := os.WriteFile(filepath.Join(s.dir, "target"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	temp, err := d.write(".temp", []byte("new"))
	if err != nil {
		t.Fatal(err)
	}
	defer temp.cleanup()
	if err := temp.installNoReplace("target"); !errors.Is(err, os.ErrExist) {
		t.Fatalf("err=%v", err)
	}
	got, err := d.read("target", 32)
	if err != nil || string(got) != "old" {
		t.Fatalf("target=%q err=%v", got, err)
	}
}

func TestVectorPartitionOwnedTempReplaceV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.beginMutationDirV1()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := os.WriteFile(filepath.Join(s.dir, "target"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	temp, err := d.write(".temp", []byte("replacement"))
	if err != nil {
		t.Fatal(err)
	}
	if err := temp.replace("target"); err != nil {
		t.Fatal(err)
	}
	got, err := d.read("target", 32)
	if err != nil || string(got) != "replacement" {
		t.Fatalf("target=%q err=%v", got, err)
	}
	identity, err := d.childIdentity("target")
	if err != nil || !rootpublication.SamePhysicalIdentity(identity, temp.identity) {
		t.Fatalf("target identity=%+v err=%v, want %+v", identity, err, temp.identity)
	}
	if err := temp.cleanup(); err != nil {
		t.Fatal(err)
	}
	if temp.file != nil {
		t.Fatal("cleanup left original temporary file open")
	}
	if _, err := d.read(".temp", 32); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary name err=%v, want absent", err)
	}
}

func TestVectorPartitionOwnedTempSourceReboundV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, tc := range []struct {
		name          string
		prepareTarget func(t *testing.T, s *VectorPartitionStoreV1)
		install       func(*vectorPartitionOwnedTempV1) error
		checkTarget   func(t *testing.T, d *vectorPartitionMutationDirV1)
	}{
		{
			name:    "install-no-replace",
			install: func(temp *vectorPartitionOwnedTempV1) error { return temp.installNoReplace("target") },
			checkTarget: func(t *testing.T, d *vectorPartitionMutationDirV1) {
				t.Helper()
				if _, err := d.read("target", 32); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("target err=%v, want absent", err)
				}
			},
		},
		{
			name: "replace",
			prepareTarget: func(t *testing.T, s *VectorPartitionStoreV1) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(s.dir, "target"), []byte("old"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			install: func(temp *vectorPartitionOwnedTempV1) error { return temp.replace("target") },
			checkTarget: func(t *testing.T, d *vectorPartitionMutationDirV1) {
				t.Helper()
				got, err := d.read("target", 32)
				if err != nil || string(got) != "old" {
					t.Fatalf("target=%q err=%v", got, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := OpenVectorPartitionStoreV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			d, err := s.beginMutationDirV1()
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if tc.prepareTarget != nil {
				tc.prepareTarget(t, s)
			}
			temp, err := d.write(".temp", []byte("original"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(s.dir, ".temp")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(s.dir, ".temp"), []byte("rebound"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := tc.install(temp); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("install err=%v, want rebound-source rejection", err)
			}
			tc.checkTarget(t, d)
			if err := temp.cleanup(); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("cleanup err=%v, want rebound-source rejection", err)
			}
			if temp.file != nil {
				t.Fatal("cleanup left original temporary file open")
			}
			got, err := d.read(".temp", 32)
			if err != nil || string(got) != "rebound" {
				t.Fatalf("rebound temp=%q err=%v", got, err)
			}
		})
	}
}

func TestVectorPartitionOwnedTempTargetReboundV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, tc := range []struct {
		name    string
		install func(*vectorPartitionOwnedTempV1) error
	}{
		{name: "install-no-replace", install: func(temp *vectorPartitionOwnedTempV1) error { return temp.installNoReplace("target") }},
		{name: "replace", install: func(temp *vectorPartitionOwnedTempV1) error { return temp.replace("target") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := OpenVectorPartitionStoreV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			d, err := s.beginMutationDirV1()
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			temp, err := d.write(".temp", []byte("owned"))
			if err != nil {
				t.Fatal(err)
			}
			d.afterTempInstall = func(target string) {
				if err := os.Remove(filepath.Join(s.dir, target)); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(s.dir, target), []byte("target-decoy"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := tc.install(temp); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("install err=%v, want target-rebind rejection", err)
			}
			if err := temp.cleanup(); !errors.Is(err, ErrVectorPartitionManifestInvalid) && err != nil {
				t.Fatalf("cleanup err=%v", err)
			}
			if temp.file != nil {
				t.Fatal("cleanup left original temporary file open")
			}
			got, err := d.read("target", 32)
			if err != nil || string(got) != "target-decoy" {
				t.Fatalf("target=%q err=%v", got, err)
			}
		})
	}
}

func TestVectorPartitionOwnedTempSymlinkSourceReboundV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, tc := range []struct {
		name          string
		prepareTarget func(t *testing.T, s *VectorPartitionStoreV1)
		install       func(*vectorPartitionOwnedTempV1) error
		checkTarget   func(t *testing.T, d *vectorPartitionMutationDirV1)
	}{
		{
			name:    "install-no-replace",
			install: func(temp *vectorPartitionOwnedTempV1) error { return temp.installNoReplace("target") },
			checkTarget: func(t *testing.T, d *vectorPartitionMutationDirV1) {
				t.Helper()
				if _, err := d.read("target", 32); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("target err=%v, want absent", err)
				}
			},
		},
		{
			name: "replace",
			prepareTarget: func(t *testing.T, s *VectorPartitionStoreV1) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(s.dir, "target"), []byte("old"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
			install: func(temp *vectorPartitionOwnedTempV1) error { return temp.replace("target") },
			checkTarget: func(t *testing.T, d *vectorPartitionMutationDirV1) {
				t.Helper()
				got, err := d.read("target", 32)
				if err != nil || string(got) != "old" {
					t.Fatalf("target=%q err=%v", got, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := OpenVectorPartitionStoreV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			d, err := s.beginMutationDirV1()
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if tc.prepareTarget != nil {
				tc.prepareTarget(t, s)
			}
			temp, err := d.write(".temp", []byte("original"))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(s.dir, ".temp")); err != nil {
				t.Fatal(err)
			}
			victim := filepath.Join(s.dir, "victim")
			if err := os.WriteFile(victim, []byte("victim-bytes"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("victim", filepath.Join(s.dir, ".temp")); err != nil {
				t.Skipf("symlink unsupported: %v", err)
			}
			if err := tc.install(temp); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("install err=%v, want symlink-source rejection", err)
			}
			tc.checkTarget(t, d)
			if err := temp.cleanup(); err == nil {
				t.Fatal("cleanup accepted a rebound symlink")
			}
			if temp.file != nil {
				t.Fatal("cleanup left original temporary file open")
			}
			info, err := os.Lstat(filepath.Join(s.dir, ".temp"))
			if err != nil || info.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("rebound temp info=%v err=%v, want symlink", info, err)
			}
			got, err := os.ReadFile(victim)
			if err != nil || string(got) != "victim-bytes" {
				t.Fatalf("symlink destination=%q err=%v", got, err)
			}
		})
	}
}

func TestVectorPartitionMutationWriteTempCollisionV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.beginMutationDirV1()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := os.WriteFile(filepath.Join(s.dir, ".temp"), []byte("already-here"), 0o600); err != nil {
		t.Fatal(err)
	}
	if temp, err := d.write(".temp", []byte("new")); !errors.Is(err, os.ErrExist) || temp != nil {
		t.Fatalf("write temp=%v err=%v, want nil and EEXIST", temp, err)
	}
	got, err := d.read(".temp", 32)
	if err != nil || string(got) != "already-here" {
		t.Fatalf("collision entry=%q err=%v", got, err)
	}
}

func TestVectorPartitionMutationWritePreSyncFailureCleanupV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.beginMutationDirV1()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	injected := errors.New("injected pre-sync failure")
	var retained *os.File
	d.beforeWriteSync = func(f *os.File) error {
		retained = f
		return injected
	}
	temp, err := d.write(".temp", []byte("partially-written"))
	if !errors.Is(err, injected) || temp != nil {
		t.Fatalf("write temp=%v err=%v, want nil and injected failure", temp, err)
	}
	if retained == nil {
		t.Fatal("pre-sync hook did not receive retained descriptor")
	}
	if err := retained.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("retained descriptor close err=%v, want closed", err)
	}
	if _, err := d.read(".temp", 32); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("owned temporary name err=%v, want absent", err)
	}
}

func TestVectorPartitionMutationWritePreSyncFailureReboundTempV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	d, err := s.beginMutationDirV1()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	injected := errors.New("injected pre-sync failure")
	var retained *os.File
	d.beforeWriteSync = func(f *os.File) error {
		retained = f
		if err := os.Remove(filepath.Join(s.dir, ".temp")); err != nil {
			t.Fatalf("rebind remove: %v", err)
		}
		if err := os.WriteFile(filepath.Join(s.dir, ".temp"), []byte("rebound-decoy"), 0o600); err != nil {
			t.Fatalf("rebind create: %v", err)
		}
		return injected
	}
	temp, err := d.write(".temp", []byte("partially-written"))
	if !errors.Is(err, injected) || !errors.Is(err, ErrVectorPartitionManifestInvalid) || temp != nil {
		t.Fatalf("write temp=%v err=%v, want rebound cleanup rejection and injected failure", temp, err)
	}
	if retained == nil {
		t.Fatal("pre-sync hook did not receive retained descriptor")
	}
	if err := retained.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("retained descriptor close err=%v, want closed", err)
	}
	got, err := d.read(".temp", 32)
	if err != nil || string(got) != "rebound-decoy" {
		t.Fatalf("rebound temporary=%q err=%v", got, err)
	}
}

func legacyVectorPartitionStoreV1PublishGenerationTempReboundV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, tc := range []struct {
		name    string
		rebind  func(t *testing.T, store *VectorPartitionStoreV1, tempName string) (string, []byte)
		inspect func(t *testing.T, path string)
	}{
		{
			name: "regular-file",
			rebind: func(t *testing.T, store *VectorPartitionStoreV1, tempName string) (string, []byte) {
				t.Helper()
				decoy := []byte("regular-decoy")
				path := filepath.Join(store.dir, tempName)
				if err := os.WriteFile(path, decoy, 0o600); err != nil {
					t.Fatal(err)
				}
				return path, decoy
			},
			inspect: func(t *testing.T, path string) {
				t.Helper()
				info, err := os.Lstat(path)
				if err != nil || !info.Mode().IsRegular() {
					t.Fatalf("rebound temp info=%v err=%v, want regular file", info, err)
				}
			},
		},
		{
			name: "symlink",
			rebind: func(t *testing.T, store *VectorPartitionStoreV1, tempName string) (string, []byte) {
				t.Helper()
				decoy := []byte("symlink-destination-decoy")
				victim := filepath.Join(store.dir, "generation-temp-victim")
				if err := os.WriteFile(victim, decoy, 0o600); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(store.dir, tempName)
				if err := os.Symlink(filepath.Base(victim), path); err != nil {
					t.Fatal(err)
				}
				return path, decoy
			},
			inspect: func(t *testing.T, path string) {
				t.Helper()
				info, err := os.Lstat(path)
				if err != nil || info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("rebound temp info=%v err=%v, want symlink", info, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := OpenVectorPartitionStoreV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			manifest := testVectorPartitionManifestV1()
			generationName := fmt.Sprintf("%s-%s-%d.vpm", safeVPM(manifest.Collection), safeVPM(manifest.IndexName), manifest.Generation)
			var reboundPath string
			var decoy []byte
			stop := setVectorPartitionPublishHookForTestV1(func(at string) error {
				if at != "generation_temp_synced" {
					return nil
				}
				entries, err := os.ReadDir(store.dir)
				if err != nil {
					t.Fatal(err)
				}
				prefix := "." + generationName + "."
				var tempName string
				for _, entry := range entries {
					if strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), ".tmp") {
						if tempName != "" {
							t.Fatalf("multiple generation temporary files: %q and %q", tempName, entry.Name())
						}
						tempName = entry.Name()
					}
				}
				if tempName == "" {
					t.Fatal("generation temporary file not found at sync boundary")
				}
				if err := os.Remove(filepath.Join(store.dir, tempName)); err != nil {
					t.Fatal(err)
				}
				reboundPath, decoy = tc.rebind(t, store, tempName)
				return nil
			})
			err = store.publishLocked(manifest)
			stop()
			if err == nil {
				t.Fatal("publication accepted a rebound generation temporary file")
			}
			if reboundPath == "" {
				t.Fatal("generation-temp hook did not run")
			}
			tc.inspect(t, reboundPath)
			got, err := os.ReadFile(reboundPath)
			if err != nil || !bytes.Equal(got, decoy) {
				t.Fatalf("rebound decoy=%q err=%v", got, err)
			}
			if _, err := os.Lstat(filepath.Join(store.dir, generationName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("final generation err=%v, want absent", err)
			}
			activeName := safeVPM(manifest.Collection) + "-" + safeVPM(manifest.IndexName) + ".active"
			if _, err := os.Lstat(filepath.Join(store.dir, activeName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("active pointer err=%v, want absent", err)
			}
		})
	}
}

func legacyVectorPartitionStoreV1PublishActiveTempReboundV1(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	for _, tc := range []struct {
		name    string
		rebind  func(t *testing.T, store *VectorPartitionStoreV1, tempName string) (string, []byte)
		inspect func(t *testing.T, path string)
	}{
		{
			name: "regular-file",
			rebind: func(t *testing.T, store *VectorPartitionStoreV1, tempName string) (string, []byte) {
				t.Helper()
				decoy := []byte("regular-active-decoy")
				path := filepath.Join(store.dir, tempName)
				if err := os.WriteFile(path, decoy, 0o600); err != nil {
					t.Fatal(err)
				}
				return path, decoy
			},
			inspect: func(t *testing.T, path string) {
				t.Helper()
				info, err := os.Lstat(path)
				if err != nil || !info.Mode().IsRegular() {
					t.Fatalf("rebound temp info=%v err=%v, want regular file", info, err)
				}
			},
		},
		{
			name: "symlink",
			rebind: func(t *testing.T, store *VectorPartitionStoreV1, tempName string) (string, []byte) {
				t.Helper()
				decoy := []byte("symlink-active-destination-decoy")
				victim := filepath.Join(store.dir, "active-temp-victim")
				if err := os.WriteFile(victim, decoy, 0o600); err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(store.dir, tempName)
				if err := os.Symlink(filepath.Base(victim), path); err != nil {
					t.Fatal(err)
				}
				return path, decoy
			},
			inspect: func(t *testing.T, path string) {
				t.Helper()
				info, err := os.Lstat(path)
				if err != nil || info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("rebound temp info=%v err=%v, want symlink", info, err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := OpenVectorPartitionStoreV1(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			manifest := testVectorPartitionManifestV1()
			activeName := safeVPM(manifest.Collection) + "-" + safeVPM(manifest.IndexName) + ".active"
			var reboundPath string
			var decoy []byte
			stop := setVectorPartitionPublishHookForTestV1(func(at string) error {
				if at != "active_temp_synced" {
					return nil
				}
				entries, err := os.ReadDir(store.dir)
				if err != nil {
					t.Fatal(err)
				}
				prefix := "." + activeName + "."
				var tempName string
				for _, entry := range entries {
					if strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), ".tmp") {
						if tempName != "" {
							t.Fatalf("multiple active temporary files: %q and %q", tempName, entry.Name())
						}
						tempName = entry.Name()
					}
				}
				if tempName == "" {
					t.Fatal("active temporary file not found at sync boundary")
				}
				if err := os.Remove(filepath.Join(store.dir, tempName)); err != nil {
					t.Fatal(err)
				}
				reboundPath, decoy = tc.rebind(t, store, tempName)
				return nil
			})
			err = store.publishLocked(manifest)
			stop()
			if err == nil {
				t.Fatal("publication accepted a rebound active temporary file")
			}
			if reboundPath == "" {
				t.Fatal("active-temp hook did not run")
			}
			tc.inspect(t, reboundPath)
			got, err := os.ReadFile(reboundPath)
			if err != nil || !bytes.Equal(got, decoy) {
				t.Fatalf("rebound decoy=%q err=%v", got, err)
			}
			generation, err := store.Open(manifest.Collection, manifest.IndexName, manifest.Generation)
			if err != nil || generation.Generation != manifest.Generation {
				t.Fatalf("prepared generation=%+v err=%v", generation, err)
			}
			if raw, err := os.ReadFile(filepath.Join(store.dir, activeName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("active pointer=%q err=%v, want absent", raw, err)
			}
		})
	}
}

func TestVectorPartitionLifecycleContractDoc(t *testing.T) {
	doc := readRepoText(t, "TreeDB/docs/spec/vector-partition-raft-v1.md")
	requireTextContains(t, "vector partition lifecycle contract", doc,
		"Local publication installs an immutable BUILD checkpoint, then READY and\nLOCAL_ACTIVATE deltas.",
		"A crash or retry therefore observes building, ready but\ninactive, or ready and active state",
		"A durable DELETE_PREPARE initially transitions any non-active building or ready\ngeneration, including a retired ready generation, to deleting.",
		"An identical\nDELETE_PREPARE retry is idempotent.",
		"Recovery derives every state from the highest VCP1 checkpoint plus its exact\nVLC1 tail",
	)
}

func TestVectorPartitionStorageFormatContractDoc(t *testing.T) {
	doc := readRepoText(t, "TreeDB/docs/spec/storage-format.md")
	requireTextContains(t, "vector partition storage format", doc,
		"### Vector-partition manifests (`vector_partitions/`)",
		"one (exactly one)\nrouter-asset frame",
		"wire version `3`",
		"Version 3 has this fixed,\nuntagged order",
		"adds the membership-digest string between the asset checksum and byte length",
		"decoder accepts only version 3",
		"The highest checkpoint epoch is the sole authority",
		"VPR1 is the bounded, versioned, checksummed reclaim payload",
		"Raft-snapshot-included namespace",
	)
}

func TestVectorPartitionStoreV1CleanupRefusesReachableGeneration(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	if err := deleteVectorPartitionStoreForTest(s, "docs", "embedding", 7, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("active generation deleted")
	}
	newer := m
	newer.Generation = 8
	newer.RouterGeneration = 8
	newer.RouterAsset.Ref.Generation = newer.Generation
	newer.Canonicalize()
	if err := s.publishLocked(newer); err != nil {
		t.Fatal(err)
	}
	for _, e := range []VectorPartitionCleanupEligibilityV1{{Active: true}, {ReaderPins: 1}, {SnapshotReferences: 1}, {CatalogReferences: 1}} {
		if err := deleteVectorPartitionStoreForTest(s, "docs", "embedding", 7, e); err == nil {
			t.Fatalf("eligible=%+v deleted", e)
		}
	}
	if err := deleteVectorPartitionStoreForTest(s, "docs", "embedding", 7, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.dir, safeVPM("docs")+"-"+safeVPM("embedding")+".active"), []byte("8\ntrailing"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := deleteVectorPartitionStoreForTest(s, "docs", "embedding", 8, VectorPartitionCleanupEligibilityV1{}); err == nil {
		t.Fatal("corrupt active pointer did not fail closed")
	}
}

func TestVectorPartitionStoreV1DeleteAcceptsManifestOrderDifferentFromReclaimOrder(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.Assets[0].Ref.FileID = 20
	m.Assets[1].Ref.FileID = 10
	m.RouterAsset.Ref.FileID = 30
	m.Canonicalize()
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	if err := deactivateVectorPartitionStoreForTest(s, m.Collection, m.IndexName); err != nil {
		t.Fatal(err)
	}
	if err := deleteVectorPartitionStoreForTest(s, m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatalf("delete non-ref-ordered manifest: %v", err)
	}
	reclaim, err := s.openDeleteTombstone(m.Collection, m.IndexName, m.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := []uint32{
		reclaim.OriginalRefs[0].FileID,
		reclaim.OriginalRefs[1].FileID,
		reclaim.OriginalRefs[2].FileID,
	}, []uint32{10, 20, 30}; !reflect.DeepEqual(got, want) {
		t.Fatalf("reclaim file order=%v want %v", got, want)
	}
}

func legacyVectorPartitionStoreV1PublishFaultWindowsReopenOldOrCompleteNew(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	boundaries := []string{"generation_temp_synced", "generation_linked", "generation_dir_synced", "active_temp_synced", "active_renamed", "active_dir_synced", "retired_removed", "publication_complete"}
	for _, boundary := range boundaries {
		t.Run(boundary, func(t *testing.T) {
			root := t.TempDir()
			store, err := OpenVectorPartitionStoreV1(root)
			if err != nil {
				t.Fatal(err)
			}
			old := testVectorPartitionManifestV1()
			if err := store.publishLocked(old); err != nil {
				t.Fatal(err)
			}
			candidate := old
			candidate.Generation, candidate.RouterGeneration = old.Generation+1, old.Generation+1
			candidate.RouterAsset.Ref.Generation = candidate.Generation
			candidate.Canonicalize()
			stop := setVectorPartitionPublishHookForTestV1(func(at string) error {
				if at == boundary {
					return errors.New("injected publication interruption")
				}
				return nil
			})
			err = store.publishLocked(candidate)
			stop()
			if err == nil || !strings.Contains(err.Error(), "injected publication interruption") {
				t.Fatalf("publish err=%v want injected interruption", err)
			}
			reopened, err := OpenExistingVectorPartitionStoreV1(root)
			if err != nil {
				t.Fatal(err)
			}
			active, err := reopened.OpenActive(old.Collection, old.IndexName)
			if err != nil {
				t.Fatalf("reopen active after %s: %v", boundary, err)
			}
			if active.Generation != old.Generation && active.Generation != candidate.Generation {
				t.Fatalf("active generation after %s=%d want old=%d or complete new=%d", boundary, active.Generation, old.Generation, candidate.Generation)
			}
			for _, generation := range []uint64{old.Generation, candidate.Generation} {
				m, openErr := reopened.Open(old.Collection, old.IndexName, generation)
				if openErr != nil && generation == old.Generation {
					t.Fatalf("old generation unreadable after %s: %v", boundary, openErr)
				}
				if openErr == nil && m.Generation != generation {
					t.Fatalf("generation %d decoded as %d", generation, m.Generation)
				}
			}
		})
	}
}

func legacyVectorPartitionStoreV1SameGenerationBuildingPromotionCrashSafe(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	boundaries := []string{"generation_temp_synced", "promotion_inactive_synced", "promotion_renamed", "generation_linked", "promotion_dir_synced", "generation_dir_synced", "active_temp_synced", "active_renamed", "active_dir_synced", "retired_removed", "publication_complete"}
	for _, boundary := range boundaries {
		t.Run(boundary, func(t *testing.T) {
			root := t.TempDir()
			store, err := OpenVectorPartitionStoreV1(root)
			if err != nil {
				t.Fatal(err)
			}
			ready := testVectorPartitionManifestV1()
			building := ready
			building.State, building.RouterGeneration, building.RouterAsset, building.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
			building.Canonicalize()
			if err := store.publishLocked(building); err != nil {
				t.Fatal(err)
			}
			stop := setVectorPartitionPublishHookForTestV1(func(at string) error {
				if at == boundary {
					return errors.New("injected promotion interruption")
				}
				return nil
			})
			err = store.publishLocked(ready)
			stop()
			if err == nil || !strings.Contains(err.Error(), "injected promotion interruption") {
				t.Fatalf("promotion err=%v", err)
			}
			reopened, err := OpenExistingVectorPartitionStoreV1(root)
			if err != nil {
				t.Fatal(err)
			}
			got, err := reopened.Open(ready.Collection, ready.IndexName, ready.Generation)
			if err != nil || (got.State != "building" && got.State != "ready") {
				t.Fatalf("reopen promotion state=%q err=%v", got.State, err)
			}
			if got.State == "ready" {
				if _, activeErr := reopened.OpenActive(ready.Collection, ready.IndexName); activeErr != nil {
					if _, inactiveErr := reopened.readInactiveGeneration(ready.Collection, ready.IndexName); inactiveErr != nil {
						t.Fatalf("prepared-only promoted ready lacks inactive authority: active=%v inactive=%v", activeErr, inactiveErr)
					}
				}
			}
			if err := reopened.publishLocked(ready); err != nil {
				t.Fatalf("idempotent promotion retry: %v", err)
			}
			active, err := reopened.OpenActive(ready.Collection, ready.IndexName)
			if err != nil || active.Generation != ready.Generation {
				t.Fatalf("retry active=%+v err=%v", active, err)
			}
		})
	}

	t.Run("negative_transitions", func(t *testing.T) {
		store, err := OpenVectorPartitionStoreV1(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		ready := testVectorPartitionManifestV1()
		building := ready
		building.State, building.RouterGeneration, building.RouterAsset, building.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
		building.Canonicalize()
		if err := store.publishLocked(building); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name   string
			mutate func(*VectorPartitionManifestV1)
		}{
			{name: "source", mutate: func(m *VectorPartitionManifestV1) { m.SourceChecksum++ }},
			{name: "balance", mutate: func(m *VectorPartitionManifestV1) { m.BalancePolicy = "different" }},
			{name: "placement", mutate: func(m *VectorPartitionManifestV1) { m.Placements[0].GroupID = "other-group" }},
			{name: "membership", mutate: func(m *VectorPartitionManifestV1) { m.Memberships[0].VectorOrdinal++ }},
			{name: "asset", mutate: func(m *VectorPartitionManifestV1) { m.Assets[0].ID = "different-asset" }},
		} {
			t.Run(tc.name, func(t *testing.T) {
				wrong := ready
				wrong.Placements = append([]VectorPartitionPlacementV1(nil), ready.Placements...)
				wrong.Memberships = append([]VectorPartitionMembershipV1(nil), ready.Memberships...)
				wrong.Assets = append([]VectorPartitionAssetV1(nil), ready.Assets...)
				tc.mutate(&wrong)
				wrong.Canonicalize()
				if vectorPartitionBuildingPromotionIdentityV1(building, wrong) {
					t.Fatal("building projection accepted changed ready topology")
				}
				if err := store.publishLocked(wrong); err == nil {
					t.Fatal("changed ready promotion accepted")
				}
			})
		}
		if err := store.publishLocked(ready); err != nil {
			t.Fatal(err)
		}
		if err := store.publishLocked(building); err == nil {
			t.Fatal("ready-to-building transition accepted")
		}
	})
}

func TestVectorPartitionStoreV1DeactivateDurablyRetiresActiveGeneration(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	if err := deactivateVectorPartitionStoreForTest(s, m.Collection, m.IndexName); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenActive(m.Collection, m.IndexName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("active after deactivate err=%v, want not exist", err)
	}
	retired, err := s.OpenRetired(m.Collection, m.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	if retired.Generation != m.Generation {
		t.Fatalf("retired generation=%d want %d", retired.Generation, m.Generation)
	}
	if err := deleteVectorPartitionStoreForTest(s, m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatalf("retired generation should be cleanup eligible: %v", err)
	}
}

func legacyVectorPartitionStoreV1CleanupResumesDurableTombstone(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	if err := deactivateVectorPartitionStoreForTest(s, m.Collection, m.IndexName); err != nil {
		t.Fatal(err)
	}
	if err := s.writeDeleteTombstone(m); err != nil {
		t.Fatal(err)
	}
	if err := deleteVectorPartitionStoreForTest(s, m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.deleteTombstonePath(m.Collection, m.IndexName, m.Generation)); err != nil {
		t.Fatalf("cleanup reclaim record missing: %v", err)
	}
	if _, err := s.Open(m.Collection, m.IndexName, m.Generation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted manifest open err=%v, want not exist", err)
	}
}

func TestVectorPartitionStoreV1BuildingPublicationCannotResurrectDeletingGeneration(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	m.Canonicalize()
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	if err := deleteVectorPartitionStoreForTest(s, m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatal(err)
	}
	if err := s.publishValidatedBuilding(m); err == nil {
		t.Fatal("building retry resurrected deleting generation")
	}
	if _, err := s.Open(m.Collection, m.IndexName, m.Generation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest relinked after tombstone: %v", err)
	}
}

func TestVectorPartitionStoreV1DeleteSerializesBuildingPublication(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	m.Canonicalize()
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	restore := setVectorPartitionDeleteAfterTombstoneForTestV1(func() { close(entered); <-release })
	defer restore()
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- deleteVectorPartitionStoreForTest(s, m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{})
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("delete did not persist tombstone")
	}
	publishDone := make(chan error, 1)
	go func() { publishDone <- s.publishValidatedBuilding(m) }()
	select {
	case err := <-publishDone:
		t.Fatalf("publication escaped delete barrier: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delete deadlocked")
	}
	select {
	case err := <-publishDone:
		if err == nil {
			t.Fatal("building publication resurrected delete")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("publication deadlocked")
	}
	if _, err := s.Open(m.Collection, m.IndexName, m.Generation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest relinked after serialized delete: %v", err)
	}
}

func TestCollectionVectorPartitionReclaimV1ReclaimsCoResidentRecords(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, _ := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}})
	defer d.Close()
	lease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatal(err)
	}
	cfg := *col.meta.Options.ColumnStore
	refs, resources, err := AppendColumnPhysicalAssetsWithStableResources(d.ColumnAssetRootDir(), cfg, 801, []StableColumnPhysicalAssetAppend{
		{Payload: []byte("a0"), Kind: ColumnAssetKindTCS1PartImage, Generation: 801, PartID: 1},
		{Payload: []byte("a1"), Kind: ColumnAssetKindTCS1PartImage, Generation: 802, PartID: 2},
	}, d.StableResourceIdentityPinRegistry(), lease)
	lease.Release()
	if err != nil {
		t.Fatal(err)
	}
	resources.Release()
	store, err := OpenVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	for generation, offset := range []int{0, 1} {
		m := testVectorPartitionManifestV1()
		m.Collection = col.name
		m.State = "building"
		m.Generation = uint64(generation + 11)
		m.RouterGeneration = 0
		m.SourceRowCount = 1
		m.PartitionCount = 1
		m.Placements = []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}}
		m.Memberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}}
		m.OverlapMemberships = nil
		m.Representatives = nil
		m.Assets = []VectorPartitionAssetV1{{ID: "partition/0", PartitionID: 0, Checksum: strings.Repeat("a", 64), Bytes: uint64(refs[offset].Length), Ref: refs[offset]}}
		m.RouterAsset = VectorPartitionAssetV1{}
		m.ReadySetDigest = ""
		for i := range m.Assets {
			raw, e := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), m.Assets[i].Ref)
			if e != nil {
				t.Fatal(e)
			}
			sum := sha256.Sum256(raw)
			m.Assets[i].Checksum = hex.EncodeToString(sum[:])
		}
		m.Canonicalize()
		if err := m.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
			t.Fatal(err)
		}
		if err := store.publishLocked(m); err != nil {
			t.Fatal(err)
		}
		if err := col.DeleteVectorPartitionGenerationV1(m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := col.ReclaimVectorPartitionGenerationV1(t.Context(), "embedding", 10); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("never-created generation below lifecycle floor reclaim err=%v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	injected := errors.New("post-install delete complete")
	restore := setVectorPartitionLifecycleStoreHookForTestV1(func(boundary string) error {
		if boundary == "after_delta_install" {
			return injected
		}
		return nil
	})
	restored := false
	defer func() {
		if !restored {
			restore()
		}
	}()
	if _, err := col.ReclaimVectorPartitionGenerationV1(ctx, "embedding", 11); !errors.Is(err, injected) {
		t.Fatalf("reclaim err=%v, want post-install failure", err)
	}
	restore()
	restored = true
	if _, err := store.openDeleteTombstone(col.name, "embedding", 11); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed generation 11 remained deleting: %v", err)
	}
	if _, err := store.openDeleteTombstone(col.name, "embedding", 12); err != nil {
		t.Fatalf("co-resident generation 12 lost after partial completion: %v", err)
	}
	if _, err := col.ReclaimVectorPartitionGenerationV1(ctx, "embedding", 11); err != nil {
		t.Fatalf("retry completed generation while draining co-resident debt: %v", err)
	}
	for _, generation := range []uint64{11, 12} {
		if _, err := store.openDeleteTombstone(col.name, "embedding", generation); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reclaim record %d remains: %v", generation, err)
		}
	}
	if _, err := col.ReclaimVectorPartitionGenerationV1(ctx, "embedding", 11); err != nil {
		t.Fatalf("retry after every reclaim record completed: %v", err)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), refs[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shared segment remains: %v", err)
	}
}

func prepareMixedVectorPartitionReclaimV1(t *testing.T, generation uint64) (string, *backenddb.DB, *Collection, *VectorPartitionStoreV1, []ColumnAssetRef, ColumnAssetRef, string) {
	t.Helper()
	requireVectorPartitionPersistenceV1(t)
	requireColumnAssetExactDestructiveGCTest(t)
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	col := openColumnStoreCollectionM10B(t, d)
	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	liveRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(liveRefs) == 0 {
		t.Fatal("mixed reclaim test requires live manifest refs")
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, generation+100, 99)
	if candidate.FileID != liveRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires a mixed segment", candidate.FileID, liveRefs[0].FileID)
	}
	for _, ref := range liveRefs {
		if ref.FileID != candidate.FileID {
			t.Fatalf("live ref file_id=%d candidate file_id=%d, test requires one mixed segment", ref.FileID, candidate.FileID)
		}
	}
	store, err := OpenVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	publishDeletedVectorPartitionReclaimCandidateV1(t, d, col, store, candidate, generation)
	segmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatal(err)
	}
	return dir, d, col, store, liveRefs, candidate, segmentPath
}

func publishDeletedVectorPartitionReclaimCandidateV1(t *testing.T, d *backenddb.DB, col *Collection, store *VectorPartitionStoreV1, candidate ColumnAssetRef, generation uint64) {
	t.Helper()
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("read candidate: %v", err)
	}
	sum := sha256.Sum256(raw)
	m := testVectorPartitionManifestV1()
	m.Collection = col.name
	m.State = "building"
	m.SourceRowCount = 1
	m.Generation = generation
	m.RouterGeneration = 0
	m.PartitionCount = 1
	m.Placements = []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}}
	m.Memberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}}
	m.OverlapMemberships = nil
	m.Representatives = nil
	m.Assets = []VectorPartitionAssetV1{{
		ID:          "partition/0",
		PartitionID: 0,
		Checksum:    hex.EncodeToString(sum[:]),
		Bytes:       uint64(candidate.Length),
		Ref:         candidate,
	}}
	m.RouterAsset = VectorPartitionAssetV1{}
	m.ReadySetDigest = ""
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err != nil {
		t.Fatalf("mixed reclaim manifest: %v", err)
	}
	if err := store.publishLocked(m); err != nil {
		t.Fatalf("publish mixed reclaim manifest: %v", err)
	}
	if err := col.DeleteVectorPartitionGenerationV1(m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); err != nil {
		t.Fatalf("delete mixed reclaim manifest: %v", err)
	}
}

func TestCollectionVectorPartitionReclaimV1PersistsMixedDebtAcrossReopenAndFallbackAdvance(t *testing.T) {
	dir, d, col, store, beforeRefs, candidate, oldSegmentPath := prepareMixedVectorPartitionReclaimV1(t, 31)
	dClosed := false
	defer func() {
		if !dClosed {
			_ = d.Close()
		}
	}()

	if _, err := col.ReclaimVectorPartitionGenerationV1(t.Context(), "embedding", 31); err == nil || !strings.Contains(err.Error(), "not physically complete") {
		t.Fatalf("first reclaim err=%v, want durable-fallback incomplete", err)
	}
	afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, afterRefs)
	state, err := store.openDeleteTombstone(col.name, "embedding", 31)
	if err != nil {
		t.Fatalf("open persisted reclaim state: %v", err)
	}
	if len(state.OriginalRefs) != 1 || state.OriginalRefs[0] != candidate || len(state.SupersededRefs) != len(beforeRefs) {
		t.Fatalf("reclaim state original=%+v superseded=%+v want candidate plus %d live refs", state.OriginalRefs, state.SupersededRefs, len(beforeRefs))
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("fallback-pinned mixed segment missing: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close after incomplete reclaim: %v", err)
	}
	dClosed = true
	reopenedDB := openCollectionCommandWALDB(t, dir)
	defer reopenedDB.Close()
	reopened := openColumnStoreCollectionM10B(t, reopenedDB)
	reopenedRefs := columnManifestAssetRefsForCollectionM12A(t, reopenedDB, reopened)
	assertColumnAssetRefsEqualM15C(t, afterRefs, reopenedRefs)
	if got, err := reopened.Get([]byte("e1")); err != nil || got == nil {
		t.Fatalf("Get after reopen got=%s err=%v", got, err)
	}
	if _, err := reopened.ReclaimVectorPartitionGenerationV1(t.Context(), "embedding", 31); err == nil || !strings.Contains(err.Error(), "not physically complete") {
		t.Fatalf("retry before fallback advance err=%v, want incomplete", err)
	}
	advanceColumnAssetDurableFallbackM15C(t, reopenedDB)
	if _, err := reopened.ReclaimVectorPartitionGenerationV1(t.Context(), "embedding", 31); err != nil {
		t.Fatalf("retry after fallback advance: %v", err)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old mixed segment remains: %v", err)
	}
	reopenedStore, err := OpenExistingVectorPartitionStoreV1(reopenedDB.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reopenedStore.deleteTombstonePath(reopened.name, "embedding", 31)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed reclaim journal remains: %v", err)
	}
	for _, ref := range reopenedRefs {
		if _, err := readColumnPhysicalAssetFromManager(reopenedDB.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("remapped ref %+v unreadable after physical deletion: %v", ref, err)
		}
	}
	if got, err := reopened.Get([]byte("e1")); err != nil || got == nil {
		t.Fatalf("Get after physical deletion got=%s err=%v", got, err)
	}
}

func TestCollectionVectorPartitionReclaimV1PersistenceFailurePreventsRemapPublish(t *testing.T) {
	_, d, col, store, beforeRefs, _, oldSegmentPath := prepareMixedVectorPartitionReclaimV1(t, 32)
	defer d.Close()
	injected := errors.New("injected reclaim journal persistence failure")
	restore := setVectorPartitionReclaimPersistHooksForTestV1(func(_ string, state vectorPartitionReclaimStateV1) error {
		if len(state.SupersededRefs) != 0 {
			return injected
		}
		return nil
	}, nil)
	defer restore()
	if _, err := col.ReclaimVectorPartitionGenerationV1(t.Context(), "embedding", 32); !errors.Is(err, injected) {
		t.Fatalf("reclaim err=%v, want injected persistence failure", err)
	}
	afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	assertColumnAssetRefsEqualM15C(t, beforeRefs, afterRefs)
	state, err := store.openDeleteTombstone(col.name, "embedding", 32)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.SupersededRefs) != 0 {
		t.Fatalf("failed replacement changed durable reclaim state: %+v", state.SupersededRefs)
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("persistence failure removed mixed segment: %v", err)
	}
}

func TestCollectionVectorPartitionReclaimV1CancellationAfterJournalCommitRetries(t *testing.T) {
	_, d, col, store, beforeRefs, _, _ := prepareMixedVectorPartitionReclaimV1(t, 33)
	defer d.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	restore := setVectorPartitionReclaimPersistHooksForTestV1(nil, func(_ string, state vectorPartitionReclaimStateV1) error {
		if len(state.SupersededRefs) != 0 {
			cancel()
		}
		return nil
	})
	restored := false
	defer func() {
		if !restored {
			restore()
		}
	}()
	if _, err := col.ReclaimVectorPartitionGenerationV1(ctx, "embedding", 33); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reclaim err=%v, want context canceled", err)
	}
	restore()
	restored = true
	assertColumnAssetRefsEqualM15C(t, beforeRefs, columnManifestAssetRefsForCollectionM12A(t, d, col))
	state, err := store.openDeleteTombstone(col.name, "embedding", 33)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.SupersededRefs) != len(beforeRefs) {
		t.Fatalf("canceled reclaim persisted superseded=%d want %d", len(state.SupersededRefs), len(beforeRefs))
	}
	if _, err := col.ReclaimVectorPartitionGenerationV1(t.Context(), "embedding", 33); err == nil || !strings.Contains(err.Error(), "not physically complete") {
		t.Fatalf("retry err=%v, want rewrite success with fallback debt retained", err)
	}
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, columnManifestAssetRefsForCollectionM12A(t, d, col))
}

func TestCollectionVectorPartitionReclaimV1PartialMultiRecordJournalRetries(t *testing.T) {
	_, d, col, store, beforeRefs, firstCandidate, _ := prepareMixedVectorPartitionReclaimV1(t, 34)
	defer d.Close()
	secondCandidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 235, 100)
	if secondCandidate.FileID != firstCandidate.FileID {
		t.Fatalf("second candidate file_id=%d first=%d, want same mixed segment", secondCandidate.FileID, firstCandidate.FileID)
	}
	publishDeletedVectorPartitionReclaimCandidateV1(t, d, col, store, secondCandidate, 35)
	injected := errors.New("injected second reclaim journal failure")
	commits := 0
	restore := setVectorPartitionReclaimPersistHooksForTestV1(func(_ string, state vectorPartitionReclaimStateV1) error {
		if len(state.SupersededRefs) == 0 {
			return nil
		}
		commits++
		if commits == 2 {
			return injected
		}
		return nil
	}, nil)
	restored := false
	defer func() {
		if !restored {
			restore()
		}
	}()
	if _, err := col.ReclaimVectorPartitionGenerationV1(t.Context(), "embedding", 34); !errors.Is(err, injected) {
		t.Fatalf("partial reclaim err=%v, want injected second write failure", err)
	}
	assertColumnAssetRefsEqualM15C(t, beforeRefs, columnManifestAssetRefsForCollectionM12A(t, d, col))
	state34, err := store.openDeleteTombstone(col.name, "embedding", 34)
	if err != nil {
		t.Fatal(err)
	}
	state35, err := store.openDeleteTombstone(col.name, "embedding", 35)
	if err != nil {
		t.Fatal(err)
	}
	if (len(state34.SupersededRefs) == 0) == (len(state35.SupersededRefs) == 0) {
		t.Fatalf("partial write states superseded=(%d,%d), want exactly one committed", len(state34.SupersededRefs), len(state35.SupersededRefs))
	}
	restore()
	restored = true
	if _, err := col.ReclaimVectorPartitionGenerationV1(t.Context(), "embedding", 34); err == nil || !strings.Contains(err.Error(), "not physically complete") {
		t.Fatalf("partial retry err=%v, want rewrite success with fallback debt retained", err)
	}
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, columnManifestAssetRefsForCollectionM12A(t, d, col))
	for _, generation := range []uint64{34, 35} {
		state, err := store.openDeleteTombstone(col.name, "embedding", generation)
		if err != nil {
			t.Fatal(err)
		}
		if len(state.SupersededRefs) != len(beforeRefs) {
			t.Fatalf("retry generation=%d superseded=%d want %d", generation, len(state.SupersededRefs), len(beforeRefs))
		}
	}
}

func TestVectorPartitionReclaimRecordV1ChecksumFailsClosed(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.writeDeleteTombstone(m); err != nil {
		t.Fatal(err)
	}
	p := s.deleteTombstonePath(m.Collection, m.IndexName, m.Generation)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(p, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.openDeleteTombstone(m.Collection, m.IndexName, m.Generation); err == nil {
		t.Fatal("corrupt reclaim record accepted")
	}
}

func TestVectorPartitionReclaimRecordV1MalformedVersionTruncationAndCountFailClosed(t *testing.T) {
	state, err := newVectorPartitionReclaimStateV1(testVectorPartitionManifestV1())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeVectorPartitionReclaimRecordV1(state)
	if err != nil {
		t.Fatal(err)
	}
	clone := func() []byte { return append([]byte(nil), raw...) }
	resign := func(record []byte) {
		sum := sha256.Sum256(record[:len(record)-sha256.Size])
		copy(record[len(record)-sha256.Size:], sum[:])
	}
	originalCountOffset := 12
	originalCountOffset += 4 + int(binary.BigEndian.Uint32(raw[originalCountOffset:]))
	originalCountOffset += 4 + int(binary.BigEndian.Uint32(raw[originalCountOffset:]))
	originalCountOffset += 8
	tests := map[string][]byte{
		"truncated": raw[:len(raw)-1],
		"overflow_length": func() []byte {
			x := clone()
			binary.BigEndian.PutUint32(x[8:12], ^uint32(0))
			return x
		}(),
		"unknown_version": func() []byte {
			x := clone()
			binary.BigEndian.PutUint32(x[4:8], vectorPartitionReclaimVersionV1+1)
			return x
		}(),
		"overflow_count": func() []byte {
			x := clone()
			binary.BigEndian.PutUint32(x[originalCountOffset:], ^uint32(0))
			resign(x)
			return x
		}(),
	}
	for name, malformed := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeVectorPartitionReclaimRecordV1(malformed); !errors.Is(err, ErrVectorPartitionManifestInvalid) {
				t.Fatalf("decode err=%v, want fail-closed invalid record", err)
			}
		})
	}
}

func TestVectorPartitionStoreV1SameGenerationPublicationIsExactByteIdempotent(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	if err := s.publishLocked(m); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	changed := m
	changed.Assets[0].Checksum = strings.Repeat("c", 64)
	changed.Canonicalize()
	if err := s.Publish(changed); err == nil {
		t.Fatal("same generation with different bytes overwrote published manifest")
	}
}

func TestCollectionVectorPartitionManifestV1PublicationSharesMutationBarrier(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	_, graph, view, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexName = def.Name
	m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	_ = view
	refs, resources := appendVectorPartitionStableAssetsV1(t, d, col, 711, m.Generation)
	for i := range m.Assets {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[i])
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		m.Assets[i].Ref, m.Assets[i].Bytes, m.Assets[i].Checksum = refs[i], uint64(refs[i].Length), hex.EncodeToString(sum[:])
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[2])
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	m.RouterAsset.Ref, m.RouterAsset.Bytes, m.RouterAsset.Checksum = refs[2], uint64(refs[2].Length), hex.EncodeToString(sum[:])
	m.Canonicalize()
	held := col.lockMutation()
	done := make(chan error, 1)
	go func() { done <- col.PublishVectorPartitionManifestV1(m, resources) }()
	select {
	case err := <-done:
		t.Fatalf("publish escaped mutation barrier: %v", err)
	default:
	}
	held.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCollectionVectorPartitionBuildingManifestProtectsAssetsFromGC(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	m.IndexName, m.IndexDefinitionDigest = def.Name, VectorIndexDefinitionDigestV1(def)
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	refs, resources := appendVectorPartitionStableAssetsV1(t, d, col, 913, m.Generation)
	resources.Release() // Building publication has no producer authority transfer.
	for i := range m.Assets {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[i])
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		m.Assets[i].Ref, m.Assets[i].Bytes, m.Assets[i].Checksum = refs[i], uint64(refs[i].Length), hex.EncodeToString(sum[:])
	}
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m, nil); err != nil {
		t.Fatalf("publish building: %v", err)
	}
	gc, err := col.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{DryRun: true, CandidateRefs: refs[:2]})
	if err != nil {
		t.Fatal(err)
	}
	if gc.BytesEligible != 0 {
		t.Fatalf("building manifest assets became GC eligible: %+v", gc)
	}
}

func TestCollectionVectorPartitionSameGenerationBuildingPromotionPublishesReady(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	ready := testVectorPartitionManifestV1()
	ready.IndexName, ready.IndexDefinitionDigest = def.Name, VectorIndexDefinitionDigestV1(def)
	ready.SourceGeneration, ready.SourceChecksum, ready.SourceSchemaHash, ready.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	ready, resources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, ready, 914)
	building := ready
	building.State, building.RouterGeneration, building.RouterAsset, building.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	building.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(building, nil); err != nil {
		resources.Release()
		t.Fatalf("publish building: %v", err)
	}
	before, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{})
	if err != nil || before.Sources.PreparedRefs != 2 || before.Sources.PinnedRefs != 0 {
		resources.Release()
		t.Fatalf("building reachability=%+v err=%v", before.Sources, err)
	}
	if err := col.PublishVectorPartitionManifestV1(ready, resources); err != nil {
		t.Fatalf("collection-authorized same-generation promotion: %v", err)
	}
	after, err := col.PlanColumnAssetReachability(t.Context(), ColumnAssetReachabilityOptions{})
	if err != nil || after.Sources.PreparedRefs != 3 || after.Sources.PinnedRefs != 3 {
		t.Fatalf("ready promotion reachability=%+v err=%v", after.Sources, err)
	}
	store, err := OpenExistingVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if active, err := store.OpenActive(ready.Collection, ready.IndexName); err != nil || active.Generation != ready.Generation {
		t.Fatalf("promoted active=%+v err=%v", active, err)
	}
}

func TestCollectionVectorPartitionPublishRejectsReadOnlyWithoutNamespaceTrace(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	ready := testVectorPartitionManifestV1()
	ready.IndexName, ready.IndexDefinitionDigest = def.Name, VectorIndexDefinitionDigestV1(def)
	ready.SourceGeneration, ready.SourceChecksum, ready.SourceSchemaHash, ready.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	ready, resources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, ready, 915)
	building := ready
	building.State, building.RouterGeneration, building.RouterAsset, building.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	building.Canonicalize()
	dir := d.Dir()
	if err := d.Close(); err != nil {
		resources.Release()
		t.Fatal(err)
	}
	defer resources.Release()
	readonly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	readonlyCol, err := NewCollectionManager(readonly).OpenCollection(col.name)
	if err != nil {
		t.Fatal(err)
	}
	namespace := filepath.Join(dir, "vector_partitions")
	if _, err := os.Stat(namespace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected preexisting namespace: %v", err)
	}
	if err := readonlyCol.PublishVectorPartitionManifestV1(building, nil); !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("building read-only publish err=%v", err)
	}
	if _, err := os.Stat(namespace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("building read-only publish left namespace trace: %v", err)
	}
	if err := readonlyCol.PublishVectorPartitionManifestV1(ready, resources); !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("ready read-only publish err=%v", err)
	}
	if resources.Owner() != rootpublication.ResourceOwnerReleased {
		t.Fatalf("ready resources owner=%v want released", resources.Owner())
	}
	if _, err := os.Stat(namespace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ready read-only publish left namespace trace: %v", err)
	}
}

func TestVectorPartitionLifecycleRawStoreRejectsReadOnlyWithoutNamespaceMutation(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexName, m.IndexDefinitionDigest = def.Name, VectorIndexDefinitionDigestV1(def)
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	m, resources := vectorPartitionManifestWithFreshStableAssetsV1(t, d, col, m, 916)
	if err := col.PublishVectorPartitionManifestV1(m, resources); err != nil {
		resources.Release()
		t.Fatal(err)
	}
	dir := d.Dir()
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	readonly, err := backenddb.Open(backenddb.Options{Dir: dir, ReadOnly: true, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	readonlyCol, err := NewCollectionManager(readonly).OpenCollection(col.name)
	if err != nil {
		t.Fatal(err)
	}
	store, err := OpenExistingVectorPartitionStoreV1(readonly.Dir())
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotVectorPartitionNamespaceV1(t, dir)
	if err := store.Deactivate(m.Collection, m.IndexName); !errors.Is(err, ErrVectorPartitionCollectionAuthorityRequired) {
		t.Fatalf("raw read-only deactivate err=%v", err)
	}
	if err := store.Delete(m.Collection, m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); !errors.Is(err, ErrVectorPartitionCollectionAuthorityRequired) {
		t.Fatalf("raw read-only delete err=%v", err)
	}
	operations := 0
	restore := setVectorPartitionBarrierBeforeMutationHookForTestV1(func(string) { operations++ })
	defer restore()
	if err := readonlyCol.DeactivateVectorPartitionV1(m.IndexName); !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("collection read-only deactivate err=%v", err)
	}
	if err := readonlyCol.DeleteVectorPartitionGenerationV1(m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{}); !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("collection read-only delete err=%v", err)
	}
	if _, err := readonlyCol.ReclaimVectorPartitionGenerationV1(t.Context(), m.IndexName, m.Generation); !errors.Is(err, backenddb.ErrReadOnly) {
		t.Fatalf("collection read-only reclaim err=%v", err)
	}
	if operations != 0 {
		t.Fatalf("read-only lifecycle reached storage mutation hook %d times", operations)
	}
	if after := snapshotVectorPartitionNamespaceV1(t, dir); !bytes.Equal(before, after) {
		t.Fatalf("read-only raw lifecycle changed vector-partition namespace: before=%q after=%q", before, after)
	}
}

func snapshotVectorPartitionNamespaceV1(t testing.TB, root string) []byte {
	t.Helper()
	namespace := filepath.Join(root, "vector_partitions")
	var snapshot []byte
	if err := filepath.WalkDir(namespace, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(namespace, path)
		if err != nil {
			return err
		}
		snapshot = append(snapshot, rel...)
		snapshot = append(snapshot, 0)
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		snapshot = append(snapshot, raw...)
		snapshot = append(snapshot, 0)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestCollectionVectorPartitionBuildingPublicationAndGCLinearize(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	newFixture := func(t *testing.T, fileID uint32) (*backenddb.DB, *Collection, VectorPartitionManifestV1, []ColumnAssetRef) {
		_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatal(err)
		}
		if err := col.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
		if err != nil {
			t.Fatal(err)
		}
		m := testVectorPartitionManifestV1()
		refs, resources := appendVectorPartitionStableAssetsV1(t, d, col, fileID, m.Generation)
		resources.Release()
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[0])
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
		m.IndexName, m.IndexDefinitionDigest = def.Name, VectorIndexDefinitionDigestV1(def)
		m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
		m.PartitionCount, m.Placements, m.Assets = 1, []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}}, m.Assets[:1]
		m.Memberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}, {VectorOrdinal: 1, PartitionID: 0}}
		m.Assets[0].Ref, m.Assets[0].Bytes, m.Assets[0].Checksum = refs[0], uint64(refs[0].Length), hex.EncodeToString(sum[:])
		m.Canonicalize()
		return d, col, m, refs
	}
	t.Run("publish_first_retains", func(t *testing.T) {
		d, col, m, refs := newFixture(t, 951)
		defer d.Close()
		if err := col.PublishVectorPartitionManifestV1(m, nil); err != nil {
			t.Fatal(err)
		}
		stats, err := col.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{CandidateRefs: refs})
		if err != nil {
			t.Fatal(err)
		}
		if stats.SegmentsDeleted != 0 {
			t.Fatalf("GC deleted published building asset: %+v", stats)
		}
	})
	t.Run("gc_first_blocks_then_fails_closed", func(t *testing.T) {
		d, col, m, refs := newFixture(t, 952)
		defer d.Close()
		entered, release := make(chan struct{}), make(chan struct{})
		restore := setColumnAssetStableDeleteAfterPlanTestHook(func() { close(entered); <-release })
		defer restore()
		type gcResult struct {
			stats ColumnAssetGCStats
			err   error
		}
		gcDone := make(chan gcResult, 1)
		go func() {
			stats, err := col.ColumnAssetGC(t.Context(), ColumnAssetGCOptions{CandidateRefs: refs})
			gcDone <- gcResult{stats: stats, err: err}
		}()
		select {
		case <-entered:
		case result := <-gcDone:
			t.Fatalf("GC returned before after-plan hook: stats=%+v err=%v", result.stats, result.err)
		case <-time.After(30 * time.Second):
			t.Fatal("GC did not reach after-plan hook")
		}
		rawStore, err := OpenVectorPartitionStoreV1(d.Dir())
		if err != nil {
			t.Fatal(err)
		}
		if err := rawStore.Publish(m); err == nil {
			t.Fatal("raw store publication accepted")
		}
		publishDone := make(chan error, 1)
		go func() { publishDone <- col.PublishVectorPartitionManifestV1(m, nil) }()
		select {
		case err := <-publishDone:
			t.Fatalf("publication escaped GC mutation authority: %v", err)
		default:
		}
		close(release)
		select {
		case result := <-gcDone:
			if result.err != nil {
				t.Fatalf("GC: %v", result.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("GC deadlocked")
		}
		select {
		case err := <-publishDone:
			if err == nil {
				t.Fatal("publication succeeded after GC removed its asset")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("publication deadlocked")
		}
		store, err := OpenExistingVectorPartitionStoreV1(d.Dir())
		if err == nil {
			if _, openErr := store.Open(m.Collection, m.IndexName, m.Generation); !errors.Is(openErr, os.ErrNotExist) {
				t.Fatalf("dangling building manifest open err=%v", openErr)
			}
		}
	})
}

// TestCollectionVectorPartitionDeleteFencesBuildingRetryEndToEnd proves the
// collection-owned building path cannot recreate a generation after Delete has
// durably claimed it.  In particular, the retry traverses source and asset
// validation plus the collection mutation authority before it blocks at the
// store's root storage barrier behind deletion.
func TestCollectionVectorPartitionDeleteFencesBuildingRetryEndToEnd(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	m.IndexName, m.IndexDefinitionDigest = def.Name, VectorIndexDefinitionDigestV1(def)
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	refs, resources := appendVectorPartitionStableAssetsV1(t, d, col, 981, m.Generation)
	resources.Release() // Building publication has no producer authority transfer.
	for i := range m.Assets {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[i])
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		m.Assets[i].Ref, m.Assets[i].Bytes, m.Assets[i].Checksum = refs[i], uint64(refs[i].Length), hex.EncodeToString(sum[:])
	}
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m, nil); err != nil {
		t.Fatalf("publish initial building manifest: %v", err)
	}
	store, err := OpenExistingVectorPartitionStoreV1(d.Dir())
	if err != nil {
		t.Fatal(err)
	}

	entered, release := make(chan struct{}), make(chan struct{})
	restore := setVectorPartitionDeleteAfterTombstoneForTestV1(func() {
		close(entered)
		<-release
	})
	defer restore()
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- col.DeleteVectorPartitionGenerationV1(m.IndexName, m.Generation, VectorPartitionCleanupEligibilityV1{})
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("delete did not durably write its tombstone")
	}

	retryDone := make(chan error, 1)
	go func() { retryDone <- col.PublishVectorPartitionManifestV1(m, nil) }()
	select {
	case err := <-retryDone:
		t.Fatalf("collection building retry escaped delete storage barrier: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatalf("delete: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delete deadlocked")
	}
	select {
	case err := <-retryDone:
		if err == nil || !strings.Contains(err.Error(), "deleting") {
			t.Fatalf("collection building retry err=%v, want deleting tombstone rejection", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("collection building retry deadlocked")
	}
	if _, err := store.Open(m.Collection, m.IndexName, m.Generation); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted generation relinked by building retry: open err=%v", err)
	}
}

func TestVectorPartitionManifestV1CanonicalRoundTrip(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
	if err != nil {
		t.Fatal(err)
	}
	if got.ReadySetDigest == "" || got.Placements[0].PartitionID != 0 {
		t.Fatalf("bad round trip: %+v", got)
	}
}

func TestVectorPartitionStoreV1CanonicalReopen(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	m := testVectorPartitionManifestV1()
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.publishLocked(m); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open("docs", "embedding", 7); err != nil {
		t.Fatal(err)
	}
}

func TestVectorPartitionStoreV1OpenBindsDecodedIdentityAndBoundsPointers(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexName = "other"
	m.Canonicalize()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	spoofed := filepath.Join(s.dir, fmt.Sprintf("%s-%s-%d.vpm", safeVPM("docs"), safeVPM("embedding"), m.Generation))
	if err := os.WriteFile(spoofed, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open("docs", "embedding", m.Generation); err == nil {
		t.Fatal("stored manifest identity mismatch accepted")
	}
	if err := os.WriteFile(filepath.Join(s.dir, safeVPM("docs")+"-"+safeVPM("embedding")+".active"), []byte(strings.Repeat("1", 33)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenActive("docs", "embedding"); err == nil {
		t.Fatal("oversized active pointer accepted")
	}
}

func TestVectorPartitionManifestJSONV1RejectsUnknownAndTrailing(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestJSONV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVectorPartitionManifestJSONV1(raw, DefaultVectorPartitionManifestLimits()); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVectorPartitionManifestJSONV1(append(raw, []byte(` {}`)...), DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	if _, err := DecodeVectorPartitionManifestJSONV1([]byte(`{"unknown":1}`), DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("unknown JSON field accepted")
	}
}

func TestVectorPartitionManifestV1RejectsTrailingAndMixedReadySet(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeVectorPartitionManifestV1(append(raw, 0), DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("trailing accepted")
	}
	m.ReadySetDigest = strings.Repeat("0", 64)
	if _, err := EncodeVectorPartitionManifestV1(m); err != nil {
		t.Fatal(err)
	} // encoder canonicalizes the digest
	m.Canonicalize()
	m.Placements[0].GroupID = "other"
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("mixed ready set accepted")
	}
}

func TestVectorPartitionManifestV1DecodeCapsBeforeAllocation(t *testing.T) {
	m := testVectorPartitionManifestV1()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultVectorPartitionManifestLimits()
	limits.MaxBytes = len(raw) - 1
	if _, err := DecodeVectorPartitionManifestV1(raw, limits); err == nil {
		t.Fatal("byte cap accepted")
	}
}

func TestVectorPartitionManifestV1RejectsUntypedAssetAuthority(t *testing.T) {
	m := testVectorPartitionManifestV1()
	m.Assets[0].Ref = ColumnAssetRef{}
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("untyped asset reference accepted")
	}
}

func TestVectorPartitionManifestV1IntegrityRejectsSemanticMutation(t *testing.T) {
	base := testVectorPartitionManifestV1()
	for name, mutate := range map[string]func(*VectorPartitionManifestV1){
		"identity":  func(m *VectorPartitionManifestV1) { m.SourceChecksum++ },
		"placement": func(m *VectorPartitionManifestV1) { m.Placements[0].GroupID = "raft-other" },
		"membership": func(m *VectorPartitionManifestV1) {
			m.OverlapMemberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 1}}
		},
		"representative": func(m *VectorPartitionManifestV1) {
			m.Representatives = []VectorPartitionMembershipV1{{VectorOrdinal: 1, PartitionID: 1}}
		},
		"policy":     func(m *VectorPartitionManifestV1) { m.BalancePolicy = "other" },
		"generation": func(m *VectorPartitionManifestV1) { m.Generation++ },
		"asset":      func(m *VectorPartitionManifestV1) { m.Assets[0].Checksum = strings.Repeat("c", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			m := base
			m.Placements = append([]VectorPartitionPlacementV1(nil), base.Placements...)
			m.Memberships = append([]VectorPartitionMembershipV1(nil), base.Memberships...)
			m.Assets = append([]VectorPartitionAssetV1(nil), base.Assets...)
			mutate(&m) // preserve the old integrity digest intentionally.
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeVectorPartitionManifestJSONV1(raw, DefaultVectorPartitionManifestLimits()); err == nil {
				t.Fatal("semantic mutation accepted with stale record integrity digest")
			}
		})
	}
}

func TestVectorPartitionManifestV1StrictMalformedJSONAndBinaryMatrix(t *testing.T) {
	base := testVectorPartitionManifestV1()
	jsonCases := map[string]func(*VectorPartitionManifestV1){
		"membership_ordinal_out_of_range": func(m *VectorPartitionManifestV1) { m.Memberships[1].VectorOrdinal = m.SourceRowCount },
		"membership_unknown_partition":    func(m *VectorPartitionManifestV1) { m.Memberships[1].PartitionID = m.PartitionCount },
		"membership_noncanonical": func(m *VectorPartitionManifestV1) {
			m.Memberships[0], m.Memberships[1] = m.Memberships[1], m.Memberships[0]
		},
		"router_generation":        func(m *VectorPartitionManifestV1) { m.RouterGeneration++ },
		"router_missing_reference": func(m *VectorPartitionManifestV1) { m.RouterAsset.Ref = ColumnAssetRef{} },
		"asset_sha":                func(m *VectorPartitionManifestV1) { m.Assets[0].Checksum = "not-a-sha256" },
		"asset_crc_semantic":       func(m *VectorPartitionManifestV1) { m.Assets[0].Ref.Checksum++ },
	}
	for name, mutate := range jsonCases {
		t.Run("json/"+name, func(t *testing.T) {
			m := base
			m.Placements = append([]VectorPartitionPlacementV1(nil), base.Placements...)
			m.Memberships = append([]VectorPartitionMembershipV1(nil), base.Memberships...)
			m.Assets = append([]VectorPartitionAssetV1(nil), base.Assets...)
			mutate(&m) // Keep the certified digest, as an untrusted JSON record would.
			raw, err := json.Marshal(m)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeVectorPartitionManifestJSONV1(raw, DefaultVectorPartitionManifestLimits()); err == nil {
				t.Fatal("malformed JSON manifest accepted")
			}
		})
	}
	raw, err := EncodeVectorPartitionManifestV1(base)
	if err != nil {
		t.Fatal(err)
	}
	unknownVersion := append([]byte(nil), raw...)
	binary.BigEndian.PutUint32(unknownVersion[4:8], 99)
	if _, err := DecodeVectorPartitionManifestV1(unknownVersion, DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("unknown binary version accepted")
	}
	if _, err := DecodeVectorPartitionManifestV1(append(raw, []byte("trailing")...), DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("binary trailing data accepted")
	}
}

func TestVectorPartitionManifestV1TotalMembershipCap(t *testing.T) {
	m := testVectorPartitionManifestV1()
	m.Representatives = nil
	m.Canonicalize()
	limits := DefaultVectorPartitionManifestLimits()
	limits.MaxMemberships = len(m.Memberships)
	if err := m.Validate(limits); err != nil {
		t.Fatalf("exact total membership cap: %v", err)
	}
	m.OverlapMemberships = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 1}}
	m.Canonicalize()
	if err := m.Validate(limits); err == nil {
		t.Fatal("split membership cap overflow accepted")
	}
}

func TestVectorPartitionManifestV1DefaultLimitsSupportMillionRowsAndOverlap(t *testing.T) {
	m := testVectorPartitionManifestV1()
	m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	m.PartitionCount = 2
	m.Placements = []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}, {PartitionID: 1, GroupID: "raft-a"}}
	m.SourceRowCount = 1_000_000
	m.Memberships = make([]VectorPartitionMembershipV1, m.SourceRowCount)
	for i := range m.Memberships {
		m.Memberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 0}
	}
	m.OverlapMemberships = make([]VectorPartitionMembershipV1, 200_000)
	for i := range m.OverlapMemberships {
		m.OverlapMemberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 1}
	}
	m.Representatives = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}}
	m.Canonicalize()
	raw, err := EncodeVectorPartitionManifestV1(m)
	if err != nil {
		t.Fatalf("encode 1M rows + 20%% overlap: %v", err)
	}
	got, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
	if err != nil {
		t.Fatalf("decode 1M rows + 20%% overlap: %v", err)
	}
	if got.SourceRowCount != 1_000_000 || len(got.OverlapMemberships) != 200_000 || len(got.Representatives) != 1 {
		t.Fatalf("decoded aggregate limits wrong: rows=%d overlap=%d representatives=%d", got.SourceRowCount, len(got.OverlapMemberships), len(got.Representatives))
	}
	legacy := DefaultVectorPartitionManifestLimits()
	legacy.MaxSourceRows, legacy.MaxTotalMemberships, legacy.MaxMemberships = 0, 0, 1<<20
	if err := got.Validate(legacy); err == nil {
		t.Fatal("legacy MaxMemberships aggregate cap accepted overlapping manifest")
	}
}

func TestVectorPartitionManifestV1RejectsNonCanonicalRouterPartitionAndDigest(t *testing.T) {
	m := testVectorPartitionManifestV1()
	m.RouterAsset.PartitionID = 1
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("nonzero router partition accepted")
	}
	m = testVectorPartitionManifestV1()
	m.ReadySetDigest = m.readyDigest()
	m.RouterAsset.PartitionID = 1
	if m.ReadySetDigest == m.readyDigest() {
		t.Fatal("ready digest does not distinguish router partition identity")
	}
}

func TestVectorPartitionManifestV1RejectsRouterAssetFromForeignGeneration(t *testing.T) {
	m := testVectorPartitionManifestV1()
	m.RouterAsset.Ref.Generation = m.Generation + 1
	m.Canonicalize()
	if err := m.Validate(DefaultVectorPartitionManifestLimits()); err == nil {
		t.Fatal("router asset from a foreign generation accepted")
	}
}

func TestVectorPartitionStoreV1RejectsRawPublicationAuthority(t *testing.T) {
	requireVectorPartitionPersistenceV1(t)
	s, err := OpenVectorPartitionStoreV1(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ready := testVectorPartitionManifestV1()
	building := ready
	building.State, building.RouterGeneration, building.RouterAsset, building.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	building.Canonicalize()
	for _, m := range []VectorPartitionManifestV1{ready, building} {
		if err := s.Publish(m); err == nil {
			t.Fatal("raw publication bypass accepted")
		}
	}
}

func testVectorPartitionManifestV1() VectorPartitionManifestV1 {
	h := strings.Repeat("a", 64)
	b := strings.Repeat("b", 64)
	ref := func(partID uint64, fileID uint32, bytes int64) ColumnAssetRef {
		return ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: "test", Generation: 7, PartID: partID, FileID: fileID, Length: bytes}
	}
	m := VectorPartitionManifestV1{State: "ready", Collection: "docs", IndexName: "embedding", IndexDefinitionDigest: h, SourceGeneration: 4, SourceChecksum: 9, SourceSchemaHash: 11, SourceRowCount: 2, Generation: 7, RouterGeneration: 7, PartitionCount: 2, BalancePolicy: "disjoint_v1", Placements: []VectorPartitionPlacementV1{{0, "raft-a"}, {1, "raft-a"}}, Memberships: []VectorPartitionMembershipV1{{0, 0}, {1, 1}}, Representatives: []VectorPartitionMembershipV1{{0, 0}}, Assets: []VectorPartitionAssetV1{{ID: "partition/0", PartitionID: 0, Checksum: b, Bytes: 12, Ref: ref(1, 1, 12)}, {ID: "partition/1", PartitionID: 1, Checksum: b, Bytes: 13, Ref: ref(2, 2, 13)}}, RouterAsset: VectorPartitionAssetV1{ID: "router", Checksum: b, Bytes: 14, Ref: ref(3, 3, 14)}}
	m.Canonicalize()
	return m
}

func scaledVectorPartitionManifestV1(rows int) VectorPartitionManifestV1 {
	m := testVectorPartitionManifestV1()
	m.SourceRowCount = uint64(rows)
	m.PartitionCount = 1
	m.Placements = []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}}
	m.Memberships = make([]VectorPartitionMembershipV1, rows)
	for i := range m.Memberships {
		m.Memberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 0}
	}
	m.Representatives = []VectorPartitionMembershipV1{{VectorOrdinal: 0, PartitionID: 0}}
	m.Assets = m.Assets[:1]
	m.Canonicalize()
	return m
}

func TestVectorPartitionManifestContextDigestMatchesStableJSONV1(t *testing.T) {
	if err := validateVectorPartitionManifestIntegrityShapeV1(); err != nil {
		t.Fatal(err)
	}
	for _, m := range []VectorPartitionManifestV1{
		testVectorPartitionManifestV1(),
		{
			Format:                "<format>&\u2028",
			State:                 "building",
			Collection:            "docs",
			IndexName:             "embedding",
			IndexDefinitionDigest: strings.Repeat("a", 64),
			IntegrityDigest:       strings.Repeat("b", 64),
			SourceGeneration:      1,
			SourceChecksum:        2,
			SourceSchemaHash:      3,
			SourceRowCount:        4,
			Generation:            5,
			PartitionCount:        6,
			BalancePolicy:         "test",
			Placements:            nil,
			Memberships:           []VectorPartitionMembershipV1{},
			OverlapMemberships:    nil,
			Representatives:       []VectorPartitionMembershipV1{},
			Assets:                nil,
			RouterAsset:           VectorPartitionAssetV1{ID: "<router>&", Ref: ColumnAssetRef{Kind: ColumnAssetKind("<kind>"), Offset: -1}},
			ReadySetDigest:        "",
		},
	} {
		m.IntegrityDigest = ""
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		want := hex.EncodeToString(sum[:])
		got, err := m.integrityDigestWithContextV1(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("context digest=%s want stable JSON digest=%s\njson=%s", got, want, raw)
		}
	}
}

func TestEncodeVectorPartitionManifestWithContextV1CancelsDuringLargeCanonicalSortScan(t *testing.T) {
	m := scaledVectorPartitionManifestV1(32 << 10)
	ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 24}
	if _, err := encodeVectorPartitionManifestWithContextV1(ctx, m); !errors.Is(err, context.Canceled) {
		t.Fatalf("encode cancellation err=%v want context.Canceled", err)
	}
	if ctx.calls < ctx.cancelAfter {
		t.Fatalf("context calls=%d want at least %d", ctx.calls, ctx.cancelAfter)
	}
}

func TestSortVectorPartitionSliceWithContextV1CancelsBeforeScratchAllocation(t *testing.T) {
	values := []int{2, 1}
	ctx := &cancelAfterErrContextV1{Context: context.Background(), cancelAfter: 2}
	if err := sortVectorPartitionSliceWithContextV1(ctx, values, func(a, b int) bool { return a < b }); !errors.Is(err, context.Canceled) {
		t.Fatalf("sort cancellation err=%v want context.Canceled", err)
	}
	if !reflect.DeepEqual(values, []int{2, 1}) {
		t.Fatalf("canceled pre-allocation sort mutated values=%v", values)
	}
}

func BenchmarkVectorPartitionManifestV1Scale(b *testing.B) {
	for _, rows := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			m := testVectorPartitionManifestV1()
			m.SourceRowCount = uint64(rows)
			m.PartitionCount = 1
			m.Placements = []VectorPartitionPlacementV1{{PartitionID: 0, GroupID: "raft-a"}}
			m.Memberships = make([]VectorPartitionMembershipV1, rows)
			for i := range m.Memberships {
				m.Memberships[i] = VectorPartitionMembershipV1{VectorOrdinal: uint64(i), PartitionID: 0}
			}
			m.Assets = m.Assets[:1]
			m.Canonicalize()
			raw, err := EncodeVectorPartitionManifestV1(m)
			if err != nil {
				b.Fatal(err)
			}
			if rows == 1_000_000 && len(raw) > rows*64 {
				b.Fatalf("encoded disjoint metadata=%d bytes (%0.2f/vector), exceeds 64 B/vector gate", len(raw), float64(len(raw))/float64(rows))
			}
			b.ReportMetric(float64(len(raw))/float64(rows), "metadata-bytes/vector")
			b.Run("decode_validate", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					got, err := DecodeVectorPartitionManifestV1(raw, DefaultVectorPartitionManifestLimits())
					if err != nil || got.SourceRowCount != uint64(rows) {
						b.Fatalf("decode: %v", err)
					}
				}
			})
			b.Run("encode", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := EncodeVectorPartitionManifestV1(m); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkVectorPartitionStoreV1WarmOpen(b *testing.B) {
	requireVectorPartitionPersistenceV1(b)
	s, err := OpenVectorPartitionStoreV1(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.State, m.RouterGeneration, m.RouterAsset, m.ReadySetDigest = "building", 0, VectorPartitionAssetV1{}, ""
	m.Canonicalize()
	if err := s.publishLocked(m); err != nil {
		b.Fatal(err)
	}
	b.Run("open", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := s.Open(m.Collection, m.IndexName, m.Generation); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkVectorPartitionStatusV1Warm measures the public status path after a
// real ready publication.  It deliberately uses producer-issued stable
// resources so this does not benchmark an authority-bypassing fixture.
func BenchmarkVectorPartitionStatusV1Warm(b *testing.B) {
	requireVectorPartitionPersistenceV1(b)
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(b, 3, 2, []columnGraphRebuildInputRowV2A{{id: "a", vector: []float32{1, 0, 0}}, {id: "b", vector: []float32{0, 1, 0}}})
	defer d.Close()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		b.Fatal(err)
	}
	m := testVectorPartitionManifestV1()
	m.IndexName = def.Name
	m.IndexDefinitionDigest = VectorIndexDefinitionDigestV1(def)
	_, graph, _, err := col.columnVectorGraphPhysicalRowReaderSnapshotView(def.Name)
	if err != nil {
		b.Fatal(err)
	}
	m.SourceGeneration, m.SourceChecksum, m.SourceSchemaHash, m.SourceRowCount = graph.BaseManifestGeneration, graph.BaseManifestChecksum, graph.BaseSchemaHash, uint64(graph.RowCount)
	refs, resources := appendVectorPartitionStableAssetsV1(b, d, col, 702, m.Generation)
	for i := range m.Assets {
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[i])
		if err != nil {
			b.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		m.Assets[i].Ref, m.Assets[i].Bytes, m.Assets[i].Checksum = refs[i], uint64(refs[i].Length), hex.EncodeToString(sum[:])
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), refs[2])
	if err != nil {
		b.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	m.RouterAsset.Ref, m.RouterAsset.Bytes, m.RouterAsset.Checksum = refs[2], uint64(refs[2].Length), hex.EncodeToString(sum[:])
	m.Canonicalize()
	if err := col.PublishVectorPartitionManifestV1(m, resources); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.VectorPartitionStatusV1(def.Name, m.Generation); err != nil {
			b.Fatal(err)
		}
	}
}
