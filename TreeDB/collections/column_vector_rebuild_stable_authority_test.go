package collections

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

type columnVectorStableLogicalRefKey struct {
	kind       string
	namespace  string
	generation uint64
	partID     uint64
	fileID     uint64
	offset     int64
	length     int64
	checksum   uint32
}

func TestNextColumnPhysicalAssetSegmentAppenderWithStableResourcesRetainsFreshExclusiveAuthority(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("vector-stable-fresh", cfg)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()

	first, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(root, *normalized, registry)
	if err != nil {
		t.Fatalf("new first stable fresh appender: %v", err)
	}
	refs, err := first.appendKinds([]columnPhysicalAssetAppendItem{
		{payload: []byte("adjacency-layer-0"), kind: ColumnAssetKindTCS1TypedColumnPart, generation: 7, partID: 11},
		{payload: []byte("adjacency-layer-1"), kind: ColumnAssetKindTCS1TypedColumnPart, generation: 7, partID: 12},
	})
	if err != nil {
		_ = first.abort()
		t.Fatal(err)
	}
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	resources := first.stableResources
	first.stableResources = nil
	if resources == nil {
		t.Fatal("fresh stable appender returned no resource authority")
	}
	defer resources.Release()
	if first.closeStats.FileSyncCount != 1 {
		t.Fatalf("content syncs=%d want 1", first.closeStats.FileSyncCount)
	}
	if got := len(resources.Descriptors()); got != 1 {
		t.Fatalf("physical obligations=%d want 1 for two same-kind refs", got)
	}
	if got := len(resources.Descriptors()[0].LogicalObligations()); got != len(refs) {
		t.Fatalf("logical obligations=%d want refs=%d", got, len(refs))
	}
	var namespaceSyncs uint64
	for _, stats := range resources.Stats(time.Now()) {
		namespaceSyncs += stats.NamespaceSyncs
	}
	if namespaceSyncs != 1 {
		t.Fatalf("namespace syncs=%d want 1", namespaceSyncs)
	}
	if got := registry.ActivePins(); got != 1 {
		t.Fatalf("active pins before publication=%d want 1", got)
	}

	conflictFileID := first.fileID + 1
	conflictPath, err := columnAssetSegmentPath(root, ColumnAssetRef{
		Namespace: normalized.AssetManager.Namespace,
		FileID:    conflictFileID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(conflictPath, []byte("reserved-by-other-writer"), 0o600); err != nil {
		t.Fatal(err)
	}
	second, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(root, *normalized, registry)
	if err != nil {
		t.Fatalf("new second stable fresh appender after collision: %v", err)
	}
	defer func() { _ = second.abort() }()
	if second.fileID <= conflictFileID {
		t.Fatalf("stable fresh allocator reused collision file_id=%d want > %d", second.fileID, conflictFileID)
	}
	gotConflict, err := os.ReadFile(conflictPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotConflict) != "reserved-by-other-writer" {
		t.Fatalf("collision payload=%q want untouched", gotConflict)
	}
}

func TestNextColumnPhysicalAssetSegmentAppenderWithStableResourcesRejectsUnlinkBeforeObserve(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("vector-stable-pre-observe", cfg)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	unlinked := false
	restore := setColumnAssetStableBeforeObserveTestHook(func(parent, _ *os.File, name string) {
		if err := rootpublication.RemoveStableChildFile(parent, name); err != nil {
			t.Fatal(err)
		}
		unlinked = true
	})
	defer restore()
	appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(root, *normalized, registry)
	if !errors.Is(err, rootpublication.ErrResourceConflict) {
		if appender != nil {
			_ = appender.abort()
		}
		t.Fatalf("pre-observe unlink error=%v want ErrResourceConflict", err)
	}
	if !unlinked || appender != nil {
		t.Fatalf("pre-observe unlink=%t appender=%v want unlinked/no appender", unlinked, appender)
	}
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("active pins after rejected construction=%d want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("active identities after rejected construction=%d want 0", got)
	}
}

func TestReplaceColumnVectorGraphPreparedPhysicalAssetReleasesSupersededAuthority(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("vector-stable-replace", cfg)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	prepare := func(payload string, partID uint64) columnVectorGraphPreparedPhysicalAsset {
		appender, err := newNextColumnPhysicalAssetSegmentAppenderWithStableResources(root, *normalized, registry)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := appender.appendKinds([]columnPhysicalAssetAppendItem{{
			payload: []byte(payload), kind: ColumnAssetKindTCS1TypedColumnPart, generation: 7, partID: partID,
		}}); err != nil {
			_ = appender.abort()
			t.Fatal(err)
		}
		if err := appender.close(); err != nil {
			t.Fatal(err)
		}
		resources := appender.stableResources
		appender.stableResources = nil
		if resources == nil {
			t.Fatal("closed stable appender returned no resources")
		}
		return columnVectorGraphPreparedPhysicalAsset{stableResources: resources}
	}

	current := prepare("first", 1)
	next := prepare("next", 2)
	if got := registry.ActivePins(); got != 2 {
		t.Fatalf("active pins before replacement=%d want 2", got)
	}
	replaceColumnVectorGraphPreparedPhysicalAsset(&current, next)
	if current.stableResources != next.stableResources {
		t.Fatal("replacement did not retain next authority")
	}
	if got := registry.ActivePins(); got != 1 {
		t.Fatalf("active pins after replacement=%d want 1", got)
	}
	current.releaseStableResources()
	if got := registry.ActivePins(); got != 0 {
		t.Fatalf("active pins after final release=%d want 0", got)
	}
	if got := registry.ActiveIdentities(); got != 0 {
		t.Fatalf("active identities after final release=%d want 0", got)
	}
}

func TestColumnVectorGraphRebuildStableAuthorityMatchesEveryPublishedAsset(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0}},
	}
	_, d, col, def := openColumnGraphQuantizedGuardrailTestCollection1926(t, rows)
	defer func() { _ = d.Close() }()
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	hookCalls := 0
	publishHookCalls := 0
	var frozenResources *rootpublication.StableResourceSet
	wantPublishedSegments := 0
	restore := setColumnVectorGraphStableAuthorityTestHook(func(resources *rootpublication.StableResourceSet, assets []columnVectorIndexStateAssetSnapshot) error {
		hookCalls++
		frozenResources = resources
		if resources == nil {
			return fmt.Errorf("rebuild prepared no stable resources")
		}
		want := make(map[columnVectorStableLogicalRefKey]int, len(assets))
		uniqueSegments := make(map[uint32]struct{})
		for _, asset := range assets {
			ref := asset.Ref
			key := columnVectorStableLogicalRefKey{
				kind: string(ref.Kind), namespace: ref.Namespace, generation: ref.Generation,
				partID: ref.PartID, fileID: uint64(ref.FileID), offset: ref.Offset,
				length: ref.Length, checksum: ref.Checksum,
			}
			want[key]++
			uniqueSegments[ref.FileID] = struct{}{}
		}
		got := make(map[columnVectorStableLogicalRefKey]int, len(assets))
		for _, descriptor := range resources.Descriptors() {
			fields := descriptor.ReachabilityFields()
			if len(fields) != 1 || fields[0] != rootpublication.ReachabilityVectorGraphPack {
				return fmt.Errorf("vector descriptor reachability=%v want [%s]", fields, rootpublication.ReachabilityVectorGraphPack)
			}
			for _, logical := range descriptor.LogicalObligations() {
				key := columnVectorStableLogicalRefKey{
					kind: logical.Kind, namespace: logical.Namespace, generation: logical.Generation,
					partID: logical.PartID, fileID: logical.FileID, offset: logical.Offset,
					length: logical.Length, checksum: logical.Checksum,
				}
				got[key]++
			}
		}
		if !reflect.DeepEqual(got, want) {
			return fmt.Errorf("stable logical union=%v want published assets=%v", got, want)
		}
		if len(resources.Descriptors()) != len(uniqueSegments) {
			return fmt.Errorf("physical obligations=%d want fresh segments=%d", len(resources.Descriptors()), len(uniqueSegments))
		}
		wantPublishedSegments = len(uniqueSegments)
		if active := registry.ActivePins(); active != baselinePins+uint64(len(uniqueSegments)) {
			return fmt.Errorf("active pins during publish=%d want baseline+segments=%d", active, baselinePins+uint64(len(uniqueSegments)))
		}
		return nil
	})
	defer restore()
	restorePublish := setColumnVectorGraphStablePublishTestHook(func(prepared *columnVectorGraphPreparedPhysicalAsset) error {
		publishHookCalls++
		resources := prepared.stableResources
		if resources == nil || resources != frozenResources {
			return fmt.Errorf("publication resources=%p want frozen=%p", resources, frozenResources)
		}
		if prepared.stableSegments != uint64(wantPublishedSegments) || prepared.stableContentSyncs != prepared.stableSegments || prepared.stableNamespaceSyncs != prepared.stableSegments {
			return fmt.Errorf("publication stable counters segments=%d content_syncs=%d namespace_syncs=%d want all=%d", prepared.stableSegments, prepared.stableContentSyncs, prepared.stableNamespaceSyncs, wantPublishedSegments)
		}
		var pinHighWater uint64
		for _, stats := range resources.Stats(time.Now()) {
			pinHighWater += stats.PinHighWater
		}
		if pinHighWater != prepared.stableSegments {
			return fmt.Errorf("publication pin high-water=%d want segments=%d", pinHighWater, prepared.stableSegments)
		}
		t.Logf("stable vector publication: segments=%d content_syncs=%d namespace_syncs=%d pin_high_water=%d", prepared.stableSegments, prepared.stableContentSyncs, prepared.stableNamespaceSyncs, pinHighWater)
		if active := registry.ActivePins(); active != baselinePins+uint64(wantPublishedSegments) {
			return fmt.Errorf("active pins during system publication=%d want baseline+segments=%d", active, baselinePins+uint64(wantPublishedSegments))
		}
		return nil
	})
	defer restorePublish()

	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	if hookCalls != 1 {
		t.Fatalf("stable authority hook calls=%d want 1", hookCalls)
	}
	if publishHookCalls != 1 {
		t.Fatalf("stable publication hook calls=%d want 1", publishHookCalls)
	}
	// The visible closure and each independently recoverable slot may retain
	// duplicate pins for the same identity. Require every newly published
	// segment to remain pinned without coupling the assertion to that ownership
	// multiplicity.
	if got, wantMin := registry.ActivePins(), baselinePins+uint64(wantPublishedSegments); got < wantMin {
		t.Fatalf("active pins after publish=%d want at least baseline+new segments=%d", got, wantMin)
	}
	if got, want := registry.ActiveIdentities(), baselineIdentities+wantPublishedSegments; got != want {
		t.Fatalf("active identities after publish=%d want baseline+new segments=%d", got, want)
	}
}

func TestColumnVectorGraphRebuildStableAuthorityIncludesCalibratedScalarU8Alpha(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0}},
	}
	q := scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha")
	_, d, col, def := openColumnGraphQuantizedTestCollection1926(t, rows, []QuantizedVectorIndexDefinition{q})
	defer func() { _ = d.Close() }()
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	const wantSegments = 7
	hookCalls := 0
	publishHookCalls := 0
	restore := setColumnVectorGraphStableAuthorityTestHook(func(resources *rootpublication.StableResourceSet, assets []columnVectorIndexStateAssetSnapshot) error {
		hookCalls++
		if resources == nil {
			return fmt.Errorf("rebuild prepared no stable resources")
		}
		wantQuantized := make(map[columnVectorStableLogicalRefKey]int, 2)
		roles := make(map[string]int, 2)
		for _, asset := range assets {
			if asset.Role != columnVectorIndexStateAssetRoleQuantizedCodes && asset.Role != columnVectorIndexStateAssetRoleQuantizedAlpha {
				continue
			}
			roles[asset.Role]++
			ref := asset.Ref
			wantQuantized[columnVectorStableLogicalRefKey{
				kind: string(ref.Kind), namespace: ref.Namespace, generation: ref.Generation,
				partID: ref.PartID, fileID: uint64(ref.FileID), offset: ref.Offset,
				length: ref.Length, checksum: ref.Checksum,
			}]++
		}
		if roles[columnVectorIndexStateAssetRoleQuantizedCodes] != 1 || roles[columnVectorIndexStateAssetRoleQuantizedAlpha] != 1 {
			return fmt.Errorf("quantized stable roles=%v want one codes and one alpha", roles)
		}
		gotQuantized := make(map[columnVectorStableLogicalRefKey]int, 2)
		for _, descriptor := range resources.Descriptors() {
			for _, logical := range descriptor.LogicalObligations() {
				key := columnVectorStableLogicalRefKey{
					kind: logical.Kind, namespace: logical.Namespace, generation: logical.Generation,
					partID: logical.PartID, fileID: logical.FileID, offset: logical.Offset,
					length: logical.Length, checksum: logical.Checksum,
				}
				if wantQuantized[key] != 0 {
					gotQuantized[key]++
				}
			}
		}
		if !reflect.DeepEqual(gotQuantized, wantQuantized) {
			return fmt.Errorf("stable quantized union=%v want codes+alpha=%v", gotQuantized, wantQuantized)
		}
		if got := len(resources.Descriptors()); got != wantSegments {
			return fmt.Errorf("physical obligations=%d want %d with separate codes and alpha segments", got, wantSegments)
		}
		if active := registry.ActivePins(); active != baselinePins+wantSegments {
			return fmt.Errorf("active pins during publish=%d want baseline+segments=%d", active, baselinePins+wantSegments)
		}
		return nil
	})
	defer restore()
	restorePublish := setColumnVectorGraphStablePublishTestHook(func(prepared *columnVectorGraphPreparedPhysicalAsset) error {
		publishHookCalls++
		if prepared.stableSegments != wantSegments || prepared.stableContentSyncs != wantSegments || prepared.stableNamespaceSyncs != wantSegments {
			return fmt.Errorf("publication stable counters segments=%d content_syncs=%d namespace_syncs=%d want all=%d", prepared.stableSegments, prepared.stableContentSyncs, prepared.stableNamespaceSyncs, wantSegments)
		}
		var pinHighWater uint64
		for _, stats := range prepared.stableResources.Stats(time.Now()) {
			pinHighWater += stats.PinHighWater
		}
		if pinHighWater != wantSegments {
			return fmt.Errorf("publication pin high-water=%d want segments=%d", pinHighWater, wantSegments)
		}
		t.Logf("stable calibrated scalar-u8 publication: segments=%d content_syncs=%d namespace_syncs=%d pin_high_water=%d", prepared.stableSegments, prepared.stableContentSyncs, prepared.stableNamespaceSyncs, pinHighWater)
		return nil
	})
	defer restorePublish()

	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	if hookCalls != 1 || publishHookCalls != 1 {
		t.Fatalf("stable authority hook calls=%d publication hook calls=%d want 1 each", hookCalls, publishHookCalls)
	}
	if got, wantMin := registry.ActivePins(), baselinePins+wantSegments; got < wantMin {
		t.Fatalf("active pins after publish=%d want at least baseline+new segments=%d", got, wantMin)
	}
	if got, want := registry.ActiveIdentities(), baselineIdentities+wantSegments; got != want {
		t.Fatalf("active identities after publish=%d want baseline+new segments=%d", got, want)
	}
}

func TestColumnVectorGraphRebuildStableAuthorityHookFailureReleasesPins(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphTypedColumnVectorTestCollection1782(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	registry := d.StableResourceIdentityPinRegistry()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	injected := errors.New("injected vector stable authority failure")
	restore := setColumnVectorGraphStableAuthorityTestHook(func(resources *rootpublication.StableResourceSet, assets []columnVectorIndexStateAssetSnapshot) error {
		if resources == nil || len(assets) == 0 || registry.ActivePins() <= baselinePins {
			t.Fatalf("hook resources=%v assets=%d pins=%d baseline=%d", resources, len(assets), registry.ActivePins(), baselinePins)
		}
		return injected
	})
	defer restore()
	status, err := col.RebuildVectorIndex(def.Name)
	if !errors.Is(err, injected) {
		t.Fatalf("RebuildVectorIndex error=%v want injected", err)
	}
	if !reflect.DeepEqual(status, VectorIndexStatus{}) {
		t.Fatalf("RebuildVectorIndex status=%+v want empty on failed build", status)
	}
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active pins after failed preparation=%d want baseline=%d", got, baselinePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active identities after failed preparation=%d want baseline=%d", got, baselineIdentities)
	}
}

func TestColumnVectorGraphStableAuthorityRejectsEachMissingTransitiveChild(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable vector authority requires exact relative namespace support")
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, -1}},
		{id: "doc-b", vector: []float32{0.5, -0.5, 0.25}},
		{id: "doc-c", vector: []float32{-0.25, 0.75, 0}},
	}

	// First obtain the complete closure from the public producer API. The hook
	// observes the exact state.Assets list produced by the same preparation; it
	// does not construct substitute refs or snapshots.
	openFixture := func(t testing.TB) (*backenddb.DB, *Collection, VectorIndexDefinition) {
		t.Helper()
		_, d, collection, def := openColumnGraphQuantizedTestCollection1926(t, rows, []QuantizedVectorIndexDefinition{
			scalarU8AlphaQuantizedIndex2843("embedding.scalar_u8.alpha"),
		})
		return d, collection, def
	}
	baselineDB, baselineCollection, def := openFixture(t)
	registry := baselineDB.StableResourceIdentityPinRegistry()
	if err := baselineDB.Checkpoint(); err != nil {
		_ = baselineDB.Close()
		t.Fatalf("settle initial publication before pin baseline: %v", err)
	}
	baselinePins := registry.ActivePins()
	baselineIdentities := registry.ActiveIdentities()
	var assets []columnVectorIndexStateAssetSnapshot
	restoreObserve := setColumnVectorGraphStableAuthorityTestHook(func(_ *rootpublication.StableResourceSet, got []columnVectorIndexStateAssetSnapshot) error {
		assets = append([]columnVectorIndexStateAssetSnapshot(nil), got...)
		return nil
	})
	closure, err := baselineCollection.PrepareVectorIndexStableClosure(def.Name)
	restoreObserve()
	if err != nil {
		_ = baselineDB.Close()
		t.Fatalf("PrepareVectorIndexStableClosure complete closure: %v", err)
	}
	resources, err := closure.TakeStableResources()
	if err != nil {
		closure.Release()
		_ = baselineDB.Close()
		t.Fatalf("TakeStableResources complete closure: %v", err)
	}
	descriptors := resources.Descriptors()
	obligations := make([]rootpublication.StableLogicalObligation, 0, len(assets))
	for _, descriptor := range descriptors {
		obligations = append(obligations, descriptor.LogicalObligations()...)
	}
	if len(descriptors) != 7 || len(obligations) != 12 || len(assets) != 12 {
		resources.Release()
		_ = baselineDB.Close()
		t.Fatalf("complete production closure descriptors=%d obligations=%d assets=%d want 7/12/12", len(descriptors), len(obligations), len(assets))
	}
	wantRoles := map[string]int{
		columnVectorIndexStateAssetRoleAdjacency:      2,
		columnVectorIndexStateAssetRoleInverseNorm:    1,
		columnVectorIndexStateAssetRoleRowRefs:        5,
		columnVectorIndexStateAssetRoleDocumentIDs:    1,
		columnVectorIndexStateAssetRoleQuantizedCodes: 1,
		columnVectorIndexStateAssetRoleQuantizedAlpha: 1,
		columnVectorIndexStateAssetRoleHNSWSearchPack: 1,
	}
	gotRoles := make(map[string]int, len(wantRoles))
	wantObligations := make(map[rootpublication.StableLogicalObligation]columnVectorIndexStateAssetSnapshot, len(assets))
	for _, asset := range assets {
		gotRoles[asset.Role]++
		obligation := stableColumnLogicalObligation(asset.Ref, rootpublication.ReachabilityVectorGraphPack)
		if _, duplicate := wantObligations[obligation]; duplicate {
			resources.Release()
			_ = baselineDB.Close()
			t.Fatalf("duplicate producer asset obligation role=%q id=%q ref=%+v", asset.Role, asset.AssetID, asset.Ref)
		}
		wantObligations[obligation] = asset
	}
	if !reflect.DeepEqual(gotRoles, wantRoles) {
		resources.Release()
		_ = baselineDB.Close()
		t.Fatalf("complete production closure role multiset=%v want %v", gotRoles, wantRoles)
	}
	// Keep the accepted-role registry independent of the produced-role
	// multiset. All eight manifest roles are classified here; normalized
	// vectors are the sole role intentionally deferred by the current producer.
	declaredRoles := []string{
		columnVectorIndexStateAssetRoleAdjacency,
		columnVectorIndexStateAssetRoleInverseNorm,
		columnVectorIndexStateAssetRoleNormalizedVectors,
		columnVectorIndexStateAssetRoleRowRefs,
		columnVectorIndexStateAssetRoleDocumentIDs,
		columnVectorIndexStateAssetRoleQuantizedCodes,
		columnVectorIndexStateAssetRoleQuantizedAlpha,
		columnVectorIndexStateAssetRoleHNSWSearchPack,
	}
	for _, role := range declaredRoles {
		if role == columnVectorIndexStateAssetRoleNormalizedVectors {
			if gotRoles[role] != 0 {
				resources.Release()
				_ = baselineDB.Close()
				t.Fatalf("deferred role %q unexpectedly produced %d assets", role, gotRoles[role])
			}
			continue
		}
		if _, ok := wantRoles[role]; !ok {
			resources.Release()
			_ = baselineDB.Close()
			t.Fatalf("declared role %q is neither produced nor explicitly deferred", role)
		}
	}
	if len(declaredRoles) != len(wantRoles)+1 {
		resources.Release()
		_ = baselineDB.Close()
		t.Fatalf("declared roles=%d produced roles=%d want exactly one deferred role", len(declaredRoles), len(wantRoles))
	}
	for _, obligation := range obligations {
		if _, ok := wantObligations[obligation]; !ok {
			resources.Release()
			_ = baselineDB.Close()
			t.Fatalf("complete production closure contains obligation absent from real state.Assets: %+v", obligation)
		}
	}
	baselineNamespaces := stableColumnVectorGraphNamespaceTokens(resources)
	resources.Release()
	assertStableColumnVectorGraphAuthorityReleased(t, registry, baselineNamespaces, baselinePins, baselineIdentities, "complete closure release")
	if err := baselineDB.Close(); err != nil {
		t.Fatalf("close complete-closure DB: %v", err)
	}
	assertStableColumnVectorGraphRegistryZero(t, registry, "complete closure DB close")

	runOmission := func(t *testing.T, name string, wantOmitted int, decide func(rootpublication.StableLogicalObligation) columnAssetStableCaptureTestDecision) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			d, collection, def := openFixture(t)
			registry := d.StableResourceIdentityPinRegistry()
			if err := d.Checkpoint(); err != nil {
				_ = d.Close()
				t.Fatalf("settle initial publication before omission pin baseline: %v", err)
			}
			baselinePins := registry.ActivePins()
			baselineIdentities := registry.ActiveIdentities()
			visibleRootBefore := columnVectorGraphStableVisibleManifestRoot(t, d)
			var omitted int
			var omittedNamespaces []*rootpublication.StableNamespaceToken
			restoreOmission := setColumnAssetStableObligationTestHook(func(_ ColumnAssetRef, obligation rootpublication.StableLogicalObligation, namespace *rootpublication.StableNamespaceToken) columnAssetStableCaptureTestDecision {
				if namespace != nil {
					omittedNamespaces = append(omittedNamespaces, namespace)
				}
				decision := decide(obligation)
				if decision != columnAssetStableCaptureKeep {
					omitted++
				}
				return decision
			})
			missingClosure, prepareErr := collection.PrepareVectorIndexStableClosure(def.Name)
			restoreOmission()
			if missingClosure != nil {
				missingClosure.Release()
				_ = d.Close()
				t.Fatal("missing production child returned a closure")
			}
			if !errors.Is(prepareErr, rootpublication.ErrUnresolvedResource) ||
				!strings.Contains(prepareErr.Error(), "missing prepared obligation") ||
				!strings.Contains(prepareErr.Error(), string(rootpublication.ReachabilityVectorGraphPack)) {
				_ = d.Close()
				t.Fatalf("%s error=%v want exact unresolved %s obligation", name, prepareErr, rootpublication.ReachabilityVectorGraphPack)
			}
			if omitted != wantOmitted {
				_ = d.Close()
				t.Fatalf("%s omitted=%d want %d real producer tokens/obligations", name, omitted, wantOmitted)
			}
			if got := columnVectorGraphStableVisibleManifestRoot(t, d); got != visibleRootBefore {
				_ = d.Close()
				t.Fatalf("%s published manifest root=%d before=%d", name, got, visibleRootBefore)
			}
			if len(omittedNamespaces) != len(obligations) {
				_ = d.Close()
				t.Fatalf("%s namespace authorities=%d want logical obligations=%d", name, len(omittedNamespaces), len(obligations))
			}
			assertStableColumnVectorGraphAuthorityReleased(t, registry, omittedNamespaces, baselinePins, baselineIdentities, "missing "+name)
			if err := d.Close(); err != nil {
				t.Fatalf("close missing-child DB: %v", err)
			}
			assertStableColumnVectorGraphRegistryZero(t, registry, "missing "+name+" DB close")
		})
	}

	for _, targetObligation := range obligations {
		targetObligation := targetObligation
		targetAsset := wantObligations[targetObligation]
		name := "logical/" + string(targetAsset.Role) + "/" + targetAsset.AssetID
		runOmission(t, name, 1, func(obligation rootpublication.StableLogicalObligation) columnAssetStableCaptureTestDecision {
			if obligation == targetObligation {
				return columnAssetStableCaptureOmitObligation
			}
			return columnAssetStableCaptureKeep
		})
	}
	for descriptorIndex, descriptor := range descriptors {
		targets := make(map[rootpublication.StableLogicalObligation]struct{}, len(descriptor.LogicalObligations()))
		for _, obligation := range descriptor.LogicalObligations() {
			targets[obligation] = struct{}{}
		}
		name := fmt.Sprintf("physical/segment-%d", descriptorIndex)
		runOmission(t, name, len(targets), func(obligation rootpublication.StableLogicalObligation) columnAssetStableCaptureTestDecision {
			if _, ok := targets[obligation]; ok {
				return columnAssetStableCaptureOmitToken
			}
			return columnAssetStableCaptureKeep
		})
	}
}

func stableColumnVectorGraphNamespaceTokens(resources *rootpublication.StableResourceSet) []*rootpublication.StableNamespaceToken {
	if resources == nil {
		return nil
	}
	namespaces := make([]*rootpublication.StableNamespaceToken, 0, resources.Len())
	for _, token := range resources.Tokens() {
		if namespace := token.Namespace(); namespace != nil {
			namespaces = append(namespaces, namespace)
		}
	}
	return namespaces
}

func assertStableColumnVectorGraphAuthorityReleased(t *testing.T, registry *rootpublication.IdentityPinRegistry, namespaces []*rootpublication.StableNamespaceToken, wantPins uint64, wantIdentities int, context string) {
	t.Helper()
	if got := registry.ActivePins(); got != wantPins {
		t.Fatalf("%s active pins=%d want baseline=%d", context, got, wantPins)
	}
	if got := registry.ActiveIdentities(); got != wantIdentities {
		t.Fatalf("%s active identities=%d want baseline=%d", context, got, wantIdentities)
	}
	for i, namespace := range namespaces {
		if err := namespace.Stabilize(); !errors.Is(err, rootpublication.ErrResourceOwnership) {
			t.Fatalf("%s namespace[%d] stabilization after release=%v want ErrResourceOwnership", context, i, err)
		}
	}
}

func assertStableColumnVectorGraphRegistryZero(t *testing.T, registry *rootpublication.IdentityPinRegistry, context string) {
	t.Helper()
	if pins, identities := registry.ActivePins(), registry.ActiveIdentities(); pins != 0 || identities != 0 {
		t.Fatalf("%s active pins=%d identities=%d want zero", context, pins, identities)
	}
}

func columnVectorGraphStableVisibleManifestRoot(t *testing.T, d *backenddb.DB) uint64 {
	t.Helper()
	snapshot := d.AcquireSnapshot()
	if snapshot == nil {
		t.Fatal("acquire collection snapshot for visibility check")
	}
	defer func() { _ = snapshot.Close() }()
	catalog, err := loadCollectionCatalog(snapshot, "docs")
	if err != nil {
		t.Fatalf("load collection catalog for visibility check: %v", err)
	}
	if catalog == nil {
		t.Fatal("collection catalog missing for visibility check")
	}
	return catalog.rootID(collectionColumnManifestRootName("docs"))
}

func BenchmarkColumnVectorGraphStableResourceCapture(b *testing.B) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		b.Skip("stable vector authority requires exact relative namespace support")
	}
	cfg := testColumnStoreConfig(nil)
	normalized, err := normalizeColumnStoreConfig("vector-stable-capture-benchmark", cfg)
	if err != nil {
		b.Fatal(err)
	}
	root := b.TempDir()
	registry := rootpublication.NewIdentityPinRegistry()
	items := []columnPhysicalAssetAppendItem{
		{payload: []byte("vector-adjacency"), kind: ColumnAssetKindTCS1TypedColumnPart, partID: 1},
		{payload: []byte("vector-hnsw-pack"), kind: ColumnAssetKindTCS1HNSWSearchPack, partID: 2},
	}
	b.SetBytes(int64(len(items[0].payload) + len(items[1].payload)))
	b.ReportAllocs()
	b.ResetTimer()
	var descriptors, obligations, contentSyncs, namespaceSyncs uint64
	var pinHighWater uint64
	for i := 0; i < b.N; i++ {
		authority, err := newColumnVectorGraphStableResourceAccumulator(registry)
		if err != nil {
			b.Fatal(err)
		}
		appender, err := authority.newAppender(root, *normalized)
		if err != nil {
			authority.abandon()
			b.Fatal(err)
		}
		generation := uint64(i + 1)
		for j := range items {
			items[j].generation = generation
		}
		refs, err := appender.appendKinds(items)
		if err != nil {
			_ = appender.abort()
			authority.abandon()
			b.Fatal(err)
		}
		if err := authority.closeAppender(appender); err != nil {
			authority.abandon()
			b.Fatal(err)
		}
		assets := make([]columnVectorIndexStateAssetSnapshot, len(refs))
		for j, ref := range refs {
			assets[j] = columnVectorIndexStateAssetSnapshot{Ref: ref, AssetBytes: ref.Length}
		}
		resources, err := authority.freeze(assets)
		if err != nil {
			b.Fatal(err)
		}
		descriptors += uint64(len(resources.Descriptors()))
		for _, descriptor := range resources.Descriptors() {
			obligations += uint64(len(descriptor.LogicalObligations()))
		}
		for _, stats := range resources.Stats(time.Now()) {
			contentSyncs += stats.Syncs
			namespaceSyncs += stats.NamespaceSyncs
			if stats.PinHighWater > pinHighWater {
				pinHighWater = stats.PinHighWater
			}
		}
		resources.Release()
	}
	b.StopTimer()
	if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		b.Fatalf("released vector authority pins=%d identities=%d want 0,0", registry.ActivePins(), registry.ActiveIdentities())
	}
	b.ReportMetric(float64(descriptors)/float64(b.N), "descriptors/op")
	b.ReportMetric(float64(obligations)/float64(b.N), "logical-obligations/op")
	b.ReportMetric(float64(pinHighWater), "pin-high-water")
	b.ReportMetric(float64(contentSyncs)/float64(b.N), "content-syncs/op")
	b.ReportMetric(float64(namespaceSyncs)/float64(b.N), "namespace-syncs/op")
}
