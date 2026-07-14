package collections

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

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
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active pins after publish=%d want baseline=%d", got, baselinePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active identities after publish=%d want baseline=%d", got, baselineIdentities)
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
	if got := registry.ActivePins(); got != baselinePins {
		t.Fatalf("active pins after publish=%d want baseline=%d", got, baselinePins)
	}
	if got := registry.ActiveIdentities(); got != baselineIdentities {
		t.Fatalf("active identities after publish=%d want baseline=%d", got, baselineIdentities)
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
	if _, err := col.RebuildVectorIndex(def.Name); !errors.Is(err, injected) {
		t.Fatalf("RebuildVectorIndex error=%v want injected", err)
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
	roles := []ColumnAssetKind{
		ColumnAssetKindTCS1TypedColumnPart,
		ColumnAssetKindTCS1Int64Values,
		ColumnAssetKindTCS1HNSWSearchPack,
	}
	for omitted := range roles {
		t.Run(string(roles[omitted]), func(t *testing.T) {
			cfg := testColumnStoreConfig(nil)
			normalized, err := normalizeColumnStoreConfig("vector-missing-child-"+string(roles[omitted]), cfg)
			if err != nil {
				t.Fatal(err)
			}
			registry := rootpublication.NewIdentityPinRegistry()
			authority, err := newColumnVectorGraphStableResourceAccumulator(registry)
			if err != nil {
				t.Fatal(err)
			}
			appender, err := authority.newAppender(t.TempDir(), *normalized)
			if err != nil {
				authority.abandon()
				t.Fatal(err)
			}
			items := make([]columnPhysicalAssetAppendItem, 0, len(roles)-1)
			for i, kind := range roles {
				if i == omitted {
					continue
				}
				items = append(items, columnPhysicalAssetAppendItem{payload: []byte("child-" + string(kind)), kind: kind, generation: 1, partID: uint64(i + 1)})
			}
			refs, err := appender.appendKinds(items)
			if err != nil {
				_ = appender.abort()
				authority.abandon()
				t.Fatal(err)
			}
			if err := authority.closeAppender(appender); err != nil {
				authority.abandon()
				t.Fatal(err)
			}
			resources, err := authority.builder.Freeze()
			if err != nil {
				authority.abandon()
				t.Fatal(err)
			}
			authority.builder = nil
			defer resources.Release()

			expected := make([]ColumnPreparedAsset, 0, len(roles))
			for _, ref := range refs {
				expected = append(expected, ColumnPreparedAsset{Ref: ref})
			}
			last := refs[len(refs)-1]
			missing := last
			missing.Kind = roles[omitted]
			missing.PartID = uint64(omitted + 1)
			missing.Offset = last.Offset + last.Length
			missing.Length = 1
			missing.Checksum++
			expected = append(expected, ColumnPreparedAsset{Ref: missing})
			err = validateStableVectorGraphResourcesMatchPrepared(expected, resources)
			if err == nil || !strings.Contains(err.Error(), string(rootpublication.ReachabilityVectorGraphPack)) {
				t.Fatalf("missing %s error=%v want exact %s field", roles[omitted], err, rootpublication.ReachabilityVectorGraphPack)
			}
			resources.Release()
			if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
				t.Fatalf("missing %s release pins=%d identities=%d", roles[omitted], registry.ActivePins(), registry.ActiveIdentities())
			}
		})
	}
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
		resources.Release()
	}
	b.StopTimer()
	if registry.ActivePins() != 0 || registry.ActiveIdentities() != 0 {
		b.Fatalf("released vector authority pins=%d identities=%d want 0,0", registry.ActivePins(), registry.ActiveIdentities())
	}
	b.ReportMetric(1, "descriptors/op")
	b.ReportMetric(2, "logical-obligations/op")
	b.ReportMetric(1, "pin-high-water")
	b.ReportMetric(1, "content-syncs/op")
	b.ReportMetric(1, "namespace-syncs/op")
}
