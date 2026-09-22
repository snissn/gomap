package collections

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestQueryReadyBuildProducesDeterministicCompleteAssetSet(t *testing.T) {
	d, col, identity, parts := queryReadyBuildFixture(t, 7)
	defer func() { _ = d.Close() }()
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	first, err := coordinator.prepare(context.Background(), queryReadyBuildRequest{Kind: queryReadyBuildBase, Identity: identity, Parts: parts})
	if err != nil {
		t.Fatalf("prepare first: %v", err)
	}
	defer func() { _ = first.Abort() }()
	second, err := coordinator.prepare(context.Background(), queryReadyBuildRequest{Kind: queryReadyBuildBase, Identity: identity, Parts: slices.Clone(parts)})
	if err != nil {
		t.Fatalf("prepare second: %v", err)
	}
	defer func() { _ = second.Abort() }()
	if first.Asset.Ref.Kind != ColumnAssetKindQueryReadyBase || first.Asset.Ref.Generation != identity.Generation || first.Asset.Ref.PartID != queryReadyBaseAssetPartID {
		t.Fatalf("first asset identity=%+v", first.Asset.Ref)
	}
	if first.Asset.Ref.Kind != second.Asset.Ref.Kind || first.Asset.Ref.Generation != second.Asset.Ref.Generation || first.Asset.Ref.PartID != second.Asset.Ref.PartID || first.Asset.Ref.Length != second.Asset.Ref.Length || first.Asset.Ref.Checksum != second.Asset.Ref.Checksum {
		t.Fatalf("logical asset set is not deterministic: first=%+v second=%+v", first.Asset.Ref, second.Asset.Ref)
	}
	descriptor, err := second.OpenFileDescriptor()
	if err != nil {
		t.Fatalf("open-file descriptor: %v", err)
	}
	if descriptor.Path == "" || descriptor.Offset <= 0 || descriptor.Length != second.Asset.Ref.Length || descriptor.Identity != identity || descriptor.Kind != typedcolumn.QueryReadyGenerationBase {
		t.Fatalf("incomplete nonzero-range descriptor: %+v", descriptor)
	}
	if !typedcolumn.QueryReadyGenerationFileOpenSupported() {
		t.Skip("query-ready generation file mapping is unsupported on this platform")
	}
	key := typedcolumn.QueryReadyGenerationOpenKey{Identity: identity, ManifestHash: sha256.Sum256([]byte("query-ready-build-handoff"))}
	cache := typedcolumn.NewQueryReadyGenerationOpenCache(key)
	defer func() { _ = cache.Close() }()
	opened, err := cache.Open(typedcolumn.QueryReadyGenerationOpenFiles{
		Key: key, Base: descriptor, SnapshotGeneration: identity.Generation,
		Bound: typedcolumn.QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 1, MaxAccumulatedDeltaParts: len(parts)},
	})
	if err != nil {
		t.Fatalf("M3 open of M5 nonzero asset range: %v", err)
	}
	if opened.PartCount() != len(parts) || cache.Stats().MappedFiles != 1 || cache.Stats().PayloadBytesCopied != 0 || cache.Stats().WholePartDecodesDuringOpen != 0 {
		t.Fatalf("M3 open did not retain direct query-ready views: parts=%d stats=%+v", opened.PartCount(), cache.Stats())
	}
	if len(first.Dependencies) != len(parts) || len(second.Dependencies) != len(parts) || first.Dependencies[0] != second.Dependencies[0] {
		t.Fatalf("incomplete/non-deterministic dependencies: first=%+v second=%+v", first.Dependencies, second.Dependencies)
	}
	if first.Stats.AssetsProduced != 1 || first.Stats.PartsProduced != len(parts) || first.Stats.Rows == 0 || first.Stats.ManagerRegistrationTime <= 0 || first.Stats.HandoffTime <= 0 {
		t.Fatalf("incomplete build/handoff stats: %+v", first.Stats)
	}
}

func TestQueryReadyPreparedAssetCannotEnterAuthoritativePublishPlan(t *testing.T) {
	for _, kind := range []ColumnAssetKind{ColumnAssetKindQueryReadyBase, ColumnAssetKindQueryReadyDelta, ColumnAssetKindQueryReadyConsolidatedBase} {
		t.Run(string(kind), func(t *testing.T) {
			asset := ColumnPreparedAsset{
				Ref: ColumnAssetRef{
					Kind:       kind,
					Namespace:  "query-ready-test",
					Generation: 7,
					PartID:     queryReadyBaseAssetPartID,
					FileID:     1,
					Offset:     64,
					Length:     128,
				},
				Rows:         1,
				Bytes:        128,
				GenerationID: 7,
				Reason:       "query_ready_build",
			}
			if err := validateColumnAssetRefForPlan(asset.Ref); err != nil {
				t.Fatalf("query-ready ref must remain valid for prepared-asset lifecycle registration: %v", err)
			}
			if err := validateColumnPreparedAssetForPlan(asset); err == nil || !strings.Contains(err.Error(), "non-authoritative") {
				t.Fatalf("validateColumnPreparedAssetForPlan err=%v want non-authoritative rejection", err)
			}
		})
	}
}

func TestQueryReadyBuildCancellationLeavesNoRegisteredPartialGeneration(t *testing.T) {
	d, col, identity, parts := queryReadyBuildFixture(t, 8)
	defer func() { _ = d.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.beforeRegister = cancel
	if _, err := coordinator.prepare(ctx, queryReadyBuildRequest{Kind: queryReadyBuildBase, Identity: identity, Parts: parts}); !errors.Is(err, context.Canceled) {
		t.Fatalf("prepare err=%v want context.Canceled", err)
	}
	columnAssetLifecycleProcessRegistries.Lock()
	defer columnAssetLifecycleProcessRegistries.Unlock()
	for _, record := range columnAssetLifecycleProcessRegistries.records {
		if record.Class == ColumnAssetLifecycleRegistryPreparedAsset && record.Owner == "query-ready-generation-8" {
			t.Fatalf("canceled build left prepared registry record: %+v", record)
		}
	}
}

func TestQueryReadyBuildBoundedInFlightWork(t *testing.T) {
	d, col, identity, parts := queryReadyBuildFixture(t, 9)
	defer func() { _ = d.Close() }()
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = coordinator.prepare(context.Background(), queryReadyBuildRequest{Kind: queryReadyBuildBase, Identity: identity, Parts: parts})
	var boundErr *QueryReadyBuildBoundError
	if !errors.As(err, &boundErr) || boundErr.RequiredBytes <= boundErr.MaxBytes {
		t.Fatalf("prepare err=%v bound=%+v want typed in-flight rejection", err, boundErr)
	}
	if stats := coordinator.stats(); stats.ActiveWorkers != 0 || stats.InFlightBytes != 0 || stats.BoundRejections != 1 || stats.MaxQueueDepth != 0 {
		t.Fatalf("coordinator stats after rejection=%+v", stats)
	}
}

func TestQueryReadyConsolidationBoundRejectionDoesNotAllocatePlanningSlice(t *testing.T) {
	d, col, identity, parts := queryReadyBuildFixture(t, 91)
	defer func() { _ = d.Close() }()
	built, err := typedcolumn.BuildQueryReadyBaseGeneration(identity, parts)
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	base, err := typedcolumn.OpenQueryReadyBaseGeneration(built.Bytes, identity)
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	largeViews := make([]typedcolumn.QueryReadyBasePartView, 4096)
	for i := range largeViews {
		largeViews[i] = base.Parts[0]
	}
	largeBase := &typedcolumn.QueryReadyBaseGeneration{Identity: base.Identity, Parts: largeViews}
	tombstones := make([]typedcolumn.Tombstone, 4096)
	largeDelta := &typedcolumn.QueryReadyDeltaGeneration{
		Base:       &typedcolumn.QueryReadyBaseGeneration{Identity: base.Identity, Parts: largeViews},
		Tombstones: tombstones,
	}
	smallRequest := queryReadyBuildRequest{Kind: queryReadyBuildConsolidatedBase, Identity: identity, Base: base, ThroughGeneration: identity.Generation}
	largeRequest := queryReadyBuildRequest{Kind: queryReadyBuildConsolidatedBase, Identity: identity, Base: largeBase, Deltas: []*typedcolumn.QueryReadyDeltaGeneration{largeDelta}, ThroughGeneration: identity.Generation}
	smallRequired, err := estimateQueryReadyBuildWorkingBytes(smallRequest)
	if err != nil {
		t.Fatalf("estimate small: %v", err)
	}
	largeRequired, err := estimateQueryReadyBuildWorkingBytes(largeRequest)
	if err != nil {
		t.Fatalf("estimate large: %v", err)
	}
	if largeRequired <= smallRequired+int64(len(tombstones))*256 {
		t.Fatalf("large reservation=%d does not cover all part/tombstone metadata above small=%d", largeRequired, smallRequired)
	}
	if allocs := testing.AllocsPerRun(20, func() {
		if _, err := estimateQueryReadyBuildWorkingBytes(largeRequest); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("allocation-free admission estimate allocated %.1f times", allocs)
	}
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	smallAllocs := testing.AllocsPerRun(20, func() {
		if _, err := coordinator.prepare(context.Background(), smallRequest); err == nil {
			panic("small request unexpectedly admitted")
		}
	})
	largeAllocs := testing.AllocsPerRun(20, func() {
		if _, err := coordinator.prepare(context.Background(), largeRequest); err == nil {
			panic("large request unexpectedly admitted")
		}
	})
	if largeAllocs > smallAllocs || largeAllocs > 2 {
		t.Fatalf("bound rejection allocations scale with parts: small=%.1f large=%.1f", smallAllocs, largeAllocs)
	}
}

func TestQueryReadyConsolidationAdmissionIgnoresFutureDeltaPayloads(t *testing.T) {
	d, col, identity, parts := queryReadyBuildFixture(t, 92)
	defer func() { _ = d.Close() }()
	built, err := typedcolumn.BuildQueryReadyBaseGeneration(identity, parts)
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	base, err := typedcolumn.OpenQueryReadyBaseGeneration(built.Bytes, identity)
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	largeViews := make([]typedcolumn.QueryReadyBasePartView, 4096)
	for i := range largeViews {
		largeViews[i] = base.Parts[0]
	}
	futureIdentity := identity
	futureIdentity.Generation++
	future := &typedcolumn.QueryReadyDeltaGeneration{
		Kind:     typedcolumn.QueryReadyGenerationDelta,
		Identity: futureIdentity,
		Base:     &typedcolumn.QueryReadyBaseGeneration{Identity: futureIdentity, Parts: largeViews},
	}
	request := queryReadyBuildRequest{
		Kind: queryReadyBuildConsolidatedBase, Identity: identity, Base: base,
		Deltas: []*typedcolumn.QueryReadyDeltaGeneration{future}, ThroughGeneration: identity.Generation,
	}
	baseOnlyRequired, err := estimateQueryReadyBuildWorkingBytes(queryReadyBuildRequest{
		Kind: queryReadyBuildConsolidatedBase, Identity: identity, Base: base, ThroughGeneration: identity.Generation,
	})
	if err != nil {
		t.Fatalf("estimate base-only consolidation: %v", err)
	}
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: baseOnlyRequired})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := coordinator.prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("future delta payload rejected selected-prefix consolidation: %v", err)
	}
	defer func() { _ = prepared.Abort() }()
	if prepared.Stats.ReservedInFlightBytes != baseOnlyRequired || len(prepared.Dependencies) != len(base.Parts) {
		t.Fatalf("prepared future-filtered consolidation stats=%+v dependencies=%d", prepared.Stats, len(prepared.Dependencies))
	}
}

func TestQueryReadyBuildZeroQueueBackpressureBoundsConcurrentWorkers(t *testing.T) {
	d, col, identity, parts := queryReadyBuildFixture(t, 11)
	defer func() { _ = d.Close() }()
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	coordinator.afterAdmission = func() {
		once.Do(func() { close(entered) })
		<-release
	}
	firstDone := make(chan error, 1)
	go func() {
		prepared, err := coordinator.prepare(context.Background(), queryReadyBuildRequest{Kind: queryReadyBuildBase, Identity: identity, Parts: parts})
		if prepared != nil {
			_ = prepared.Abort()
		}
		firstDone <- err
	}()
	<-entered
	_, err = coordinator.prepare(context.Background(), queryReadyBuildRequest{Kind: queryReadyBuildBase, Identity: identity, Parts: parts})
	var backpressure *QueryReadyBuildBackpressureError
	if !errors.As(err, &backpressure) || backpressure.MaxWorkers != 1 {
		t.Fatalf("second prepare err=%v want zero-queue backpressure", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	stats := coordinator.stats()
	if stats.PeakWorkers != 1 || stats.AdmissionRejections != 1 || stats.MaxQueueDepth != 0 || stats.PeakInFlight > 16<<20 {
		t.Fatalf("bounded coordinator stats=%+v", stats)
	}
}

func TestQueryReadyDeltaPublishDoesNotRewriteBase(t *testing.T) {
	d, col, identity, parts := queryReadyBuildFixture(t, 10)
	defer func() { _ = d.Close() }()
	identity.Generation++
	parts[0].SourceGeneration = identity.Generation
	before := sha256.Sum256(parts[0].Image.Bytes)
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := coordinator.prepare(context.Background(), queryReadyBuildRequest{Kind: queryReadyBuildDelta, Identity: identity, Parts: parts})
	if err != nil {
		t.Fatalf("prepare delta: %v", err)
	}
	defer func() { _ = prepared.Abort() }()
	if after := sha256.Sum256(parts[0].Image.Bytes); after != before {
		t.Fatal("delta handoff rewrote source/base part bytes")
	}
	if prepared.Asset.Ref.Kind != ColumnAssetKindQueryReadyDelta || prepared.Stats.BaseBytesRewritten != 0 {
		t.Fatalf("delta asset/stats=%+v/%+v", prepared.Asset.Ref, prepared.Stats)
	}
}

func TestQueryReadyConsolidationBuildMatchesSnapshot(t *testing.T) {
	d, col, baseIdentity, baseParts := queryReadyBuildFixture(t, 12)
	defer func() { _ = d.Close() }()
	baseBuilt, err := typedcolumn.BuildQueryReadyBaseGeneration(baseIdentity, baseParts)
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	base, err := typedcolumn.OpenQueryReadyBaseGeneration(baseBuilt.Bytes, baseIdentity)
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	deltaIdentity := baseIdentity
	deltaIdentity.Generation++
	deltaImage := queryReadyBuildImage(t, deltaIdentity.Generation, []int64{1, 3}, []int64{100, 300})
	deltaBuilt, err := typedcolumn.BuildQueryReadyDeltaGeneration(deltaIdentity, []typedcolumn.QueryReadyBasePartInput{{SourceGeneration: deltaIdentity.Generation, Image: deltaImage}}, []typedcolumn.Tombstone{{PrimaryID: 2, GenerationID: deltaIdentity.Generation}})
	if err != nil {
		t.Fatalf("build delta: %v", err)
	}
	delta, err := typedcolumn.OpenQueryReadyDeltaGeneration(deltaBuilt.Bytes, deltaIdentity)
	if err != nil {
		t.Fatalf("open delta: %v", err)
	}
	want, err := typedcolumn.NewQueryReadyBaseDeltaReader(base, []*typedcolumn.QueryReadyDeltaGeneration{delta}, typedcolumn.QueryReadyBaseDeltaOptions{SnapshotGeneration: deltaIdentity.Generation, Bound: typedcolumn.QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8}})
	if err != nil {
		t.Fatalf("prepare expected reader: %v", err)
	}
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := coordinator.prepare(context.Background(), queryReadyBuildRequest{Kind: queryReadyBuildConsolidatedBase, Identity: deltaIdentity, Base: base, Deltas: []*typedcolumn.QueryReadyDeltaGeneration{delta}, ThroughGeneration: deltaIdentity.Generation})
	if err != nil {
		t.Fatalf("prepare consolidation: %v", err)
	}
	defer func() { _ = prepared.Abort() }()
	raw, err := os.ReadFile(prepared.AssetPath)
	if err != nil {
		t.Fatalf("read prepared consolidation: %v", err)
	}
	start, end := prepared.Asset.Ref.Offset, prepared.Asset.Ref.Offset+prepared.Asset.Ref.Length
	if start < 0 || end > int64(len(raw)) {
		t.Fatalf("prepared asset range=%d:%d file=%d", start, end, len(raw))
	}
	consolidated, err := typedcolumn.OpenQueryReadyConsolidatedBaseGeneration(raw[start:end], deltaIdentity)
	if err != nil {
		t.Fatalf("open prepared consolidation: %v", err)
	}
	got, err := typedcolumn.NewQueryReadyConsolidatedBaseDeltaReader(consolidated, nil, typedcolumn.QueryReadyBaseDeltaOptions{SnapshotGeneration: deltaIdentity.Generation, Bound: typedcolumn.QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8}})
	if err != nil {
		t.Fatalf("prepare consolidated reader: %v", err)
	}
	for _, id := range []int64{1, 2, 3} {
		wantValue, wantOK, wantErr := want.ValueAtLatest(id, "value")
		gotValue, gotOK, gotErr := got.ValueAtLatest(id, "value")
		if wantErr != nil || gotErr != nil || wantOK != gotOK || wantValue != gotValue {
			t.Fatalf("id=%d before=(%d,%v,%v) after=(%d,%v,%v)", id, wantValue, wantOK, wantErr, gotValue, gotOK, gotErr)
		}
	}
}

func TestQueryReadyConsolidationPreservesInheritedTombstones(t *testing.T) {
	d, col, baseIdentity, baseParts := queryReadyBuildFixture(t, 14)
	defer func() { _ = d.Close() }()
	baseBuilt, err := typedcolumn.BuildQueryReadyBaseGeneration(baseIdentity, baseParts)
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	base, err := typedcolumn.OpenQueryReadyBaseGeneration(baseBuilt.Bytes, baseIdentity)
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	secondIdentity := baseIdentity
	secondIdentity.Generation++
	secondBuilt, err := typedcolumn.BuildQueryReadyDeltaGeneration(secondIdentity, nil, []typedcolumn.Tombstone{{PrimaryID: 2, GenerationID: secondIdentity.Generation}})
	if err != nil {
		t.Fatalf("build second generation: %v", err)
	}
	second, err := typedcolumn.OpenQueryReadyDeltaGeneration(secondBuilt.Bytes, secondIdentity)
	if err != nil {
		t.Fatalf("open second generation: %v", err)
	}
	firstConsolidation, err := typedcolumn.ConsolidateQueryReadyBaseDelta(base, []*typedcolumn.QueryReadyDeltaGeneration{second}, secondIdentity.Generation)
	if err != nil {
		t.Fatalf("first consolidation: %v", err)
	}
	consolidated, err := typedcolumn.OpenQueryReadyConsolidatedBaseGeneration(firstConsolidation.Bytes, secondIdentity)
	if err != nil {
		t.Fatalf("open first consolidation: %v", err)
	}
	thirdIdentity := secondIdentity
	thirdIdentity.Generation++
	thirdImage := queryReadyBuildImage(t, thirdIdentity.Generation, []int64{1}, []int64{100})
	thirdBuilt, err := typedcolumn.BuildQueryReadyDeltaGeneration(thirdIdentity, []typedcolumn.QueryReadyBasePartInput{{SourceGeneration: thirdIdentity.Generation, Image: thirdImage}}, nil)
	if err != nil {
		t.Fatalf("build third generation: %v", err)
	}
	third, err := typedcolumn.OpenQueryReadyDeltaGeneration(thirdBuilt.Bytes, thirdIdentity)
	if err != nil {
		t.Fatalf("open third generation: %v", err)
	}
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := coordinator.prepare(context.Background(), queryReadyBuildRequest{
		Kind: queryReadyBuildConsolidatedBase, Identity: thirdIdentity,
		ConsolidatedBase: consolidated,
		Deltas:           []*typedcolumn.QueryReadyDeltaGeneration{third}, ThroughGeneration: thirdIdentity.Generation,
	})
	if err != nil {
		t.Fatalf("prepare reconsolidation: %v", err)
	}
	defer func() { _ = prepared.Abort() }()
	raw, err := os.ReadFile(prepared.AssetPath)
	if err != nil {
		t.Fatalf("read prepared reconsolidation: %v", err)
	}
	start, end := prepared.Asset.Ref.Offset, prepared.Asset.Ref.Offset+prepared.Asset.Ref.Length
	reconsolidated, err := typedcolumn.OpenQueryReadyConsolidatedBaseGeneration(raw[start:end], thirdIdentity)
	if err != nil {
		t.Fatalf("open prepared reconsolidation: %v", err)
	}
	if reconsolidated.OriginBaseParts != consolidated.OriginBaseParts || reconsolidated.AccumulatedDeltaParts != consolidated.AccumulatedDeltaParts+len(third.Base.Parts) || len(reconsolidated.Tombstones) != 1 {
		t.Fatalf("reconsolidated lineage/tombstones=%d/%d/%v", reconsolidated.OriginBaseParts, reconsolidated.AccumulatedDeltaParts, reconsolidated.Tombstones)
	}
	reader, err := typedcolumn.NewQueryReadyConsolidatedBaseDeltaReader(reconsolidated, nil, typedcolumn.QueryReadyBaseDeltaOptions{SnapshotGeneration: thirdIdentity.Generation, Bound: typedcolumn.QueryReadyDeltaBoundPolicy{MaxVisibleGenerations: 4, MaxAccumulatedDeltaParts: 8}})
	if err != nil {
		t.Fatalf("prepare reconsolidated reader: %v", err)
	}
	if value, ok, err := reader.ValueAtLatest(2, "value"); err != nil || ok {
		t.Fatalf("inherited deletion resurrected: value=%d ok=%v err=%v tombstones=%v", value, ok, err, reconsolidated.Tombstones)
	}
}

func TestQueryReadyConsolidationRejectsDescriptorIdentityMismatchBeforeAdmission(t *testing.T) {
	d, col, identity, parts := queryReadyBuildFixture(t, 13)
	defer func() { _ = d.Close() }()
	built, err := typedcolumn.BuildQueryReadyBaseGeneration(identity, parts)
	if err != nil {
		t.Fatalf("build base: %v", err)
	}
	base, err := typedcolumn.OpenQueryReadyBaseGeneration(built.Bytes, identity)
	if err != nil {
		t.Fatalf("open base: %v", err)
	}
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	wrong := identity
	wrong.Generation++
	_, err = coordinator.prepare(context.Background(), queryReadyBuildRequest{
		Kind: queryReadyBuildConsolidatedBase, Identity: wrong, Base: base, ThroughGeneration: identity.Generation,
	})
	if err == nil {
		t.Fatal("mismatched descriptor identity unexpectedly admitted")
	}
	if stats := coordinator.stats(); stats.ActiveWorkers != 0 || stats.PeakWorkers != 0 || stats.InFlightBytes != 0 {
		t.Fatalf("identity mismatch allocated/admitted build work: %+v", stats)
	}
}

func queryReadyBuildFixture(t testing.TB, generation uint64) (*backenddb.DB, *Collection, typedcolumn.QueryReadyBaseIdentity, []typedcolumn.QueryReadyBasePartInput) {
	t.Helper()
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	col := openColumnAssetLifecycleTestCollection1954(t, d)
	image := queryReadyBuildImage(t, generation, []int64{1, 2}, []int64{10, 20})
	identity := typedcolumn.QueryReadyBaseIdentity{Generation: generation, SchemaHash: sha256.Sum256([]byte(col.Meta().Name))}
	return d, col, identity, []typedcolumn.QueryReadyBasePartInput{{SourceGeneration: generation, Image: image}}
}

func queryReadyBuildImage(t testing.TB, partID uint64, ids, values []int64) typedcolumn.ColumnPartImage {
	t.Helper()
	opts := typedcolumn.Options{
		SchemaVersion: 1,
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			{Name: "id", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone},
			{Name: "value", Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone},
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{"id"}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: "id"}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: 16},
	}
	part, err := typedcolumn.BuildColumnPart(partID, opts, typedcolumn.Batch{Rows: len(ids), Columns: map[string][]int64{"id": ids, "value": values}})
	if err != nil {
		t.Fatalf("build typed part: %v", err)
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{})
	if err != nil {
		t.Fatalf("build typed image: %v", err)
	}
	return image
}

func BenchmarkQueryReadyBuildExistingManagerHandoff(b *testing.B) {
	d, col, identity, parts := queryReadyBuildFixture(b, 20)
	defer func() { _ = d.Close() }()
	coordinator, err := newQueryReadyBuildCoordinator(col, queryReadyBuildLimits{MaxWorkers: 1, MaxInFlightBytes: 16 << 20})
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		kind queryReadyBuildKind
	}{
		{name: "base", kind: queryReadyBuildBase},
		{name: "delta", kind: queryReadyBuildDelta},
	} {
		b.Run(tc.name, func(b *testing.B) {
			requestIdentity := identity
			requestParts := slices.Clone(parts)
			if tc.kind == queryReadyBuildDelta {
				requestIdentity.Generation++
				requestParts[0].SourceGeneration = requestIdentity.Generation
			}
			request := queryReadyBuildRequest{Kind: tc.kind, Identity: requestIdentity, Parts: requestParts}
			var sample queryReadyBuildStats
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				prepared, err := coordinator.prepare(context.Background(), request)
				if err != nil {
					b.Fatal(err)
				}
				sample = prepared.Stats
				if err := prepared.Abort(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(sample.OutputBytes), "asset-bytes/op")
			b.ReportMetric(float64(sample.EstimatedPeakInFlightBytes), "reserved-inflight-bytes/op")
			b.ReportMetric(float64(sample.EncodedBufferPeakBytes), "encoded-buffer-bytes/op")
			b.ReportMetric(float64(sample.BytesCopied), "copied-bytes/op")
			b.ReportMetric(float64(sample.BytesHashed), "hashed-bytes/op")
			b.ReportMetric(float64(sample.BytesChecksummed), "checksummed-bytes/op")
			b.ReportMetric(float64(sample.AssetPreparationTime.Nanoseconds()), "asset-prepare-ns/op")
			b.ReportMetric(float64(sample.ManagerRegistrationTime.Nanoseconds()), "register-ns/op")
		})
	}
}
