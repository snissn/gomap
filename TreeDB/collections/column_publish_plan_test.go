package collections

import (
	"errors"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

func TestColumnPublishPlanDisabledFastPathAllocatesZeroM10A(t *testing.T) {
	disabledCfg := &ColumnStoreConfig{}
	var touched int
	inputs := []ColumnPublishPlanInput{
		{
			Collection:  "events",
			ColumnStore: nil,
			Hooks: ColumnPublishPlanHooks{
				ExtractDocuments: func() error {
					touched++
					return nil
				},
			},
		},
		{
			Collection:  "events",
			ColumnStore: disabledCfg,
			Hooks: ColumnPublishPlanHooks{
				PrepareAssets: func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
					touched++
					return ColumnPublishPreparedAssets{}, nil
				},
			},
		},
	}
	for _, input := range inputs {
		allocs := testing.AllocsPerRun(1000, func() {
			plan, err := BuildColumnPublishPlan(input)
			if err != nil {
				t.Fatalf("BuildColumnPublishPlan disabled: %v", err)
			}
			if plan.Enabled {
				t.Fatalf("disabled plan returned enabled result: %+v", plan)
			}
		})
		if allocs != 0 {
			t.Fatalf("disabled BuildColumnPublishPlan allocated %f times, want 0", allocs)
		}
	}
	if touched != 0 {
		t.Fatalf("disabled BuildColumnPublishPlan touched hooks %d times, want 0", touched)
	}
}

func TestColumnPublishPlanBuildsDurabilityClosureM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	var called []string
	input := testColumnPublishPlanInputM10A(identity, asset)
	input.MeasureAllocations = true
	input.Hooks.ExtractDocuments = func() error {
		called = append(called, "extract")
		return nil
	}
	input.Hooks.EncodeDeclaredColumns = func(ColumnPublishDeclaredColumnEncodeInput) error {
		called = append(called, "encode_columns")
		return nil
	}
	input.Hooks.BuildSystemDelta = func(in ColumnPublishSystemDeltaInput) error {
		called = append(called, "system_delta")
		if in.Plan.UpdatedActiveManifest != identity {
			t.Fatalf("system delta saw identity %+v want %+v", in.Plan.UpdatedActiveManifest, identity)
		}
		return nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	if !plan.Enabled {
		t.Fatal("enabled column publish plan returned disabled result")
	}
	if got, want := strings.Join(called, ","), "extract,encode_columns,system_delta"; got != want {
		t.Fatalf("stage order=%q want %q", got, want)
	}
	if plan.Collection != "events" || plan.Operation != ColumnPublishOperationInsert {
		t.Fatalf("unexpected plan identity: %+v", plan)
	}
	if plan.AppliedCommandLSN != 101 || plan.RecoveryAuthoritativeAppliedCommandLSN != 101 {
		t.Fatalf("unexpected applied LSNs: %+v", plan)
	}
	if plan.UpdatedActiveManifest != identity || plan.RecoveryAuthoritativeManifest != identity {
		t.Fatalf("unexpected manifest identities: %+v", plan)
	}
	if plan.RootDelta.RootName != collectionColumnManifestRootName("events") || plan.RootDelta.BaseRootID != 44 {
		t.Fatalf("unexpected root delta: %+v", plan.RootDelta)
	}
	if decoded, err := decodeColumnManifestIdentityRecord(plan.RootDelta.IdentityRecord[:]); err != nil || decoded.Generation != identity.Generation || decoded.Checksum != identity.Checksum {
		t.Fatalf("bad deterministic identity record decoded=%+v err=%v", decoded, err)
	}
	if plan.RequiredAssetCount != 1 || plan.RequiredAssetBytes != asset.Bytes || !plan.RequiredAssetFlush || !plan.RequiredAssetSync {
		t.Fatalf("unexpected required asset closure: %+v", plan)
	}
	if len(plan.PreparedAssets) != 1 || plan.PreparedAssets[0] != asset {
		t.Fatalf("unexpected prepared assets: %+v", plan.PreparedAssets)
	}
	if plan.Lifecycle.PublishedRefs != 1 || plan.Lifecycle.PublishedBytes != asset.Bytes || plan.Lifecycle.PreparedRefs != 1 || plan.Lifecycle.PreparedBytes != asset.Bytes {
		t.Fatalf("unexpected lifecycle summary: %+v", plan.Lifecycle)
	}
	if plan.StageMetrics.Allocs == 0 {
		t.Fatalf("stage metrics were not populated: %+v", plan.StageMetrics)
	}
}

func TestColumnPublishPlanFailsClosedBeforeRootPublishM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	errStage := errors.New("stage failed")
	tests := []struct {
		name      string
		configure func(*ColumnPublishPlanInput, *[]string)
		wantCalls string
	}{
		{
			name: "extract failure",
			configure: func(input *ColumnPublishPlanInput, calls *[]string) {
				input.Hooks.ExtractDocuments = func() error {
					*calls = append(*calls, "extract")
					return errStage
				}
			},
			wantCalls: "extract",
		},
		{
			name: "declared column encode failure",
			configure: func(input *ColumnPublishPlanInput, calls *[]string) {
				input.Hooks.EncodeDeclaredColumns = func(ColumnPublishDeclaredColumnEncodeInput) error {
					*calls = append(*calls, "encode_columns")
					return errStage
				}
			},
			wantCalls: "extract,encode_columns",
		},
		{
			name: "asset prepare failure",
			configure: func(input *ColumnPublishPlanInput, calls *[]string) {
				input.Hooks.PrepareAssets = func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
					*calls = append(*calls, "prepare_assets")
					return ColumnPublishPreparedAssets{}, errStage
				}
			},
			wantCalls: "extract,encode_columns,prepare_assets",
		},
		{
			name: "manifest encode failure",
			configure: func(input *ColumnPublishPlanInput, calls *[]string) {
				input.Hooks.EncodeManifest = func(ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
					*calls = append(*calls, "manifest_encode")
					return ColumnPublishManifestEncodeResult{}, errStage
				}
			},
			wantCalls: "extract,encode_columns,prepare_assets,manifest_encode",
		},
		{
			name: "closure validation failure",
			configure: func(input *ColumnPublishPlanInput, calls *[]string) {
				input.Hooks.ValidateClosure = func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
					*calls = append(*calls, "closure_validation")
					return ColumnPublishDurabilityClosure{}, errStage
				}
			},
			wantCalls: "extract,encode_columns,prepare_assets,manifest_encode,closure_validation",
		},
		{
			name: "system delta failure",
			configure: func(input *ColumnPublishPlanInput, calls *[]string) {
				input.Hooks.BuildSystemDelta = func(ColumnPublishSystemDeltaInput) error {
					*calls = append(*calls, "system_delta")
					return errStage
				}
			},
			wantCalls: "extract,encode_columns,prepare_assets,manifest_encode,closure_validation,system_delta",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls []string
			input := testColumnPublishPlanInputM10A(identity, asset)
			input.Hooks.ExtractDocuments = func() error {
				calls = append(calls, "extract")
				return nil
			}
			input.Hooks.EncodeDeclaredColumns = func(ColumnPublishDeclaredColumnEncodeInput) error {
				calls = append(calls, "encode_columns")
				return nil
			}
			input.Hooks.PrepareAssets = func(in ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
				calls = append(calls, "prepare_assets")
				return ColumnPublishPreparedAssets{Assets: []ColumnPreparedAsset{asset}, RowCount: 10, ColumnPayloadBytes: asset.Bytes}, nil
			}
			input.Hooks.EncodeManifest = func(in ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
				calls = append(calls, "manifest_encode")
				return ColumnPublishManifestEncodeResult{Identity: identity, ManifestBytes: 256}, nil
			}
			input.Hooks.ValidateClosure = func(in ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
				calls = append(calls, "closure_validation")
				return ColumnPublishDurabilityClosure{PreparedAssets: []ColumnPreparedAsset{asset}, RequiredAssets: 1, RequiredBytes: asset.Bytes, FlushRequired: true, SyncRequired: true}, nil
			}
			input.Hooks.BuildSystemDelta = func(in ColumnPublishSystemDeltaInput) error {
				calls = append(calls, "system_delta")
				return nil
			}
			tt.configure(&input, &calls)

			plan, err := BuildColumnPublishPlan(input)
			if err == nil || !errors.Is(err, errStage) {
				t.Fatalf("BuildColumnPublishPlan err=%v want %v", err, errStage)
			}
			if plan.Enabled {
				t.Fatalf("failed plan returned enabled result: %+v", plan)
			}
			if got := strings.Join(calls, ","); got != tt.wantCalls {
				t.Fatalf("calls=%q want %q", got, tt.wantCalls)
			}
		})
	}
}

func TestColumnManifestPublishSystemDeltaUpdatesRootAndMetadataTogetherM10A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		t.Fatalf("create column-enabled collection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	baseMeta := col.Meta()
	state := d.State()
	identity := ColumnManifestIdentity{Generation: 11, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef1234}
	planInput := testColumnPublishPlanInputM10A(identity, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	ordered, err := plan.RootDelta.OrderedRootPublishInput()
	if err != nil {
		t.Fatalf("OrderedRootPublishInput: %v", err)
	}
	_, rootIDs, err := d.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{ordered}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
			BaseMeta:           baseMeta,
			BaseCommitSeq:      state.CommitSeq,
			BaseSystemRoot:     state.SystemRootPageID,
			BaseManifestRootID: 0,
			Plan:               plan,
		}, rootIDs)
	})
	if err != nil {
		t.Fatalf("publish column manifest root: %v", err)
	}
	if len(rootIDs) != 1 || rootIDs[0] == 0 {
		t.Fatalf("unexpected root IDs: %v", rootIDs)
	}
	reopened, err := NewCollectionManager(d).OpenCollection("events")
	if err != nil {
		t.Fatalf("open updated collection: %v", err)
	}
	meta := reopened.Meta()
	cfg := meta.Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil || *cfg.ActiveManifest != identity {
		t.Fatalf("active manifest not updated atomically: %+v", cfg)
	}
	if cfg.RecoveryAuthoritativeManifest == nil || *cfg.RecoveryAuthoritativeManifest != identity || cfg.RecoveryAuthoritativeAppliedCommandLSN != plan.AppliedCommandLSN {
		t.Fatalf("recovery-authoritative metadata not updated atomically: %+v", cfg)
	}
	cacheID, ok := reopened.ColumnStoreCacheIdentity()
	if !ok || cacheID.ManifestRoot != rootIDs[0] || cacheID.ManifestGeneration != identity.Generation || cacheID.RecoveryAuthoritativeAppliedCommandLSN != plan.AppliedCommandLSN {
		t.Fatalf("unexpected cache identity: %+v ok=%v rootIDs=%v", cacheID, ok, rootIDs)
	}
}

func TestColumnManifestPublishSystemDeltaFailureLeavesRootsUnpublishedM10A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "events",
		Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)},
	}); err != nil {
		t.Fatalf("create column-enabled collection: %v", err)
	}
	col, err := mgr.OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	baseMeta := col.Meta()
	state := d.State()
	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 12, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x12345678}, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	ordered, err := plan.RootDelta.OrderedRootPublishInput()
	if err != nil {
		t.Fatalf("OrderedRootPublishInput: %v", err)
	}
	_, _, err = d.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{ordered}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
			BaseMeta:           baseMeta,
			BaseCommitSeq:      state.CommitSeq,
			BaseSystemRoot:     state.SystemRootPageID,
			BaseManifestRootID: 999,
			Plan:               plan,
		}, rootIDs)
	})
	if err == nil || !strings.Contains(err.Error(), "concurrent root modification") {
		t.Fatalf("publish err=%v want concurrent root modification", err)
	}
	after := d.State()
	if after.SystemRootPageID != state.SystemRootPageID || after.CommitSeq != state.CommitSeq {
		t.Fatalf("failed publish changed backend state before=%+v after=%+v", state, after)
	}
	reopened, err := NewCollectionManager(d).OpenCollection("events")
	if err != nil {
		t.Fatalf("open collection after failed publish: %v", err)
	}
	if cfg := reopened.Meta().Options.ColumnStore; cfg == nil || cfg.ActiveManifest != nil || cfg.RecoveryAuthoritativeManifest != nil || cfg.RecoveryAuthoritativeAppliedCommandLSN != 0 {
		t.Fatalf("failed publish leaked column metadata: %+v", cfg)
	}
	if id, ok := reopened.ColumnStoreCacheIdentity(); !ok || id.ManifestRoot != 0 || id.ManifestGeneration != 0 {
		t.Fatalf("failed publish leaked root identity: %+v ok=%v", id, ok)
	}
}

func BenchmarkColumnPublishPlanM10A(b *testing.B) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	b.Run("disabled_hook", func(b *testing.B) {
		input := ColumnPublishPlanInput{Collection: "events", ColumnStore: nil}
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			plan, err := BuildColumnPublishPlan(input)
			if err != nil {
				b.Fatal(err)
			}
			if plan.Enabled {
				b.Fatal(plan)
			}
		}
	})
	b.Run("enabled_skeleton", func(b *testing.B) {
		cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
		if err != nil {
			b.Fatal(err)
		}
		assets := []ColumnPreparedAsset{asset}
		prepared := ColumnPublishPreparedAssets{Assets: assets, RowCount: 10, ColumnPayloadBytes: asset.Bytes}
		closure := ColumnPublishDurabilityClosure{PreparedAssets: assets, RequiredAssets: 1, RequiredBytes: asset.Bytes, FlushRequired: true, SyncRequired: true}
		input := ColumnPublishPlanInput{
			Collection:            "events",
			ColumnStore:           cfg,
			ColumnStoreNormalized: true,
			Operation:             ColumnPublishOperationInsert,
			AppliedCommandLSN:     101,
			BaseManifestRootID:    44,
			Hooks: ColumnPublishPlanHooks{
				ExtractDocuments: func() error {
					return nil
				},
				EncodeDeclaredColumns: func(ColumnPublishDeclaredColumnEncodeInput) error {
					return nil
				},
				PrepareAssets: func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
					return prepared, nil
				},
				EncodeManifest: func(ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
					return ColumnPublishManifestEncodeResult{Identity: identity, ManifestBytes: 256}, nil
				},
				ValidateClosure: func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
					return closure, nil
				},
				BuildSystemDelta: func(ColumnPublishSystemDeltaInput) error {
					return nil
				},
			},
		}
		var stages ColumnPublishStageMetrics
		var last ColumnPublishPlan
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			plan, err := BuildColumnPublishPlan(input)
			if err != nil {
				b.Fatal(err)
			}
			if !plan.Enabled || plan.RequiredAssetBytes != asset.Bytes {
				b.Fatal(plan)
			}
			stages.DocumentExtraction += plan.StageMetrics.DocumentExtraction
			stages.DeclaredColumnEncoding += plan.StageMetrics.DeclaredColumnEncoding
			stages.AssetPreparation += plan.StageMetrics.AssetPreparation
			stages.AssetFlushSync += plan.StageMetrics.AssetFlushSync
			stages.ManifestEncode += plan.StageMetrics.ManifestEncode
			stages.RootDeltaConstruction += plan.StageMetrics.RootDeltaConstruction
			stages.SystemDeltaConstruction += plan.StageMetrics.SystemDeltaConstruction
			last = plan
		}
		b.ReportMetric(float64(stages.DocumentExtraction.Nanoseconds())/float64(b.N), "extract_ns/op")
		b.ReportMetric(float64(stages.DeclaredColumnEncoding.Nanoseconds())/float64(b.N), "declared_encode_ns/op")
		b.ReportMetric(float64(stages.AssetPreparation.Nanoseconds())/float64(b.N), "asset_prepare_ns/op")
		b.ReportMetric(float64(stages.AssetFlushSync.Nanoseconds())/float64(b.N), "asset_flush_sync_ns/op")
		b.ReportMetric(float64(stages.ManifestEncode.Nanoseconds())/float64(b.N), "manifest_encode_ns/op")
		b.ReportMetric(float64(stages.RootDeltaConstruction.Nanoseconds())/float64(b.N), "root_delta_ns/op")
		b.ReportMetric(float64(stages.SystemDeltaConstruction.Nanoseconds())/float64(b.N), "system_delta_ns/op")
		b.ReportMetric(float64(last.Rows), "rows/op")
		b.ReportMetric(float64(last.RequiredAssetCount), "prepared_refs/op")
		b.ReportMetric(float64(last.RequiredAssetBytes), "prepared_asset_B/op")
		b.ReportMetric(float64(last.ManifestBytes), "manifest_B/op")
	})
}

func testColumnPublishPlanInputM10A(identity ColumnManifestIdentity, asset ColumnPreparedAsset) ColumnPublishPlanInput {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		panic(err)
	}
	return ColumnPublishPlanInput{
		Collection:            "events",
		ColumnStore:           cfg,
		ColumnStoreNormalized: true,
		Operation:             ColumnPublishOperationInsert,
		AppliedCommandLSN:     101,
		BaseManifestRootID:    44,
		Hooks: ColumnPublishPlanHooks{
			PrepareAssets: func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
				return ColumnPublishPreparedAssets{Assets: []ColumnPreparedAsset{asset}, RowCount: 10, ColumnPayloadBytes: asset.Bytes}, nil
			},
			EncodeManifest: func(in ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
				return ColumnPublishManifestEncodeResult{Identity: identity, ManifestBytes: 256}, nil
			},
			ValidateClosure: func(in ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
				return ColumnPublishDurabilityClosure{PreparedAssets: []ColumnPreparedAsset{asset}, RequiredAssets: 1, RequiredBytes: asset.Bytes, FlushRequired: true, SyncRequired: true}, nil
			},
		},
	}
}

func testColumnPublishPreparedAssetM10A() ColumnPreparedAsset {
	return ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:     ColumnAssetKindTCS1PartImage,
			FileID:   7,
			Offset:   4096,
			Length:   8192,
			Checksum: 0xdecafbad,
		},
		Bytes:        8192,
		PublishID:    3,
		GenerationID: 7,
		Reason:       "m10a-test",
	}
}
