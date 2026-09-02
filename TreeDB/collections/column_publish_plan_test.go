package collections

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
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

func TestColumnPublishPlanRejectsNonEmptyDisabledColumnStoreM10A(t *testing.T) {
	cfg := testColumnStoreConfig(nil)
	cfg.Enabled = false
	input := ColumnPublishPlanInput{
		Collection:        "events",
		ColumnStore:       cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 101,
	}
	for _, normalized := range []bool{false, true} {
		input.ColumnStoreNormalized = normalized
		plan, err := BuildColumnPublishPlan(input)
		if !errors.Is(err, ErrColumnPublishPlanRequiresEnabledColumnStore) {
			t.Fatalf("BuildColumnPublishPlan normalized=%v err=%v want sentinel %v", normalized, err, ErrColumnPublishPlanRequiresEnabledColumnStore)
		}
		if plan.Enabled {
			t.Fatalf("invalid disabled column_store returned enabled plan: %+v", plan)
		}
	}
}

func TestColumnPublishPlanAllowsBenchmarkRelaxedProfileM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	cfg := testColumnStoreConfig(nil)
	cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
	input.ColumnStore = cfg
	input.ColumnStoreNormalized = false
	input.Hooks.ValidateClosure = func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
		return ColumnPublishDurabilityClosure{
			PreparedAssets: []ColumnPreparedAsset{asset},
			RequiredAssets: 1,
			RequiredBytes:  asset.Bytes,
			FlushRequired:  false,
			SyncRequired:   false,
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan benchmark-relaxed: %v", err)
	}
	if !plan.Enabled || plan.RequiredAssetBytes != asset.Bytes || plan.RequiredAssetFlush || plan.RequiredAssetSync {
		t.Fatalf("unexpected benchmark-relaxed plan: %+v", plan)
	}
}

func TestColumnPublishPlanUsesFixedWidthAssetBytesM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	asset.Bytes = 1 << 33
	asset.Ref.Length = asset.Bytes
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}

	plan, err := BuildColumnPublishPlan(testColumnPublishPlanInputM10A(identity, asset))
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan large asset: %v", err)
	}
	if plan.RequiredAssetBytes != asset.Bytes || plan.Lifecycle.PublishedBytes != asset.Bytes || plan.Lifecycle.PreparedBytes != asset.Bytes {
		t.Fatalf("large byte counts not preserved: plan=%+v asset=%+v", plan, asset)
	}
}

func TestColumnPublishPlanAllowsZeroAssetChecksumM12A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Checksum = 0
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}

	if _, err := BuildColumnPublishPlan(testColumnPublishPlanInputM10A(identity, asset)); err != nil {
		t.Fatalf("BuildColumnPublishPlan zero asset checksum: %v", err)
	}
}

func TestColumnPublishPlanRejectsUnsupportedTypedStoragePartReason1787(t *testing.T) {
	for _, kind := range []ColumnAssetKind{ColumnAssetKindTCS1PartImage, ColumnAssetKindTCS1TypedColumnPart} {
		t.Run(string(kind), func(t *testing.T) {
			asset := testColumnPublishPreparedAssetM10A()
			asset.Ref.Kind = kind
			asset.Reason = "udpate"
			asset.PartRole = ColumnManifestPartRoleDelta
			if err := validateColumnPreparedAssetForPlan(asset); err == nil || !strings.Contains(err.Error(), "unsupported column manifest part reason") {
				t.Fatalf("validateColumnPreparedAssetForPlan err=%v want unsupported reason", err)
			}
		})
	}
}

func TestColumnPublishPlanBuildsDurabilityClosureM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	wantIdentity := testColumnPublishExpectedManifestIdentityM10A(t, identity, asset)
	var called []string
	input := testColumnPublishPlanInputM10A(identity, asset)
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
		if in.Plan.UpdatedActiveManifest != wantIdentity {
			t.Fatalf("system delta saw identity %+v want %+v", in.Plan.UpdatedActiveManifest, wantIdentity)
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
	if plan.UpdatedActiveManifest != wantIdentity || plan.RecoveryAuthoritativeManifest != wantIdentity {
		t.Fatalf("unexpected manifest identities: %+v", plan)
	}
	if plan.RootDelta.RootName != collectionColumnManifestRootName("events") || plan.RootDelta.BaseRootID != 44 {
		t.Fatalf("unexpected root delta: %+v", plan.RootDelta)
	}
	if decoded, err := decodeColumnManifestIdentityRecord(plan.RootDelta.IdentityRecord[:]); err != nil || decoded.Generation != wantIdentity.Generation || decoded.Checksum != wantIdentity.Checksum {
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
}

func TestColumnManifestRootDeltaPublishInputsValidateStoredIdentityRecordM10B(t *testing.T) {
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	delta := ColumnManifestRootDelta{
		RootName:       collectionColumnManifestRootName("events"),
		BaseRootID:     44,
		StoragePolicy:  RootStorageFast,
		Identity:       identity,
		IdentityRecord: encodeColumnManifestIdentityRecordArray(identity),
	}
	delta.IdentityRecord[0] ^= 0xff

	if _, err := delta.OrderedRootDeltaPublishInput(); err == nil || !strings.Contains(err.Error(), "identity record") {
		t.Fatalf("OrderedRootDeltaPublishInput err=%v want identity record mismatch", err)
	}
	if _, cleanup, err := delta.OrderedRootDeltaBatchPublishInput(); err == nil || !strings.Contains(err.Error(), "identity record") {
		if cleanup != nil {
			cleanup()
		}
		t.Fatalf("OrderedRootDeltaBatchPublishInput err=%v want identity record mismatch", err)
	}
}

func TestColumnManifestRootDeltaPublishInputsPublishStoredIdentityRecordM10B(t *testing.T) {
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	delta := ColumnManifestRootDelta{
		RootName:       collectionColumnManifestRootName("events"),
		BaseRootID:     44,
		StoragePolicy:  RootStorageFast,
		Identity:       identity,
		IdentityRecord: encodeColumnManifestIdentityRecordArray(identity),
	}
	ordered, err := delta.OrderedRootDeltaPublishInput()
	if err != nil {
		t.Fatalf("OrderedRootDeltaPublishInput: %v", err)
	}
	got := readColumnManifestIdentityRecordIteratorM10B(t, ordered.Iter)
	if !bytes.Equal(got, delta.IdentityRecord[:]) {
		t.Fatalf("delta iterator identity record=%x want %x", got, delta.IdentityRecord)
	}
	batched, cleanup, err := delta.OrderedRootDeltaBatchPublishInput()
	if err != nil {
		t.Fatalf("OrderedRootDeltaBatchPublishInput: %v", err)
	}
	defer cleanup()
	var (
		batchKeys    []string
		batchRecords [][]byte
	)
	if err := batched.Delta.Replay(func(entry batch.Entry) error {
		batchKeys = append(batchKeys, string(entry.Key))
		batchRecords = append(batchRecords, append([]byte(nil), entry.Value...))
		return nil
	}); err != nil {
		t.Fatalf("Replay delta batch: %v", err)
	}
	if len(batchRecords) != 1 || len(batchKeys) != 1 || batchKeys[0] != columnManifestIdentityRecordKey || !bytes.Equal(batchRecords[0], delta.IdentityRecord[:]) {
		t.Fatalf("delta batch keys=%v identity records=%x want key=%q value=%x", batchKeys, batchRecords, columnManifestIdentityRecordKey, delta.IdentityRecord)
	}
}

func readColumnManifestIdentityRecordIteratorM10B(t *testing.T, it iterator.UnsafeIterator) []byte {
	t.Helper()
	if it == nil {
		t.Fatal("nil iterator")
	}
	defer func() { _ = it.Close() }()
	it.Seek([]byte(columnManifestIdentityRecordKey))
	if !it.Valid() {
		t.Fatal("identity iterator missing record")
	}
	if got := string(it.Key()); got != columnManifestIdentityRecordKey {
		t.Fatalf("identity iterator key=%q want %q", got, columnManifestIdentityRecordKey)
	}
	value := it.ValueCopy(nil)
	if err := it.Error(); err != nil {
		t.Fatalf("identity iterator error: %v", err)
	}
	return value
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
			name: "root delta failure",
			configure: func(input *ColumnPublishPlanInput, calls *[]string) {
				input.Hooks.BuildRootDelta = func(ColumnPublishRootDeltaInput) (ColumnManifestRootDelta, error) {
					*calls = append(*calls, "root_delta")
					return ColumnManifestRootDelta{}, errStage
				}
			},
			wantCalls: "extract,encode_columns,prepare_assets,manifest_encode,closure_validation,root_delta",
		},
		{
			name: "system delta failure",
			configure: func(input *ColumnPublishPlanInput, calls *[]string) {
				input.Hooks.BuildRootDelta = func(in ColumnPublishRootDeltaInput) (ColumnManifestRootDelta, error) {
					*calls = append(*calls, "root_delta")
					return ColumnManifestRootDelta{
						RootName:       in.ColumnStore.ManifestRoot.Name,
						BaseRootID:     in.BaseManifestRootID,
						StoragePolicy:  in.ColumnStore.ManifestRoot.StoragePolicy,
						Identity:       in.Manifest.Identity,
						IdentityRecord: encodeColumnManifestIdentityRecordArray(in.Manifest.Identity),
						Records:        cloneColumnManifestRecords(in.Manifest.Records),
					}, nil
				}
				input.Hooks.BuildSystemDelta = func(ColumnPublishSystemDeltaInput) error {
					*calls = append(*calls, "system_delta")
					return errStage
				}
			},
			wantCalls: "extract,encode_columns,prepare_assets,manifest_encode,closure_validation,root_delta,system_delta",
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
				return testColumnPublishManifestResultM10A(t, in)
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

func TestColumnPublishPlanRejectsInvalidRootDeltaM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	tests := []struct {
		name      string
		mutate    func(*ColumnManifestRootDelta)
		wantError string
	}{
		{
			name: "wrong root name",
			mutate: func(delta *ColumnManifestRootDelta) {
				delta.RootName = "events/column/wrong"
			},
			wantError: "root name",
		},
		{
			name: "wrong base root",
			mutate: func(delta *ColumnManifestRootDelta) {
				delta.BaseRootID++
			},
			wantError: "base root id",
		},
		{
			name: "wrong storage policy",
			mutate: func(delta *ColumnManifestRootDelta) {
				delta.StoragePolicy = RootStorageCompressed
			},
			wantError: "storage policy",
		},
		{
			name: "wrong identity",
			mutate: func(delta *ColumnManifestRootDelta) {
				delta.Identity.Generation++
			},
			wantError: "identity",
		},
		{
			name: "wrong identity record",
			mutate: func(delta *ColumnManifestRootDelta) {
				delta.IdentityRecord[0] ^= 0xff
			},
			wantError: "identity record",
		},
		{
			name: "missing manifest records",
			mutate: func(delta *ColumnManifestRootDelta) {
				delta.Records = nil
			},
			wantError: "manifest records omitted",
		},
		{
			name: "omitted manifest mutation",
			mutate: func(delta *ColumnManifestRootDelta) {
				delta.MutationDelta = true
				delta.Mutations = nil
			},
			wantError: "mutation delta does not produce logical post-state",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := testColumnPublishPlanInputM10A(identity, asset)
			input.Hooks.BuildRootDelta = func(in ColumnPublishRootDeltaInput) (ColumnManifestRootDelta, error) {
				delta := ColumnManifestRootDelta{
					RootName:       in.ColumnStore.ManifestRoot.Name,
					BaseRootID:     in.BaseManifestRootID,
					StoragePolicy:  in.ColumnStore.ManifestRoot.StoragePolicy,
					Identity:       in.Manifest.Identity,
					IdentityRecord: encodeColumnManifestIdentityRecordArray(in.Manifest.Identity),
					Records:        cloneColumnManifestRecords(in.Manifest.Records),
				}
				tt.mutate(&delta)
				return delta, nil
			}

			plan, err := BuildColumnPublishPlan(input)
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("BuildColumnPublishPlan err=%v want %q", err, tt.wantError)
			}
			if plan.Enabled {
				t.Fatalf("invalid root delta returned enabled plan: %+v", plan)
			}
		})
	}
}

func TestColumnPublishPlanRejectsNegativeManifestBytesM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 8, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xbeefcafe}
	input := testColumnPublishPlanInputM10A(identity, asset)
	input.Hooks.EncodeManifest = func(ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
		return ColumnPublishManifestEncodeResult{Identity: identity, ManifestBytes: -1}, nil
	}
	plan, err := BuildColumnPublishPlan(input)
	if err == nil || !strings.Contains(err.Error(), "manifest byte count") {
		t.Fatalf("BuildColumnPublishPlan err=%v want negative manifest byte count rejection", err)
	}
	if plan.Enabled {
		t.Fatalf("negative manifest bytes returned enabled plan: %+v", plan)
	}
}

func TestColumnPublishPlanRejectsPreparedAssetByteOverflowM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	hugeAsset := asset
	hugeAsset.Ref.FileID = 8
	hugeAsset.Ref.PartID = 2
	hugeAsset.Ref.Length = math.MaxInt64
	hugeAsset.Bytes = math.MaxInt64
	oneByteAsset := asset
	oneByteAsset.Ref.FileID = 9
	oneByteAsset.Ref.PartID = 3
	oneByteAsset.Ref.Length = 1
	oneByteAsset.Bytes = 1

	identity := ColumnManifestIdentity{Generation: 9, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xbeefcaf1}
	input := testColumnPublishPlanInputM10A(identity, hugeAsset)
	input.Hooks.PrepareAssets = func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
		return ColumnPublishPreparedAssets{
			Assets:             []ColumnPreparedAsset{hugeAsset, oneByteAsset},
			RowCount:           2,
			ColumnPayloadBytes: math.MaxInt64,
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err == nil || !strings.Contains(err.Error(), "asset[1] bytes overflow") {
		t.Fatalf("BuildColumnPublishPlan err=%v want indexed asset byte overflow", err)
	}
	if plan.Enabled {
		t.Fatalf("overflowing prepared bytes returned enabled plan: %+v", plan)
	}
}

func TestColumnPublishPlanRejectsUnsupportedAssetKindM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	asset.Ref.Kind = ColumnAssetKind("future-kind")
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}

	plan, err := BuildColumnPublishPlan(testColumnPublishPlanInputM10A(identity, asset))
	if err == nil || !strings.Contains(err.Error(), "prepared asset[0]") || !strings.Contains(err.Error(), "unsupported column asset ref kind") {
		t.Fatalf("BuildColumnPublishPlan err=%v want indexed unsupported kind", err)
	}
	if plan.Enabled {
		t.Fatalf("unsupported asset kind returned enabled plan: %+v", plan)
	}
}

func TestColumnPublishPlanRejectsClosurePreparedAssetMismatchM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	closureAsset := asset
	closureAsset.Ref.Offset += int64(asset.Bytes)
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	input.Hooks.ValidateClosure = func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
		return ColumnPublishDurabilityClosure{
			PreparedAssets: []ColumnPreparedAsset{closureAsset},
			RequiredAssets: 1,
			RequiredBytes:  closureAsset.Bytes,
			FlushRequired:  true,
			SyncRequired:   true,
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err == nil || !strings.Contains(err.Error(), "does not match manifest prepared asset") {
		t.Fatalf("BuildColumnPublishPlan err=%v want closure prepared asset mismatch", err)
	}
	if plan.Enabled {
		t.Fatalf("mismatched closure prepared asset returned enabled plan: %+v", plan)
	}
}

func TestColumnPublishPlanReportsClosureAssetValidationBeforeByteSumM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	invalid := asset
	invalid.Ref.Kind = ColumnAssetKind("future-kind")
	invalid.Ref.Length = math.MaxInt64
	invalid.Bytes = math.MaxInt64
	overflow := asset
	overflow.Ref.FileID = 8
	overflow.Ref.PartID = 2
	overflow.Ref.Offset = math.MaxInt64
	overflow.Ref.Length = 1
	overflow.Bytes = 1
	input.Hooks.ValidateClosure = func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
		return ColumnPublishDurabilityClosure{
			PreparedAssets: []ColumnPreparedAsset{invalid, overflow},
			RequiredAssets: 2,
			RequiredBytes:  math.MaxInt64,
			FlushRequired:  true,
			SyncRequired:   true,
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err == nil || !strings.Contains(err.Error(), "closure asset[0]") || !strings.Contains(err.Error(), "unsupported column asset ref kind") {
		t.Fatalf("BuildColumnPublishPlan err=%v want closure asset validation before byte sum", err)
	}
	if plan.Enabled {
		t.Fatalf("invalid closure asset returned enabled plan: %+v", plan)
	}
}

func TestColumnPublishPlanAllowsReorderedClosurePreparedAssetsM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	second := asset
	second.Ref.FileID = 8
	second.Ref.PartID = 2
	second.Ref.Offset += asset.Bytes
	second.PublishID++
	second.GenerationID++
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	input.Hooks.PrepareAssets = func(ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
		return ColumnPublishPreparedAssets{Assets: []ColumnPreparedAsset{asset, second}, RowCount: 20, ColumnPayloadBytes: asset.Bytes + second.Bytes}, nil
	}
	input.Hooks.ValidateClosure = func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
		return ColumnPublishDurabilityClosure{
			PreparedAssets: []ColumnPreparedAsset{second, asset},
			RequiredAssets: 2,
			RequiredBytes:  asset.Bytes + second.Bytes,
			FlushRequired:  true,
			SyncRequired:   true,
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan reordered closure assets: %v", err)
	}
	if len(plan.PreparedAssets) != 2 || plan.PreparedAssets[0] != asset || plan.PreparedAssets[1] != second {
		t.Fatalf("plan prepared assets should retain manifest order: %+v", plan.PreparedAssets)
	}
}

func TestColumnPublishPlanAllowsClosurePreparedAssetReasonMismatchM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	asset.Reason = string(ColumnPublishOperationInsert)
	closureAsset := asset
	closureAsset.Reason = string(ColumnPublishOperationUpdate)
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	input.Hooks.ValidateClosure = func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
		return ColumnPublishDurabilityClosure{
			PreparedAssets: []ColumnPreparedAsset{closureAsset},
			RequiredAssets: 1,
			RequiredBytes:  closureAsset.Bytes,
			FlushRequired:  true,
			SyncRequired:   true,
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan reason-only closure asset mismatch: %v", err)
	}
	if len(plan.PreparedAssets) != 1 || plan.PreparedAssets[0] != asset {
		t.Fatalf("plan prepared asset should retain manifest asset including reason: %+v", plan.PreparedAssets)
	}
}

func TestColumnPublishPlanCopiesPreparedAssetsForManifestHookM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	input.Hooks.ValidateClosure = nil
	var hookAssets []ColumnPreparedAsset
	input.Hooks.EncodeManifest = func(in ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
		hookAssets = in.Prepared.Assets
		in.Prepared.Assets[0].Ref.Offset += int64(asset.Bytes)
		return testColumnPublishManifestResultM10A(t, in)
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	if len(plan.PreparedAssets) != 1 {
		t.Fatalf("prepared assets=%d want 1", len(plan.PreparedAssets))
	}
	if plan.PreparedAssets[0] != asset {
		t.Fatalf("plan prepared asset changed after manifest hook mutation: got %+v want %+v", plan.PreparedAssets[0], asset)
	}
	hookAssets[0].Ref.Offset += int64(asset.Bytes)
	if plan.PreparedAssets[0] != asset {
		t.Fatalf("plan prepared asset changed after manifest hook-owned slice mutation: got %+v want %+v", plan.PreparedAssets[0], asset)
	}
}

func TestColumnPublishPlanSnapshotsPreparedAssetsForClosureValidationM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	input.Hooks.ValidateClosure = func(in ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
		in.Prepared.Assets[0].Ref.Offset += int64(asset.Bytes)
		return ColumnPublishDurabilityClosure{
			PreparedAssets: in.Prepared.Assets,
			RequiredAssets: len(in.Prepared.Assets),
			RequiredBytes:  mustSumColumnPreparedAssetBytes(t, in.Prepared.Assets),
			FlushRequired:  true,
			SyncRequired:   true,
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err == nil || !strings.Contains(err.Error(), "does not match manifest prepared asset") {
		t.Fatalf("BuildColumnPublishPlan err=%v want closure mutation mismatch", err)
	}
	if plan.Enabled {
		t.Fatalf("mutated closure prepared asset returned enabled plan: %+v", plan)
	}
}

func TestColumnPublishPlanCopiesCurrentManifestForHooksM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	current := ColumnManifestIdentity{Generation: 6, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabc123}
	input := testColumnPublishPlanInputM10A(identity, asset)
	input.CurrentManifest = &current
	var prepareSeen, manifestSeen ColumnManifestIdentity
	input.Hooks.PrepareAssets = func(in ColumnPublishAssetPrepareInput) (ColumnPublishPreparedAssets, error) {
		if in.CurrentManifest == nil {
			t.Fatal("PrepareAssets CurrentManifest is nil")
		}
		prepareSeen = *in.CurrentManifest
		in.CurrentManifest.Generation = 99
		return ColumnPublishPreparedAssets{Assets: []ColumnPreparedAsset{asset}, RowCount: 10, ColumnPayloadBytes: asset.Bytes}, nil
	}
	input.Hooks.EncodeManifest = func(in ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
		if in.CurrentManifest == nil {
			t.Fatal("EncodeManifest CurrentManifest is nil")
		}
		manifestSeen = *in.CurrentManifest
		manifest, err := testColumnPublishManifestResultM10A(t, in)
		in.CurrentManifest.Generation = 100
		return manifest, err
	}

	if _, err := BuildColumnPublishPlan(input); err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	if prepareSeen != current || manifestSeen != current {
		t.Fatalf("hooks saw mutated current manifest: prepare=%+v manifest=%+v want=%+v", prepareSeen, manifestSeen, current)
	}
	if *input.CurrentManifest != current {
		t.Fatalf("input CurrentManifest mutated: got %+v want %+v", *input.CurrentManifest, current)
	}
}

func TestColumnPublishPlanCopiesClosurePreparedAssetsM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	var closureAssets []ColumnPreparedAsset
	input.Hooks.ValidateClosure = func(in ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
		closureAssets = in.Prepared.Assets
		return ColumnPublishDurabilityClosure{
			PreparedAssets: closureAssets,
			RequiredAssets: len(closureAssets),
			RequiredBytes:  mustSumColumnPreparedAssetBytes(t, closureAssets),
			FlushRequired:  true,
			SyncRequired:   true,
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	if len(plan.PreparedAssets) != 1 {
		t.Fatalf("prepared assets=%d want 1", len(plan.PreparedAssets))
	}
	closureAssets[0].Ref.Offset += int64(asset.Bytes)
	if plan.PreparedAssets[0] != asset {
		t.Fatalf("plan prepared asset changed after closure-owned slice mutation: got %+v want %+v", plan.PreparedAssets[0], asset)
	}
}

func TestColumnPublishPlanCopiesClosureForRootDeltaHookM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	var hookAssets []ColumnPreparedAsset
	input.Hooks.BuildRootDelta = func(in ColumnPublishRootDeltaInput) (ColumnManifestRootDelta, error) {
		hookAssets = in.Closure.PreparedAssets
		in.Closure.PreparedAssets[0].Ref.Offset += int64(asset.Bytes)
		return ColumnManifestRootDelta{
			RootName:       in.ColumnStore.ManifestRoot.Name,
			BaseRootID:     in.BaseManifestRootID,
			StoragePolicy:  in.ColumnStore.ManifestRoot.StoragePolicy,
			Identity:       in.Manifest.Identity,
			IdentityRecord: encodeColumnManifestIdentityRecordArray(in.Manifest.Identity),
			Records:        cloneColumnManifestRecords(in.Manifest.Records),
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	if len(plan.PreparedAssets) != 1 {
		t.Fatalf("prepared assets=%d want 1", len(plan.PreparedAssets))
	}
	if plan.PreparedAssets[0] != asset {
		t.Fatalf("plan prepared asset changed after root hook mutation: got %+v want %+v", plan.PreparedAssets[0], asset)
	}
	hookAssets[0].Ref.Offset += int64(asset.Bytes)
	if plan.PreparedAssets[0] != asset {
		t.Fatalf("plan prepared asset changed after root hook-owned slice mutation: got %+v want %+v", plan.PreparedAssets[0], asset)
	}
}

func TestColumnPublishPlanPassesHookOwnedColumnStoreConfigM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	if input.ColumnStore.ManifestRoot == nil || len(input.ColumnStore.Columns) == 0 {
		t.Fatalf("test requires normalized manifest root and columns: %+v", input.ColumnStore)
	}
	rootName := input.ColumnStore.ManifestRoot.Name
	storagePolicy := input.ColumnStore.ManifestRoot.StoragePolicy
	firstColumn := input.ColumnStore.Columns[0].Name
	input.Hooks.BuildRootDelta = func(in ColumnPublishRootDeltaInput) (ColumnManifestRootDelta, error) {
		in.ColumnStore.ManifestRoot.Name = "events/corrupted-column-manifest"
		in.ColumnStore.ManifestRoot.StoragePolicy = RootStoragePolicy("corrupted")
		in.ColumnStore.Columns[0].Name = "corrupted"
		return ColumnManifestRootDelta{
			RootName:       rootName,
			BaseRootID:     in.BaseManifestRootID,
			StoragePolicy:  storagePolicy,
			Identity:       in.Manifest.Identity,
			IdentityRecord: encodeColumnManifestIdentityRecordArray(in.Manifest.Identity),
			Records:        cloneColumnManifestRecords(in.Manifest.Records),
		}, nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	if plan.ManifestRootName != rootName || plan.RootDelta.RootName != rootName {
		t.Fatalf("hook-owned config mutation leaked into root names: plan=%+v root=%q", plan, rootName)
	}
	if input.ColumnStore.ManifestRoot.Name != rootName || input.ColumnStore.ManifestRoot.StoragePolicy != storagePolicy {
		t.Fatalf("input manifest root mutated by hook: %+v", input.ColumnStore.ManifestRoot)
	}
	if input.ColumnStore.Columns[0].Name != firstColumn {
		t.Fatalf("input columns mutated by hook: got %q want %q", input.ColumnStore.Columns[0].Name, firstColumn)
	}
}

func TestColumnPublishPlanCopiesPreparedAssetsForSystemDeltaHookM10A(t *testing.T) {
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	input := testColumnPublishPlanInputM10A(identity, asset)
	var hookAssets []ColumnPreparedAsset
	input.Hooks.BuildSystemDelta = func(in ColumnPublishSystemDeltaInput) error {
		hookAssets = in.Plan.PreparedAssets
		in.Plan.PreparedAssets[0].Ref.Offset += int64(asset.Bytes)
		return nil
	}

	plan, err := BuildColumnPublishPlan(input)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	if len(plan.PreparedAssets) != 1 {
		t.Fatalf("prepared assets=%d want 1", len(plan.PreparedAssets))
	}
	if plan.PreparedAssets[0] != asset {
		t.Fatalf("plan prepared asset changed after hook mutation: got %+v want %+v", plan.PreparedAssets[0], asset)
	}
	hookAssets[0].Ref.Offset += int64(asset.Bytes)
	if plan.PreparedAssets[0] != asset {
		t.Fatalf("plan prepared asset changed after hook-owned slice mutation: got %+v want %+v", plan.PreparedAssets[0], asset)
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
	identity = plan.UpdatedActiveManifest
	plan.UpdatedActiveManifest.Format = ""
	plan.UpdatedActiveManifest.Version = 0
	plan.RecoveryAuthoritativeManifest.Format = ""
	plan.RecoveryAuthoritativeManifest.Version = 0
	plan.RecoveryAuthoritativeAppliedCommandLSN = 202
	if plan.RecoveryAuthoritativeAppliedCommandLSN == plan.AppliedCommandLSN {
		t.Fatalf("test requires distinct applied and recovery-authoritative LSNs: %+v", plan)
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
	if cfg.RecoveryAuthoritativeManifest == nil || *cfg.RecoveryAuthoritativeManifest != identity || cfg.RecoveryAuthoritativeAppliedCommandLSN != plan.RecoveryAuthoritativeAppliedCommandLSN {
		t.Fatalf("recovery-authoritative metadata not updated atomically: %+v", cfg)
	}
	cacheID, ok := reopened.ColumnStoreCacheIdentity()
	if !ok || cacheID.ManifestRoot != rootIDs[0] || cacheID.ManifestGeneration != identity.Generation || cacheID.RecoveryAuthoritativeAppliedCommandLSN != plan.RecoveryAuthoritativeAppliedCommandLSN {
		t.Fatalf("unexpected cache identity: %+v ok=%v rootIDs=%v", cacheID, ok, rootIDs)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	raw, ok, err := getSystemValue(snap, systemCollectionMetaKey("events"))
	if err != nil || !ok {
		t.Fatalf("load raw collection metadata: ok=%v err=%v", ok, err)
	}
	var disk collectionMetaDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatalf("decode raw collection metadata: %v", err)
	}
	rawCfg := disk.Options.ColumnStore
	if rawCfg == nil || rawCfg.ActiveManifest == nil || *rawCfg.ActiveManifest != identity {
		t.Fatalf("raw active manifest was not stored normalized: %+v", rawCfg)
	}
	if rawCfg.RecoveryAuthoritativeManifest == nil || *rawCfg.RecoveryAuthoritativeManifest != identity {
		t.Fatalf("raw recovery-authoritative manifest was not stored normalized: %+v", rawCfg)
	}
}

func TestColumnManifestPublishSystemDeltaRejectsInvalidRootIDsM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(
		ColumnManifestIdentity{Generation: 15, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcba},
		testColumnPublishPreparedAssetM10A(),
	)
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	state := d.State()

	for _, rootIDs := range [][]uint64{nil, []uint64{}, []uint64{0}} {
		iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
			BaseMeta:           col.Meta(),
			BaseCommitSeq:      state.CommitSeq,
			BaseSystemRoot:     state.SystemRootPageID,
			BaseManifestRootID: 0,
			Plan:               plan,
		}, rootIDs)
		if iter != nil {
			_ = iter.Close()
			t.Fatalf("rootIDs=%v returned iterator with err=%v", rootIDs, err)
		}
		if err == nil {
			t.Fatalf("rootIDs=%v err=nil, want invalid rootIDs rejection", rootIDs)
		}
	}
}

func TestColumnManifestPublishSystemDeltaDoesNotReadPublishedRootBeforeCommitM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(
		ColumnManifestIdentity{Generation: 15, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcbd},
		testColumnPublishPreparedAssetM10A(),
	)
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	state := d.State()

	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           col.Meta(),
		BaseCommitSeq:      state.CommitSeq,
		BaseSystemRoot:     state.SystemRootPageID,
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123456789})
	if err != nil {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator: %v", err)
	}
	if iter == nil {
		t.Fatal("buildColumnManifestPublishSystemDeltaIterator returned nil iterator")
	}
	_ = iter.Close()
}

func TestColumnManifestPublishSystemDeltaRejectsMissingLSNM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(
		ColumnManifestIdentity{Generation: 16, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcbb},
		testColumnPublishPreparedAssetM10A(),
	)
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*ColumnPublishPlan)
		want   string
	}{
		{
			name: "applied",
			mutate: func(plan *ColumnPublishPlan) {
				plan.AppliedCommandLSN = 0
			},
			want: "AppliedCommandLSN",
		},
		{
			name: "recovery authoritative",
			mutate: func(plan *ColumnPublishPlan) {
				plan.RecoveryAuthoritativeAppliedCommandLSN = 0
			},
			want: "recovery-authoritative AppliedCommandLSN",
		},
		{
			name: "recovery authoritative below applied",
			mutate: func(plan *ColumnPublishPlan) {
				plan.AppliedCommandLSN = 203
				plan.RecoveryAuthoritativeAppliedCommandLSN = 202
			},
			want: "recovery-authoritative AppliedCommandLSN regression",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutated := plan
			tt.mutate(&mutated)
			iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
				BaseMeta:           col.Meta(),
				BaseManifestRootID: 0,
				Plan:               mutated,
			}, []uint64{123})
			if iter != nil {
				_ = iter.Close()
				t.Fatalf("returned iterator with err=%v", err)
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want %q", err, tt.want)
			}
		})
	}
}

func TestColumnManifestPublishSystemDeltaRejectsRecoveryLSNRegressionM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(
		ColumnManifestIdentity{Generation: 17, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcbc},
		testColumnPublishPreparedAssetM10A(),
	)
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}

	baseMeta := col.Meta()
	cfg := baseMeta.Options.ColumnStore.copy()
	active := ColumnManifestIdentity{Generation: 16, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcba}
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &active
	cfg.RecoveryAuthoritativeAppliedCommandLSN = plan.RecoveryAuthoritativeAppliedCommandLSN + 1
	baseMeta.Options.ColumnStore = &cfg

	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           baseMeta,
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123})
	if iter != nil {
		_ = iter.Close()
		t.Fatalf("returned iterator with err=%v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "recovery-authoritative AppliedCommandLSN regression") {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want recovery-authoritative LSN regression", err)
	}
}

func TestColumnManifestPublishSystemDeltaRejectsAppliedLSNBelowBaseRecoveryM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(
		ColumnManifestIdentity{Generation: 18, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcbe},
		testColumnPublishPreparedAssetM10A(),
	)
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	plan.AppliedCommandLSN = 90
	plan.RecoveryAuthoritativeAppliedCommandLSN = 100

	baseMeta := col.Meta()
	cfg := baseMeta.Options.ColumnStore.copy()
	active := ColumnManifestIdentity{Generation: 17, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcbd}
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &active
	cfg.RecoveryAuthoritativeAppliedCommandLSN = 100
	baseMeta.Options.ColumnStore = &cfg

	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           baseMeta,
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123})
	if iter != nil {
		_ = iter.Close()
		t.Fatalf("returned iterator with err=%v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "AppliedCommandLSN regression") {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want AppliedCommandLSN regression", err)
	}
}

func TestColumnManifestPublishSystemDeltaRejectsNonAdvancingGenerationM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(
		ColumnManifestIdentity{Generation: 18, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcbf},
		testColumnPublishPreparedAssetM10A(),
	)
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}

	baseMeta := col.Meta()
	cfg := baseMeta.Options.ColumnStore.copy()
	active := ColumnManifestIdentity{Generation: plan.UpdatedActiveManifest.Generation, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcddcbe}
	cfg.ActiveManifest = &active
	cfg.RecoveryAuthoritativeManifest = &active
	cfg.RecoveryAuthoritativeAppliedCommandLSN = plan.AppliedCommandLSN
	baseMeta.Options.ColumnStore = &cfg

	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           baseMeta,
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123})
	if iter != nil {
		_ = iter.Close()
		t.Fatalf("returned iterator with err=%v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "manifest generation regression") {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want generation regression", err)
	}
}

func TestColumnManifestPublishSystemDeltaRejectsForeignCollectionM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 11, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef1234}, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	plan.Collection = "other"
	plan.ManifestRootName = collectionColumnManifestRootName("other")
	plan.RootDelta.RootName = plan.ManifestRootName
	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           col.Meta(),
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123})
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "does not match collection") {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want collection/root mismatch", err)
	}
}

func TestColumnManifestPublishSystemDeltaRevalidatesRootStoragePolicyM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 11, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef1234}, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	plan.RootDelta.StoragePolicy = RootStoragePolicy("corrupted")
	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           col.Meta(),
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123})
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "unsupported root storage policy") {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want storage policy rejection", err)
	}
}

func TestColumnManifestPublishSystemDeltaRejectsStalePlanBaseM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 12, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef5678}, testColumnPublishPreparedAssetM10A())
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	if plan.ManifestRootBaseID == 0 || plan.RootDelta.BaseRootID == 0 {
		t.Fatalf("test requires non-zero stale plan base: %+v", plan)
	}
	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           col.Meta(),
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123})
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "concurrent root modification") {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want stale plan base rejection", err)
	}
}

func TestColumnManifestPublishSystemDeltaRejectsStaleSnapshotBaseM10A(t *testing.T) {
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
	baseState := d.State()
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "other"}); err != nil {
		t.Fatalf("create other collection: %v", err)
	}
	if state := d.State(); state.CommitSeq == baseState.CommitSeq || state.SystemRootPageID == baseState.SystemRootPageID {
		t.Fatalf("test requires collection create to advance commit/root: before=%+v after=%+v", baseState, state)
	}

	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 12, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef5678}, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           baseMeta,
		BaseCommitSeq:      baseState.CommitSeq,
		BaseSystemRoot:     baseState.SystemRootPageID,
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123})
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want stale snapshot base rejection", err)
	}
}

func TestColumnManifestPublishSystemDeltaRejectsMissingSnapshotBaseM10A(t *testing.T) {
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
	baseState := d.State()
	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 12, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef6789}, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}

	tests := []struct {
		name           string
		baseCommitSeq  uint64
		baseSystemRoot uint64
	}{
		{name: "missing commit", baseSystemRoot: baseState.SystemRootPageID},
		{name: "missing root", baseCommitSeq: baseState.CommitSeq},
		{name: "missing both"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
				BaseMeta:           col.Meta(),
				BaseCommitSeq:      tt.baseCommitSeq,
				BaseSystemRoot:     tt.baseSystemRoot,
				BaseManifestRootID: 0,
				Plan:               plan,
			}, []uint64{123})
			if iter != nil {
				_ = iter.Close()
				t.Fatalf("returned iterator with err=%v", err)
			}
			if err == nil || !strings.Contains(err.Error(), "requires BaseCommitSeq and BaseSystemRoot") {
				t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want missing snapshot base rejection", err)
			}
		})
	}
}

func TestColumnManifestPublishSystemDeltaRejectsIdentityMismatchM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 13, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef9012}, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	plan.UpdatedActiveManifest.Generation++
	iter, err := col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
		BaseMeta:           col.Meta(),
		BaseManifestRootID: 0,
		Plan:               plan,
	}, []uint64{123})
	if iter != nil {
		_ = iter.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "active manifest identity") {
		t.Fatalf("buildColumnManifestPublishSystemDeltaIterator err=%v want active manifest identity mismatch", err)
	}
}

func TestColumnManifestPublishSystemDeltaDefersPublishedRootMismatchToOpenM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 14, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef3456}, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}

	wrongDelta := plan.RootDelta
	wrongDelta.Identity = ColumnManifestIdentity{Generation: 99, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1111222233334444}
	wrongDelta.IdentityRecord = encodeColumnManifestIdentityRecordArray(wrongDelta.Identity)
	ordered, err := wrongDelta.OrderedRootPublishInput()
	if err != nil {
		t.Fatalf("OrderedRootPublishInput: %v", err)
	}
	_, _, err = d.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{ordered}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
			BaseMeta:           baseMeta,
			BaseCommitSeq:      state.CommitSeq,
			BaseSystemRoot:     state.SystemRootPageID,
			BaseManifestRootID: 0,
			Plan:               plan,
		}, rootIDs)
	})
	if err != nil {
		t.Fatalf("PublishOrderedRootGroupWithSystemBuilder: %v", err)
	}
	if _, err := NewCollectionManager(d).OpenCollection("events"); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("OpenCollection err=%v want committed root identity mismatch", err)
	}
}

func TestColumnManifestPublishSystemDeltaDefersMalformedPublishedRootIdentityToOpenM10A(t *testing.T) {
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
	planInput := testColumnPublishPlanInputM10A(ColumnManifestIdentity{Generation: 14, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xabcdef3456}, testColumnPublishPreparedAssetM10A())
	planInput.BaseManifestRootID = 0
	plan, err := BuildColumnPublishPlan(planInput)
	if err != nil {
		t.Fatalf("BuildColumnPublishPlan: %v", err)
	}
	rootTable, err := memtable.NewWithCapacityMode(1, memtable.ModeHashSorted)
	if err != nil {
		t.Fatalf("new root memtable: %v", err)
	}
	rootTable.Set([]byte(columnManifestIdentityRecordKey), []byte{1, 2, 3})
	rootTable.Freeze()

	_, _, err = d.PublishOrderedRootGroupWithSystemBuilder([]backenddb.OrderedRootPublishInput{{
		BaseRoot: 0,
		Iter:     rootTable.NewIterator(nil, nil),
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return col.buildColumnManifestPublishSystemDeltaIterator(ColumnManifestPublishSystemDeltaInput{
			BaseMeta:           baseMeta,
			BaseCommitSeq:      state.CommitSeq,
			BaseSystemRoot:     state.SystemRootPageID,
			BaseManifestRootID: 0,
			Plan:               plan,
		}, rootIDs)
	})
	if err != nil {
		t.Fatalf("PublishOrderedRootGroupWithSystemBuilder: %v", err)
	}
	_, err = NewCollectionManager(d).OpenCollection("events")
	if err == nil ||
		!errors.Is(err, ErrColumnManifestIdentityMalformed) ||
		!strings.Contains(err.Error(), "invalid identity record") ||
		!strings.Contains(err.Error(), "length=") {
		t.Fatalf("OpenCollection err=%v want malformed committed root identity", err)
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
	if id, ok := reopened.ColumnStoreCacheIdentity(); !ok || id.ManifestRoot != 0 || id.ManifestGeneration != 0 || id.RecoveryAuthoritativeAppliedCommandLSN != 0 {
		t.Fatalf("failed publish leaked root identity: %+v ok=%v", id, ok)
	}
}

func BenchmarkColumnPublishPlanM10A(b *testing.B) {
	asset := testColumnPublishPreparedAssetM10A()
	b.Run("disabled_hook", func(b *testing.B) {
		input := ColumnPublishPlanInput{Collection: "events", ColumnStore: nil}
		b.ReportAllocs()
		b.ResetTimer()
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
				EncodeManifest: func(in ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
					return testColumnPublishManifestResultM10A(b, in)
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
		b.ResetTimer()
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
			stages.AssetClosureValidation += plan.StageMetrics.AssetClosureValidation
			stages.ManifestEncode += plan.StageMetrics.ManifestEncode
			stages.RootDeltaConstruction += plan.StageMetrics.RootDeltaConstruction
			stages.SystemDeltaConstruction += plan.StageMetrics.SystemDeltaConstruction
			last = plan
		}
		b.StopTimer()
		b.ReportMetric(float64(stages.DocumentExtraction.Nanoseconds())/float64(b.N), "extract_ns/op")
		b.ReportMetric(float64(stages.DeclaredColumnEncoding.Nanoseconds())/float64(b.N), "declared_encode_ns/op")
		b.ReportMetric(float64(stages.AssetPreparation.Nanoseconds())/float64(b.N), "asset_prepare_ns/op")
		b.ReportMetric(float64(stages.AssetClosureValidation.Nanoseconds())/float64(b.N), "asset_closure_validation_ns/op")
		b.ReportMetric(float64(stages.ManifestEncode.Nanoseconds())/float64(b.N), "manifest_encode_ns/op")
		b.ReportMetric(float64(stages.RootDeltaConstruction.Nanoseconds())/float64(b.N), "root_delta_ns/op")
		b.ReportMetric(float64(stages.SystemDeltaConstruction.Nanoseconds())/float64(b.N), "system_delta_ns/op")
		b.ReportMetric(float64(last.Rows), "rows/op")
		b.ReportMetric(float64(last.RequiredAssetCount), "required_refs/op")
		b.ReportMetric(float64(last.RequiredAssetBytes), "required_asset_B/op")
		b.ReportMetric(float64(last.ManifestBytes), "manifest_B/op")
	})
}

func testColumnPublishPlanInputM10A(identity ColumnManifestIdentity, asset ColumnPreparedAsset) ColumnPublishPlanInput {
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		panic(err)
	}
	asset = testColumnPublishPreparedAssetForIdentityM10A(asset, identity)
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
				return encodeColumnManifestForWrite(in)
			},
			ValidateClosure: func(in ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
				return ColumnPublishDurabilityClosure{PreparedAssets: []ColumnPreparedAsset{asset}, RequiredAssets: 1, RequiredBytes: asset.Bytes, FlushRequired: true, SyncRequired: true}, nil
			},
		},
	}
}

func testColumnPublishPreparedAssetForIdentityM10A(asset ColumnPreparedAsset, identity ColumnManifestIdentity) ColumnPreparedAsset {
	if identity.Generation != 0 {
		asset.Ref.Generation = identity.Generation
		asset.GenerationID = identity.Generation
	}
	return asset
}

func testColumnPublishManifestResultM10A(t testing.TB, in ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
	t.Helper()
	manifest, err := encodeColumnManifestForWrite(in)
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	return manifest, nil
}

func testColumnPublishExpectedManifestIdentityM10A(t testing.TB, identity ColumnManifestIdentity, asset ColumnPreparedAsset) ColumnManifestIdentity {
	t.Helper()
	cfg, err := normalizeColumnStoreConfig("events", testColumnStoreConfig(nil))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	asset = testColumnPublishPreparedAssetForIdentityM10A(asset, identity)
	manifest, err := encodeColumnManifestForWrite(ColumnPublishManifestEncodeInput{
		Collection:        "events",
		ColumnStore:       *cfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 101,
		Prepared: ColumnPublishPreparedAssets{
			Assets:             []ColumnPreparedAsset{asset},
			RowCount:           10,
			ColumnPayloadBytes: asset.Bytes,
		},
	})
	if err != nil {
		t.Fatalf("encodeColumnManifestForWrite: %v", err)
	}
	return manifest.Identity
}

func mustSumColumnPreparedAssetBytes(t testing.TB, assets []ColumnPreparedAsset) int64 {
	t.Helper()
	total, err := checkedSumColumnPreparedAssetBytes(assets)
	if err != nil {
		t.Fatalf("checkedSumColumnPreparedAssetBytes: %v", err)
	}
	return total
}

func testColumnPublishPreparedAssetM10A() ColumnPreparedAsset {
	return ColumnPreparedAsset{
		Ref: ColumnAssetRef{
			Kind:       ColumnAssetKindTCS1PartImage,
			Namespace:  "events/column-assets",
			Generation: 7,
			PartID:     1,
			FileID:     7,
			Offset:     4096,
			Length:     8192,
			Checksum:   0xdecafbad,
		},
		Bytes:        8192,
		PublishID:    3,
		GenerationID: 7,
		Reason:       string(ColumnPublishOperationInsert),
	}
}
