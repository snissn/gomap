package collections

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This diagnostic exercises only the exact preparation invocation at the
// existing hook. A cloned set here is not a graph receipt, a complete cut,
// an admitted maintenance reservation, or authority for a pathname reopen.
func TestNativePreparedOutputCustodyAtExistingHook(t *testing.T) {
	if !ordinaryColumnStableAuthorityEnabled() {
		t.Skip("stable namespace authority unavailable")
	}
	for _, failHook := range []bool{false, true} {
		name := "success"
		if failHook {
			name = "hook_failure"
		}
		t.Run(name, func(t *testing.T) {
			cfg, err := normalizeColumnStoreConfig("events", &ColumnStoreConfig{
				Enabled: true,
				Columns: []ColumnStoreColumn{{Name: "n", Path: "n", ValueType: ColumnStoreValueInt64}},
			})
			if err != nil {
				t.Fatal(err)
			}
			db, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close db: %v", err)
				}
			})
			col := &Collection{db: db}
			rows := []columnDeclaredRow{{ID: []byte("a"), Values: []columnDeclaredValue{{Type: ColumnStoreValueInt64, Present: true, Int64: 3}}}}
			injected := errors.New("prepared hook failure")
			var clone *rootpublication.StableResourceSet
			var captured []ColumnPreparedAsset
			var atHook [][]byte
			calls := 0
			restore := setColumnPhysicalAssetPreparationAfterPrepareTestHook(func(actual ColumnPublishPreparedAssets) error {
				calls++
				if calls != 1 {
					return errors.New("unexpected second prepare invocation")
				}
				if actual.stableResources == nil || !actual.stableResourcesRequired || len(actual.Assets) == 0 {
					return errors.New("hook did not receive stable prepared assets")
				}
				// Keep a private value snapshot; clone string backing rather
				// than relying on producer descriptor storage after unwind.
				captured = append([]ColumnPreparedAsset(nil), actual.Assets...)
				for i := range captured {
					a := &captured[i]
					_, _, classification, e := stableColumnAssetResourceClassification(a.Ref.Kind)
					if e != nil || classification != "authoritative" {
						return errors.New("unsupported asset authority")
					}
					a.Ref.Kind = ColumnAssetKind(strings.Clone(string(a.Ref.Kind)))
					a.Ref.Namespace = strings.Clone(a.Ref.Namespace)
					a.Reason = strings.Clone(a.Reason)
					a.PartRole = ColumnManifestPartRole(strings.Clone(string(a.PartRole)))
					a.SortKey = strings.Clone(a.SortKey)
				}
				clone, err = rootpublication.CloneStableResourceSetExcludingKinds(actual.stableResources)
				if err != nil {
					return err
				}
				if clone == nil {
					return errors.New("missing cloned resource custody")
				}
				if err = validateStableColumnResourcesMatchPrepared(captured, clone); err != nil {
					return err
				}
				// This records actual pathname reads for these sample refs. It is
				// not an identity-bound read or a proof against path replacement.
				for _, asset := range captured {
					payload, e := readColumnPhysicalAssetFromManager(db.ColumnAssetRootDir(), asset.Ref)
					if e != nil {
						return e
					}
					atHook = append(atHook, payload)
				}
				if failHook {
					return injected
				}
				return nil
			})
			defer restore()
			t.Cleanup(func() {
				if clone != nil {
					if err := clone.ReleaseWithError(); err != nil {
						t.Errorf("release clone: %v", err)
					}
				}
			})
			prepared, prepareErr := col.prepareColumnPhysicalAssetRowsAtIdentity(ColumnPublishPreparedAssets{}, columnWritePublishInput{
				meta:      CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: cfg}},
				operation: ColumnPublishOperationInsert, rows: len(rows), declaredRows: rows, declaredRowsReady: true,
			}, ColumnPublishAssetPrepareInput{
				Collection: "events", ColumnStore: *cfg, Operation: ColumnPublishOperationInsert, AppliedCommandLSN: 77,
			}, rows, 7, columnPhysicalRowAssetPartID, typedColumnPartAssetPartID)
			if prepared.stableResources != nil {
				if err := prepared.stableResources.ReleaseWithError(); err != nil {
					t.Fatal(err)
				}
			}
			if calls != 1 || clone == nil {
				t.Fatalf("hook calls=%d clone=%v error=%v", calls, clone != nil, prepareErr)
			}
			if failHook && !errors.Is(prepareErr, injected) {
				t.Fatalf("prepare failure=%v", prepareErr)
			}
			if !failHook && prepareErr != nil {
				t.Fatalf("prepare=%v", prepareErr)
			}
			if err := validateStableColumnResourcesMatchPrepared(captured, clone); err != nil {
				t.Fatalf("independent custody after producer unwind: %v", err)
			}
			if len(atHook) != len(captured) {
				t.Fatal("missing actual reference reads at hook")
			}
			for i, asset := range captured {
				payload, err := readColumnPhysicalAssetFromManager(db.ColumnAssetRootDir(), asset.Ref)
				if err != nil || !bytes.Equal(payload, atHook[i]) {
					t.Fatalf("sample ref %d after producer unwind differs: %v", i, err)
				}
			}
			bad := append([]ColumnPreparedAsset(nil), captured...)
			bad[0].Ref.Checksum ^= 1
			if err := validateStableColumnResourcesMatchPrepared(bad, clone); err == nil {
				t.Fatal("different asset identity accepted")
			}
		})
	}
}

// This is a policy control using synthetic descriptors, not an actual
// rebuildable producer or evidence that its cleanup is pinned.
func TestNativePreparedOutputOrphanPolicyControls(t *testing.T) {
	for _, test := range []struct {
		name   string
		kinds  []ColumnAssetKind
		retain bool
	}{
		{"authoritative", []ColumnAssetKind{ColumnAssetKindTCS1PartImage}, true},
		{"rebuildable_only", []ColumnAssetKind{ColumnAssetKindQueryReadyBase, ColumnAssetKindQueryReadyDelta}, false},
		{"mixed", []ColumnAssetKind{ColumnAssetKindTCS1PartImage, ColumnAssetKindQueryReadyBase}, true},
		{"unknown_fails_closed", []ColumnAssetKind{ColumnAssetKind("unknown")}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			assets := make([]ColumnPreparedAsset, len(test.kinds))
			for i, kind := range test.kinds {
				assets[i].Ref.Kind = kind
			}
			if got := columnPreparedAssetsRequireOrphanRetention(assets); got != test.retain {
				t.Fatalf("retain=%v want %v", got, test.retain)
			}
		})
	}
}
