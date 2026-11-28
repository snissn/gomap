package collections

import (
	"errors"
	"fmt"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestColumnPublishPlanLeaseCommitOnce4550(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	asset := writeColumnPublishPlanLeaseAsset4550(t, d, col, "commit", 1)
	lease := newColumnPublishPlanLease4550(t, col, asset)

	if _, err := lease.beginInstall("events", 78, 44); err == nil {
		t.Fatal("beginInstall accepted the wrong LSN")
	}
	plan, err := lease.beginInstall("events", 77, 44)
	if err != nil {
		t.Fatalf("beginInstall: %v", err)
	}
	if plan.AppliedCommandLSN != 77 || plan.ManifestRootBaseID != 44 {
		t.Fatalf("installed plan binding=%d/%d, want 77/44", plan.AppliedCommandLSN, plan.ManifestRootBaseID)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 1, 0, 0)
	if err := lease.transferStableResources(backenddb.CommandWALPublishContext{}); err != nil {
		t.Fatalf("transferStableResources: %v", err)
	}
	if err := lease.transferStableResources(backenddb.CommandWALPublishContext{}); !errors.Is(err, errColumnPublishPlanLeaseConsumed) {
		t.Fatalf("second transferStableResources error=%v, want consumed", err)
	}
	if err := lease.finishCommit(); err != nil {
		t.Fatalf("finishCommit: %v", err)
	}
	if got := lease.stateValue(); got != columnPublishPlanLeaseCommitted {
		t.Fatalf("state=%v, want committed", got)
	}
	if err := lease.finishCommit(); !errors.Is(err, errColumnPublishPlanLeaseConsumed) {
		t.Fatalf("second finishCommit error=%v, want consumed", err)
	}
	if err := lease.Abandon(); !errors.Is(err, errColumnPublishPlanLeaseConsumed) {
		t.Fatalf("Abandon after commit error=%v, want consumed", err)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 0)
	if raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref); err != nil || string(raw) != "commit" {
		t.Fatalf("committed asset raw=%q err=%v", raw, err)
	}
}

func TestColumnPublishPlanLeaseSafeAbandon4550(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	seed := writeColumnPublishPlanLeaseAsset4550(t, d, col, "seed", 1)
	asset := writeColumnPublishPlanLeaseAsset4550(t, d, col, "abandon", 2)
	lease := newColumnPublishPlanLease4550(t, col, asset)

	if err := lease.Abandon(); err != nil {
		t.Fatalf("Abandon: %v", err)
	}
	if err := lease.Abandon(); err != nil {
		t.Fatalf("idempotent Abandon: %v", err)
	}
	if got := lease.stateValue(); got != columnPublishPlanLeaseAbandoned {
		t.Fatalf("state=%v, want abandoned", got)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat cleaned segment: %v", err)
	}
	if info.Size() != asset.Ref.Offset {
		t.Fatalf("cleaned segment size=%d, want pre-plan offset %d", info.Size(), asset.Ref.Offset)
	}
	if raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), seed.Ref); err != nil || string(raw) != "seed" {
		t.Fatalf("seed asset raw=%q err=%v", raw, err)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 0)
}

func TestColumnPublishPlanLeaseLaterAppendQuarantines4550(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	asset := writeColumnPublishPlanLeaseAsset4550(t, d, col, "planned", 1)
	lease := newColumnPublishPlanLease4550(t, col, asset)
	later := writeColumnPublishPlanLeaseAsset4550(t, d, col, "later", 2)

	err := lease.Abandon()
	if !errors.Is(err, errColumnPublishPlanLeaseCleanupQuarantined) {
		t.Fatalf("Abandon error=%v, want quarantined cleanup", err)
	}
	if got := lease.stateValue(); got != columnPublishPlanLeaseQuarantined {
		t.Fatalf("state=%v, want quarantined", got)
	}
	if raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref); err != nil || string(raw) != "planned" {
		t.Fatalf("planned asset raw=%q err=%v", raw, err)
	}
	if raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), later.Ref); err != nil || string(raw) != "later" {
		t.Fatalf("later asset raw=%q err=%v", raw, err)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 1)
}

func TestColumnPublishPlanLeaseStableRebindQuarantines4550(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	asset := writeColumnPublishPlanLeaseAsset4550(t, d, col, "planned", 1)
	lease := newColumnPublishPlanLease4550(t, col, asset)
	lease.mu.Lock()
	lease.plan.stablePreparedAssets = true
	lease.mu.Unlock()
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		t.Fatal(err)
	}
	originalPath := path + ".original"
	if err := os.Rename(path, originalPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("rebound"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := lease.Abandon(); !errors.Is(err, errColumnPublishPlanLeaseCleanupQuarantined) {
		t.Fatalf("Abandon error=%v, want quarantined cleanup", err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "rebound" {
		t.Fatalf("rebound segment raw=%q err=%v", raw, err)
	}
	if raw, err := os.ReadFile(originalPath); err != nil || string(raw) != "planned" {
		t.Fatalf("original segment raw=%q err=%v", raw, err)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 1)
}

func TestColumnPublishPlanLeaseFailureOwnership4550(t *testing.T) {
	tests := []struct {
		name       string
		transfer   bool
		ambiguous  bool
		wantState  columnPublishPlanLeaseState
		wantExists bool
		wantQ      int
	}{
		{name: "pre-transfer safely abandoned", wantState: columnPublishPlanLeaseAbandoned},
		{name: "post-transfer pre-publication failure safely abandoned", transfer: true, wantState: columnPublishPlanLeaseAbandoned},
		{name: "post-transfer ambiguity quarantined", transfer: true, ambiguous: true, wantState: columnPublishPlanLeaseQuarantined, wantExists: true, wantQ: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, _ := prepareColumnStoreCommandWALDirM10B(t)
			d := openCollectionCommandWALDB(t, dir)
			col := openColumnStoreCollectionM10B(t, d)
			asset := writeColumnPublishPlanLeaseAsset4550(t, d, col, "ambiguous", 1)
			lease := newColumnPublishPlanLease4550(t, col, asset)
			if _, err := lease.beginInstall("events", 77, 44); err != nil {
				t.Fatalf("beginInstall: %v", err)
			}
			if tt.transfer {
				if err := lease.transferStableResources(backenddb.CommandWALPublishContext{}); err != nil {
					t.Fatalf("transferStableResources: %v", err)
				}
			}
			publishErr := errors.New("injected pre-publication failure")
			if tt.ambiguous {
				publishErr = fmt.Errorf("%w: injected ambiguous publication", backenddb.ErrRecoveryRequired)
			}
			if err := lease.finishFailure(publishErr); err != nil {
				t.Fatalf("finishFailure: %v", err)
			}
			if got := lease.stateValue(); got != tt.wantState {
				t.Fatalf("state=%v, want %v", got, tt.wantState)
			}
			_, readErr := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref)
			if (readErr == nil) != tt.wantExists {
				t.Fatalf("asset read error=%v, want exists=%t", readErr, tt.wantExists)
			}
			assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, tt.wantQ)
			if err := d.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !tt.ambiguous {
				return
			}
			reopened := openCollectionCommandWALDB(t, dir)
			defer func() { _ = reopened.Close() }()
			reopenedCol := openColumnStoreCollectionM10B(t, reopened)
			assertColumnPublishPlanLeaseRegistry4550(t, reopenedCol, 0, 0, 0)
			if raw, err := readColumnPhysicalAssetFromManager(reopened.ColumnAssetRootDir(), asset.Ref); err != nil || string(raw) != "ambiguous" {
				t.Fatalf("reopen retained asset raw=%q err=%v", raw, err)
			}
		})
	}
}

func TestColumnPublishPlanBuildFailureAbandonsPreparedAssets4550(t *testing.T) {
	errStage := errors.New("injected stage failure")
	asset := testColumnPublishPreparedAssetM10A()
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0xfeedbeef}
	tests := []struct {
		name   string
		inject func(*ColumnPublishPlanInput)
	}{
		{
			name: "manifest",
			inject: func(input *ColumnPublishPlanInput) {
				input.Hooks.EncodeManifest = func(ColumnPublishManifestEncodeInput) (ColumnPublishManifestEncodeResult, error) {
					return ColumnPublishManifestEncodeResult{}, errStage
				}
			},
		},
		{
			name: "closure",
			inject: func(input *ColumnPublishPlanInput) {
				input.Hooks.ValidateClosure = func(ColumnPublishClosureValidationInput) (ColumnPublishDurabilityClosure, error) {
					return ColumnPublishDurabilityClosure{}, errStage
				}
			},
		},
		{
			name: "root delta",
			inject: func(input *ColumnPublishPlanInput) {
				input.Hooks.BuildRootDelta = func(ColumnPublishRootDeltaInput) (ColumnManifestRootDelta, error) {
					return ColumnManifestRootDelta{}, errStage
				}
			},
		},
		{
			name: "system delta",
			inject: func(input *ColumnPublishPlanInput) {
				input.Hooks.BuildSystemDelta = func(ColumnPublishSystemDeltaInput) error { return errStage }
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := testColumnPublishPlanInputM10A(identity, asset)
			abandoned := 0
			input.Hooks.abandonPreparedAssets = func(assets []ColumnPreparedAsset, stable bool) error {
				abandoned++
				if stable {
					return errors.New("test plan unexpectedly required stable cleanup")
				}
				if len(assets) != 1 || assets[0].Ref != testColumnPublishPreparedAssetForIdentityM10A(asset, identity).Ref {
					return fmt.Errorf("unexpected abandoned assets: %+v", assets)
				}
				return nil
			}
			tt.inject(&input)
			if _, err := BuildColumnPublishPlan(input); !errors.Is(err, errStage) {
				t.Fatalf("BuildColumnPublishPlan error=%v, want stage failure", err)
			}
			if abandoned != 1 {
				t.Fatalf("abandon calls=%d, want 1", abandoned)
			}
		})
	}
}

func TestColumnPublishPlanLeaseCommitAbandonRace4550(t *testing.T) {
	dir, _ := prepareColumnStoreCommandWALDirM10B(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openColumnStoreCollectionM10B(t, d)
	asset := writeColumnPublishPlanLeaseAsset4550(t, d, col, "race", 1)
	lease := newColumnPublishPlanLease4550(t, col, asset)
	start := make(chan struct{})
	results := make(chan bool, 2)
	errs := make(chan error, 2)

	go func() {
		<-start
		if _, err := lease.beginInstall("events", 77, 44); err != nil {
			if errors.Is(err, errColumnPublishPlanLeaseConsumed) {
				results <- false
				return
			}
			errs <- err
			return
		}
		if err := lease.transferStableResources(backenddb.CommandWALPublishContext{}); err != nil {
			errs <- err
			return
		}
		if err := lease.finishCommit(); err != nil {
			errs <- err
			return
		}
		results <- true
	}()
	go func() {
		<-start
		err := lease.Abandon()
		switch {
		case err == nil:
			results <- true
		case errors.Is(err, errColumnPublishPlanLeaseConsumed):
			results <- false
		default:
			errs <- err
		}
	}()
	close(start)

	wins := 0
	for i := 0; i < 2; i++ {
		select {
		case won := <-results:
			if won {
				wins++
			}
		case err := <-errs:
			t.Fatalf("race transition: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("successful owners=%d, want exactly 1", wins)
	}
	if got := lease.stateValue(); got != columnPublishPlanLeaseCommitted && got != columnPublishPlanLeaseAbandoned {
		t.Fatalf("state=%v, want committed or abandoned", got)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 0)
}

func writeColumnPublishPlanLeaseAsset4550(t testing.TB, d *backenddb.DB, col *Collection, payload string, partID uint64) ColumnPreparedAsset {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil {
		t.Fatal("column store config is nil")
	}
	ref, err := writeColumnAssetToManagerSegment(d.ColumnAssetRootDir(), *cfg, []byte(payload), ColumnAssetKindTCS1PartImage, 7, partID, columnAssetM12ASegmentFileID)
	if err != nil {
		t.Fatalf("writeColumnAssetToManagerSegment: %v", err)
	}
	return ColumnPreparedAsset{Ref: ref, Rows: 1, Bytes: ref.Length, GenerationID: ref.Generation, Reason: "lease test"}
}

func newColumnPublishPlanLease4550(t testing.TB, col *Collection, asset ColumnPreparedAsset) *columnPublishPlanLease {
	t.Helper()
	lease, err := newColumnPublishPlanLease(col, ColumnPublishPlan{
		Enabled:            true,
		Collection:         "events",
		AppliedCommandLSN:  77,
		ManifestRootBaseID: 44,
		RootDelta:          ColumnManifestRootDelta{BaseRootID: 44},
		PreparedAssets:     []ColumnPreparedAsset{asset},
	})
	if err != nil {
		t.Fatalf("newColumnPublishPlanLease: %v", err)
	}
	if got := lease.stateValue(); got != columnPublishPlanLeasePrepared {
		t.Fatalf("initial state=%v, want prepared", got)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 1, 0)
	return lease
}

func assertColumnPublishPlanLeaseRegistry4550(t testing.TB, col *Collection, pending, prepared, quarantine int) {
	t.Helper()
	gotPending, gotPrepared, gotQuarantine := 0, 0, 0
	for _, record := range col.columnAssetLifecycleRegistrySnapshot() {
		switch record.Class {
		case ColumnAssetLifecycleRegistryPendingPublish:
			gotPending++
		case ColumnAssetLifecycleRegistryPreparedAsset:
			gotPrepared++
		case ColumnAssetLifecycleRegistryQuarantine:
			gotQuarantine++
		}
	}
	if gotPending != pending || gotPrepared != prepared || gotQuarantine != quarantine {
		t.Fatalf("registry pending/prepared/quarantine=%d/%d/%d, want %d/%d/%d", gotPending, gotPrepared, gotQuarantine, pending, prepared, quarantine)
	}
}

func TestColumnPublishPlanLeaseStableNonNilRebindQuarantines5922(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace support required")
	}
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openEventsCollection5922(t, d)
	if _, err := col.Insert([]byte("seed-meta"), []byte(`{"time_us":1,"kind":"like","did":"d0"}`)); err != nil {
		t.Fatalf("Insert seed: %v", err)
	}
	view, releaseView, err := col.prepareColumnPhysicalScanSnapshotView()
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	cfg := view.FullConfig
	if releaseView != nil {
		releaseView()
	}
	captureLease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatalf("AcquireStableResourceCaptureLease: %v", err)
	}
	refs, stableResources, err := AppendColumnPhysicalAssetsWithStableResources(
		d.ColumnAssetRootDir(),
		cfg,
		columnAssetM12ASegmentFileID+1,
		[]StableColumnPhysicalAssetAppend{{
			Payload:    []byte("planned"),
			Kind:       ColumnAssetKindTCS1PartImage,
			Generation: 7,
			PartID:     1,
		}},
		d.StableResourceIdentityPinRegistry(),
		captureLease,
	)
	captureLease.Release()
	if err != nil {
		t.Fatalf("AppendColumnPhysicalAssetsWithStableResources: %v", err)
	}
	asset := ColumnPreparedAsset{Ref: refs[0], Rows: 1, Bytes: refs[0].Length, GenerationID: refs[0].Generation}
	lease, err := newColumnPublishPlanLease(col, ColumnPublishPlan{
		Enabled:              true,
		Collection:           "events",
		AppliedCommandLSN:    77,
		ManifestRootBaseID:   44,
		RootDelta:            ColumnManifestRootDelta{BaseRootID: 44},
		PreparedAssets:       []ColumnPreparedAsset{asset},
		stableResources:      stableResources,
		stablePreparedAssets: true,
	})
	if err != nil {
		t.Fatalf("newColumnPublishPlanLease: %v", err)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		t.Fatal(err)
	}
	originalPath := path + ".original"
	if err := os.Rename(path, originalPath); err != nil {
		t.Fatal(err)
	}
	// Same length as "planned" (7 bytes), different inode.
	if err := os.WriteFile(path, []byte("rebound"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := lease.Abandon(); !errors.Is(err, errColumnPublishPlanLeaseCleanupQuarantined) {
		t.Fatalf("Abandon error=%v, want quarantined cleanup on rebound inode", err)
	}
	if raw, err := os.ReadFile(path); err != nil || string(raw) != "rebound" {
		t.Fatalf("rebound segment raw=%q err=%v", raw, err)
	}
	if raw, err := os.ReadFile(originalPath); err != nil || string(raw) != "planned" {
		t.Fatalf("original segment raw=%q err=%v", raw, err)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 1)
}

func TestColumnPublishPlanLeaseStableAlignedNonZeroOffsetQuarantines5922(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace support required")
	}
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openEventsCollection5922(t, d)
	if _, err := col.Insert([]byte("seed-meta"), []byte(`{"time_us":1,"kind":"like","did":"d0"}`)); err != nil {
		t.Fatalf("Insert seed: %v", err)
	}
	view, releaseView, err := col.prepareColumnPhysicalScanSnapshotView()
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	cfg := view.FullConfig
	if releaseView != nil {
		releaseView()
	}

	// Seed a fresh segment with an unaligned 3-byte payload so an aligned
	// int64_values append writes prefix zero-padding before its ref.Offset.
	seed := writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, columnAssetM12ASegmentFileID+2, []byte("abc"))
	captureLease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatalf("AcquireStableResourceCaptureLease: %v", err)
	}
	refs, stableResources, err := AppendColumnPhysicalAssetsWithStableResources(
		d.ColumnAssetRootDir(),
		cfg,
		seed.FileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload:    []byte("12345678"),
			Kind:       ColumnAssetKindTCS1Int64Values,
			Generation: 7,
			PartID:     2,
		}},
		d.StableResourceIdentityPinRegistry(),
		captureLease,
	)
	captureLease.Release()
	if err != nil {
		t.Fatalf("AppendColumnPhysicalAssetsWithStableResources: %v", err)
	}
	if refs[0].Offset <= seed.Length {
		t.Fatalf("expected prefix alignment padding before ref.Offset=%d (seed len=%d)", refs[0].Offset, seed.Length)
	}
	asset := ColumnPreparedAsset{Ref: refs[0], Rows: 1, Bytes: refs[0].Length, GenerationID: refs[0].Generation}
	lease, err := newColumnPublishPlanLease(col, ColumnPublishPlan{
		Enabled:              true,
		Collection:           "events",
		AppliedCommandLSN:    78,
		ManifestRootBaseID:   44,
		RootDelta:            ColumnManifestRootDelta{BaseRootID: 44},
		PreparedAssets:       []ColumnPreparedAsset{asset},
		stableResources:      stableResources,
		stablePreparedAssets: true,
	})
	if err != nil {
		t.Fatalf("newColumnPublishPlanLease: %v", err)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		t.Fatal(err)
	}
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Abandon(); !errors.Is(err, errColumnPublishPlanLeaseCleanupQuarantined) {
		t.Fatalf("Abandon error=%v, want quarantined cleanup when prefix alignment padding precedes ref.Offset", err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if afterInfo.Size() != beforeInfo.Size() {
		t.Fatalf("aligned non-zero offset segment was truncated from %d to %d", beforeInfo.Size(), afterInfo.Size())
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 1)
}

func TestColumnPublishPlanLeaseStableNonContiguousRefsQuarantines5922(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace support required")
	}
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openEventsCollection5922(t, d)
	if _, err := col.Insert([]byte("seed-meta"), []byte(`{"time_us":1,"kind":"like","did":"d0"}`)); err != nil {
		t.Fatalf("Insert seed: %v", err)
	}
	view, releaseView, err := col.prepareColumnPhysicalScanSnapshotView()
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	cfg := view.FullConfig
	if releaseView != nil {
		releaseView()
	}

	const segFileID = columnAssetM12ASegmentFileID + 3
	// Write prefix [0..100), then prepared ref1 [100..150), interleaved committed
	// ref [150..200), and prepared ref2 [200..250).
	_ = writeColumnAssetGCCandidateSegmentM15B(t, d.ColumnAssetRootDir(), col, segFileID, make([]byte, 100))

	captureLease1, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatalf("AcquireStableResourceCaptureLease(1): %v", err)
	}
	refs1, res1, err := AppendColumnPhysicalAssetsWithStableResources(
		d.ColumnAssetRootDir(),
		cfg,
		segFileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload:    make([]byte, 50),
			Kind:       ColumnAssetKindTCS1PartImage,
			Generation: 7,
			PartID:     10,
		}},
		d.StableResourceIdentityPinRegistry(),
		captureLease1,
	)
	captureLease1.Release()
	if err != nil {
		t.Fatalf("Append(ref1): %v", err)
	}
	defer res1.Release()

	// Interleaved committed append at [150..200)
	captureLeaseMid, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatalf("AcquireStableResourceCaptureLease(mid): %v", err)
	}
	_, resMid, err := AppendColumnPhysicalAssetsWithStableResources(
		d.ColumnAssetRootDir(),
		cfg,
		segFileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload:    make([]byte, 50),
			Kind:       ColumnAssetKindTCS1PartImage,
			Generation: 8,
			PartID:     11,
		}},
		d.StableResourceIdentityPinRegistry(),
		captureLeaseMid,
	)
	captureLeaseMid.Release()
	if err != nil {
		t.Fatalf("Append(mid): %v", err)
	}
	defer resMid.Release()

	// Second prepared ref at [200..250)
	captureLease2, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatalf("AcquireStableResourceCaptureLease(2): %v", err)
	}
	refs2, res2, err := AppendColumnPhysicalAssetsWithStableResources(
		d.ColumnAssetRootDir(),
		cfg,
		segFileID,
		[]StableColumnPhysicalAssetAppend{{
			Payload:    make([]byte, 50),
			Kind:       ColumnAssetKindTCS1PartImage,
			Generation: 7,
			PartID:     12,
		}},
		d.StableResourceIdentityPinRegistry(),
		captureLease2,
	)
	captureLease2.Release()
	if err != nil {
		t.Fatalf("Append(ref2): %v", err)
	}
	if refs1[0].Offset != 100 || refs2[0].Offset != 200 {
		res2.Release()
		t.Fatalf("unexpected offsets ref1=%d ref2=%d want 100, 200", refs1[0].Offset, refs2[0].Offset)
	}

	lease, err := newColumnPublishPlanLease(col, ColumnPublishPlan{
		Enabled:            true,
		Collection:         "events",
		AppliedCommandLSN:  79,
		ManifestRootBaseID: 44,
		RootDelta:          ColumnManifestRootDelta{BaseRootID: 44},
		PreparedAssets: []ColumnPreparedAsset{
			{Ref: refs1[0], Rows: 1, Bytes: refs1[0].Length, GenerationID: refs1[0].Generation},
			{Ref: refs2[0], Rows: 1, Bytes: refs2[0].Length, GenerationID: refs2[0].Generation},
		},
		stableResources:      res2,
		stablePreparedAssets: true,
	})
	if err != nil {
		t.Fatalf("newColumnPublishPlanLease: %v", err)
	}
	path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), refs1[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Abandon(); !errors.Is(err, errColumnPublishPlanLeaseCleanupQuarantined) {
		t.Fatalf("Abandon error=%v, want quarantined cleanup for non-contiguous prepared refs", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 250 {
		t.Fatalf("segment with interleaved committed ref was truncated to %d, want 250", info.Size())
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 1)
}

func TestColumnPublishPlanLeasePostTransferCommitAmbiguousQuarantines5922(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace support required")
	}
	dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()
	col := openEventsCollection5922(t, d)
	if _, err := col.Insert([]byte("seed-meta"), []byte(`{"time_us":1,"kind":"like","did":"d0"}`)); err != nil {
		t.Fatalf("Insert seed: %v", err)
	}
	view, releaseView, err := col.prepareColumnPhysicalScanSnapshotView()
	if err != nil {
		t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
	}
	cfg := view.FullConfig
	if releaseView != nil {
		releaseView()
	}

	captureLease, err := d.AcquireStableResourceCaptureLease()
	if err != nil {
		t.Fatalf("AcquireStableResourceCaptureLease: %v", err)
	}
	refs, stableResources, err := AppendColumnPhysicalAssetsWithStableResources(
		d.ColumnAssetRootDir(),
		cfg,
		columnAssetM12ASegmentFileID+4,
		[]StableColumnPhysicalAssetAppend{{
			Payload:    []byte("ambiguous-commit-payload"),
			Kind:       ColumnAssetKindTCS1PartImage,
			Generation: 7,
			PartID:     20,
		}},
		d.StableResourceIdentityPinRegistry(),
		captureLease,
	)
	captureLease.Release()
	if err != nil {
		t.Fatalf("AppendColumnPhysicalAssetsWithStableResources: %v", err)
	}
	asset := ColumnPreparedAsset{Ref: refs[0], Rows: 1, Bytes: refs[0].Length, GenerationID: refs[0].Generation}
	lease, err := newColumnPublishPlanLease(col, ColumnPublishPlan{
		Enabled:              true,
		Collection:           "events",
		AppliedCommandLSN:    80,
		ManifestRootBaseID:   44,
		RootDelta:            ColumnManifestRootDelta{BaseRootID: 44},
		PreparedAssets:       []ColumnPreparedAsset{asset},
		stableResources:      stableResources,
		stablePreparedAssets: true,
	})
	if err != nil {
		t.Fatalf("newColumnPublishPlanLease: %v", err)
	}
	if _, err := lease.beginInstall("events", 80, 44); err != nil {
		t.Fatalf("beginInstall: %v", err)
	}
	if err := lease.transferStableResources(backenddb.CommandWALPublishContext{}); err != nil && !errors.Is(err, rootpublication.ErrResourceOwnership) {
		t.Fatalf("transferStableResources: %v", err)
	}
	if err := lease.finishFailure(fmt.Errorf("%w: injected post-transfer ambiguity", ErrCommitAmbiguous)); err != nil {
		t.Fatalf("finishFailure(ErrCommitAmbiguous): %v", err)
	}
	if got := lease.stateValue(); got != columnPublishPlanLeaseQuarantined {
		t.Fatalf("state=%v want quarantined on post-transfer ErrCommitAmbiguous", got)
	}
	if raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref); err != nil || string(raw) != "ambiguous-commit-payload" {
		t.Fatalf("post-transfer ambiguous asset raw=%q err=%v want retained intact", raw, err)
	}
	assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 1)
}

func TestColumnPublishPlanLeaseStableWholeSegmentAbandonAndPrePublishUnlinks5922(t *testing.T) {
	if !rootpublication.StableRelativeNamespaceSupported() {
		t.Skip("stable relative namespace support required")
	}
	for _, tc := range []struct {
		name     string
		install  bool
		transfer bool
		fileID   uint32
	}{
		{name: "abandon_fresh_whole_segment", fileID: columnAssetM12ASegmentFileID + 10},
		{name: "pre_publish_after_transfer_fresh_whole_segment", install: true, transfer: true, fileID: columnAssetM12ASegmentFileID + 11},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := prepareColumnAssetReachabilityCommandWALDirM15A(t)
			d := openCollectionCommandWALDB(t, dir)
			defer func() { _ = d.Close() }()
			col := openEventsCollection5922(t, d)
			if _, err := col.Insert([]byte("seed-meta"), []byte(`{"time_us":1,"kind":"like","did":"d0"}`)); err != nil {
				t.Fatalf("Insert seed: %v", err)
			}
			view, releaseView, err := col.prepareColumnPhysicalScanSnapshotView()
			if err != nil {
				t.Fatalf("prepareColumnPhysicalScanSnapshotView: %v", err)
			}
			cfg := view.FullConfig
			if releaseView != nil {
				releaseView()
			}
			captureLease, err := d.AcquireStableResourceCaptureLease()
			if err != nil {
				t.Fatalf("AcquireStableResourceCaptureLease: %v", err)
			}
			refs, stableResources, err := AppendColumnPhysicalAssetsWithStableResources(
				d.ColumnAssetRootDir(),
				cfg,
				tc.fileID,
				[]StableColumnPhysicalAssetAppend{{
					Payload:    []byte("fresh-whole-segment-unpublished"),
					Kind:       ColumnAssetKindTCS1PartImage,
					Generation: 7,
					PartID:     30,
				}},
				d.StableResourceIdentityPinRegistry(),
				captureLease,
			)
			captureLease.Release()
			if err != nil {
				t.Fatalf("AppendColumnPhysicalAssetsWithStableResources: %v", err)
			}
			if refs[0].Offset != 0 {
				stableResources.Release()
				t.Fatalf("expected offset 0 for fresh segment, got %d", refs[0].Offset)
			}
			asset := ColumnPreparedAsset{Ref: refs[0], Rows: 1, Bytes: refs[0].Length, GenerationID: refs[0].Generation}
			path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), asset.Ref)
			if err != nil {
				stableResources.Release()
				t.Fatal(err)
			}
			lease, err := newColumnPublishPlanLease(col, ColumnPublishPlan{
				Enabled:              true,
				Collection:           "events",
				AppliedCommandLSN:    81,
				ManifestRootBaseID:   44,
				RootDelta:            ColumnManifestRootDelta{BaseRootID: 44},
				PreparedAssets:       []ColumnPreparedAsset{asset},
				stableResources:      stableResources,
				stablePreparedAssets: true,
			})
			if err != nil {
				t.Fatalf("newColumnPublishPlanLease: %v", err)
			}
			if !tc.install {
				if err := lease.Abandon(); err != nil {
					t.Fatalf("Abandon fresh whole segment: %v", err)
				}
			} else {
				if _, err := lease.beginInstall("events", 81, 44); err != nil {
					t.Fatalf("beginInstall: %v", err)
				}
				if tc.transfer {
					if err := lease.transferStableResources(backenddb.CommandWALPublishContext{}); err != nil && !errors.Is(err, rootpublication.ErrResourceOwnership) {
						t.Fatalf("transferStableResources: %v", err)
					}
				}
				if err := lease.finishFailure(errors.New("injected pre-publish failure")); err != nil {
					t.Fatalf("finishFailure fresh whole segment: %v", err)
				}
			}
			if got := lease.stateValue(); got != columnPublishPlanLeaseAbandoned {
				t.Fatalf("state=%v want abandoned", got)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("fresh unpublished whole segment still exists: stat err=%v", err)
			}
			assertColumnPublishPlanLeaseRegistry4550(t, col, 0, 0, 0)
		})
	}
}
