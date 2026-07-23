package treedb_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
	"github.com/snissn/gomap/TreeDB/internal/powerlossreopen"
)

const (
	certOrdinaryWriteBatchVariant = "public-ack-ordinary-write-batch-stable-image"
	certAllExplicitSyncVariant    = "public-ack-all-explicit-sync-forms-stable-image"
	certEmptyExplicitSyncVariant  = "public-ack-empty-explicit-sync-stable-image"
	certCleanCloseVariant         = "public-ack-clean-close-stable-image"
	certPreMetaFailureVariant     = "public-retryable-pre-meta-failure-stable-image"
	certPostMetaFailureVariant    = "public-post-meta-poison-stable-image"
	certDurablePrefixHoleVariant  = "public-durable-prefix-hole-stable-image"
	certMaintenanceUnlinkVariant  = "public-maintenance-unlink-stable-image"
	certWALUnlinkVariant          = "public-command-wal-unlink-stable-image"
)

func requireCertificationReplay(t *testing.T, variant string, point durabilitycut.Point, occurrence int) {
	t.Helper()
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if selector == (powerlossoracle.ReplaySelector{}) {
		t.Skip("exact certification replay selector is required")
	}
	wantCut := fmt.Sprintf("cut/%s/%s/%03d", variant, point, occurrence)
	if selector.CutID != wantCut || selector.VariantID != variant || selector.Seed != powerLossOracleSeed {
		t.Fatalf("replay selector=(%q,%q,%d) want=(%q,%q,%d)", selector.CutID, selector.VariantID, selector.Seed, wantCut, variant, powerLossOracleSeed)
	}
}

func certificationOptions(t *testing.T) (string, treedb.Options) {
	t.Helper()
	profileName, profile := certificationProfileFromEnv(t)
	opts := treedb.OptionsFor(profile, t.TempDir())
	opts.DisableSideStores = true
	opts.DisableBackgroundPrune = true
	opts.BackgroundCheckpointInterval = -1
	return profileName, opts
}

func requireAcceptedCertificationReopen(t *testing.T, model *powerlossoracle.Model, opts treedb.Options, readOnly bool) (*treedb.DB, func() error, powerlossreopen.Result) {
	t.Helper()
	result, reopened, closeReopened, err := powerlossreopen.Stable(model, opts, readOnly)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rejected || reopened == nil {
		if closeReopened != nil {
			_ = closeReopened()
		}
		t.Fatalf("public Open rejected certification image: %v", result.Err)
	}
	return reopened, closeReopened, result
}

// TestPowerLossCertificationOrdinaryWriteAndBatchPublicReopen exercises both
// ordinary point and batch acknowledgement paths, then takes its modeled cut
// at the production checkpoint that makes their common frontier durable.
func TestPowerLossCertificationOrdinaryWriteAndBatchPublicReopen(t *testing.T) {
	requireCertificationReplay(t, certOrdinaryWriteBatchVariant, durabilitycut.AfterMetaSync, 0)
	_, opts := certificationOptions(t)
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = db.Close()
		}
	}()
	if err := db.SetSync([]byte("cert/baseline"), []byte("stable")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot *powerlossoracle.Model
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaSync {
			snapshot = model.Clone()
		}
		return nil
	})
	if err := db.Set([]byte("cert/ordinary"), []byte("point")); err != nil {
		restore()
		t.Fatal(err)
	}
	batch := db.NewBatch()
	if err := batch.Set([]byte("cert/batch"), []byte("batch")); err != nil {
		_ = batch.Close()
		restore()
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		_ = batch.Close()
		restore()
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		restore()
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	if snapshot == nil {
		t.Fatal("checkpoint emitted no after-meta-sync witness")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	closed = true
	reopened, closeReopened, _ := requireAcceptedCertificationReopen(t, snapshot, opts, true)
	for key, want := range map[string]string{"cert/ordinary": "point", "cert/batch": "batch"} {
		got, err := reopened.Get([]byte(key))
		if err != nil || string(got) != want {
			_ = closeReopened()
			t.Fatalf("Get(%q)=%q err=%v want=%q", key, got, err, want)
		}
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

// TestPowerLossCertificationAllExplicitSyncFormsPublicReopen keeps the public
// SetSync, UpdateSync, DeleteSync, and Batch.WriteSync forms in one exact
// command. The modeled cut is the final batch sync's completed meta boundary.
func TestPowerLossCertificationAllExplicitSyncFormsPublicReopen(t *testing.T) {
	requireCertificationReplay(t, certAllExplicitSyncVariant, durabilitycut.AfterMetaSync, 0)
	profileName, opts := certificationOptions(t)
	if profileName != "no_wal_fast" {
		t.Fatalf("all-explicit-sync witness requires no_wal_fast, got %q", profileName)
	}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.SetSync([]byte("cert/sync"), []byte("set")); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateSync([]byte("cert/sync"), func(old []byte) (treedb.UpdateResult, error) {
		if string(old) != "set" {
			return treedb.UpdateResult{}, fmt.Errorf("UpdateSync old=%q want set", old)
		}
		return treedb.SetUpdate([]byte("updated")), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("cert/delete"), []byte("delete-me")); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteSync([]byte("cert/delete")); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot *powerlossoracle.Model
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaSync {
			snapshot = model.Clone()
		}
		return nil
	})
	batch := db.NewBatch()
	if err := batch.Set([]byte("cert/batch-sync"), []byte("durable")); err != nil {
		_ = batch.Close()
		restore()
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		restore()
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	if snapshot == nil {
		t.Fatal("Batch.WriteSync emitted no after-meta-sync witness")
	}
	reopened, closeReopened, _ := requireAcceptedCertificationReopen(t, snapshot, opts, true)
	checks := map[string]string{"cert/sync": "updated", "cert/batch-sync": "durable"}
	for key, want := range checks {
		got, err := reopened.Get([]byte(key))
		if err != nil || string(got) != want {
			_ = closeReopened()
			t.Fatalf("Get(%q)=%q err=%v want=%q", key, got, err, want)
		}
	}
	if got, err := reopened.Get([]byte("cert/delete")); err != nil || len(got) != 0 {
		_ = closeReopened()
		t.Fatalf("deleted key=%q err=%v", got, err)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

// TestPowerLossCertificationEmptyExplicitSyncPublicReopen proves that an
// empty WriteSync opts a relaxed command-WAL prefix up through its durable
// barrier. The retained snapshot is taken at the barrier's completed file
// sync, after the old segment and namespace debt have been closed.
func TestPowerLossCertificationEmptyExplicitSyncPublicReopen(t *testing.T) {
	// The exact occurrence is asserted by the test after observing the real
	// production sequence; it is intentionally frozen in the run plan.
	requireCertificationReplay(t, certEmptyExplicitSyncVariant, durabilitycut.AfterDependencyFileSync, 1)
	profileName, opts := certificationOptions(t)
	if profileName != "command_wal_relaxed" {
		t.Fatalf("empty explicit-sync witness requires command_wal_relaxed, got %q", profileName)
	}
	opts.CommandWALSegmentTargetBytes = 1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var snapshot *powerlossoracle.Model
	afterSyncOccurrence := -1
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterDependencyFileSync {
			afterSyncOccurrence++
			if event.Resource == durabilitycut.ResourceCommandWAL {
				snapshot = model.Clone()
			}
		}
		return nil
	})
	if err := db.Set([]byte("cert/relaxed"), []byte("durable-after-empty-sync")); err != nil {
		restore()
		t.Fatal(err)
	}
	empty := db.NewBatch()
	if err := empty.WriteSync(); err != nil {
		_ = empty.Close()
		restore()
		t.Fatal(err)
	}
	if err := empty.Close(); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	if snapshot == nil || afterSyncOccurrence != 1 {
		t.Fatalf("empty sync final command-WAL sync occurrence=%d snapshot=%t want=1,true", afterSyncOccurrence, snapshot != nil)
	}
	// The barrier is intentionally newer than the selected root, so normal
	// writable recovery must replay it; read-only open correctly asks for that
	// recovery instead of mutating the image.
	reopened, closeReopened, _ := requireAcceptedCertificationReopen(t, snapshot, opts, false)
	got, err := reopened.Get([]byte("cert/relaxed"))
	if err != nil || string(got) != "durable-after-empty-sync" {
		_ = closeReopened()
		t.Fatalf("empty-sync value=%q err=%v", got, err)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

// TestPowerLossCertificationCleanClosePublicReopen takes a stable-image crash
// immediately after the no-WAL clean-close checkpoint's meta sync. The close
// itself must return successfully before the image is accepted as evidence.
func TestPowerLossCertificationCleanClosePublicReopen(t *testing.T) {
	requireCertificationReplay(t, certCleanCloseVariant, durabilitycut.AfterMetaSync, 0)
	profileName, opts := certificationOptions(t)
	if profileName != "no_wal_fast" {
		t.Fatalf("clean-close witness requires no_wal_fast, got %q", profileName)
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("cert/baseline"), []byte("stable")); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Set([]byte("cert/close"), []byte("published-by-close")); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	var snapshot *powerlossoracle.Model
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.AfterMetaSync {
			snapshot = model.Clone()
		}
		return nil
	})
	err = db.Close()
	restore()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil {
		t.Fatal("clean Close emitted no after-meta-sync witness")
	}
	reopened, closeReopened, _ := requireAcceptedCertificationReopen(t, snapshot, opts, true)
	got, err := reopened.Get([]byte("cert/close"))
	if err != nil || string(got) != "published-by-close" {
		_ = closeReopened()
		t.Fatalf("clean-close value=%q err=%v", got, err)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

// TestPowerLossCertificationPublicationFailureClassPublicReopen binds the
// pre-meta retry and post-meta poison contracts to their real public handle
// behavior while reopening the corresponding stable-only crash image.
func TestPowerLossCertificationPublicationFailureClassPublicReopen(t *testing.T) {
	selector, err := powerlossoracle.ReplaySelectorFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if selector == (powerlossoracle.ReplaySelector{}) {
		t.Skip("exact certification replay selector is required")
	}
	var variant string
	var point durabilitycut.Point
	switch selector.VariantID {
	case certPreMetaFailureVariant:
		variant, point = certPreMetaFailureVariant, durabilitycut.BeforeMetaWrite
	case certPostMetaFailureVariant:
		variant, point = certPostMetaFailureVariant, durabilitycut.AfterMetaWrite
	default:
		t.Fatalf("unsupported publication-failure variant %q", selector.VariantID)
	}
	requireCertificationReplay(t, variant, point, 0)
	profileName, opts := certificationOptions(t)
	if profileName != "no_wal_fast" {
		t.Fatalf("publication-failure witness requires no_wal_fast, got %q", profileName)
	}
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetSync([]byte("cert/baseline"), []byte("stable")); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Set([]byte("cert/candidate"), []byte(variant)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	cutErr := fmt.Errorf("power-loss certification cut at %s", point)
	var snapshot *powerlossoracle.Model
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.Point == point {
			snapshot = model.Clone()
			return cutErr
		}
		return nil
	})
	err = db.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) || snapshot == nil {
		_ = db.Close()
		t.Fatalf("Checkpoint error=%v snapshot=%t want injected cut", err, snapshot != nil)
	}
	if variant == certPreMetaFailureVariant {
		if errors.Is(err, treedb.ErrRecoveryRequired) {
			_ = db.Close()
			t.Fatalf("pre-meta failure poisoned writable handle: %v", err)
		}
		if err := db.Checkpoint(); err != nil {
			_ = db.Close()
			t.Fatalf("retry checkpoint after pre-meta failure: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	} else {
		if !errors.Is(err, treedb.ErrRecoveryRequired) {
			_ = db.Close()
			t.Fatalf("post-meta failure=%v want ErrRecoveryRequired", err)
		}
		if setErr := db.Set([]byte("cert/poison-check"), []byte("x")); !errors.Is(setErr, treedb.ErrRecoveryRequired) {
			_ = db.Close()
			t.Fatalf("write after post-meta failure=%v want ErrRecoveryRequired", setErr)
		}
		_ = db.Close()
	}
	reopened, closeReopened, _ := requireAcceptedCertificationReopen(t, snapshot, opts, true)
	got, err := reopened.Get([]byte("cert/candidate"))
	if err != nil {
		_ = closeReopened()
		t.Fatal(err)
	}
	if len(got) != 0 {
		_ = closeReopened()
		t.Fatalf("stable image exposed uncommitted candidate %q", got)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

// TestPowerLossCertificationDurablePrefixHolePublicReopen is an intentional
// negative control: after a durable command-WAL acknowledgement, it removes
// the required value-log segment and makes that illegal namespace deletion
// stable. Normal public recovery must reject the durable replay hole.
func TestPowerLossCertificationDurablePrefixHolePublicReopen(t *testing.T) {
	requireCertificationReplay(t, certDurablePrefixHoleVariant, durabilitycut.AfterDeletionDirectorySync, 0)
	profileName, opts := certificationOptions(t)
	if profileName != "command_wal_durable" {
		t.Fatalf("durable-prefix-hole witness requires command_wal_durable, got %q", profileName)
	}
	opts.ValueLog.PointerThreshold = 1
	opts.ValueLog.ForcePointers = true
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		return model.Observe(opts.Dir, event)
	})
	// Bounded durable pointer writes use self-contained SetMaterializedRID
	// frames, so recovery can legitimately reconstruct a missing value-log
	// segment. Exceed the materialized-value limit to force an external SetRID;
	// the selected segment is then a real durable-prefix dependency.
	batch := db.NewBatch()
	if err := batch.Set([]byte("cert/durable-hole"), bytes.Repeat([]byte("h"), backenddb.RawKVCommandWALMaterializedRIDMaxValueBytes+1)); err != nil {
		_ = batch.Close()
		restore()
		t.Fatal(err)
	}
	if err := batch.WriteSync(); err != nil {
		_ = batch.Close()
		restore()
		t.Fatal(err)
	}
	if err := batch.Close(); err != nil {
		restore()
		t.Fatal(err)
	}
	var valueLogPath string
	for _, relative := range model.StablePaths() {
		if strings.HasPrefix(filepath.ToSlash(relative), "value_vlog/") {
			valueLogPath = filepath.Join(opts.Dir, filepath.FromSlash(relative))
			break
		}
	}
	if valueLogPath == "" {
		restore()
		t.Fatal("durable pointer write produced no stable value-log segment")
	}
	if err := os.Remove(valueLogPath); err != nil {
		restore()
		t.Fatal(err)
	}
	if err := model.Observe(opts.Dir, durabilitycut.Event{Resource: durabilitycut.ResourceValueLog, Root: opts.Dir, Namespace: durabilitycut.NamespaceUnlink, OldPath: valueLogPath}); err != nil {
		restore()
		t.Fatal(err)
	}
	valueLogDir := filepath.Dir(valueLogPath)
	if err := model.Observe(opts.Dir, durabilitycut.Event{Point: durabilitycut.BeforeDeletionDirectorySync, Resource: durabilitycut.ResourceValueLog, Root: opts.Dir, Path: valueLogDir}); err != nil {
		restore()
		t.Fatal(err)
	}
	if err := model.Observe(opts.Dir, durabilitycut.Event{Point: durabilitycut.AfterDeletionDirectorySync, Resource: durabilitycut.ResourceValueLog, Root: opts.Dir, Path: valueLogDir}); err != nil {
		restore()
		t.Fatal(err)
	}
	restore()
	result, reopened, closeReopened, err := powerlossreopen.Stable(model, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	if reopened != nil {
		_ = closeReopened()
		t.Fatal("public Open accepted durable prefix with a missing RID segment")
	}
	if !result.Rejected || !errors.Is(result.Err, backenddb.ErrCommandWALMissingValueLogRID) {
		if closeReopened != nil {
			_ = closeReopened()
		}
		t.Fatalf("public Open error=%v want ErrCommandWALMissingValueLogRID", result.Err)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

// TestPowerLossCertificationMaintenanceUnlinkPublicReopen captures the real
// CompactStorage zero-byte value-log unlink immediately before its parent
// directory sync. The stable image must retain the old name and reopen safely.
func TestPowerLossCertificationMaintenanceUnlinkPublicReopen(t *testing.T) {
	requireCertificationReplay(t, certMaintenanceUnlinkVariant, durabilitycut.BeforeDeletionDirectorySync, 0)
	profileName, opts := certificationOptions(t)
	if profileName != "no_wal_fast" {
		t.Fatalf("maintenance-unlink witness requires no_wal_fast, got %q", profileName)
	}
	// This witness exercises backend value-log maintenance directly. The public
	// profile may enable outer leaves in the cached leaf log, but OpenBackend has
	// no leaf-log manager; keep the fixture on inline index leaves so the modeled
	// cut belongs solely to the value-log unlink and namespace-sync boundary.
	opts.IndexOuterLeavesInValueLog = false
	bootstrap, closeBootstrap, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.SetSync([]byte("cert/live"), []byte("value")); err != nil {
		_ = closeBootstrap()
		t.Fatal(err)
	}
	if err := closeBootstrap(); err != nil {
		t.Fatal(err)
	}
	valueLogDir := backenddb.ValueLogDirPath(opts.Dir)
	if err := os.MkdirAll(valueLogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	emptyPath := filepath.Join(valueLogDir, "value-l42-000001.log")
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	backend, closeBackend, err := treedb.OpenBackend(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RefreshValueLogSet(); err != nil {
		_ = closeBackend()
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		_ = closeBackend()
		t.Fatal(err)
	}
	cutErr := errors.New("power-loss certification maintenance unlink cut")
	var snapshot *powerlossoracle.Model
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.BeforeDeletionDirectorySync && event.Resource == durabilitycut.ResourceValueLog {
			snapshot = model.Clone()
			return cutErr
		}
		return nil
	})
	_, compactErr := backend.CompactStorage(context.Background(), backenddb.CompactStorageOptions{})
	restore()
	if !errors.Is(compactErr, cutErr) || !errors.Is(compactErr, backenddb.ErrRecoveryRequired) || snapshot == nil {
		_ = closeBackend()
		t.Fatalf("CompactStorage error=%v snapshot=%t want cut and ErrRecoveryRequired", compactErr, snapshot != nil)
	}
	_ = closeBackend()
	reopened, closeReopened, _ := requireAcceptedCertificationReopen(t, snapshot, opts, true)
	got, err := reopened.Get([]byte("cert/live"))
	if err != nil || string(got) != "value" {
		_ = closeReopened()
		t.Fatalf("maintenance stable value=%q err=%v", got, err)
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}

// TestPowerLossCertificationCommandWALUnlinkPublicReopen advances two durable
// roots, then cuts real covered-WAL cleanup after unlink and before the parent
// directory sync. The stable image retains the old name and must reopen.
func TestPowerLossCertificationCommandWALUnlinkPublicReopen(t *testing.T) {
	requireCertificationReplay(t, certWALUnlinkVariant, durabilitycut.BeforeDeletionDirectorySync, 0)
	profileName, opts := certificationOptions(t)
	if profileName != "command_wal_relaxed" {
		t.Fatalf("WAL-unlink witness requires command_wal_relaxed, got %q", profileName)
	}
	opts.CommandWALSegmentTargetBytes = 1
	db, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Set([]byte("cert/wal-a"), []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("cert/wal-b"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Set([]byte("cert/wal-c"), []byte("c")); err != nil {
		t.Fatal(err)
	}
	model, err := powerlossoracle.Capture(opts.Dir)
	if err != nil {
		t.Fatal(err)
	}
	cutErr := errors.New("power-loss certification command-WAL unlink cut")
	var snapshot *powerlossoracle.Model
	restore := durabilitycut.Install(func(event durabilitycut.Event) error {
		if err := model.Observe(opts.Dir, event); err != nil {
			return err
		}
		if event.Point == durabilitycut.BeforeDeletionDirectorySync && event.Resource == durabilitycut.ResourceCommandWAL {
			snapshot = model.Clone()
			return cutErr
		}
		return nil
	})
	err = db.Checkpoint()
	restore()
	if !errors.Is(err, cutErr) || !errors.Is(err, treedb.ErrRecoveryRequired) || snapshot == nil {
		t.Fatalf("Checkpoint cleanup error=%v snapshot=%t want cut and ErrRecoveryRequired", err, snapshot != nil)
	}
	reopened, closeReopened, _ := requireAcceptedCertificationReopen(t, snapshot, opts, true)
	for key, want := range map[string]string{"cert/wal-a": "a", "cert/wal-b": "b", "cert/wal-c": "c"} {
		got, err := reopened.Get([]byte(key))
		if err != nil || string(got) != want {
			_ = closeReopened()
			t.Fatalf("Get(%q)=%q err=%v want=%q", key, got, err, want)
		}
	}
	if err := closeReopened(); err != nil {
		t.Fatal(err)
	}
}
