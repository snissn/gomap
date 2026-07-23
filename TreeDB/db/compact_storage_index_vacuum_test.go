package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestCompactStorageIndexVacuumPlanReportsDebtWithoutMutation(t *testing.T) {
	dir := t.TempDir()
	d, fixture := openVacuumM0Fixture(t, vacuumM0Options(dir))
	defer func() { _ = d.Close() }()

	before := compactStorageNamespaceFingerprint(t, dir)
	plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{Mode: CompactStorageFull})
	if err != nil {
		t.Fatalf("CompactStoragePlan: %v", err)
	}
	after := compactStorageNamespaceFingerprint(t, dir)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("CompactStoragePlan mutated namespace\nbefore=%v\nafter=%v", before, after)
	}
	assertNoCompactStorageVacuumArtifacts(t, dir)

	phase := compactStorageIndexVacuumPhase(t, plan)
	wantStatus := CompactStoragePhaseStatusPlanned
	if runtime.GOOS == "windows" {
		wantStatus = CompactStoragePhaseStatusUnsupported
	}
	if !phase.Required || phase.Status != wantStatus {
		t.Fatalf("plan index vacuum phase=%+v, want required/%s without attempt", phase, wantStatus)
	}
	debt := plan.RemainingDebt
	if !debt.IndexVacuumRequired || debt.IndexVacuumFreelistReclaimablePages == 0 || debt.IndexVacuumTotalPages == 0 {
		t.Fatalf("plan hid M0 index debt: debt=%+v fixture=%+v", debt, fixture)
	}
	if debt.IndexVacuumReason == "" {
		t.Fatalf("plan did not explain index debt: %+v", debt)
	}
}

func TestCompactStorageIndexVacuumNoDebtSkipsReplacementAndCheckpoint(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("settle fixture: %v", err)
	}

	indexBefore := compactStorageFileFingerprint(t, vacuumM0IndexPath(dir))
	vacuumEntered := false
	d.compactStorageVacuumIndexOnlineHook = func(context.Context, bool) error {
		vacuumEntered = true
		return nil
	}
	t.Cleanup(func() { d.compactStorageVacuumIndexOnlineHook = nil })

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageFull})
	if err != nil {
		t.Fatalf("CompactStorage: %v", err)
	}
	if vacuumEntered {
		t.Fatal("zero-debt CompactStorage invoked production index replacement")
	}
	if compactStoragePhaseSeen(stats.Phases, "checkpoint-after-index-vacuum") {
		t.Fatalf("zero-debt CompactStorage added index-vacuum checkpoint: %+v", stats.Phases)
	}
	if indexAfter := compactStorageFileFingerprint(t, vacuumM0IndexPath(dir)); indexAfter != indexBefore {
		t.Fatalf("zero-debt CompactStorage changed index.db: before=%s after=%s", indexBefore, indexAfter)
	}
	phase := compactStorageIndexVacuumPhase(t, stats)
	if phase.Required || phase.Status != CompactStoragePhaseStatusNotRequired {
		t.Fatalf("zero-debt index vacuum phase=%+v, want not-required", phase)
	}
	if stats.RemainingDebt.IndexVacuumRequired {
		t.Fatalf("zero-debt final audit invented index debt: %+v", stats.RemainingDebt)
	}
	if !stats.PolicyFullyCompacted || !stats.FullyCompacted || stats.ByteMinimized {
		t.Fatalf("zero-debt Full completion flags: fully=%t policy=%t byte=%t", stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized)
	}
}

func TestCompactStorageIndexVacuumHighDebtUsesProductionAndReportsFinalAudit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online index replacement is unsupported on Windows")
	}
	for _, mode := range []CompactStorageMode{CompactStorageFull, CompactStorageExhaustive} {
		t.Run(string(mode), func(t *testing.T) {
			dir := t.TempDir()
			opts := vacuumM0Options(dir)
			d, fixture := openVacuumM0Fixture(t, opts)
			plan, err := d.CompactStoragePlan(context.Background(), CompactStorageOptions{Mode: mode})
			if err != nil {
				_ = d.Close()
				t.Fatalf("CompactStoragePlan(%s): %v", mode, err)
			}
			if !plan.RemainingDebt.IndexVacuumRequired || plan.RemainingDebt.IndexVacuumFreelistReclaimablePages == 0 {
				_ = d.Close()
				t.Fatalf("M0 plan missing index debt: %+v", plan.RemainingDebt)
			}

			var beforeValueLogBytes, beforeLeafLogBytes int64
			d.compactStorageBeforePhase = func(name string) {
				if name != "index-vacuum" {
					return
				}
				beforeValueLogBytes = vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "value_vlog"))
				beforeLeafLogBytes = vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "leaf_vlog"))
			}
			d.compactStorageAfterPhase = func(name string) {
				if name != "index-vacuum" {
					return
				}
				if got := vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "value_vlog")); got != beforeValueLogBytes {
					t.Fatalf("index-only phase changed persistent value-log bytes: before=%d after=%d", beforeValueLogBytes, got)
				}
				if got := vacuumM0DirBytes(t, vacuumM0StoragePath(dir, "leaf_vlog")); got != beforeLeafLogBytes {
					t.Fatalf("index-only phase changed persistent leaf-log bytes: before=%d after=%d", beforeLeafLogBytes, got)
				}
			}
			t.Cleanup(func() {
				d.compactStorageBeforePhase = nil
				d.compactStorageAfterPhase = nil
			})

			stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: mode})
			if err != nil {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s): %v", mode, err)
			}
			afterIndexBytes := vacuumM0FileBytes(t, vacuumM0IndexPath(dir))
			if afterIndexBytes*100 > fixture.IndexBytes*60 {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) index shrink before=%d after=%d want >=40%%", mode, fixture.IndexBytes, afterIndexBytes)
			}
			if got := vacuumM0Digest(t, d); got != fixture.LogicalDigest {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) digest=%s want %s", mode, got, fixture.LogicalDigest)
			}

			phase := compactStorageIndexVacuumPhase(t, stats)
			if !phase.Required || phase.Status != CompactStoragePhaseStatusSucceeded {
				_ = d.Close()
				t.Fatalf("successful index vacuum phase=%+v", phase)
			}
			if stats.IndexVacuum.TotalDuration <= 0 || stats.IndexVacuum.PrecloneTraversalPages == 0 {
				_ = d.Close()
				t.Fatalf("successful index vacuum omitted production timing/work stats: %+v", stats.IndexVacuum)
			}
			if stats.RemainingDebt.IndexVacuumRequired || stats.RemainingDebt.IndexVacuumFreelistReclaimablePages != 0 {
				_ = d.Close()
				t.Fatalf("successful final audit is not truthful: %+v", stats.RemainingDebt)
			}
			if !stats.PolicyFullyCompacted || !stats.FullyCompacted {
				_ = d.Close()
				t.Fatalf("successful %s policy completion false: %+v", mode, stats)
			}
			if got, want := stats.ByteMinimized, mode == CompactStorageExhaustive; got != want {
				_ = d.Close()
				t.Fatalf("CompactStorage(%s) ByteMinimized=%t want %t", mode, got, want)
			}

			if err := d.Close(); err != nil {
				t.Fatalf("close after CompactStorage(%s): %v", mode, err)
			}
			reopened, err := Open(opts)
			if err != nil {
				t.Fatalf("reopen after CompactStorage(%s): %v", mode, err)
			}
			defer func() { _ = reopened.Close() }()
			if got := vacuumM0Digest(t, reopened); got != fixture.LogicalDigest {
				t.Fatalf("reopen after CompactStorage(%s) digest=%s want %s", mode, got, fixture.LogicalDigest)
			}
		})
	}
}

func TestCompactStorageIndexVacuumTransientFailureIsDeferred(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online index replacement is unsupported on Windows")
	}
	dir := t.TempDir()
	d, fixture := openVacuumM0Fixture(t, vacuumM0Options(dir))
	defer func() { _ = d.Close() }()
	d.compactStorageVacuumIndexOnlineHook = func(context.Context, bool) error { return ErrVacuumConcurrentMutation }
	t.Cleanup(func() { d.compactStorageVacuumIndexOnlineHook = nil })

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("transient index vacuum should be reported as deferred, got error: %v", err)
	}
	phase := compactStorageIndexVacuumPhase(t, stats)
	if !phase.Required || phase.Status != CompactStoragePhaseStatusDeferred {
		t.Fatalf("transient index vacuum phase=%+v, want deferred", phase)
	}
	if !strings.Contains(phase.Reason, ErrVacuumConcurrentMutation.Error()) {
		t.Fatalf("transient reason=%q want typed cause %q", phase.Reason, ErrVacuumConcurrentMutation)
	}
	assertCompactStorageIndexDebtIncomplete(t, stats, fixture)
}

func TestCompactStorageIndexVacuumPermanentFailureIsReported(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online index replacement is unsupported on Windows")
	}
	for _, wantErr := range []error{
		errors.New("injected permanent index vacuum failure"),
		ErrVacuumRecoverableRootSetRequired,
	} {
		t.Run(wantErr.Error(), func(t *testing.T) {
			dir := t.TempDir()
			d, fixture := openVacuumM0Fixture(t, vacuumM0Options(dir))
			defer func() { _ = d.Close() }()
			d.compactStorageVacuumIndexOnlineHook = func(context.Context, bool) error { return wantErr }

			stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageFull})
			if !errors.Is(err, wantErr) {
				t.Fatalf("CompactStorage error=%v want permanent cause %v", err, wantErr)
			}
			phase := compactStorageIndexVacuumPhase(t, stats)
			if !phase.Required || phase.Status != CompactStoragePhaseStatusFailed {
				t.Fatalf("permanent index vacuum phase=%+v", phase)
			}
			if !strings.Contains(phase.Reason, wantErr.Error()) {
				t.Fatalf("permanent reason=%q want %q", phase.Reason, wantErr)
			}
			assertCompactStorageIndexDebtIncomplete(t, stats, fixture)
		})
	}
}

func TestCompactStorageIndexVacuumUnsupportedIsReported(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("unsupported-platform contract runs on Windows")
	}
	dir := t.TempDir()
	d, fixture := openVacuumM0Fixture(t, vacuumM0Options(dir))
	defer func() { _ = d.Close() }()

	stats, err := d.CompactStorage(context.Background(), CompactStorageOptions{Mode: CompactStorageExhaustive})
	if err != nil {
		t.Fatalf("unsupported index vacuum should be a reported disposition: %v", err)
	}
	phase := compactStorageIndexVacuumPhase(t, stats)
	if !phase.Required || phase.Status != CompactStoragePhaseStatusUnsupported {
		t.Fatalf("unsupported index vacuum phase=%+v", phase)
	}
	if !strings.Contains(phase.Reason, ErrVacuumUnsupported.Error()) {
		t.Fatalf("unsupported reason=%q want %q", phase.Reason, ErrVacuumUnsupported)
	}
	assertCompactStorageIndexDebtIncomplete(t, stats, fixture)
}

func TestCompactStorageIndexVacuumDebtIncludesRetiringLeafGeneration(t *testing.T) {
	debt := CompactStorageDebt{}
	compactStorageApplyLeafGenerationIndexDebt(&debt, LeafGenerationGCStats{GenerationsRetiring: 1})
	if !debt.IndexVacuumRequired || debt.IndexVacuumReason != "leaf_generation" {
		t.Fatalf("debt=%+v want required leaf-generation vacuum", debt)
	}
}

func compactStorageIndexVacuumPhase(t *testing.T, stats CompactStorageStats) CompactStoragePhaseStats {
	t.Helper()
	for _, phase := range stats.Phases {
		if phase.Name == "index-vacuum" {
			return phase
		}
	}
	t.Fatalf("CompactStorage report missing index-vacuum phase: %+v", stats.Phases)
	return CompactStoragePhaseStats{}
}

func assertCompactStorageIndexDebtIncomplete(t *testing.T, stats CompactStorageStats, fixture vacuumM0Fixture) {
	t.Helper()
	if stats.FullyCompacted || stats.PolicyFullyCompacted || stats.ByteMinimized {
		t.Fatalf("incomplete index vacuum overstated completion: fully=%t policy=%t byte=%t", stats.FullyCompacted, stats.PolicyFullyCompacted, stats.ByteMinimized)
	}
	if !stats.RemainingDebt.IndexVacuumRequired || stats.RemainingDebt.IndexVacuumFreelistReclaimablePages == 0 {
		t.Fatalf("final audit hid remaining index debt: debt=%+v fixture=%+v", stats.RemainingDebt, fixture)
	}
}

func assertNoCompactStorageVacuumArtifacts(t *testing.T, root string) {
	t.Helper()
	indexDir := filepath.Dir(vacuumM0IndexPath(root))
	for _, name := range []string{indexNewFileName, indexBakFileName, indexReadyFileName} {
		path := filepath.Join(indexDir, name)
		if _, err := os.Stat(path); err == nil {
			t.Fatalf("plan created index vacuum artifact %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stat index vacuum artifact %s: %v", path, err)
		}
	}
}

func compactStorageNamespaceFingerprint(t *testing.T, root string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if filepath.ToSlash(rel) == "LOCK" {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		out[filepath.ToSlash(rel)] = hex.EncodeToString(h.Sum(nil))
		return nil
	})
	if err != nil {
		t.Fatalf("fingerprint %s: %v", root, err)
	}
	return out
}

func compactStorageFileFingerprint(t *testing.T, path string) string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()
	h := sha256.New()
	if _, err := io.Copy(h, file); err != nil {
		t.Fatalf("hash %s: %v", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fmt.Sprintf("%d:%s", info.Size(), hex.EncodeToString(h.Sum(nil)))
}
